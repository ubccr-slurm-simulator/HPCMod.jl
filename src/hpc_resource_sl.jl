using Logging
using DataFrames
using Agents
using Random
using Printf
using PrettyPrint
using Dates
using DocStringExtensions


function SimulationSL(;
    id=1,
    init_datetime::DateTime=DateTime(2024,11,1,0,0,0),
    timestep::Millisecond=Millisecond(3600*1000),
    rng::AbstractRNG=Random.default_rng(123),
    user_extra_step::Union{Function,Nothing}=nothing,
    model_extra_step::Union{Function,Nothing}=nothing,
    workload_done_check_freq::Int=1)

    Dates.value(Millisecond(24*3600*1000)) % Dates.value(timestep) != 0 && throw("Day should be multiple of timestep!")
    
    sim = SimulationSL(
        id,
        Day(1)÷timestep,
        timestep,
        0,
        init_datetime,
        init_datetime,
        workload_done_check_freq,
        Vector{HPCResourceSL}(),
        Dict{String, ResourceId}(),
        Vector{UserSL}(),
        Dict{String, UserId}(),
        NO_NEXT_EVENT,
        Vector{HPCEventSL}(),
        nothing,
        nothing,
        rng,
        nothing,
        nothing,
        user_extra_step, model_extra_step,
        Dict{String, GRESType}(),
        Dict{String, GRESModel}()
        )
    
    sim.model = StandardABM(
        UserSL,
        sim.space;
        model_step! = model_step_sl!,
        properties=Dict(
            :sim => sim),
        sim.rng,
        scheduler=Schedulers.Randomly()
    )

    sim
end

"
Get vector of integer ids from the vector of string ids using id dict
"
function get_ids_from_str_ids(str_ids::Vector{String}, id_dict::Dict{String, Int})
    ids = Vector{Int64}()
    for str_id in str_ids
        if !haskey(id_dict, str_id)
            id_dict[str_id] = length(id_dict) + 1
        end
        push!(ids, id_dict[str_id])
    end
    return ids
end

function add_resource!(sim::SimulationSL; name::String="HPCResourceSL")
    resource_id = length(sim.resource) + 1
    push!(sim.resource, HPCResourceSL(
        resource_id,
        name,
        Vector{ComputeNodeSL}(), Dict{String, NodeId}(),
        Vector{PartitionSL}(), Dict{String, PartitionId}(), 0,
        Vector{QoSSL}(), Dict{String, QoSId}(), 0,
        Vector{AccountSL}(), Dict{String, AccountId}(),
        Vector{UserAccountSL}(), Dict{String, UserAccountId}(),
        Dict{String, NodeFeatureId}(),
        Vector{JobOnResourceSL}(),
        Dict{JobId,JobOnResourceSL}(),
        Vector{BatchJobSL}(),
        0
        ))
    sim.resource_id[name] = resource_id
    sim.resource[end]
end
#ComputeNodeSL()
# add cluster
# add cluster Name=micro Fairshare=1 QOS=normal,supporters
#resource = HPCResourceSL(;name="micro")


function add_nodes!(
    sim::SimulationSL,
    resource::HPCResourceSL,
    nodesname_list::Vector{String};
    sockets::Union{Missing, Int64}=missing,
    cores_per_socket::Union{Missing, Int64}=missing,
    threads_per_core::Int64=1,
    memory::Union{Missing, Int64}=missing,
    features::Vector{String}=Vector{String}(),
    gres::Vector{String}=Vector{String}(),
    gres_model::Vector{String}=Vector{String}(),
    state::NodeState=NODE_STATE_IDLE
)
    # cpus, cpus_per_node, nodes
    ismissing(cores_per_socket) && throw("cores_per_socket should be set")
    ismissing(sockets) && throw("sockets should be set")
    cpus = threads_per_core * cores_per_socket * sockets

    ismissing(memory) && throw("memory should be set")
    length(gres_model)==0 && length(gres)>0 && (gres_model = [v*"-ModelA" for v in gres])

    # Features Ids
    features_ids=get_ids_from_str_ids(features, resource.NodeFeatures)
    # GRES
    gres_ids=get_ids_from_str_ids(gres, sim.GRESTypes)
    gres_model_ids = get_ids_from_str_ids(gres_model, sim.GRESModels)

    for name in nodesname_list
        # Check name uniqueness
        haskey(resource.node_id, name) && throw("Node $(name) already exists")
        # Add
        node_id = length(resource.node) + 1
        push!(resource.node, ComputeNodeSL(
            node_id,
            cores_per_socket, 
            memory, 
            cpus, 
            cpus, 
            copy(features_ids), 
            copy(gres_ids), 
            copy(gres_model_ids), 
            Vector{JobId}(), 
            name, 
            state, 
            memory, 
            sockets, 
            threads_per_core))
        resource.node_id[name] = node_id
    end

    # init other things
    resource.gres_max = 0
    for node in resource.node
        if length(node.gres) > resource.gres_max
            resource.gres_max = length(node.gres)
        end
    end
    nothing
end

function add_partition!(
    sim::SimulationSL,
    resource::HPCResourceSL,
    name::String,
    nodes::Vector{String};
    Default::Bool=false,
    DefMemPerCPU::Int=2800,
    MaxTime::Int=TIME_INFINITE,
    priority_job_factor::Int=0,
    State::PartitionState=PARTITION_UP
)
    haskey(resource.partition_id, name) && throw("Partition $(name) already exists")
    
    push!(resource.partition, PartitionSL(
            DefMemPerCPU,
            MaxTime,
            name,
            sort([resource.node_id[node] for node in nodes]),
            priority_job_factor,
            State
    ))
    resource.partition_id[name] = length(resource.partition)
    Default && (resource.default_partition_id = resource.partition_id[name])
    nothing
end

function add_qos!(sim::SimulationSL, resource::HPCResourceSL, name::String; priority::Int=0, default::Bool=false)
    haskey(resource.qos_id, name) && throw("QoS $(name) already exists")

    push!(resource.qos, QoSSL(
        name,
        priority
    ))
    resource.qos_id[name] = length(resource.qos)
    default && (resource.default_qos_id = resource.qos_id[name])
    nothing
end

function add_account!(sim::SimulationSL, resource::HPCResourceSL, name::String; fairshare::Int=100)
    haskey(resource.account_id, name) && throw("Account $(name) already exists")

    account_id = length(resource.account) + 1
    push!(resource.account, AccountSL(
        name,
        account_id,
        fairshare,
        Vector{UserId}()
    ))
    resource.account_id[name] = account_id
    nothing
end

function add_user!(
    sim::SimulationSL, resource::HPCResourceSL, name::String, default_account::String;
    accounts::Vector{String}=Vector{String}()
    )
    haskey(resource.user_account_id, name) && throw("User $(name) already exists")

    accounts_id = sort(unique([[resource.account_id[default_account]]; [resource.account_id[acc] for acc in accounts]]))

    user_id = length(sim.user) + 1
    push!(sim.user, UserSL(
        user_id,
        (1,),
        name,
        fill(NO_USER_ACCOUNT,length(sim.resource)),
        NO_NEXT_EVENT,
        Vector{HPCEventSL}()
    ))
    sim.user_id[name] = user_id

    # add agent to model
    add_agent!(sim.user[end], sim.model)

    user_account_id = length(resource.user_account) + 1
    push!(resource.user_account, UserAccountSL(
        user_account_id,
        name,
        user_id,
        resource.account_id[default_account],
        accounts_id
    ))
    resource.user_account_id[name] = user_account_id
    sim.user[user_id].resource_user_account_id[resource.id] = user_account_id

    sim.user[end]
end


function add_job!(
    sim::SimulationSL, resource::HPCResourceSL;
    user::String=missing,
    cpus::Union{Missing,Int64}=missing,
    cpus_per_node::Union{Missing,Int64}=missing,
    nodes::Union{Missing,Int64}=missing,
    job_id::Union{Missing,Int64}=missing,
    account::Union{Missing,String}=missing,
    partition::Union{Missing,String}=missing,
    qos::Union{Missing,String}=missing,
    gres_per_node::Vector{String}=Vector{String}(),
    gres_model_per_node::Vector{String}=Vector{String}(),
    mem_per_cpu::Union{Missing,Int64}=missing,
    node_sharing::NodeSharing=NODE_SHARING_UNSET,
    features=Vector{String}(),
    req_walltime::Union{Missing,Millisecond}=missing,
    sim_walltime::Union{Missing,Millisecond}=missing,
    submit_time::Union{Missing,DateTime}=missing,
    dt::Union{Missing,Millisecond}=missing,
    priority::Int=0
    )

    user_id = sim.user_id[user]
    user_account_id = resource.user_account_id[user]
    account_id = ismissing(account) ? resource.user_account[user_account_id].default_account_id : resource.account_id[account]
    partition_id = ismissing(partition) ? resource.default_partition_id : resource.partition_id[partition]
    qos_id = ismissing(qos) ? resource.default_qos_id : resource.qos_id[qos]

    ismissing(mem_per_cpu) && (mem_per_cpu = resource.partition[partition_id].def_mem_per_cpu)

    ismissing(req_walltime) && throw("req_walltime should be set!")
    ismissing(sim_walltime) && (sim_walltime = req_walltime)

    # submit_time and dt
    ismissing(submit_time) && ismissing(dt) && throw("either submit_time or dt should be set!")
    ismissing(submit_time) && !ismissing(dt) && (submit_time = sim.init_datetime + dt)
    submit_time != sim.init_datetime + dt && throw("submit_time and sim.init_time + dt should be same")

    # cpus, cpus_per_node, nodes
    ismissing(cpus) && ismissing(cpus_per_node) && ismissing(nodes) && throw("Two of cpus, cpus_per_node or nodes should be set!")
    !ismissing(cpus) && !ismissing(cpus_per_node) && ismissing(nodes) && (nodes = cpus ÷ cpus_per_node)
    !ismissing(cpus) && ismissing(cpus_per_node) && !ismissing(nodes) && (cpus_per_node = cpus ÷ nodes)
    ismissing(cpus) && !ismissing(cpus_per_node) && !ismissing(nodes) && (cpus = cpus_per_node * nodes)

    cpus % cpus_per_node != 0 && throw("cpus should be multiple of cpus_per_node")
    cpus % nodes != 0 && throw("cpus should be multiple of nodes")


    features_id::Vector{NodeFeatureId} = [resource.NodeFeatures[feature] for feature in features]
    gres_per_node_id::Vector{GRESType} = [sim.GRESTypes[gres] for gres in gres_per_node]
    if length(gres_per_node_id) > 0
        if length(gres_per_node_id) == length(gres_model_per_node)
            gres_model_per_node_id::Vector{GRESModel} = [sim.GRESModels[gres] for gres in gres_model_per_node]
        elseif length(gres_model_per_node)==0
            gres_model_per_node_id=fill(GRES_MODEL_ANY, length(gres_per_node_id))
        else
            throw("GRES Model size do not match GRES type")
        end
    else
        gres_model_per_node_id = Vector{GRESModel}()
    end
    start_time = DateTime(0)
    end_time = DateTime(0)
    walltime=Millisecond(0)
    nodes_list=Vector{NodeId}()

    job = BatchJobSL(
        job_id, resource.id, user_id, user_account_id, account_id, partition_id, qos_id, 
        cpus, cpus_per_node, nodes, 
        gres_per_node_id, gres_model_per_node_id,
        mem_per_cpu, node_sharing, features_id, req_walltime, 
        sim_walltime, submit_time, priority, start_time, end_time, walltime, nodes_list)


    push!(sim.user[user_id].events_list, HPCEventSL(submit_time, dt, SUBMIT_JOB, job))
    sim.user[user_id].next_event==NO_NEXT_EVENT && (sim.user[user_id].next_event = 1)
    job

end

function is_workload_done_sl(model, s)
    sim = getproperty(model, :sim)
    return false
    # s % sim.workload_done_check_freq != 0 && return false

    # length(sim.resource.queue) > 0 && return false
    # length(sim.resource.executing) > 0 && return false

    # for user in sim.users_list
    #     length(user.inividual_jobs) > 0 && return false
    #     length(user.tasks_active) > 0 && return false
    #     length(user.tasks_to_do) > 0 && return false
    # end

    # return true
end

"""
get datetime from simulation step
"""
function get_datetime(sim::SimulationSL, step::Int64)
    sim.init_datetime + sim.timestep * step
end

"""
get simulation step from datetime
"""
function get_step(sim::SimulationSL, datetime::DateTime)
    Dates.value(datetime - sim.init_datetime) ÷ Dates.value(sim.timestep)
end

"""
get nearby simulation step from datetime
"""
function get_round_step(sim::SimulationSL, datetime::DateTime)
    round(datetime - sim.init_datetime, sim.timestep) ÷ sim.timestep
end


function submit_job(sim::SimulationSL, model::StandardABM, resource::HPCResourceSL, user::UserSL, job::BatchJobSL)
    if job.submit_time == DATETIME_UNSET
        job.submit_time = sim.cur_datetime
    elseif job.submit_time != sim.cur_datetime
        error("Preplaned job: job.submit_time != abmtime(model)")
    end
    
    @debug "Submitting Job: $(job.id) at time $(sim.cur_datetime)"
                

    # job.task.nodetime_left_unplanned -= job.nodes * job.walltime

    push!(resource.queue, JobOnResourceSL(job))

    # push!(job.task.current_jobs, job.id)

    return
end

function find_runnable_nodes!(
    sim::SimulationSL, resource::HPCResourceSL, jobonres::JobOnResourceSL)
    
    length(jobonres.gres_used)!=resource.gres_max && (jobonres.gres_used = fill(false, resource.gres_max))
    length(jobonres.runnable_nodes_bool)==0 && (jobonres.runnable_nodes_bool = fill(false, length(resource.node)))

    job = jobonres.job

    for (node_id,node) in enumerate(resource.node)
        jobonres.runnable_nodes_bool[node_id] = false
        # cpus_per_node::Int64
        job.cpus_per_node > node.cpus && continue
        job.mem_per_cpu * job.cpus_per_node > node.real_memory && continue

        has_required_features = true
        for feature in job.features
            if feature ∉ node.features
                has_required_features=false
                break
            end
        end

        !has_required_features && continue

        # GRES match
        if length(job.gres_per_node) >0
            fill!(jobonres.gres_used, false)

            length(job.gres_per_node) > length(node.gres) && continue

            for ijob in 1:length(job.gres_per_node)
                if job.gres_model_per_node[ijob]==GRES_MODEL_ANY
                    for inode in 1:length(node.gres)
                        if job.gres_per_node[ijob] == node.gres[inode] && jobonres.gres_used[inode]==false
                            jobonres.gres_used[inode] = true
                            break
                        end
                    end
                else
                    for inode in 1:length(node.gres)
                        if job.gres_per_node[ijob] == node.gres[inode] && job.gres_model_per_node[ijob]==node.gres_model[inode] && jobonres.gres_used[inode]==false
                            jobonres.gres_used[inode] = true
                            break
                        end
                    end
                end
            end

            if sum(jobonres.gres_used)!=length(job.gres_per_node)
                continue
            end
        end

        jobonres.runnable_nodes_bool[node_id] = true
    end

    if sum(jobonres.runnable_nodes_bool) < job.nodes
        @error "Not enough resources for this job ($(job.id))"
        jobonres.runnable_nodes_bool .= false
    end

    jobonres.runnable_nodes = [i for (i,v) in enumerate(jobonres.runnable_nodes_bool) if v==true]
    jobonres.currently_runnable_nodes = fill(false, length(jobonres.runnable_nodes))
    
    @debug "Job $(job.id) has following runnabe nodes: $([resource.node[node_id].name for node_id in jobonres.runnable_nodes])"
end

"
Check that resource follows conventions
"
function check_resource(sim::SimulationSL, resource::HPCResourceSL)
    errors_count = 0
    for (i,node) in enumerate(resource.node)
        i!=node.id && (errors_count += 1)
    end
    
    errors_count > 0 && throw("Node.id should match index in resource.node vector") 
end

function attempt_to_allocate(sim::SimulationSL, resource::HPCResourceSL, jobonres::JobOnResourceSL)::Bool
    length(jobonres.runnable_nodes) == 0 && find_runnable_nodes!(sim, resource, jobonres)
    
    return false
end

function generate_thinktime_zero(sim::SimulationSL, user::UserSL)::Int64
    0
end

function generate_thinktime_gamma(sim::SimulationSL, user::UserSL)::Int64
    shape = 0.23743230
    scale = 1.0 / 0.05508324
    gamma = Gamma(shape, scale)
    round(Int64, rand(sim.rng, gamma))
end

function user_step!(sim::SimulationSL, model::StandardABM, user::UserSL)
    
    if user.next_event != NO_NEXT_EVENT
        while user.events_list[user.next_event].when <= sim.cur_datetime && user.next_event <= length(user.events_list)
            if user.events_list[user.next_event].event_type == SUBMIT_JOB
                submit_job(
                    sim, model, 
                    sim.resource[user.events_list[user.next_event].event.resource_id], 
                    user, 
                    user.events_list[user.next_event].event)
            else
                @error "Unknown Event!"
            end
            user.next_event += 1
        end
    end

    # if length(user.inividual_jobs) == 0 && length(user.tasks_to_do) == 0 && length(user.tasks_active) == 0
    #     return
    # end
    # # process finished jobs, archive finished tasks
    # for job in user.jobs_to_process
    #     job.task.nodetime_left -= job.nodes * job.walltime
    #     job.task.nodetime_done += job.nodes * job.walltime

    #     # in what time user will check this job
    #     job.task.next_check_time = abmtime(model) + user.thinktime_generator(sim, user)

    #     popat!(job.task.current_jobs, findfirst(==(job.id), job.task.current_jobs))
    #     push!(job.task.jobs, job.id)
    # end

    # # retire completed active tasks
    # i = 1
    # while i <= length(user.tasks_active)
    #     if user.tasks_active[i].nodetime_left <= 0 && user.tasks_active[i].nodetime_total > 0 && user.tasks_active[i].next_check_time <= abmtime(model)
    #         task = popat!(user.tasks_active, i)

    #         task.nodetime_left = 0
    #         task.end_time = abmtime(model)

    #         push!(user.tasks_done, task)
    #     else
    #         i += 1
    #     end
    # end
    # resize!(user.jobs_to_process, 0)

    # # users extra step
    # isnothing(sim.user_extra_step) == false && sim.user_extra_step(sim, model, UserSimple)

    # # activate new tasks
    # while length(user.tasks_to_do) > 0 && length(user.tasks_active) < user.max_concurrent_tasks && first(user.tasks_to_do).submit_time <= abmtime(model)
    #     task = pop!(user.tasks_to_do)
    #     task.start_time = abmtime(model)
    #     push!(user.tasks_active, task)
    # end

    # # submit new job within active tasks
    # global task_split!
    # for task in user.tasks_active
    #     if length(task.current_jobs) < task.max_concurrent_jobs && task.nodetime_left > 0 && task.next_check_time <= abmtime(model)
    #         job = task_split![task.task_split_schema](sim, task; user)
    #         submit_job(sim, model, sim.resource, job)
    #     end
    # end

    # # submit new individual job
    # while length(user.inividual_jobs) > 0 && first(user.inividual_jobs).submit_time <= abmtime(model)
    #     job = pop!(user.inividual_jobs)
    #     submit_job(sim, model, sim.resource, job)
    # end

    return
end

"""
free nodes are good to fit this job
"""
function place_job!(model::StandardABM, resource::HPCResourceSL, job_position_at_queue::Int64)
    # job = popat!(resource.queue, job_position_at_queue)
    # resource.executing[job.id] = job
    # run_till = abmtime(model) + job.walltime
    # job.start_time = abmtime(model)
    # node_count = 0

    # for node_id in 1:resource.nodes
    #     if resource.node_used_by_job[node_id] == 0
    #         resource.node_used_by_job[node_id] = job.id
    #         resource.node_released_at[node_id] = run_till
    #         node_count += 1

    #         push!(job.nodes_list, node_id)

    #         if node_count == job.nodes
    #             break
    #         end
    #     end
    # end
    # resource.node_released_at_sorted = sort(resource.node_released_at)
    # job
end

function run_scheduler_fifo!(sim::SimulationSL, resource::HPCResourceSL)
    while length(resource.queue) > 0
        if !attempt_to_allocate(sim, resource, resource.queue[1])
            break
        end
    end
end

function run_scheduler_backfill!(sim::SimulationSL, resource::HPCResourceSL)
    # while length(resource.queue) > 0
    #     nodes_free = resource.nodes - used_nodes(resource)
    #     #println("Time: $(abmtime(model)) free nodes: $(nodes_free) queue $(length(resource.queue))")

    #     # any jobs with fit by node count
    #     job_fit = findfirst(
    #         x -> x.nodes <= nodes_free, resource.queue)

    #     isnothing(job_fit) && return

    #     # when next priority job start
    #     next_fifo_job = resource.queue[1]
    #     next_fifo_job_starttime = resource.node_released_at_sorted[next_fifo_job.nodes]

    #     next_fifo_job_starttime <= 0 && return

    #     # any jobs with fit by node count and walltime
    #     job_fit = findfirst(
    #         x -> x.nodes <= nodes_free && x.walltime <= next_fifo_job_starttime, resource.queue)
    #     isnothing(job_fit) && return

    #     # schedule the job
    #     job = place_job!(model, resource, job_fit)
    #     job.scheduled_by = Backfill
    #     #println("jobs_fit_nodes: $(jobs_fit_nodes) next_fifo_job_starttime $(next_fifo_job_starttime) jobs_fit_nodes_and_time $(jobs_fit_nodes_and_time)")

    #     break
    # end
end


function run_scheduler!(sim::SimulationSL, resource::HPCResourceSL)
    length(resource.queue) == 0 && return
    
    println("job in resource.queue")
    for job in resource.queue
        println("$(job.id) $(job.priority)")
    end
    # Sort
    sort!(resource.queue, by = x -> x.priority, order=Base.Order.Reverse)

    println("job in resource.queue")
    for job in resource.queue
        println("$(job.id) $(job.priority)")
    end
    
    #if resource.scheduler_fifo
        run_scheduler_fifo!(sim, resource)
    #end
    #if resource.scheduler_backfill
        run_scheduler_backfill!(sim, resource)
    #end
end


"""
Check for finished jobs
    the convention is that job run all the way till current time, 
    excluding current time.
"""
function check_finished_job!(sim::SimulationSL, resource::HPCResourceSL)
    # cur_time = abmtime(model)
    # for i in 1:resource.nodes
    #     if resource.node_released_at[i] >= 0 && resource.node_released_at[i] <= cur_time
    #         job_id = resource.node_used_by_job[i]
    #         job = pop!(resource.executing, job_id)
    #         job.end_time = abmtime(model)
    #         user = model[job.task.user_id]
    #         # clear from nodes
    #         for i2 in i:resource.nodes
    #             if resource.node_used_by_job[i2] == job.id
    #                 resource.node_used_by_job[i2] = 0
    #                 resource.node_released_at[i2] = -1
    #             end
    #         end
    #         push!(resource.history, job)
    #         push!(user.jobs_to_process, job)
    #     end
    # end
    # resource.node_released_at_sorted = sort(resource.node_released_at)
    return
end

function model_step_stats!(sim::SimulationSL)
    # abmtime(sim.model) % sim.resource.stats.calc_freq != 0 && return

    # ncol(sim.resource.stats.node_occupancy_by_user) == 0 && error("sim.resource.stats.node_occupancy_by_user was not initialized!")
    # by_user = zeros(Int64, ncol(sim.resource.stats.node_occupancy_by_user))
    # by_job = zeros(Int64, ncol(sim.resource.stats.node_occupancy_by_user))
    # by_task = zeros(Int64, ncol(sim.resource.stats.node_occupancy_by_user))
    # by_user[1] = abmtime(sim.model)
    # by_job[1] = abmtime(sim.model)
    # by_task[1] = abmtime(sim.model)
    # for (job_id, job) in sim.resource.executing
    #     for node_id in job.nodes_list
    #         col_id = node_id + 1
    #         by_user[col_id] != 0 && error("node can be occupied only by one job, but it is not!")
    #         by_user[col_id] = job.task.user_id
    #         by_job[col_id] = job_id
    #         by_task[col_id] = job.task.id
    #     end
    # end
    # push!(sim.resource.stats.node_occupancy_by_user, by_user)
    # push!(sim.resource.stats.node_occupancy_by_job, by_job)
    # push!(sim.resource.stats.node_occupancy_by_task, by_task)
end


function model_step_sl!(model::StandardABM)
    sim::SimulationSL = model.sim
    sim.cur_datetime = get_datetime(sim, abmtime(model))
    @debug "model_step! cur_datetime: $(sim.cur_datetime)\n"
    # it is right before abmtime(model) time
    # check finished job
    check_finished_job!(sim, sim.resource[1])


    # it is abmtime(model) time
    # schedule
    run_scheduler!(sim, sim.resource[1])

    # ask users to do their staff
    # @debug "ids: $(ids)"
    for id in abmscheduler(model)(model)
        # here `agent_step2!` may delete agents, so we check for it manually
        hasid(model, id) || continue
        user_step!(sim, model, model[id])
    end

    # schedule
    run_scheduler!(sim, sim.resource[1])

    # more stats
    model_step_stats!(sim)

    # model extra step
    isnothing(sim.model_extra_step) == false && sim.model_extra_step(sim, model)
end

function run!(sim::SimulationSL; nsteps::Int64=-1, run_till_no_jobs::Bool=false)
    model::StandardABM = sim.model
    # Users Statistics
    adata0 = [
        #(:mean_tasks_to_do, u -> length(u.tasks_to_do), mean)
        #(:mean_tasks_active, u -> length(u.tasks_active), mean)
    ]
    # Resource Statistics
    mdata0 = [
        #(:used_nodes, m -> sum(m.sim.resource.node_used_by_job .!= 0)),
        #(:jobs_in_queue, m -> length(m.sim.resource.queue)),
        #(:jobs_running, m -> length(m.sim.resource.executing)),
        #(:jobs_done, m -> length(m.sim.resource.history)),
    ]
    if nsteps != -1
        run_till_no_jobs = false
    end

    if run_till_no_jobs
        end_criteria = is_workload_done_sl
    else
        end_criteria = nsteps
    end

    @debug "run!(sim::SimulationSL; nsteps=$(nsteps), run_till_no_jobs=$(run_till_no_jobs)):
        Running the model..."

    sim.adf, sim.mdf = run!(
        model, end_criteria;
        adata=[(v[2], v[3]) for v in adata0],
        mdata=[v[2] for v in mdata0])

    length(adata0) > 0 && rename!(sim.adf, [[:time]; [v[1] for v in adata0]])
    length(mdata0) > 0 && rename!(sim.mdf, [[:time]; [v[1] for v in mdata0]])

    sim.adf, sim.mdf
end

