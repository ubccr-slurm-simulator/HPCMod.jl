using Logging
using DataFrames
using Agents
using Random
using Printf
using PrettyPrint
using Dates
using DocStringExtensions

function abmdatetime(init_datetime::DateTime, dt::Float64)
    return init_datetime + Millisecond(round(dt*1000))
end

function abmdatetime(model::EventQueueABM)
    return abmdatetime(abmproperties(model).init_datetime, abmtime(model)*1000)
end

function secondsfrominit(init_datetime::DateTime, m_datetime::DateTime)
    return (m_datetime - init_datetime).value / 1000.0
end

"
Get vector of integer ids from the vector of string ids using id dict
"
function get_ids_from_str_ids(str_ids::Vector{String}, id_dict::Dict{String, Int}, id_vec::Vector{String})
    ids = Vector{Int64}()
    for str_id in str_ids
        if !haskey(id_dict, str_id)
            new_id = length(id_vec) + 1
            push!(id_vec, str_id)
            id_dict[str_id] = new_id
        end
        push!(ids, id_dict[str_id])
    end
    return ids
end

function add_resource!(model::EventQueueABM; name::String="HPCResourceSL",ares_tracking_df_freq::Int=-1)
    sim::SimulationSL = abmproperties(model)
    #agent_id = Agents.nextid(model)
    resource_id = length(sim.resource) + 1

    resource = add_agent!((1,1), HPCResourceSL, model,
        resource_id,
        name,
        Vector{ComputeNodeSL}(), Dict{String, NodeId}(),
        Vector{PartitionSL}(), Dict{String, PartitionId}(), 0,
        Vector{QoSSL}(), Dict{String, QoSId}(), 0,
        Vector{AccountSL}(), Dict{String, AccountId}(),
        Vector{UserAccountSL}(), Dict{String, UserAccountId}(),
        Vector{NodeFeatureId}(),
        Dict{String, NodeFeatureId}(),
        Vector{JobOnResourceSL}(),
        Dict{JobId,JobOnResourceSL}(),
        Vector{BatchJobSL}(),
        Vector{Int}(),
        0,
        0,
        ares_tracking_df_freq,
        DataFrame()
    )

    add_event!(resource, RUN_SCHEDULER_EVENT,0, model)
    add_event!(resource, CHECK_WALLTIMELIMIT_EVENT,0, model)

    push!(sim.resource, resource)
    sim.resource_id[name] = resource_id

    resource
end




function add_nodes!(
    model::EventQueueABM,
    resource::HPCResourceSL,
    nodesname_list::Vector{String};
    sockets::Union{Missing, Int64}=missing,
    cores_per_socket::Union{Missing, Int64}=missing,
    memory::Union{Missing, Int64}=missing,
    features::Vector{String}=Vector{String}(),
    gres::Vector{String}=Vector{String}(),
    gres_model::Vector{String}=Vector{String}(),
    state::NodeState=NODE_STATE_IDLE
)
    sim::SimulationSL = abmproperties(model)
    # cpus, cpus_per_node, nodes
    ismissing(cores_per_socket) && throw("cores_per_socket should be set")
    ismissing(sockets) && throw("sockets should be set")
    cpus = cores_per_socket * sockets

    ismissing(memory) && throw("memory should be set")
    length(gres_model)==0 && length(gres)>0 && (gres_model = [v*"-ModelA" for v in gres])

    # Features Ids
    features_ids=get_ids_from_str_ids(features, resource.NodeFeatures_id, resource.NodeFeatures)
    # GRES
    gres_ids=get_ids_from_str_ids(gres, sim.ARESTypes_id, sim.ARESTypes)
    gres_model_ids = get_ids_from_str_ids(gres_model, sim.ARESModels_id, sim.ARESModels)

    ares_type::Vector{ARESType} = [sim.ARESTypes_id[s] for s in ["CPU", "Memory"]]
    ares_model::Vector{ARESModel} = [sim.ARESModels_id[s] for s in ["CPU", "Memory"]]
    ares_total::Vector{Int} = [cpus, memory]


    for gres_type in sort(unique(gres_ids))
        models = gres_model_ids[gres_ids .== gres_type]
        for model in sort(unique(models))
            push!(ares_type, gres_type)
            push!(ares_model, model)
            push!(ares_total, sum(models.==model))
        end
    end

    ares_used::Vector{Int} = fill(0,length(ares_type))
    ares_free::Vector{Int} = copy(ares_total)

    for name in nodesname_list
        # Check name uniqueness
        haskey(resource.node_id, name) && throw("Node $(name) already exists")
        # Add
        node_id = length(resource.node) + 1
        job_slots = cpus
        push!(resource.node, ComputeNodeSL(
            node_id,
            name,
            copy(ares_type),
            copy(ares_model),
            copy(ares_total),
            copy(features_ids), 
            copy(ares_used),
            copy(ares_free),
            state, 
            Set{JobId}()
            ))
        resource.node_id[name] = node_id
    end

    nothing
end

function add_partition!(
    model::EventQueueABM,
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

function add_qos!(model::EventQueueABM, resource::HPCResourceSL, name::String; priority::Int=0, default::Bool=false)
    haskey(resource.qos_id, name) && throw("QoS $(name) already exists")

    push!(resource.qos, QoSSL(
        name,
        priority
    ))
    resource.qos_id[name] = length(resource.qos)
    default && (resource.default_qos_id = resource.qos_id[name])
    nothing
end

function add_account!(model::EventQueueABM, resource::HPCResourceSL, name::String; fairshare::Int=100)
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
    model::EventQueueABM, resource::HPCResourceSL, name::String, default_account::String;
    accounts::Vector{String}=Vector{String}()
    )
    haskey(resource.user_account_id, name) && throw("User $(name) already exists")
    sim::SimulationSL = abmproperties(model)

    accounts_id = sort(unique([[resource.account_id[default_account]]; [resource.account_id[acc] for acc in accounts]]))

    user_id = length(sim.user) + 1

    user = add_agent!((1,1), UserSL, model,
        user_id,
        name,
        fill(NO_USER_ACCOUNT,length(sim.resource)),
        NO_NEXT_EVENT,
        Vector{HPCEventSL}()
        )

    push!(sim.user, user)
    sim.user_id[name] = user_id

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
    model::EventQueueABM, resource::HPCResourceSL;
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
    dt::Union{Missing,Float64,Int}=missing,
    priority::Int=0
    )
    sim::SimulationSL = abmproperties(model)

    user_id = sim.user_id[user]
    user_account_id = resource.user_account_id[user]
    account_id = ismissing(account) ? resource.user_account[user_account_id].default_account_id : resource.account_id[account]
    partition_id = ismissing(partition) ? resource.default_partition_id : resource.partition_id[partition]
    qos_id = ismissing(qos) ? resource.default_qos_id : resource.qos_id[qos]

    ismissing(mem_per_cpu) && (mem_per_cpu = resource.partition[partition_id].def_mem_per_cpu)

    ismissing(req_walltime) && throw("req_walltime should be set!")
    ismissing(sim_walltime) && (sim_walltime = req_walltime)

    # submit_time and dt
    typeof(dt) == Int && (dt = Float64(dt))
    ismissing(submit_time) && ismissing(dt) && throw("either submit_time or dt should be set!")
    ismissing(submit_time) && !ismissing(dt) && (submit_time = abmdatetime(abmdatetime(model), dt))
    ismissing(dt) && (dt = secondsfrominit(abmdatetime(model), submit_time))
    submit_time != abmdatetime(abmdatetime(model), dt) && throw("submit_time and sim.init_time + dt should be same")

    t = secondsfrominit(sim.init_datetime, submit_time)

    # cpus, cpus_per_node, nodes
    ismissing(cpus) && ismissing(cpus_per_node) && ismissing(nodes) && throw("Two of cpus, cpus_per_node or nodes should be set!")
    !ismissing(cpus) && !ismissing(cpus_per_node) && ismissing(nodes) && (nodes = cpus ÷ cpus_per_node)
    !ismissing(cpus) && ismissing(cpus_per_node) && !ismissing(nodes) && (cpus_per_node = cpus ÷ nodes)
    ismissing(cpus) && !ismissing(cpus_per_node) && !ismissing(nodes) && (cpus = cpus_per_node * nodes)

    cpus % cpus_per_node != 0 && throw("cpus should be multiple of cpus_per_node")
    cpus % nodes != 0 && throw("cpus should be multiple of nodes")

    ares_type_per_node::Vector{ARESType} = [sim.ARESTypes_id[s] for s in ("CPU", "Memory")]
    ares_model_per_node::Vector{ARESModel} = [sim.ARESModels_id[s] for s in ("CPU", "Memory")]
    ares_req_per_node::Vector{Int} = [cpus_per_node, cpus_per_node*mem_per_cpu]
    
    features_id::Vector{NodeFeatureId} = [resource.NodeFeatures_id[feature] for feature in features]
    gres_per_node_id::Vector{ARESType} = [sim.ARESTypes_id[gres] for gres in gres_per_node]
    if length(gres_per_node_id) > 0
        
        if length(gres_per_node_id) == length(gres_model_per_node)
            gres_model_per_node_id::Vector{ARESModel} = [sim.ARESModels_id[gres] for gres in gres_model_per_node]
        elseif length(gres_model_per_node)==0
            # Some generic GRES of that Type
            gres_model_per_node_id=fill(GRES_MODEL_ANY, length(gres_per_node_id))
        else
            throw("GRES Model size do not match GRES type")
        end

        for gres_type in sort(unique(gres_per_node_id))
            models = gres_model_per_node_id[gres_per_node_id .== gres_type]
            for model in sort(unique(models))
                push!(ares_type_per_node, gres_type)
                push!(ares_model_per_node, model)
                push!(ares_req_per_node, sum(models.==model))
            end
        end
    else
        gres_model_per_node_id = Vector{ARESModel}()
    end
    start_time = DateTime(0)
    end_time = DateTime(0)
    walltime=Millisecond(0)
    nodes_list=Vector{NodeId}()

    

    job = BatchJobSL(
        job_id, resource.id, user_id, user_account_id, account_id, partition_id, qos_id, 
        #cpus, cpus_per_node, 
        nodes, 
        #gres_per_node_id, gres_model_per_node_id,
        #mem_per_cpu, 
        node_sharing,
        ares_type_per_node,
        ares_model_per_node,
        ares_req_per_node,
        features_id, req_walltime,
        sim_walltime, submit_time, priority, JOBSTATUS_UNKNOWN, start_time, end_time, walltime, nodes_list, missing)


    push!(sim.user[user_id].events_list, HPCEventSL(submit_time, t, SUBMIT_JOB, job))
    sim.user[user_id].next_event==NO_NEXT_EVENT && (sim.user[user_id].next_event = 1)

    add_event!(sim.user[user_id], PROCESS_USER_EVENT, t, model)

    job
end

function is_workload_done_sl(model, s)
    sim = getproperty(model, :sim)
    s % sim.workload_done_check_freq != 0 && return false

    for resource in sim.resource
        length(resource.queue)==0 && length(resource.executing)==0 && continue
        
        length(resource.queue) > 0 && sum((jobonres.job.status==JOBSTATUS_INQUEUE for jobonres in resource.queue)) > 0 && return false
        length(resource.executing) > 0 && sum((jobonres.job.status==JOBSTATUS_RUNNING for (job_id,jobonres) in resource.executing)) > 0 && return false
    end
    
    for user in sim.user
        user.next_event == NO_NEXT_EVENT && continue
        user.next_event > length(user.events_list) && continue
        return false
    end

    return true
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
    if job.submit_time == DATETIME_UNSET_L
        job.submit_time = abmdatetime(model)
    elseif job.submit_time != abmdatetime(model)
        error("Preplaned job: job.submit_time != abmdatetime(model)")
    end
    
    @debug "Submitting Job: $(job.id) at time abmtime(model)) ($(abmdatetime(model)))"
                

    # job.task.nodetime_left_unplanned -= job.nodes * job.walltime

    #push!(resource.queue, JobOnResourceSL(job))

    #job.status = JOBSTATUS_INQUEUE
    # push!(job.task.current_jobs, job.id)

    return
end

function find_runnable_nodes!(
    sim::SimulationSL, resource::HPCResourceSL, jobonres::JobOnResourceSL)
    
    length(jobonres.gres_counted)!=resource.gres_max && (jobonres.gres_counted = fill(false, resource.gres_max))
    length(jobonres.runnable_nodes_bool)==0 && (jobonres.runnable_nodes_bool = fill(false, length(resource.node)))


    job = jobonres.job
    for (node_id,node) in enumerate(resource.node)
        jobonres.runnable_nodes_bool[node_id] = false
        has_required_features = true
        for feature in job.features
            if feature ∉ node.features
                has_required_features=false
                break
            end
        end

        !has_required_features && continue

        has_required_ares = true
        for (iinjob,ares_type) in enumerate(job.ares_type_per_node)           
            ares_count::Int = 0
            if job.ares_model_per_node[iinjob]==GRES_MODEL_ANY
                for ares_node_index in 1:length(node.ares_type)
                    if node.ares_type[ares_node_index]==ares_type
                        ares_count += node.ares_total[ares_node_index]
                    end
                end
            else
                for ares_node_index in 1:length(node.ares_type)
                    if node.ares_type[ares_node_index]==ares_type && node.ares_model[ares_node_index]==job.ares_model_per_node[iinjob]
                        ares_count += node.ares_total[ares_node_index]
                    end
                end
            end
            # @debug "for", ares_type, job.ares_model_per_node[iinjob], "need", job.ares_req_per_node[iinjob], "got", ares_count
            ares_count < job.ares_req_per_node[iinjob] && (has_required_ares = false) && break
        end

        # @debug "Node", node.name, "fit", has_required_ares
        has_required_ares==false && continue

        jobonres.runnable_nodes_bool[node_id] = true
    end

    if sum(jobonres.runnable_nodes_bool) < job.nodes
        @error "Not enough resources for this job ($(job.id))"
        jobonres.runnable_nodes_bool .= false
    end

    jobonres.runnable_nodes = [i for (i,v) in enumerate(jobonres.runnable_nodes_bool) if v==true]
    jobonres.currently_runnable_nodes = fill(false, length(jobonres.runnable_nodes))
    jobonres.runnable_nodes_availtime = fill(DATETIME_UNSET_L, length(jobonres.runnable_nodes))
    @debug "Job $(job.id) has following runnabe nodes: $([resource.node[node_id].name for node_id in jobonres.runnable_nodes])"
end

"
return true if found nodes
"
function find_currently_runnable_nodes!(
    sim::SimulationSL, resource::HPCResourceSL, jobonres::JobOnResourceSL)
    length(jobonres.runnable_nodes) == 0 && find_runnable_nodes!(sim, resource, jobonres)
    job = jobonres.job

    for (inode,node_id) in enumerate(jobonres.runnable_nodes)
        node = resource.node[node_id]
        
        jobonres.currently_runnable_nodes[inode] = false

        #keep features
        has_required_features = true
        for feature in job.features
            if feature ∉ node.features
                has_required_features=false
                break
            end
        end

        !has_required_features && continue

        # new
        has_required_ares = true
        for (iinjob,ares_type) in enumerate(job.ares_type_per_node)
            ares_need::Int = job.ares_req_per_node[iinjob]
            if job.ares_model_per_node[iinjob]==GRES_MODEL_ANY
                for ares_node_index in 1:length(node.ares_type)
                    if node.ares_type[ares_node_index]==ares_type
                        ares_need -= (node.ares_free[ares_node_index] >= ares_need) ? ares_need : node.ares_free[ares_node_index]
                    end
                end
            else
                for ares_node_index in 1:length(node.ares_type)
                    if node.ares_type[ares_node_index]==ares_type && node.ares_model[ares_node_index]==job.ares_model_per_node[iinjob]
                        ares_need -= (node.ares_free[ares_node_index] >= ares_need) ? ares_need : node.ares_free[ares_node_index]
                    end
                end
            end
            # @debug "for", ares_type, job.ares_model_per_node[iinjob], "need", job.ares_req_per_node[iinjob], "got", ares_count
            ares_need != 0 && (has_required_ares = false) && break
        end
        !has_required_ares && continue

        jobonres.currently_runnable_nodes[inode] = true
    end

    if sum(jobonres.currently_runnable_nodes) < job.nodes
        # not enough resources
        @debug "Job $(job.id) has following currently runnabe nodes: \
        $([resource.node[node_id].name for (node_id,avail) in zip(jobonres.runnable_nodes,jobonres.currently_runnable_nodes) if avail])"

        return false
    end

    @debug "Job $(job.id) has following currently runnabe nodes: \
        $([resource.node[node_id].name for (node_id,avail) in zip(jobonres.runnable_nodes,jobonres.currently_runnable_nodes) if avail])"

    return true
end

"
find runnable nodes availtime
"
function find_runnable_nodes_availtime!(
    sim::SimulationSL, resource::HPCResourceSL, jobonres::JobOnResourceSL)
    length(jobonres.runnable_nodes) == 0 && find_runnable_nodes!(sim, resource, jobonres)
    job = jobonres.job

    for (inode,node_id) in enumerate(jobonres.runnable_nodes)
        node = resource.node[node_id]
        
        jobonres.currently_runnable_nodes[inode] = false
        # cpus_per_node::Int64
        if job.cpus_per_node > node.cpus_free
            
        end

        job.mem_per_cpu * job.cpus_per_node > node.memory_free && continue

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
            fill!(jobonres.gres_counted, false)

            length(job.gres_per_node) > length(node.gres) && continue

            for ijob in 1:length(job.gres_per_node)
                if job.gres_model_per_node[ijob]==GRES_MODEL_ANY
                    for igres in 1:length(node.gres)
                        if job.gres_per_node[ijob] == node.gres[igres] && jobonres.gres_counted[igres]==false && node.gres_used[igres]==NOT_USED_BY_JOB
                            jobonres.gres_counted[igres] = true
                            break
                        end
                    end
                else
                    for igres in 1:length(node.gres)
                        if job.gres_per_node[ijob] == node.gres[igres] && job.gres_model_per_node[ijob]==node.gres_model[igres] && jobonres.gres_counted[igres]==false && node.gres_used[igres]==NOT_USED_BY_JOB
                            jobonres.gres_counted[igres] = true
                            break
                        end
                    end
                end
            end

            if sum(jobonres.gres_counted)!=length(job.gres_per_node)
                continue
            end
        end

        jobonres.currently_runnable_nodes[inode] = true
    end

    if sum(jobonres.currently_runnable_nodes) < job.nodes
        # not enough resources
        @debug "Job $(job.id) has following currently runnabe nodes: \
        $([resource.node[node_id].name for (node_id,avail) in zip(jobonres.runnable_nodes,jobonres.currently_runnable_nodes) if avail])"

        return false
    end

    @debug "Job $(job.id) has following currently runnabe nodes: \
        $([resource.node[node_id].name for (node_id,avail) in zip(jobonres.runnable_nodes,jobonres.currently_runnable_nodes) if avail])"

    return true
end

function place_job!(
    sim::SimulationSL, resource::HPCResourceSL, jobonres::JobOnResourceSL, node_ids::Vector{NodeId})
    job = jobonres.job

    # planned release time
    release_time = sim.cur_datetime + job.req_walltime

    ares_used::Vector{Vector{Int}} = Vector{Vector{Int}}()
    
    for node_id in node_ids
        node = resource.node[node_id]

        # new ares
        for (iinjob,ares_type) in enumerate(job.ares_type_per_node)
            ares_need::Int = job.ares_req_per_node[iinjob]
            if job.ares_model_per_node[iinjob]==GRES_MODEL_ANY
                for ares_node_index in 1:length(node.ares_type)
                    if node.ares_type[ares_node_index]==ares_type
                        ares_alloc = node.ares_free[ares_node_index] >= ares_need ? ares_need : node.ares_free[ares_node_index]
                        ares_alloc == 0 && continue
                        node.ares_free[ares_node_index] -= ares_alloc
                        node.ares_used[ares_node_index] += ares_alloc
                        ares_need -= ares_alloc
                        push!(ares_used, [node_id, ares_node_index, ares_alloc])
                    end
                end
            else
                for ares_node_index in 1:length(node.ares_type)
                    if node.ares_type[ares_node_index]==ares_type && node.ares_model[ares_node_index]==job.ares_model_per_node[iinjob]
                        ares_alloc = node.ares_free[ares_node_index] >= ares_need ? ares_need : node.ares_free[ares_node_index]
                        ares_alloc == 0 && continue
                        node.ares_free[ares_node_index] -= ares_alloc
                        node.ares_used[ares_node_index] += ares_alloc
                        ares_need -= ares_alloc
                        push!(ares_used, [node_id, ares_node_index, ares_alloc])
                    end
                end
            end
            # @debug "for", ares_type, job.ares_model_per_node[iinjob], "need", job.ares_req_per_node[iinjob], "got", ares_count
            ares_need != 0 && throw("Not enough resources but prior allocating it seems it was having enough! (ares still needed: $(ares_need))")
            #ares_count < job.ares_req_per_node[iinjob] && (has_required_ares = false) && break
        end

        push!(node.jobs_on_node, job.id)

        # sanity check
        for iares in 1:length(node.ares_type)
            node.ares_total[iares] != node.ares_free[iares] + node.ares_used[iares] && throw("Miscalulated resource use! Shouldn't happen!")
            node.ares_used[iares] < 0 && throw("Miscalulated resource use! Shouldn't happen!")
            node.ares_free[iares] < 0 && throw("Miscalulated resource use! Shouldn't happen!")
            node.ares_free[iares] > node.ares_total[iares] && throw("Miscalulated resource use! Shouldn't happen!")
            node.ares_used[iares] > node.ares_total[iares] && throw("Miscalulated resource use! Shouldn't happen!")
        end

    end
    @debug "Job $(jobonres.job.id) allocated on:  $([resource.node[node_id].name for node_id in node_ids])"

    jobonres.job.ares_used = transpose(reduce(hcat,ares_used))

    jobonres.job.start_time = sim.cur_datetime
    jobonres.job.nodes_list = copy(node_ids)
    jobonres.job.status = JOBSTATUS_RUNNING
    resource.executing[jobonres.job.id] = jobonres
    # for lazy job removal negative priority is for running jobs but not exclusive
    jobonres.job.priority > 0 && (jobonres.job.priority = - jobonres.job.priority)
    jobonres.job.priority == 0 && (jobonres.job.priority = -1)

    resource.cleanup_arrays += 1

    return true
end


function finish_job!(
    sim::SimulationSL, resource::HPCResourceSL, jobonres::JobOnResourceSL)

    job = jobonres.job

    # new
    for (node_id, ares_node_index, ares_units ) in eachrow(job.ares_used)
        resource.node[node_id].ares_used[ares_node_index] -= ares_units
        resource.node[node_id].ares_free[ares_node_index] += ares_units
    end

    for node_id in job.nodes_list
        node = resource.node[node_id]
        delete!(node.jobs_on_node, job.id)
        # sanity check
        for iares in 1:length(node.ares_type)
            node.ares_total[iares] != node.ares_free[iares] + node.ares_used[iares] && throw("Miscalulated resource use! Shouldn't happen!")
            node.ares_used[iares] < 0 && throw("Miscalulated resource use! Shouldn't happen!")
            node.ares_free[iares] < 0 && throw("Miscalulated resource use! Shouldn't happen!")
            node.ares_free[iares] > node.ares_total[iares] && throw("Miscalulated resource use! Shouldn't happen!")
            node.ares_used[iares] > node.ares_total[iares] && throw("Miscalulated resource use! Shouldn't happen!")
        end
    end

    @debug "Job $(jobonres.job.id) deallocated from:  $([resource.node[node_id].name for node_id in job.nodes_list])"

    jobonres.job.end_time = sim.cur_datetime
    jobonres.job.walltime = jobonres.job.end_time - jobonres.job.start_time
    jobonres.job.status = JOBSTATUS_DONE
    push!(resource.history, deepcopy(jobonres.job))

    resource.cleanup_arrays += 1

    return true

end

function cleanup_arrays!(sim::SimulationSL, resource::HPCResourceSL)

end
"
Check that resource follows conventions
"
function check_resource(model::EventQueueABM, resource::HPCResourceSL)::Bool
    sim::SimulationSL = abmproperties(model)

    errors_count = 0
    for (i,node) in enumerate(resource.node)
        i!=node.id && (errors_count += 1)
    end
    
    errors_count > 0 && throw("Node.id should match index in resource.node vector")
    return true
end

function attempt_to_allocate!(sim::SimulationSL, resource::HPCResourceSL, jobonres::JobOnResourceSL)::Bool
    if find_currently_runnable_nodes!(sim, resource, jobonres)
        return place_job!(
            sim, resource, jobonres, 
            [node_id for (node_id,avail) in zip(jobonres.runnable_nodes,jobonres.currently_runnable_nodes) if avail][1:jobonres.job.nodes])
    end
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

# function user_step!(sim::SimulationSL, model::StandardABM, user::UserSL)
    
#     if user.next_event != NO_NEXT_EVENT
#         while user.next_event <= length(user.events_list) && user.events_list[user.next_event].when <= sim.cur_datetime
#             if user.events_list[user.next_event].event_type == SUBMIT_JOB
#                 submit_job(
#                     sim, model, 
#                     sim.resource[user.events_list[user.next_event].event.resource_id], 
#                     user, 
#                     user.events_list[user.next_event].event)
#             else
#                 @error "Unknown Event!"
#             end
#             user.next_event += 1
#         end
#     end

#     return
# end

function run_scheduler_fifo!(sim::SimulationSL, resource::HPCResourceSL)
    job_scheduled = 0
    for ijob in resource.jobs_order
        jobonres = resource.queue[ijob]
        jobonres.job.status != JOBSTATUS_INQUEUE && continue
        if !attempt_to_allocate!(sim, resource, jobonres)
            break
        else
            job_scheduled += 1
        end
    end
end

function run_scheduler_backfill!(sim::SimulationSL, resource::HPCResourceSL)
    return
    # calculate when next priority job start
    next_priority_ijob = -1
    for ijob in resource.jobs_order
        resource.queue[ijob].job.status != JOBSTATUS_INQUEUE && continue
        next_priority_ijob = ijob
        break
    end
    # no high priority jobs, so nothing to do
    next_priority_ijob < 0 && return
    next_priority_job = resource.queue[next_priority_ijob]
    
end


function run_scheduler_old!(sim::SimulationSL, resource::HPCResourceSL)
    length(resource.queue) == 0 && return

    # Sort
    length(resource.jobs_order) != length(resource.queue) && resize!(resource.jobs_order, length(resource.queue))
    sortperm!(resource.jobs_order, resource.queue, by = x -> x.job.priority, order=Base.Order.Reverse)
    
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
    cur_time = sim.cur_datetime

    # jobs which finished on themselves
    for (job_id,jobonres) in resource.executing
        jobonres.job.sim_walltime < Millisecond(0) && continue
        jobonres.job.status != JOBSTATUS_RUNNING && continue
        jobonres.job.start_time + jobonres.job.sim_walltime > cur_time && continue
        

        finish_job!(sim, resource, jobonres)
    end

    # jobs which finished by reaching req_walltime
    for (job_id,jobonres) in resource.executing
        jobonres.job.sim_walltime >= Millisecond(0) && continue
        jobonres.job.status != JOBSTATUS_RUNNING && continue
        jobonres.job.start_time + jobonres.job.req_walltime > cur_time && continue
        

        finish_job!(sim, resource, jobonres)
    end

    return
end


function track_ares!(sim::SimulationSL, resource::HPCResourceSL)
    if nrow(resource.ares_tracking_df)==0
        iares_str::Vector{String} = Vector{String}()

        for node in resource.node
            append!(iares_str, ["$(node.name):$(sim.ARESModels[iares])" for iares in node.ares_model])
        end
        resource.ares_tracking_df[!, "t"] = Vector{DateTime}()
        for colname in iares_str
            resource.ares_tracking_df[!, colname] = Vector{Int}()
        end
    end

    new_row::Vector{Any} = [sim.cur_datetime]

    for node in resource.node
        append!(new_row, node.ares_used)
    end

    push!(resource.ares_tracking_df, new_row)
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
    sim.cur_step = abmtime(model)
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

    # cleanup_arrays
    sim.resource[1].cleanup_arrays > 0 && cleanup_arrays!(sim, sim.resource[1])

    # model extra step
    isnothing(sim.model_extra_step) == false && sim.model_extra_step(sim, model)

    # more stats
    model_step_stats!(sim)

    sim.resource[1].ares_tracking_df_freq > 0 && \
        sim.cur_step % sim.resource[1].ares_tracking_df_freq==0 && \
        track_ares!(sim, sim.resource[1])
end

function run_model!(model::EventQueueABM; nsteps::Int64=-1, run_till_no_jobs::Bool=false)
    sim::SimulationSL = abmproperties(model)
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

function show_queue(model::EventQueueABM, resource::HPCResourceSL)::Nothing
    sim::SimulationSL = abmproperties(model)
    msg = "JOBSTATUS_RUNNING\n"
    msg *= "job.id nodes runtime\n"
    for (job_id,jobonres) in resource.executing
        jobonres.job.status != JOBSTATUS_RUNNING && continue

        nodes = (length(jobonres.job.nodes_list)==0) ? "NA" : join([resource.node[i].name for i in jobonres.job.nodes_list],",")
        
        runtime = "NA"
        if jobonres.job.status == JOBSTATUS_RUNNING
            runtime = duration_format(sim.cur_datetime - jobonres.job.start_time)
        end
        msg *= "$(jobonres.job.id) $(nodes) $(runtime)\n"
    end
    msg *= "JOBSTATUS_INQUEUE\n"
    msg *= "job.id priority waittime\n"
    for jobonres in resource.queue
        jobonres.job.status != JOBSTATUS_INQUEUE && continue
        waittime = duration_format(sim.cur_datetime - jobonres.job.submit_time)
        msg *= "$(jobonres.job.id) $(jobonres.job.priority) $(waittime)\n"
    end
    @info msg
    return nothing
end


function ares_str(sim::SimulationSL, 
    ares_type::Vector{ARESType}, 
    ares_model::Vector{ARESModel},
    ares_units::Vector{Int})::String
    s = ""
    for iares in 1:length(ares_type)
        if sim.ARESTypes[ares_type[iares]] == sim.ARESModels[ares_model[iares]]
            s *= "$(sim.ARESTypes[ares_type[iares]])"
        else
            s *= "$(sim.ARESTypes[ares_type[iares]]):$(sim.ARESModels[ares_model[iares]])"
        end

        ares_units[iares] != 1 && (s *= ":$(ares_units[iares])")
        iares != length(ares_type) && (s *= ",")
    end
    s
end

function ares_str(sim::SimulationSL,
    node::ComputeNodeSL)::String
    ares_str(sim, node.ares_type, node.ares_model, node.ares_total)
end


function show_history(model::EventQueueABM, resource::HPCResourceSL)
    sim::SimulationSL = abmproperties(model)
    msg = "JOBSTATUS_DONE\n"
    msg *= "job.id cpus nodes runtime gres nodes_list\n"
    for job in resource.history
        nodes_list = (length(job.nodes_list)==0) ? "NA" : join([resource.node[i].name for i in job.nodes_list],",")
        
        walltime = "NA"
        if job.status == JOBSTATUS_DONE
            walltime = duration_format(job.walltime)
        end
        
        gres_list = (length(job.gres_per_node)==0) ? "NA" : gres_str(sim, resource, job.gres_per_node, job.gres_model_per_node)

         msg *= "$(job.id) $(job.cpus) $(job.nodes) $(job.submit_time) $(job.start_time) $(job.end_time) $(walltime) $(gres_list) $(nodes_list)\n"
    end
    @info msg
    nothing
end


function show_node_info(model::EventQueueABM, resource::HPCResourceSL)::Nothing
    sim::SimulationSL = abmproperties(model)
    msg = "Node Info:"
    for (inode,node) in enumerate(resource.node)
        msg *= "$(inode) $(node.name):"
        for iares in 1:length(node.ares_type)
            if sim.ARESTypes[node.ares_type[iares]] == sim.ARESModels[node.ares_model[iares]]
                 msg *= " $(sim.ARESTypes[node.ares_type[iares]]):$(node.ares_used[iares])/$(node.ares_total[iares])"
            else
                 msg *= " $(sim.ARESTypes[node.ares_type[iares]]):$(sim.ARESModels[node.ares_model[iares]]):$(node.ares_used[iares])/$(node.ares_total[iares])"
            end
        end
         msg *= " State=$(node.node_state) JobsOnNode=$(node.jobs_on_node)\n"
    end
    @info msg
    nothing
end


function process_user_event!(agent::UserSL, model::EventQueueABM)
    @debug5 "process_user_event! $(abmproperties(model).cur_datetime) $(abmtime(model)) $(agent.id) $(agent.user_id)"
    if user.next_event != NO_NEXT_EVENT
        while user.next_event <= length(user.events_list) && user.events_list[user.next_event].t <= abmtime(model)
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

    return
end

function process_user_event_time(agent::UserSL, model::EventQueueABM, propensity::Float64)
    @debug5 "submit_job_time"
    return 30.0
end


function run_scheduler_time(agent::HPCResourceSL, model::EventQueueABM, propensity::Float64)
    @info "run_scheduler_time"

    return 1.0
end

function check_walltimelimit_time(agent::HPCResourceSL, model::EventQueueABM, propensity::Float64)
    @info "check_walltimelimit_time"
    return 300.0
end

function run_scheduler!(agent::HPCResourceSL, model::EventQueueABM)
    @debug5 "run_scheduler!  $(abmproperties(model).cur_datetime) $(abmtime(model))"


    add_event!(agent, RUN_SCHEDULER_EVENT,1, model)
end

function check_walltimelimit!(agent::HPCResourceSL, model::EventQueueABM)
    @debug5 "check_walltimelimit!"
    add_event!(agent, CHECK_WALLTIMELIMIT_EVENT,300, model)
end


function create_model_sl(;
    id=1,
    init_datetime::DateTime=DateTime(2024,11,1,0,0,0),
    #timestep::Millisecond=Millisecond(3600*1000),
    rng::AbstractRNG=Random.default_rng(123),
    user_extra_step::Union{Function,Nothing}=nothing,
    model_extra_step::Union{Function,Nothing}=nothing,
    workload_done_check_freq::Float64=3600.0
    )

    # set events
    process_user_event_event = AgentEvent(
        action! = process_user_event!, propensity = 1.0, types = UserSL, timing=process_user_event_time)
    run_scheduler_event = AgentEvent(
        action! = run_scheduler!, propensity = 1.0, types = HPCResourceSL, timing=run_scheduler_time)
    check_walltimelimit_event = AgentEvent(
        action! = check_walltimelimit!, propensity = 1.0, types = HPCResourceSL, timing=check_walltimelimit_time)

    events=(process_user_event_event, run_scheduler_event, check_walltimelimit_event)
    
    ARESTypes::Vector{String} = ["CPU", "Memory", "GPU"]
    ARESTypes_id::Dict{String, ARESType} =  Dict( k=>v for (v,k) in enumerate(ARESTypes))
    ARESModels::Vector{String} =  ["CPU", "Memory", "GPU"]
    ARESModels_id::Dict{String, ARESModel} =  Dict( k=>v for (v,k) in enumerate(ARESTypes))

    sim = SimulationSL(
        id,
        init_datetime,
        workload_done_check_freq,
        Vector{HPCResourceSL}(),
        Dict{String, ResourceId}(),
        Vector{UserSL}(),
        Dict{String, UserId}(),
        NO_NEXT_EVENT,
        Vector{HPCEventSL}(),
        #nothing,
        #nothing,
        #rng,
        nothing,
        nothing,
        user_extra_step, model_extra_step,
        ARESTypes,
        ARESTypes_id,
        ARESModels,
        ARESModels_id
        )
    
    model = EventQueueABM(
        Union{HPCResourceSL,UserSL}, events, GridSpace((10,10)); 
        rng, 
        warn = false,
        properties=sim,
        autogenerate_on_add = false,
        autogenerate_after_action = false)

    # tests to insure proper indexing
    #@assert HPCAGENT_RESOURCE == getfield(model, :type_func)(resource)
    #@assert HPCAGENT_USER == getfield(model, :type_func)(user1)
    @assert abmevents(model)[PROCESS_USER_EVENT] === process_user_event_event
    @assert abmevents(model)[RUN_SCHEDULER_EVENT] === run_scheduler_event
    @assert abmevents(model)[CHECK_WALLTIMELIMIT_EVENT] === check_walltimelimit_event

    return model
end
