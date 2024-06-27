using Agents
using Random
using DocStringExtensions

using Statistics: mean, std
using DataFrames

import Agents: run!
using Tidier
using DataStructures
using Distributions
using Printf


used_nodes(r::HPCResource) = sum(r.node_used_by_job .!= 0)
used_nodes(m::StandardABM) = used_nodes(m.sim.resource)

get_nodename(node_id) = @sprintf("N%04d", node_id)

"""
if user_id is specified it is up to programmer to add it to user.tasks_to_do (or not)
    task_id=0 will set task_id=-user_id for job's replay purposes
"""
function CompTask(
    sim::Simulation,
    user_id::Int64;
    task_id::Int64=-1,
    nodetime::Int64=72,
    submit_time::Int64=0,
    task_split_schema::TaskSplitSchema=UserPreferred,
    max_concurrent_jobs::Int64=1,
    nodes_prefered::Int64=-1,
    walltime_prefered::Int64=-1,
    inividual_jobs_task::Bool=false
)
    task_split_schema != JobReplay && max_concurrent_jobs > 1 && error("concurrent jobs within same task are not implemented yet")

    user_id = isnothing(user_id) ? user.id : user_id

    if inividual_jobs_task
        task_id = -user_id
    end

    if task_id == -1 && inividual_jobs_task == false
        task_id = sim.last_task_id + 1
    else
        task_id in keys(sim.task_dict) && error("Such task (task_id=$(task_id)) already exists")
    end
    if task_id > 0
        sim.last_task_id = task_id
    end

    task = CompTask(
        task_id, user_id,
        nodetime, nodetime, nodetime, 0,
        submit_time, -1, -1,
        task_split_schema,
        max_concurrent_jobs,
        nodes_prefered,
        walltime_prefered,
        Vector{Int64}(), Vector{Int64}(),
        0)

    push!(sim.task_list, task)
    task_split_schema != JobReplay && push!(sim.users_dict[user_id].tasks_to_do, sim.task_list[end])
    sim.task_dict[task_id] = sim.task_list[end]

    return sim.task_list[end]
end

"""
Create a BatchJob for task
"""
function BatchJob(
    sim::Simulation, task::CompTask;
    nodes::Int64=1,
    walltime::Int64=1,
    submit_time::Int64=-1,
    job_id::Int64=-1,
    jobs_list::Union{SortedSet,Nothing}=nothing
)::BatchJob

    if job_id == -1
        sim.last_job_id += 1
    else
        sim.last_job_id = job_id
        sim.last_job_id in keys(sim.jobs_dict) && error("Such job (job_id=$(job_id)) already exists")
    end

    if submit_time == -1
        submit_time = sim.model !== nothing ? submit_time = abmtime(sim.model) : 0
    end

    push!(sim.jobs_list, BatchJob(sim.last_job_id, task, nodes, walltime, submit_time, -1, -1, NotScheduled, []))
    sim.jobs_dict[sim.last_job_id] = sim.jobs_list[end]

    if jobs_list !== nothing
        push!(jobs_list, sim.jobs_list[end])
    end
    return sim.jobs_list[end]
end

"""
Create new user, add it to model
"""
function User(
    sim::Simulation;
    max_concurrent_tasks::Int64=4,
    max_nodes_per_job::Int64=-1,
    max_time_per_job::Int64=-1,
    user_id::Int64=-1
)::User
    if user_id == -1
        sim.last_user_id += 1
    else
        sim.last_user_id = user_id
        sim.last_user_id in keys(sim.users_dict) && error("Such user (user_id=$(user_id)) already exists")
    end

    user = User(
        sim.last_user_id,
        (1,),
        max_concurrent_tasks,
        max_nodes_per_job,
        max_time_per_job,
        SortedSet{CompTask}(), Vector{CompTask}(), Vector{CompTask}(),
        Vector{BatchJob}(),
        CompTask(sim, sim.last_user_id; nodetime=-1, task_split_schema=JobReplay, submit_time=0, max_concurrent_jobs=1_000_000, inividual_jobs_task=true),
        SortedSet{BatchJob}(),
        generate_thinktime_zero
    )

    push!(sim.users_list, user)
    sim.users_dict[sim.last_user_id] = user

    # add agent to model
    add_agent!(user, sim.model)

    return sim.users_list[end]
end

function HPCResourceStats()
    HPCResourceStats(1, DataFrame(), DataFrame(), DataFrame())
end


function add_resource!(sim::Simulation;
    nodes=10,
    max_nodes_per_job=4,
    max_time_per_job=24 * 3,
    scheduler_fifo=true,
    scheduler_backfill=true
)::HPCResource
    sim.space !== nothing && error("sim.space already initialized")
    sim.resource !== nothing && error("sim.resource already initialized")
    nodes <= 0 && error("nodes should be positive!")

    sim.space = GridSpace((nodes,); periodic=false, metric=:manhattan)
    sim.resource = HPCResource(
        nodes,
        zeros(Int64, nodes),
        fill(Int64(-1), nodes),
        fill(Int64(-1), nodes),
        [], Dict{Int64,BatchJob}(), [],
        max_nodes_per_job, max_time_per_job,
        scheduler_fifo, scheduler_backfill,
        HPCResourceStats()
    )

    # init some of sim.resource.stats members
    ncol(sim.resource.stats.node_occupancy_by_user) != 0 && error("sim.stats.node_occupancy already initialized")
    node_occupancy_by_user = sim.resource.stats.node_occupancy_by_user
    node_occupancy_by_user.t = Vector{Int64}()
    for node_id in 1:nodes
        node_occupancy_by_user[!, get_nodename(node_id)] = Vector{Int64}()
    end
    node_occupancy_by_job = sim.resource.stats.node_occupancy_by_job
    node_occupancy_by_job.t = Vector{Int64}()
    for node_id in 1:nodes
        node_occupancy_by_job[!, get_nodename(node_id)] = Vector{Int64}()
    end
    node_occupancy_by_task = sim.resource.stats.node_occupancy_by_task
    node_occupancy_by_task.t = Vector{Int64}()
    for node_id in 1:nodes
        node_occupancy_by_task[!, get_nodename(node_id)] = Vector{Int64}()
    end

    sim.resource
end

"""
get datetime from simulation step
"""
function get_datetime(sim::Simulation, step::Int64)
    sim.timeunit * step + sim.init_datetime
end

"""
get simulation step from datetime
"""
function get_step(sim::Simulation, datetime::DateTime)
    (datetime - sim.init_datetime) ÷ sim.timeunit
end



"""
get nearby simulation step from datetime
"""
function get_round_step(sim::Simulation, datetime::DateTime)
    round(datetime - sim.init_datetime, sim.timeunit) ÷ sim.timeunit
end

"""
Simulation constructor
"""
function Simulation(
    ;
    id::Int64=1,
    timeunit::Period=Hour(1),
    init_datetime::DateTime=DateTime(2024, 1, 1),
    rng::AbstractRNG=Random.default_rng(123),
    user_extra_step::Union{Function,Nothing}=nothing,
    model_extra_step::Union{Function,Nothing}=nothing
)::Simulation
    cur_datetime::DateTime = init_datetime

    timeunits_per_day = Day(1) ÷ timeunit

    sim = Simulation(
        id,
        timeunits_per_day,
        timeunit,
        cur_datetime::DateTime,
        init_datetime::DateTime,
        0, Vector{CompTask}(), Dict{Int64,CompTask}(),
        0, Vector{BatchJob}(), Dict{Int64,BatchJob}(),
        0, Vector{User}(), Dict{Int64,User}(),
        nothing,
        nothing,
        nothing,
        rng,
        nothing,
        nothing,
        user_extra_step, model_extra_step,
        1000 # workload_done_check_freq
    )

    sim.model = StandardABM(
        User,
        sim.space;
        model_step!,
        properties=Dict(
            :sim => sim),
        sim.rng,
        scheduler=Schedulers.Randomly()
    )

    sim
end

"""
CompTask Split Strategy use user prefered values.
Constrained by max nodes allowed and max walltime by Resource and User
"""
function task_split_user_prefered_values!(sim::Simulation, task::CompTask; user::Union{User,Nothing}=nothing)::BatchJob
    task.nodetime_left_unplanned <= 0 && error("can not make job for this task nodetime<=0")
    max_nodes_per_job = sim.resource.max_nodes_per_job
    max_time_per_job = sim.resource.max_time_per_job

    if user === nothing
        user = sim.user_dict[task.user_id]
    end

    user.id != task.user_id && error("user.id != task.user_id, this should not happen!")

    # user's restriction
    if user.max_nodes_per_job > 0 && user.max_nodes_per_job < max_nodes_per_job
        max_nodes_per_job = user.max_nodes_per_job
    end
    if user.max_time_per_job > 0 && user.max_time_per_job < max_time_per_job
        max_time_per_job = user.max_time_per_job
    end

    nodes = task.nodes_prefered
    if nodes > max_nodes_per_job
        nodes = max_nodes_per_job
    end

    walltime = task.nodetime_left_unplanned ÷ nodes
    if task.nodetime_left_unplanned % nodes != 0
        walltime += 1
    end

    if walltime > task.walltime_prefered
        walltime = task.walltime_prefered
    end
    if walltime > max_time_per_job
        walltime = max_time_per_job
    end

    BatchJob(sim, task; nodes, walltime)
end

"""
CompTask Split Strategy use AdaptiveFactor scheme.
The nodes can be in range adaptive_factor_nodes[1]*nodes_prefered to adaptive_factor_nodes[2]*nodes_prefered depending on available nodes.
Similarly walltime can be in range adaptive_factor_walltime[1]*walltime_prefered to adaptive_factor_walltime[2]*walltime_prefered depending on available walltime.
Constrained by max nodes allowed and max walltime by Resource and User
"""
function task_split_adaptive_factor!(sim::Simulation, task::CompTask;
    user::Union{User,Nothing}=nothing,
    adaptive_factor_nodes=[0.5, 2.0],
    adaptive_factor_walltime=[0.25, 4.0]
)::BatchJob
    task.nodetime_left_unplanned <= 0 && error("can not make job for this task nodetime<=0")

    if user === nothing
        user = sim.user_dict[task.user_id]
    end

    user.id != task.user_id && error("user.id != task.user_id, this should not happen!")

    println("Splitting task: $(task.id) at time $(abmtime(sim.model))")
    # resource restriction
    max_nodes_per_job = sim.resource.max_nodes_per_job
    max_time_per_job = sim.resource.max_time_per_job
    # user's restriction
    if user.max_nodes_per_job > 0 && user.max_nodes_per_job < max_nodes_per_job
        max_nodes_per_job = user.max_nodes_per_job
    end
    if user.max_time_per_job > 0 && user.max_time_per_job < max_time_per_job
        max_time_per_job = user.max_time_per_job
    end

    nodes_left = min(floor(Int64, adaptive_factor_nodes[1] * task.nodes_prefered), max_nodes_per_job)
    nodes_right = min(ceil(Int64, adaptive_factor_nodes[2] * task.nodes_prefered), max_nodes_per_job)
    walltime_left = min(floor(Int64, adaptive_factor_walltime[1] * task.walltime_prefered), max_time_per_job)
    walltime_right = min(ceil(Int64, adaptive_factor_walltime[2] * task.walltime_prefered), max_time_per_job)
    nodes = nodes_right
    walltime_cap = walltime_right
    println("preferred nodes=$(task.nodes_prefered) walltime=$(task.walltime_prefered)")
    println("nodes preferred range: [$(nodes_left),$(nodes_right)] walltime: [$(walltime_left), $(walltime_right)]")


    # What's available on resource ?
    # when next priority job start
    nodes_free = sim.resource.nodes - used_nodes(sim.resource)
    if length(sim.resource.queue) > 0
        next_fifo_job = sim.resource.queue[1]
        next_fifo_job_starttime = sim.resource.node_released_at_sorted[next_fifo_job.nodes]
        println("nodes opportunity: nodes=$(nodes_free) walltime=$(next_fifo_job_starttime)")

        if next_fifo_job_starttime <= 0 || next_fifo_job_starttime < walltime_left || nodes_free < nodes_left
            nodes = min(task.nodes_prefered, max_nodes_per_job)
            walltime_cap = min(task.walltime_prefered, max_time_per_job)
            println("Use preferred: nodes=$(nodes), walltime_cap=$(walltime_cap)")
        else
            # by this time nodes_free >= nodes_left
            # no we want to ensure that nodes <= nodes_right
            nodes = min(nodes_free, nodes_right)
            walltime_cap = min(next_fifo_job_starttime, walltime_right)
            println("Will use: nodes=$(nodes), walltime_cap=$(walltime_cap)")
        end
    else
        println("no queue go with max: nodes=$(nodes), walltime_cap=$(walltime_cap)")
    end

    if nodes > max_nodes_per_job
        nodes = max_nodes_per_job
    end

    walltime = task.nodetime_left_unplanned ÷ nodes
    if task.nodetime_left_unplanned % nodes != 0
        walltime += 1
    end

    if walltime > walltime_cap
        walltime = walltime_cap
    end
    if walltime > max_time_per_job
        walltime = max_time_per_job
    end

    BatchJob(sim, task; nodes, walltime)
end

task_split! = Dict(
    UserPreferred => task_split_user_prefered_values!,
    AdaptiveFactor => task_split_adaptive_factor!
)
#    task_split_maxnode_maxtime!
#]

function submit_job(sim::Simulation, model::StandardABM, resource::HPCResource, job::BatchJob)
    if job.submit_time < 0
        job.submit_time = abmtime(model)
    elseif job.submit_time != abmtime(model)
        error("Preplaned job: job.submit_time != abmtime(model)")
    end
    job.task.nodetime_left_unplanned -= job.nodes * job.walltime

    push!(resource.queue, job)

    push!(job.task.current_jobs, job.id)

    return
end

function generate_thinktime_zero(sim::Simulation, user::User)::Int64
    0
end

function generate_thinktime_gamma(sim::Simulation, user::User)::Int64
    shape = 0.23743230
    scale = 1.0 / 0.05508324
    gamma = Gamma(shape, scale)
    round(Int64, rand(sim.rng, gamma))
end

function user_step!(sim::Simulation, model::StandardABM, user::User)
    if length(user.inividual_jobs) == 0 && length(user.tasks_to_do) == 0 && length(user.tasks_active) == 0
        return
    end
    # process finished jobs, archive finished tasks
    for job in user.jobs_to_process
        job.task.nodetime_left -= job.nodes * job.walltime
        job.task.nodetime_done += job.nodes * job.walltime

        # in what time user will check this job
        job.task.next_check_time = abmtime(model) + user.thinktime_generator(sim, user)

        popat!(job.task.current_jobs, findfirst(==(job.id), job.task.current_jobs))
        push!(job.task.jobs, job.id)
    end

    # retire completed active tasks
    i = 1
    while i <= length(user.tasks_active)
        if user.tasks_active[i].nodetime_left <= 0 && user.tasks_active[i].nodetime_total > 0 && user.tasks_active[i].next_check_time <= abmtime(model)
            task = popat!(user.tasks_active, i)

            task.nodetime_left = 0
            task.end_time = abmtime(model)

            push!(user.tasks_done, task)
        else
            i += 1
        end
    end
    resize!(user.jobs_to_process, 0)

    # users extra step
    isnothing(sim.user_extra_step) == false && sim.user_extra_step(sim, model, User)

    # activate new tasks
    while length(user.tasks_to_do) > 0 && length(user.tasks_active) < user.max_concurrent_tasks && first(user.tasks_to_do).submit_time <= abmtime(model)
        task = pop!(user.tasks_to_do)
        task.start_time = abmtime(model)
        push!(user.tasks_active, task)
    end

    # submit new job within active tasks
    global task_split!
    for task in user.tasks_active
        if length(task.current_jobs) < task.max_concurrent_jobs && task.nodetime_left > 0 && task.next_check_time <= abmtime(model)
            job = task_split![task.task_split_schema](sim, task; user)
            submit_job(sim, model, sim.resource, job)
        end
    end

    # submit new individual job
    while length(user.inividual_jobs) > 0 && first(user.inividual_jobs).submit_time <= abmtime(model)
        job = pop!(user.inividual_jobs)
        submit_job(sim, model, sim.resource, job)
    end

    return
end

"""
free nodes are good to fit this job
"""
function place_job!(model::StandardABM, resource::HPCResource, job_position_at_queue::Int64)
    job = popat!(resource.queue, job_position_at_queue)
    resource.executing[job.id] = job
    run_till = abmtime(model) + job.walltime
    job.start_time = abmtime(model)
    node_count = 0

    for node_id in 1:resource.nodes
        if resource.node_used_by_job[node_id] == 0
            resource.node_used_by_job[node_id] = job.id
            resource.node_released_at[node_id] = run_till
            node_count += 1

            push!(job.nodes_list, node_id)

            if node_count == job.nodes
                break
            end
        end
    end
    resource.node_released_at_sorted = sort(resource.node_released_at)
    job
end

function run_scheduler_fifo!(sim::Simulation, model::StandardABM, resource::HPCResource)
    while length(resource.queue) > 0
        job = resource.queue[1]
        nodes_available = resource.nodes - used_nodes(resource)

        if job.nodes <= nodes_available
            job = place_job!(model, resource, 1)
            job.scheduled_by = FIFO
        else
            break
        end
    end
end

function run_scheduler_backfill!(sim::Simulation, model::StandardABM, resource::HPCResource)
    while length(resource.queue) > 0
        nodes_free = resource.nodes - used_nodes(resource)
        #println("Time: $(abmtime(model)) free nodes: $(nodes_free) queue $(length(resource.queue))")

        # any jobs with fit by node count
        job_fit = findfirst(
            x -> x.nodes <= nodes_free, resource.queue)

        isnothing(job_fit) && return

        # when next priority job start
        next_fifo_job = resource.queue[1]
        next_fifo_job_starttime = resource.node_released_at_sorted[next_fifo_job.nodes]

        next_fifo_job_starttime <= 0 && return

        # any jobs with fit by node count and walltime
        job_fit = findfirst(
            x -> x.nodes <= nodes_free && x.walltime <= next_fifo_job_starttime, resource.queue)
        isnothing(job_fit) && return

        # schedule the job
        job = place_job!(model, resource, job_fit)
        job.scheduled_by = Backfill
        #println("jobs_fit_nodes: $(jobs_fit_nodes) next_fifo_job_starttime $(next_fifo_job_starttime) jobs_fit_nodes_and_time $(jobs_fit_nodes_and_time)")

        break
    end
end


function run_scheduler!(sim::Simulation, model::StandardABM, resource::HPCResource)
    # FIFO
    if resource.scheduler_fifo
        run_scheduler_fifo!(sim, model, resource)
    end
    if resource.scheduler_backfill
        run_scheduler_backfill!(sim, model, resource)
    end
end


"""
Check for finished jobs
    the convention is that job run all the way till current time, 
    excluding current time.
"""
function check_finished_job!(sim::Simulation, model::StandardABM, resource::HPCResource)
    cur_time = abmtime(model)
    for i in 1:resource.nodes
        if resource.node_released_at[i] >= 0 && resource.node_released_at[i] <= cur_time
            job_id = resource.node_used_by_job[i]
            job = pop!(resource.executing, job_id)
            job.end_time = abmtime(model)
            user = model[job.task.user_id]
            # clear from nodes
            for i2 in i:resource.nodes
                if resource.node_used_by_job[i2] == job.id
                    resource.node_used_by_job[i2] = 0
                    resource.node_released_at[i2] = -1
                end
            end
            push!(resource.history, job)
            push!(user.jobs_to_process, job)
        end
    end
    resource.node_released_at_sorted = sort(resource.node_released_at)
    return
end

function model_step_stats!(sim::Simulation)
    abmtime(sim.model) % sim.resource.stats.calc_freq != 0 && return

    ncol(sim.resource.stats.node_occupancy_by_user) == 0 && error("sim.resource.stats.node_occupancy_by_user was not initialized!")
    by_user = zeros(Int64, ncol(sim.resource.stats.node_occupancy_by_user))
    by_job = zeros(Int64, ncol(sim.resource.stats.node_occupancy_by_user))
    by_task = zeros(Int64, ncol(sim.resource.stats.node_occupancy_by_user))
    by_user[1] = abmtime(sim.model)
    by_job[1] = abmtime(sim.model)
    by_task[1] = abmtime(sim.model)
    for (job_id, job) in sim.resource.executing
        for node_id in job.nodes_list
            col_id = node_id + 1
            by_user[col_id] != 0 && error("node can be occupied only by one job, but it is not!")
            by_user[col_id] = job.task.user_id
            by_job[col_id] = job_id
            by_task[col_id] = job.task.id
        end
    end
    push!(sim.resource.stats.node_occupancy_by_user, by_user)
    push!(sim.resource.stats.node_occupancy_by_job, by_job)
    push!(sim.resource.stats.node_occupancy_by_task, by_task)
end

function model_step!(model::StandardABM)
    sim::Simulation = model.sim
    sim.cur_datetime = get_datetime(sim, abmtime(model))
    # it is right before abmtime(model) time
    # check finished job
    check_finished_job!(sim, model, model.sim.resource)


    # it is abmtime(model) time
    # schedule
    run_scheduler!(sim, model, model.sim.resource)

    # ask users to do their staff
    for id in abmscheduler(model)(model)
        # here `agent_step2!` may delete agents, so we check for it manually
        hasid(model, id) || continue
        #agent_step2!(model[id], model)
        # println("model_step!: User: $(model[id].id) $(model[id].pos[1])")
        user_step!(sim, model, model[id])
    end

    # schedule
    run_scheduler!(sim, model, model.sim.resource)

    # more stats
    model_step_stats!(sim)

    # model extra step
    isnothing(sim.model_extra_step) == false && sim.model_extra_step(sim, model)
end

function is_workload_done(model, s)
    sim = getproperty(model, :sim)
    s % sim.workload_done_check_freq != 0 && return false

    length(sim.resource.queue) > 0 && return false
    length(sim.resource.executing) > 0 && return false

    for user in sim.users_list
        length(user.inividual_jobs) > 0 && return false
        length(user.tasks_active) > 0 && return false
        length(user.tasks_to_do) > 0 && return false
    end

    return true
end


function run!(sim::Simulation; nsteps::Int64=-1, run_till_no_jobs::Bool=false)
    model::StandardABM = sim.model
    # Users Statistics
    adata0 = [
        (:mean_tasks_to_do, u -> length(u.tasks_to_do), mean)
        (:mean_tasks_active, u -> length(u.tasks_active), mean)
    ]
    # Resource Statistics
    mdata0 = [
        (:used_nodes, m -> sum(m.sim.resource.node_used_by_job .!= 0)),
        (:jobs_in_queue, m -> length(m.sim.resource.queue)),
        (:jobs_running, m -> length(m.sim.resource.executing)),
        (:jobs_done, m -> length(m.sim.resource.history)),
    ]
    if nsteps != -1
        run_till_no_jobs = false
    end

    if run_till_no_jobs
        end_criteria = is_workload_done
    else
        end_criteria = nsteps
    end

    sim.adf, sim.mdf = run!(
        model, end_criteria;
        adata=[(v[2], v[3]) for v in adata0],
        mdata=[v[2] for v in mdata0])

    rename!(sim.adf, [[:time]; [v[1] for v in adata0]])
    rename!(sim.mdf, [[:time]; [v[1] for v in mdata0]])

    sim.adf, sim.mdf
end
