using Agents
using Random
using DocStringExtensions

using Statistics: mean,std
using DataFrames

import Agents: run!
using Tidier

include("hpc_user_model_types.jl")

used_nodes(r::HPCResource) = sum(r.node_used_by_job .!= 0)
used_nodes(m::StandardABM) = used_nodes(m.resource)

function Task(sim::Simulation, user::User, nodetime::Int64, create_time::Int64)
    sim.last_task_id += 1
    task = Task(sim.last_task_id, user.id, nodetime, nodetime, create_time, 0, 0, 0, [])
    push!(sim.task_list, task)
    push!(user.tasks_to_do, sim.task_list[sim.last_task_id])
    return sim.task_list[sim.last_task_id]
end

function BatchJob(sim::Simulation,task::Task, nodes::Int64, walltime::Int64)
    sim.last_job_id += 1
    push!(sim.jobs_list, BatchJob(sim.last_job_id, task, nodes, walltime,0,0,0,NotScheduled,[]))
    return sim.jobs_list[sim.last_job_id]
end

function User(
    sim::Simulation; 
    max_concurrent_tasks::Int64=4, 
    task_split_schema::Int64=1,
    max_nodes_per_job::Int64=0,
    max_time_per_job::Int64=0
    )
    sim.last_user_id += 1
    push!(sim.users_list, User(
        sim.last_user_id,
        (1,),
        max_concurrent_tasks,
        max_nodes_per_job,
        max_time_per_job,
        task_split_schema,
        [],[],[],
        []))
    return sim.users_list[sim.last_user_id]
end

function add_resource!(sim::Simulation; 
    nodes=10,
    max_nodes_per_job=4,
    max_time_per_job=24*3,
    scheduler_fifo=true,
    scheduler_backfill=true
    )
    sim.space = GridSpace((nodes,); periodic = false, metric = :manhattan)
    sim.resource = HPCResource(
        nodes,
        zeros(Int64,nodes),
        fill(Int64(-1),nodes),
        fill(Int64(-1),nodes),
        [],Dict{Int64,BatchJob}(),[],
        max_nodes_per_job, max_time_per_job,
        scheduler_fifo, scheduler_backfill
    )
end

function Simulation(
    ;
    id::Int64=1,
    timeunits_per_day::Int64=24,
    rng::AbstractRNG = Random.default_rng(123),
    user_extra_step::Union{Function, Nothing}=nothing,
    model_extra_step::Union{Function, Nothing}=nothing
    )
    Simulation(
        id,
        timeunits_per_day,
        0,[],
        0,[],
        0,[],
        nothing,
        nothing,
        nothing,
        rng,
        nothing,
        nothing,
        user_extra_step, model_extra_step
    )
end

"""
Task Split Strategy 1
Max nodes allowed
Max time
"""
function task_split_maxnode_maxtime!(sim::Simulation, model::StandardABM, user::User, task::Task)::BatchJob
    max_nodes_per_job = model.resource.max_nodes_per_job
    max_time_per_job = model.resource.max_time_per_job

    # user's restriction
    if user.max_nodes_per_job > 0 && user.max_nodes_per_job < max_nodes_per_job
        max_nodes_per_job = user.max_nodes_per_job
    end
    if user.max_time_per_job > 0 && user.max_time_per_job < max_time_per_job
        max_time_per_job = user.max_time_per_job
    end

    nodes = max_nodes_per_job
    walltime = task.nodetime_left ÷ nodes
    if task.nodetime_left % nodes != 0
        walltime += 1
    end

    if walltime > max_time_per_job
        walltime = max_time_per_job
    end
    
    BatchJob(sim, task, nodes, walltime)
end

task_split! = [
    task_split_maxnode_maxtime!
]


function user_step!(sim::Simulation, model::StandardABM, user::User)
    if length(user.tasks_to_do) == 0 && length(user.tasks_active) == 0
        return
    end
    # process finished jobs, archive finished tasks
    for job in user.jobs_to_process
        job.task.nodetime_left -= job.nodes * job.walltime
        if job.task.nodetime_left <= 0
            job.task.nodetime_left = 0
            push!(job.task.jobs, job.id)
            job.task.current_jobs = 0
            job.task.finish_time = abmtime(model)

            task = popat!(user.tasks_active, findfirst(x->x.id==job.task.id,user.tasks_active))
            push!(user.tasks_done, task)
        end
    end
    resize!(user.jobs_to_process, 0)

    # users extra step
    isnothing(sim.user_extra_step) == false && sim.user_extra_step(sim, model, User)

    # activate new tasks
    while length(user.tasks_to_do) > 0 && length(user.tasks_active) < user.max_concurrent_tasks
        task = popfirst!(user.tasks_to_do)
        task.activation_time = abmtime(model)
        push!(user.tasks_active, task)
    end

    # submit new job
    global task_split!
    for task in user.tasks_active
        if task.current_jobs==0 && task.nodetime_left > 0
            job = task_split![user.task_split_schema](sim, model, user, task)
            push!(sim.resource.queue, job)
            task.current_jobs = job.id
        end
    end

    return
end

"""
free nodes are good to fit this job
"""
function place_job!(model::StandardABM, resource::HPCResource, job_position_at_queue::Int64)
    job = popat!(resource.queue, job_position_at_queue)
    resource.executing[job.id] = job
    run_till = abmtime(model)+job.walltime
    node_count = 0
    for i in 1:resource.nodes
        if resource.node_used_by_job[i] == 0
            resource.node_used_by_job[i] = job.id
            resource.node_released_at[i] = run_till
            node_count += 1

            if node_count==job.nodes
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
            job.scheduled_by=FIFO
        else
            break
        end
    end
end

function run_scheduler_backfill!(sim::Simulation, model::StandardABM, resource::HPCResource)
    while length(resource.queue) > 0
        nodes_free = resource.nodes - used_nodes(resource)

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
        job.scheduled_by=Backfill
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

function check_finished_job!(sim::Simulation, model::StandardABM, resource::HPCResource)
    cur_time = abmtime(model)
    for i in 1:resource.nodes
        if resource.node_released_at[i] >= 0 && resource.node_released_at[i] <= cur_time
            job_id = resource.node_used_by_job[i]
            job = pop!(resource.executing, job_id)
            user = model[job.task.user_id]
            # clear from nodes
            for i2 in i:resource.nodes
                if resource.node_used_by_job[i2] == job.id
                    resource.node_used_by_job[i2] = 0
                    resource.node_released_at[i2] = -1
                    push!(job.nodes_list, i2)
                end
            end
            push!(resource.history, job)
            push!(user.jobs_to_process, job)
        end
    end
    resource.node_released_at_sorted = sort(resource.node_released_at)
    return
end

function model_step!(model::StandardABM)
    sim::Simulation = model.sim
    # check finished job
    check_finished_job!(sim, model, model.resource)
    # schedule
    run_scheduler!(sim, model, model.resource)

    # ask users to do their staff
    for id in abmscheduler(model)(model)
        # here `agent_step2!` may delete agents, so we check for it manually
        hasid(model, id) || continue
        #agent_step2!(model[id], model)
        # println("model_step!: User: $(model[id].id) $(model[id].pos[1])")
        user_step!(sim, model, model[id])
    end

    # schedule
    run_scheduler!(sim, model, model.resource)

    # model extra step
    isnothing(sim.model_extra_step) == false && sim.model_extra_step(sim, model)
end

function add_model!(sim::Simulation; )
    sim.model = StandardABM(
        User,
        sim.space;
        model_step!, 
        properties = Dict(
            :resource => sim.resource,
            :sim => sim), 
        sim.rng,
        scheduler = Schedulers.Randomly()
    )
end




function run!(sim::Simulation, nsteps::Int64)
    model::StandardABM = sim.model
    # Users Statistics
    adata0 = [
        (:mean_tasks_to_do, u->length(u.tasks_to_do), mean)
        (:mean_tasks_active, u->length(u.tasks_active), mean)
        ]
    # Resource Statistics
    mdata0 = [
        (:used_nodes, m -> sum(m.resource.node_used_by_job .!= 0)),
        (:jobs_in_queue, m -> length(m.resource.queue)),
        (:jobs_running, m -> length(m.resource.executing)),
        (:jobs_done, m -> length(m.resource.history)),
        ]

    sim.adf, sim.mdf = run!(
        model, nsteps; 
        adata=[(v[2],v[3]) for v in adata0], 
        mdata=[v[2] for v in mdata0])

    rename!(sim.adf, [[:time]; [v[1] for v in adata0]])
    rename!(sim.mdf, [[:time]; [v[1] for v in mdata0]])
    sim.adf, sim.mdf
end
