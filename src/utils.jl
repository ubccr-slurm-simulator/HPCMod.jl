using Plots

"""
add_users_and_jobs_from_dataframe for jobs replay (no tasks involved)
"""
function add_users_and_jobs_from_dataframe(sim::SimulationSimple, df::DataFrame)
    # check "submit_time", "user_id", "nodes", "walltime" presence
    ["submit_time", "user_id", "nodes", "walltime"] ⊈ names(df) && error("not all column in DataFrame")

    if "job_id" ∉ names(df)
        df.job_id = 1:nrow(df)
    end

    # Add users and their jobs
    users_list = unique(df[:, :user_id])
    has_job_id = "job_id" in names(df)
    for user_id in users_list
        #println(user_id)
        user = User(
            sim;
            user_id
        )

        m_df = filter(:user_id => ==(user_id), df)
        for row in eachrow(m_df)
            BatchJob(sim, user.inividual_jobs_task;
                nodes=row.nodes, walltime=row.walltime,
                submit_time=row.submit_time,
                job_id=has_job_id ? row.job_id : -1,
                jobs_list=user.inividual_jobs)
        end
    end
end

"""
simple job replay from job_trace (DataFrame should have "submit_time", "user_id", "nodes", "walltime" columns)
"""
function jobs_replay_on_resource(job_trace;
    nodes=10, scheduler_backfill=true,
    workload_done_check_freq=1000,
    rng=Random.Xoshiro(123)
    )
    # Init simulation, seed a random generator
    @debug "jobs_replay_on_resource"
    sim = SimulationSimple(; rng)
    sim.workload_done_check_freq = workload_done_check_freq
    add_resource!(sim; nodes, scheduler_backfill)

    add_users_and_jobs_from_dataframe(sim, job_trace)

    run!(sim; run_till_no_jobs=true)

    sim
end

rectangle(x1, y1, x2, y2) = Shape([(x1,y1),(x1,y2),(x2,y2),(x2,y1),(x1,y1),(x2,y1),(x2,y2),(x1,y2)])

function plot_node_util(
    sim::SimulationSimple;
    ticks_step=1.0,
    annotation_pointsize=12
    )
    node_occupancy_by_task::DataFrame=sim.resource.stats.node_occupancy_by_task
    node_occupancy_by_job::DataFrame=sim.resource.stats.node_occupancy_by_task
    m_by_task = transpose(Matrix{Union{Missing, Int64}}(node_occupancy_by_task[:,2:ncol(node_occupancy_by_task)]))
    m_by_task[m_by_task.==0] .= missing

    m_by_jobs = transpose(Matrix{Union{Missing, Int64}}(node_occupancy_by_job[:,2:ncol(node_occupancy_by_job)]))
    m_by_jobs[m_by_jobs.==0] .= missing

    xaxis = get_datetime.((sim,), node_occupancy_by_task[:,1])
    x = (1:length(xaxis)) ./ sim.timeunits_per_day

    colGRAD = cgrad([colorant"salmon",colorant"moccasin",colorant"lightgreen",colorant"skyblue"])
    p = heatmap(
        x,1:sim.resource.nodes,
        m_by_task,fill=true,c=colGRAD,
        xlabel="Time, Days", xlim=(0,maximum(x)), xticks=0:ticks_step:maximum(x),
        ylabel="Node", ylim=(0.5,sim.resource.nodes+0.5), yticks=1:sim.resource.nodes,
        colorbar=false
    )
    xrange=maximum(x)-minimum(x)
    yrange=sim.resource.nodes
    #contour(m_by_jobs, fill=false, levels=0.5:0.5:maximum(skipmissing(m_by_task))+0.5)
    dt=maximum(x)/(length(x)-1)

    for job in sim.jobs_list
        print(job,"\n")
        sequential_nodes = true
        for i in 2:length(job.nodes_list)
            if job.nodes_list[i]-job.nodes_list[i-1] != 1
                sequential_nodes = false
            end
        end
        #plot!(rectangle2(2,2,0,0), opacity=.5)
        if sequential_nodes
            x1 = job.start_time/sim.timeunits_per_day+dt/2
            x2 = job.end_time/sim.timeunits_per_day+dt/2
            y1 = minimum(job.nodes_list)-0.5
            y2 = maximum(job.nodes_list)+0.5
            w = x2-x1
            h = y2-y1
            plot!(rectangle(x1,y1,x2,y2),
                color="black", legend=false, opacity=1)
            annotate!(
                (x1+x2)*0.5, (y1+y2)*0.5,
                text("Task Id: $(job.task.id)\n Job Id: $(job.id)",
                rotation=h/yrange>w/xrange ? 90 : 0,
                pointsize=annotation_pointsize)
            )
        else
            stop("dont know non sequantial job printing!\n")
        end
    end
    p
end

