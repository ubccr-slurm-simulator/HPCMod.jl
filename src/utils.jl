"""
add_users_and_jobs_from_dataframe for jobs replay (no tasks involved)
"""
function add_users_and_jobs_from_dataframe(sim::Simulation, df::DataFrame)
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
    sim = Simulation(; rng)
    sim.workload_done_check_freq = workload_done_check_freq
    add_resource!(sim; nodes, scheduler_backfill)

    add_users_and_jobs_from_dataframe(sim, job_trace)

    run!(sim; run_till_no_jobs=true)

    sim
end

