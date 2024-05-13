using Pkg
Pkg.activate(".")
# Pkg.add("Agents")
# Pkg.add("DocStringExtensions")
# Pkg.add("DataFrames")
# Pkg.add("Tidiers")

using HPCMod

function run_sim(;scheduler_backfill=true, rng=Random.Xoshiro(123))
    sim = Simulation(;rng)

    add_resource!(
        sim; 
        nodes=10,
        max_nodes_per_job=4,
        max_time_per_job=24*3,
        scheduler_backfill)

    add_model!(sim;)

    for n in 1:18
        user = User(
            sim;
            max_concurrent_tasks=2
            )
        add_agent!(user, sim.model)
        Task(sim; user, nodetime=100)
        Task(sim; user, nodetime=100)
        Task(sim; user, nodetime=100)
        Task(sim; user, nodetime=100)
    end

    for n in 1:2
        user = User(
            sim;
            max_concurrent_tasks=2,
            max_nodes_per_job=2
            )
        add_agent!(user, sim.model)
        Task(sim; user, nodetime=100)
        Task(sim; user, nodetime=100)
        Task(sim; user, nodetime=100)
        Task(sim; user, nodetime=100)
    end

    # user = User(
    #         sim;
    #         max_concurrent_tasks=2,
    #         max_nodes_per_job=2
    #         )
    # add_agent!(user, sim.model)

    # push!(user.inividual_jobs, BatchJob(sim, user.inividual_jobs_task, 10, 72; submit_time=24))
    # push!(user.inividual_jobs, BatchJob(sim, user.inividual_jobs_task, 1, 72; submit_time=48))

    run!(sim, 2000)

    timeunits_per_day = sim.timeunits_per_day
    mdf = @chain sim.mdf begin
        @filter(used_nodes > 0)
        @summarise begin
            mean_used_nodes = mean(used_nodes)
            sd_used_nodes = std(used_nodes)
            max_time = maximum(time)
        end
        @mutate max_days = max_time / !!timeunits_per_day 
    end
    println(mdf)
    return sim
end

sim1 = run_sim(;scheduler_backfill=true);

# @chain sim1.mdf begin
#         @filter(used_nodes > 0)
# end

sim2 = run_sim(;scheduler_backfill=false);

