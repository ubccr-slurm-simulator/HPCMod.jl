# HPCMod Example
# Check the backfiller effect
# 

using Agents
using DataFrames
using Tidier
using Random
using HPCMod

function run_sim(;scheduler_backfill=true, rng=Random.Xoshiro(123))
    sim = Simulation(;rng)

    add_resource!(
        sim; 
        nodes=10,
        max_nodes_per_job=4,
        max_time_per_job=24*3,
        scheduler_backfill)

    for n in 1:18
        user = User(
            sim;
            max_concurrent_tasks=2
            )
            CompTask(sim, user.id; nodetime=100, nodes_prefered=4, walltime_prefered=50)
            CompTask(sim, user.id; nodetime=100, nodes_prefered=4, walltime_prefered=50)
            CompTask(sim, user.id; nodetime=100, nodes_prefered=4, walltime_prefered=50)
            CompTask(sim, user.id; nodetime=100, nodes_prefered=4, walltime_prefered=50)
    end

    for n in 1:2
        user = User(
            sim;
            max_concurrent_tasks=2
            )
            CompTask(sim, user.id; nodetime=100, nodes_prefered=2, walltime_prefered=50)
            CompTask(sim, user.id; nodetime=100, nodes_prefered=2, walltime_prefered=50)
            CompTask(sim, user.id; nodetime=100, nodes_prefered=2, walltime_prefered=50)
            CompTask(sim, user.id; nodetime=100, nodes_prefered=2, walltime_prefered=50)
    end

    run!(sim; nsteps=2000)

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

sim_bf = run_sim(;scheduler_backfill=true);
sim_nobf = run_sim(;scheduler_backfill=false);

