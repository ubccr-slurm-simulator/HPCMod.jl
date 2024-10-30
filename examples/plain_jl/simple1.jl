# HPCMod Example
# Simple example with step by step creation of HPC Resource, users and their compute tasks
# Running simulation and results viewing

using Random
using Agents
using HPCMod

# Init simulation, seed a random generator
sim = SimulationSimple(;rng=Random.Xoshiro(123))
sim.workload_done_check_freq = 1
# Add HPC resource
add_resource!(
    sim; 
    nodes=10,
    max_nodes_per_job=6,
    max_time_per_job=24*3,
    scheduler_backfill=true)

# add four users
for user_id in 1:4
    user = User(
        sim;
        max_concurrent_tasks=2
        )
    for m_task_id in 1:1
        CompTask(sim, user.id;
            task_split_schema = user_id==4 ? AdaptiveFactor : UserPreferred,
            submit_time=user_id+m_task_id,
            nodetime=100, nodes_prefered=4, walltime_prefered=48)
    end
end
show(stdout,"text/plain", sim.task_list)
println()

run!(sim; run_till_no_jobs=true);

println(sim.adf[1:10,:])
println(sim.mdf[1:10,:])
println(sim.resource.stats.node_occupancy_by_task[:,:])
