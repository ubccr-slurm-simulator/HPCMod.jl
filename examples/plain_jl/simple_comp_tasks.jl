# HPCMod Example
# 

using Random
using Agents
using HPCMod

################################################################################
# All users with UserPreferred task split schema
################################################################################
# Init simulation, seed a random generator
sim1 = SimulationSimple(;rng=Random.Xoshiro(123))
sim1.workload_done_check_freq = 1
# Add HPC resource
add_resource!(
    sim1; 
    nodes=10,
    max_nodes_per_job=6,
    max_time_per_job=24,
    scheduler_backfill=true)

# add four users, last is using AdaptiveFactor
for user_id in 1:4
    user = UserSimple(
        sim1;
        max_concurrent_tasks=2
        )
    for m_task_id in 1:1
        CompTask(sim1, user.id;
            task_split_schema = UserPreferred,
            submit_time=user_id+m_task_id,
            nodetime=30, nodes_prefered=4, walltime_prefered=12)
    end
end
show(stdout,"text/plain", sim1.task_list)
println()

run!(sim1; run_till_no_jobs=true);

println(sim1.resource.stats.node_occupancy_by_task[:,:])

################################################################################
# Last user with AdaptiveFactor task split schema
################################################################################
# Init simulation, seed a random generator
sim1 = SimulationSimple(;rng=Random.Xoshiro(123))
sim1.workload_done_check_freq = 1
# Add HPC resource
add_resource!(
    sim1; 
    nodes=10,
    max_nodes_per_job=6,
    max_time_per_job=24,
    scheduler_backfill=true)

# add four users, last is using AdaptiveFactor
for user_id in 1:4
    user = UserSimple(
        sim1;
        max_concurrent_tasks=2
        )
    for m_task_id in 1:1
        CompTask(sim1, user.id;
            task_split_schema = user_id==4 ? AdaptiveFactor : UserPreferred,
            submit_time=user_id+m_task_id,
            nodetime=30, nodes_prefered=4, walltime_prefered=12)
    end
end
show(stdout,"text/plain", sim1.task_list)
println()

run!(sim1; run_till_no_jobs=true);

println(sim1.resource.stats.node_occupancy_by_task[:,:])
