using Random
using Agents
using HPCMod

rng=Random.Xoshiro(123)
sim = Simulation(;rng)

add_resource!(
    sim; 
    nodes=10,
    max_nodes_per_job=4,
    max_time_per_job=24*3,
    scheduler_backfill=true)

add_model!(sim;)

# add four users
for n in 1:2
    user = User(
        sim;
        max_concurrent_tasks=2
        )
    add_agent!(user, sim.model)
    CompTask(sim; user, nodetime=100)
    CompTask(sim; user, nodetime=100)
    CompTask(sim; user, nodetime=100)
    CompTask(sim; user, nodetime=100)
end

run!(sim; run_till_no_jobs=true)


