using Random
using Agents
using HPCMod

# Init simulation, seed a random generator
sim = Simulation(;rng=Random.Xoshiro(123))

# Add HPC resource
add_resource!(
    sim; 
    nodes=10,
    max_nodes_per_job=4,
    max_time_per_job=24*3,
    scheduler_backfill=true)

# add four users
for i_user in 1:4
    user = User(
        sim;
        max_concurrent_tasks=2
        )
    for icomptask in 1:4
        CompTask(sim; user, nodetime=100)
    end
end

adf, mdf = run!(sim; run_till_no_jobs=true)

println(adf)
println(mdf)
