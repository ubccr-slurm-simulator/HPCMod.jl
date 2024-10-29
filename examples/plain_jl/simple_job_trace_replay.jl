using Random
using Agents
using HPCMod
using Printf
using DataFrames
using DataStructures

print("Simple job-trace_replay")

job_trace = DataFrame([
    2 1 2 2;
    3 2 2 2;
    4 2 3 3;
    5 2 2 2;
    5 1 2 2;
    6 1 1 3;
], [
    "submit_time", "user_id", "nodes", "walltime"
])

println("Executing following jobs:")
println(job_trace)

println("Without backfiller:")
sim = jobs_replay_on_resource(job_trace; nodes=4, scheduler_backfill=false, workload_done_check_freq=1, rng=Random.Xoshiro(123))
println(sim.resource.stats.node_occupancy_by_job)

println("With backfiller:")
sim = jobs_replay_on_resource(job_trace; nodes=4, scheduler_backfill=true, workload_done_check_freq=1, rng=Random.Xoshiro(123))
println(sim.resource.stats.node_occupancy_by_job)

println("The tables above show nodes occupience by job at a given time.")
println("0-means no jobs running on that node, otherwise show job id.")

