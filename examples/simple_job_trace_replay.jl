using Random
using Agents
using HPCMod
using Printf
using DataFrames
using DataStructures

job_trace = DataFrame([
    2 1 2 4;
    4 2 3 4;
    6 2 2 4;
    6 1 2 4;
    7 1 1 4;
], [
    "submit_time", "user_id", "nodes", "walltime"
])

sim = jobs_replay_on_resource(job_trace; nodes=4, scheduler_backfill=false, workload_done_check_freq=1, rng=Random.Xoshiro(123))
show(stdout, "text/plain", Matrix(sim.resource.stats.node_occupancy_by_job))

sim = jobs_replay_on_resource(job_trace; nodes=4, scheduler_backfill=true, workload_done_check_freq=1, rng=Random.Xoshiro(123))
show(stdout, "text/plain", Matrix(sim.resource.stats.node_occupancy_by_job))
