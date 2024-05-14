using Random
using Agents
using HPCMod
using Printf
using DataFrames
using DataStructures


sim = jobs_replay_on_resource(DataFrame([
                4 1 3 4;
                7 1 2 4;
                2 2 2 4;
                6 1 2 4;
                6 1 2 4], [
                "submit_time", "user_id", "nodes", "walltime"
            ]), ; nodes=10, scheduler_backfill=false, workload_done_check_freq=1);
show(stdout, "text/plain", Matrix(sim.resource.stats.node_occupancy_by_job))  

@test Matrix(sim.resource.stats.node_occupancy_by_job) == ref_5jobs_1user_unordered

sim = jobs_replay_on_resource(DataFrame([
            102 4 101 3 4;
            106 7 101 1 4;
            101 2 101 2 4;
            103 6 101 2 4;
            104 6 101 2 4], [
            "job_id", "submit_time", "user_id", "nodes", "walltime"
        ]), ; nodes=4, scheduler_backfill=true, workload_done_check_freq=1);
show(stdout, "text/plain", Matrix(sim.resource.stats.node_occupancy_by_job))