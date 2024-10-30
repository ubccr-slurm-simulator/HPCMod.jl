using Pkg
using HPCMod
using Logging
using DataFrames
using Agents
using Random

ENV["JULIA_DEBUG"] = "all"
@info "ref_5jobs_1user_unordered START"
@debug "ref_5jobs_1user_unordered (debug)"

ref_5jobs_1user_unordered = [
    # t N0001  N0002  N0003  N0004  N0005  N0006  N0007  N0008  N0009  N0010
    0 0 0 0 0 0 0 0 0 0 0;
    1 0 0 0 0 0 0 0 0 0 0;
    2 3 3 0 0 0 0 0 0 0 0;
    3 3 3 0 0 0 0 0 0 0 0;
    4 3 3 1 1 1 0 0 0 0 0;
    5 3 3 1 1 1 0 0 0 0 0;
    6 4 4 1 1 1 5 5 0 0 0;
    7 4 4 1 1 1 5 5 2 2 0;
    8 4 4 0 0 0 5 5 2 2 0;
    9 4 4 0 0 0 5 5 2 2 0;
    10 0 0 0 0 0 0 0 2 2 0;
    11 0 0 0 0 0 0 0 0 0 0
]


job_trace = DataFrame([ # changing users should not have effect
4 101 3 4;
7 101 2 4;
2 102 2 4;
6 101 2 4;
6 102 2 4], [
    "submit_time", "user_id", "nodes", "walltime"
])

sim = jobs_replay_on_resource(job_trace; nodes=10, scheduler_backfill=false, workload_done_check_freq=1);


if Matrix(sim.resource.stats.node_occupancy_by_job) != ref_5jobs_1user_unordered
    @error "Missmatch in test $(i)"
    @error sim.resource.stats.node_occupancy_by_job
end

#@test Matrix(sim.resource.stats.node_occupancy_by_job) == ref_5jobs_1user_unordered
#end
#Logging.disable_logging(init_log_level)
@info "ref_5jobs_1user_unordered END"
delete!(ENV, "JULIA_DEBUG")


ids = Vector{Int}()

Schedulers.get_ids!(ids, sim.model)
ids

# User scheduler

users_scheduler = Schedulers.Randomly()

@which shuffle!

users_scheduler()
methods(abmscheduler(sim.model))


pkgs = Pkg.installed();
pkgs["Random"]
