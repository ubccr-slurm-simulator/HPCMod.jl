using Random
using Agents
using HPCMod
using Printf
using DataFrames
using DataStructures
using Logging

rng_glb = copy(Random.default_rng());
@info "Random.default_rng Next Int $(rand(rng_glb,Int64))"
    

# Replay jobs and compare node occupiency by job to refference
# the refference was manually checked for having sense

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

ref_5jobs_on_4nodes = [
    0 0 0 0 0;
    1 0 0 0 0;
    2 101 101 0 0;
    3 101 101 0 0;
    4 101 101 0 0;
    5 101 101 0 0;
    6 102 102 102 0;
    7 102 102 102 0;
    8 102 102 102 0;
    9 102 102 102 0;
    10 103 103 104 104;
    11 103 103 104 104;
    12 103 103 104 104;
    13 103 103 104 104;
    14 106 0 0 0;
    15 106 0 0 0;
    16 106 0 0 0;
    17 106 0 0 0;
    18 0 0 0 0]

ref_5jobs_on_4nodes_with_backfiller = [
    0    0    0    0    0;
    1    0    0    0    0;
    2  101  101    0    0;
    3  101  101    0    0;
    4  101  101    0    0;
    5  101  101    0    0;
    6  102  102  102    0;
    7  102  102  102  106;
    8  102  102  102  106;
    9  102  102  102  106;
   10  103  103    0  106;
   11  103  103  104  104;
   12  103  103  104  104;
   13  103  103  104  104;
   14    0    0  104  104;
   15    0    0    0    0;]

@testset "5 jobs by 1 user" begin
    # Init simulation, seed a random generator
    sim = SimulationSimple(; rng=Random.Xoshiro(123))
    sim.workload_done_check_freq = 1
    # Add HPC resource
    add_resource!(sim; scheduler_backfill=false)
    # add users
    user = User(sim)
    task = user.inividual_jobs_task
    jobs = user.inividual_jobs

    # add jobs
    job1 = BatchJob(sim, task; nodes=3, walltime=4, submit_time=4, jobs_list=jobs)
    job2 = BatchJob(sim, task; nodes=2, walltime=4, submit_time=7, jobs_list=jobs)
    job3 = BatchJob(sim, task; nodes=2, walltime=4, submit_time=2, jobs_list=jobs)
    job4 = BatchJob(sim, task; nodes=2, walltime=4, submit_time=6, jobs_list=jobs)
    job5 = BatchJob(sim, task; nodes=2, walltime=4, submit_time=6, jobs_list=jobs)

    adf, mdf = run!(sim; run_till_no_jobs=true)

    @test Matrix(sim.resource.stats.node_occupancy_by_job) == ref_5jobs_1user_unordered
end

@testset "jobs_replay_on_resource" begin
    # 5 jobs 10 nodes 1 user
    # execution order by job id 3,1,4,5,2
    sim = jobs_replay_on_resource(DataFrame([
                4 1 3 4;
                7 1 2 4;
                2 1 2 4;
                6 1 2 4;
                6 1 2 4], [
                "submit_time", "user_id", "nodes", "walltime"
            ]); nodes=10, scheduler_backfill=false, workload_done_check_freq=1)
    @test Matrix(sim.resource.stats.node_occupancy_by_job) == ref_5jobs_1user_unordered

    job_traces_list = [
        [
            4 1 3 4;
            7 1 2 4;
            2 1 2 4;
            6 1 2 4;
            6 1 2 4
        ], [ # changing users should not have effect
            4 1 3 4;
            7 1 2 4;
            2 2 2 4;
            6 2 2 4;
            6 2 2 4
        ], [ # changing users should not have effect
            4 1 3 4;
            7 1 2 4;
            2 2 2 4;
            6 1 2 4;
            6 1 2 4
        ], [ # changing users should not have effect
            4 3 3 4;
            7 1 2 4;
            2 2 2 4;
            6 2 2 4;
            6 2 2 4
        ], [ # changing users should not have effect
            4 100 3 4;
            7 100 2 4;
            2 100 2 4;
            6 100 2 4;
            6 100 2 4
        ], [ # changing users should not have effect
            4 101 3 4;
            7 101 2 4;
            2 102 2 4;
            6 101 2 4;
            6 101 2 4
        ], [ # changing users should not have effect
            4 101 3 4;
            7 101 2 4;
            2 102 2 4;
            6 101 2 4;
            6 102 2 4]
    ]

    for (i, job_trace) in enumerate(job_traces_list)
        sim = jobs_replay_on_resource(DataFrame(job_trace, [
                "submit_time", "user_id", "nodes", "walltime"
            ]); nodes=10, scheduler_backfill=false, workload_done_check_freq=1)
        if Matrix(sim.resource.stats.node_occupancy_by_job) != ref_5jobs_1user_unordered
            @error "Missmatch in test $(i)"
            @error sim.resource.stats.node_occupancy_by_job
        end
        @test Matrix(sim.resource.stats.node_occupancy_by_job) == ref_5jobs_1user_unordered
    end

    df = DataFrame([
        102 4 101 3 4;
        106 7 101 1 4;
        101 2 101 2 4;
        103 6 101 2 4;
        104 6 101 2 4], [
        "job_id", "submit_time", "user_id", "nodes", "walltime"
    ])
    sim = jobs_replay_on_resource(df; nodes=4, scheduler_backfill=false, workload_done_check_freq=1)
    @test Matrix(sim.resource.stats.node_occupancy_by_job) == ref_5jobs_on_4nodes

    sim = jobs_replay_on_resource(df; nodes=4, scheduler_backfill=true, workload_done_check_freq=1)
    @test Matrix(sim.resource.stats.node_occupancy_by_job) == ref_5jobs_on_4nodes_with_backfiller
end

