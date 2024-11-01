using HPCMod
using Random
using DataFrames
using Dates

@testset "BatchJobSimple Comparison" begin
    sim = SimulationSimple()
    add_resource!(sim)
    user = User(sim)
    task = user.inividual_jobs_task
    jobs = user.inividual_jobs

    job1 = BatchJobSimple(sim, task; nodes=2, walltime=10, submit_time=12, jobs_list=jobs)
    job2 = BatchJobSimple(sim, task; nodes=2, walltime=10, submit_time=15, jobs_list=jobs)
    job3 = BatchJobSimple(sim, task; nodes=2, walltime=10, submit_time=10, jobs_list=jobs)
    job4 = BatchJobSimple(sim, task; nodes=2, walltime=10, submit_time=14, jobs_list=jobs)
    job5 = BatchJobSimple(sim, task; nodes=2, walltime=10, submit_time=14, jobs_list=jobs)

    # compare by submit_time
    @test job1 < job2
    @test job4 > job3
    @test (job4 > job5) == false
    @test (job4 < job5) == false
    @test (job4 == job5) == false

    # pop test for user's inividual_jobs, sooner jobs pops first
    @test pop!(jobs).id==3
    @test pop!(jobs).id==1
    @test pop!(jobs).id==4
    @test pop!(jobs).id==5
    @test pop!(jobs).id==2

    # jobs are still in sim.jobs_list
    @test length(sim.jobs_list)==5

end


@testset "Examples: simple1 " begin
    include("../examples/plain_jl/simple1.jl")

    # check that it at lest produce something
    @test nrow(sim.mdf) > 50
    @test sum(sim.mdf.used_nodes) > 0
    @test sum(sim.mdf.jobs_in_queue) > 0
    @test sum(sim.mdf.jobs_running) > 0
    @test sum(sim.mdf.jobs_done) > 0
end

@testset "Examples: simple2 " begin
    include("../examples/plain_jl/simple2.jl")

    # check that it at lest produce something
    @test nrow(sim_nobf.mdf)==2001
    @test nrow(sim_bf.mdf)==2001
end

@testset "Examples: simple_comp_tasks " begin
    include("../examples/plain_jl/simple_comp_tasks.jl")

    # check that it at lest produce something
    @test abmtime(sim1.model)==19
end

@testset "DateTime Conversion " begin
    sim = SimulationSimple()
    @test get_datetime(sim, 2)==DateTime(2024,1,1,2,0,0)
    @test get_datetime(sim, 25)==DateTime(2024,1,2,1,0,0)
    @test get_datetime(sim, 24*366+2)==DateTime(2025,1,1,2,0,0)

    @test get_step(sim, DateTime(2024,1,1,2,0,0))==2
    @test get_step(sim, DateTime(2024,1,2,1,0,0))==25
    @test get_step(sim, DateTime(2025,1,1,2,0,0))==24*366+2

    @test get_round_step(sim, DateTime(2024,1,1,2,15,0))==2
    @test get_round_step(sim, DateTime(2024,1,1,1,30,0))==2
    @test get_round_step(sim, DateTime(2024,1,1,1,30,1))==2
    @test get_round_step(sim, DateTime(2024,1,2,1,10,0))==25
    @test get_round_step(sim, DateTime(2025,1,1,2,10,0))==24*366+2
end
