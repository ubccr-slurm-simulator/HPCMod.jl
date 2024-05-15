using HPCMod
using Random
using DataFrames

@testset "BatchJob Comparison" begin
    sim = Simulation()
    add_resource!(sim)
    user = User(sim)
    task = user.inividual_jobs_task
    jobs = user.inividual_jobs

    job1 = BatchJob(sim, task; nodes=2, walltime=10, submit_time=12, jobs_list=jobs)
    job2 = BatchJob(sim, task; nodes=2, walltime=10, submit_time=15, jobs_list=jobs)
    job3 = BatchJob(sim, task; nodes=2, walltime=10, submit_time=10, jobs_list=jobs)
    job4 = BatchJob(sim, task; nodes=2, walltime=10, submit_time=14, jobs_list=jobs)
    job5 = BatchJob(sim, task; nodes=2, walltime=10, submit_time=14, jobs_list=jobs)

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
    include("../examples/simple1.jl")

    # check that it at lest produce something
    @test nrow(sim.mdf) > 50
    @test sum(sim.mdf.used_nodes) > 0
    @test sum(sim.mdf.jobs_in_queue) > 0
    @test sum(sim.mdf.jobs_running) > 0
    @test sum(sim.mdf.jobs_done) > 0
end
