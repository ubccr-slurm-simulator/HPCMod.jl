using Test

@testset "HPCMod.jl" begin
    include("testset1.jl")

    # Scheduler
    @testset "Scheduler tests " begin
        include("./scheduler/scheduler_test1.jl")
    end
end
