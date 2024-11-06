using Test

@testset "HPCMod.jl" begin
    include("testset1.jl")

    # Scheduler
    @testset "Scheduler tests " begin
        include("./scheduler/scheduler_test1.jl")
    end

    # HPCResourceSL
    @testset "HPCResourceSL tests " begin
        include("./sl/test_hpc_resource_sl.jl")
    end
end
