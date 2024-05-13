using HPCMod
using Test

@testset "HPCMod.jl" begin
    @test HPCMod.hello_world() == "Hello world"
end
