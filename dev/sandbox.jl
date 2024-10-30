using Random
using Agents
using HPCMod
using Dates
using Distributions

# rename C:\Users\ns\.julia\juliaup\julia-1.10.2+0.x64.w64.mingw32\bin\libLLVM-15jl.dll to libLLVM.dll
using Plots

# Init simulation, seed a random generator
sim = SimulationSimple(;rng=Random.Xoshiro(123))

function generate_think_time(sim::SimulationSimple, user::User)
    shape=0.23743230	
    scale = 1.0/0.05508324	
    gamma = Gamma(shape, scale)
    round(Int64, rand(sim.rng, gamma))
end

shape=0.23743230	
scale = 1.0/0.05508324	

gamma = Gamma(shape, scale)


thinktime = round.((Int64,),rand(sim.rng, gamma, 4000))

histogram(thinktime, bar_width=1)

