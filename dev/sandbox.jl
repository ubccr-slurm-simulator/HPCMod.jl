using Random
using Agents
using HPCMod
using Dates


# Init simulation, seed a random generator
sim = Simulation(;rng=Random.Xoshiro(123))



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

