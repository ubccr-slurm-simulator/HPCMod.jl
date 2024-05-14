#using Pkg
#Pkg.status()

#using Revise
using HPCMod

# run dev test
include("../examples/simple1.jl")
include("../examples/simple_job_trace_replay.jl")

# run tests manually
include("../test/runtests.jl")

# run tests
Pkg.test()
