#using Pkg
#Pkg.status()

#using Revise
using HPCMod

# run dev test
include("../examples/simple1.jl")

# run tests manually
include("../test/runtests.jl")

# run tests
Pkg.test()
