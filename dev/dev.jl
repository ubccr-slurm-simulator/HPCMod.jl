using Pkg
#Pkg.status()

#using Revise
using HPCMod

# run dev test
include("../examples/simple1.jl")
include("../examples/simple_job_trace_replay.jl")
include("../examples/simple_comp_tasks.jl")

# run tests manually
include("../test/runtests.jl")

# run tests
Pkg.test()


# Start Pluto Notebook
import Pkg
Pkg.add("Pluto")
import Pluto
Pluto.run()



Randomly() = Randomly(Int[])
function (sched::Randomly)(model::ABM)
    get_ids!(sched.ids, model)
    shuffle!(abmrng(model), sched.ids)
end
