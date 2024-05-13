module HPCMod

export hello_world
function hello_world()
    "Hello world"
end

export CompTask
export BatchJob
#export Base.isless
export User
export HPCResource
export Simulation


include("hpc_user_model_types.jl")


# export CompTask
# export BatchJob
# export User
export add_resource!
# export Simulation
# export task_split_maxnode_maxtime
# export task_split
# export submit_job
# export user_step!
# export place_job!
# export run_scheduler_fifo!
# export run_scheduler_backfill!
# export run_scheduler!
# export check_finished_job!
export model_step!
export add_model!
# export is_workload_done
export run!

include("hpc_user_model.jl")

end