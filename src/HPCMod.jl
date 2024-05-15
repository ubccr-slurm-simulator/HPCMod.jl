module HPCMod

export CompTask
export BatchJob
#export Base.isless
export User
export HPCResource
export Simulation

export TaskSplitSchema, UserPreferred, AdaptiveFactor, JobReplay

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
# export is_workload_done
export run!
export get_nodename


include("hpc_user_model.jl")

export add_users_and_jobs_from_dataframe
export jobs_replay_on_resource
include("utils.jl")
end