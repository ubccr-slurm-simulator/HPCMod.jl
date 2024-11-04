module HPCMod

export CompTask
export BatchJobSimple
#export Base.isless
export UserSimple
export HPCResourceSimple
export SimulationSimple

export TaskSplitSchema, UserPreferred, AdaptiveFactor, JobReplay

include("hpc_user_model_types.jl")


# export CompTask
# export BatchJobSimple
# export UserSimple
export add_resource!
# export SimulationSimple
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

export get_datetime
export get_step
export get_round_step

export generate_thinktime_zero
export generate_thinktime_gamma

include("hpc_user_model.jl")

export add_users_and_jobs_from_dataframe
export jobs_replay_on_resource
export plot_node_util
include("utils.jl")

#export add_nodes
include("hpc_resource.jl")

end