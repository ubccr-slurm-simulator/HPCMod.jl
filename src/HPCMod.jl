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

# Slurm like resource
include("hpc_resource_sl_types.jl")


export NodeFeatureId, GRESType, GRESModel, NodeId, PartitionId
export QoSId, AccountId, UserId, JobId, ResourceId

export TIME_INFINITE, GRES_MODEL_ANY

export NodeState, PartitionState, NodeSharing, HPCEventType


export ComputeNodeSL, PartitionSL, QoSSL, AccountSL, UserSL, BatchJobSL, JobOnResourceSL, HPCEventSL, HPCResourceSL, SimulationSL

include("hpc_resource_sl.jl")

export ComputeNodeSL, model_step_sl!, SimulationSL, get_ids_from_str_ids, add_resource!
export add_nodes!, add_partition!, add_qos!, add_account!, add_user!, add_job!, run!

end