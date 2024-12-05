module HPCMod


const Debug1 = Base.CoreLogging.LogLevel(-200)
const Debug2 = Base.CoreLogging.LogLevel(-400)
const Debug3 = Base.CoreLogging.LogLevel(-600)
const Debug4 = Base.CoreLogging.LogLevel(-800)
const Debug5 = Base.CoreLogging.LogLevel(-1000)
macro debug1(exs...) Base.CoreLogging.logmsg_code((Base.CoreLogging.@_sourceinfo)..., :Debug1,  exs...) end
macro debug2(exs...) Base.CoreLogging.logmsg_code((Base.CoreLogging.@_sourceinfo)..., :Debug2,  exs...) end
macro debug3(exs...) Base.CoreLogging.logmsg_code((Base.CoreLogging.@_sourceinfo)..., :Debug3,  exs...) end
macro debug4(exs...) Base.CoreLogging.logmsg_code((Base.CoreLogging.@_sourceinfo)..., :Debug4,  exs...) end
macro debug5(exs...) Base.CoreLogging.logmsg_code((Base.CoreLogging.@_sourceinfo)..., :Debug5,  exs...) end


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
export duration_format

# Slurm like resource
include("hpc_resource_sl_types.jl")


export NodeFeatureId, GRESType, GRESModel, NodeId, PartitionId
export QoSId, AccountId, UserId, JobId, ResourceId

export TIME_INFINITE, GRES_MODEL_ANY

export NodeState, PartitionState, NodeSharing, JobStatus, HPCEventType


export ComputeNodeSL, PartitionSL, QoSSL, AccountSL, UserSL, BatchJobSL, JobOnResourceSL, HPCEventSL, HPCResourceSL, SimulationSL

include("hpc_resource_sl.jl")

export ComputeNodeSL, model_step_sl!, SimulationSL, get_ids_from_str_ids, add_resource!
export add_nodes!, add_partition!, add_qos!, add_account!, add_user!, add_job!, run_model!
export create_model_sl


include("hpc_samples.jl")

end