using Agents
using Random
using DocStringExtensions
using DataFrames: DataFrame
using DataStructures
using Dates

@enum SchedulerType NotScheduled = 1 FIFO = 2 Backfill = 3
@enum TaskSplitSchema UserPreferred = 1 AdaptiveFactor = 2 JobReplay = 3

"""
Overall single compute job to do

$(TYPEDFIELDS)
"""
mutable struct CompTask
    "CompTask id"
    id::Int64
    "user to which this task belong"
    user_id::Int64
    "total nodetime to finish this task, if -1 then no limit"
    nodetime_total::Int64
    "nodetime left to finish this task"
    nodetime_left::Int64
    "nodetime left to finish this task including submitted jobs"
    nodetime_left_unplanned::Int64
    "nodetime completed so far"
    nodetime_done::Int64
    "Task submit time, the user can start working on it after that time"
    submit_time::Int64
    "Task start time"
    start_time::Int64
    "last job end time"
    end_time::Int64
    "Strategy for task splitting"
    task_split_schema::TaskSplitSchema
    "max concurrent jobs"
    max_concurrent_jobs::Int64
    "prefered number of nodes"
    nodes_prefered::Int64
    "prefered walltime"
    walltime_prefered::Int64
    "current job, 0 if none"
    current_jobs::Vector{Int64}
    "list of jobs worked on this task"
    jobs::Vector{Int64}
    "Next check time by user, for example send next batch job"
    next_check_time::Int64
end

"""
Comparison in sense of which were submitted earlier
"""
Base.isless(t1::CompTask, t2::CompTask) = t1.submit_time < t2.submit_time

"""
a batch job represent a manageble portion of CompTask

$(TYPEDFIELDS)
"""
mutable struct BatchJobSimple
    "Job id - smaller id does not nessesary guaranee ealier submition"
    id::Int64
    task::CompTask
    nodes::Int64
    walltime::Int64
    submit_time::Int64
    start_time::Int64
    end_time::Int64
    scheduled_by::SchedulerType
    "Vector with node ids"
    nodes_list::Vector{Int64}
end

"""
Comparison in sense of which were submitted earlier
"""
Base.isless(j1::BatchJobSimple, j2::BatchJobSimple) = j1.submit_time < j2.submit_time


"""
User
$(TYPEDFIELDS)
"""
@agent struct User(GridAgent{1})
    "max concurrent tasks"
    max_concurrent_tasks::Int64
    "max number of nodes per job, -1 - there is no constrain"
    max_nodes_per_job::Int64
    "max job walltime per job, -1 - there is no constrain"
    max_time_per_job::Int64
    tasks_to_do::SortedSet{CompTask}
    tasks_active::Vector{CompTask}
    tasks_done::Vector{CompTask}
    "finished jobs for User to process"
    jobs_to_process::Vector{BatchJobSimple}
    "CompTask for inidividual jobs"
    inividual_jobs_task::CompTask
    "jobs which are not bind to task"
    inividual_jobs::SortedSet{BatchJobSimple}
    thinktime_generator::Function
end

"""
structure to store various resource usage stats
"""
mutable struct HPCResourceStats
    calc_freq::Int64
    node_occupancy_by_user::DataFrame
    node_occupancy_by_job::DataFrame
    node_occupancy_by_task::DataFrame
end



"""
HPCResourceSimple - HPC Resource
struct to track resource occupiency and jobs

$(TYPEDFIELDS)
"""
mutable struct HPCResourceSimple
    "number of nodes"
    nodes::Int64
    "array with job id using that node"
    node_used_by_job::Vector{Int64}
    "the node will be released at time, -1 if already available"
    node_released_at::Vector{Int64}
    "same as node_released_at but sorted"
    node_released_at_sorted::Vector{Int64}
    "Jobs in queue"
    queue::Vector{BatchJobSimple}
    "Jobs currently running"
    executing::Dict{Int64,BatchJobSimple}
    "Jobs finished"
    history::Vector{BatchJobSimple}
    max_nodes_per_job::Int64
    max_time_per_job::Int64
    scheduler_fifo::Bool
    scheduler_backfill::Bool
    stats::HPCResourceStats
end


mutable struct SimulationSimple
    id::Int64
    timeunits_per_day::Int64
    timeunit::Period
    cur_datetime::DateTime
    init_datetime::DateTime
    last_task_id::Int64
    task_list::Vector{CompTask}
    task_dict::Dict{Int64,CompTask}
    last_job_id::Int64
    jobs_list::Vector{BatchJobSimple}
    jobs_dict::Dict{Int64,BatchJobSimple}
    last_user_id::Int64
    users_list::Vector{User}
    users_dict::Dict{Int64,User}
    resource::Union{HPCResourceSimple,Nothing}
    space::Union{GridSpace,Nothing}
    model::Union{StandardABM,Nothing}
    rng::AbstractRNG
    adf::Union{DataFrame,Nothing}
    mdf::Union{DataFrame,Nothing}
    """
    Extra step by user: after finishing current jobs and 
    before initiating new good spot to create new tasks or 
    something else
    """
    user_extra_step::Union{Function,Nothing}
    """
    executed at the end of model_step!
    """
    model_extra_step::Union{Function,Nothing}
    workload_done_check_freq::Int64
end




