using Agents
using Random
using DocStringExtensions
using DataFrames: DataFrame

@enum SchedulerType NotScheduled=1 FIFO=2 Backfill=3

"""
Overall single compute job to do

$(TYPEDFIELDS)
"""
mutable struct Task
    "Task id"
    id::Int64
    "user to which this task belong"
    user_id::Int64
    "total nodetime to finish this task"
    nodetime_total::Int64
    "nodetime left to finish this task"
    nodetime_left::Int64
    "Create time"
    create_time::Int64
    "task activation time creation"
    activation_time::Int64
    "last job end time"
    finish_time::Int64
    "current job, 0 if none"
    current_jobs::Int64
    "list of jobs worked on this task"
    jobs::Vector{Int64}
end

"""
a batch job represent a manageble portion of Task

$(TYPEDFIELDS)
"""
mutable struct BatchJob
    "Job id - smaller id does not nessesary guaranee ealier submition"
    id::Int64
    task::Task
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
User
$(TYPEDFIELDS)
"""
@agent struct User(GridAgent{1})
    "max concurrent tasks"
    max_concurrent_tasks::Int64
    "max number of nodes per job, 0 - there is no constrain"
    max_nodes_per_job::Int64
    "max job walltime per job, 0 - there is no constrain"
    max_time_per_job::Int64
    task_split_schema::Int64
    tasks_to_do::Vector{Task}
    tasks_active::Vector{Task}
    tasks_done::Vector{Task}
    "finished jobs for User to process"
    jobs_to_process::Vector{BatchJob}
end

"""
HPCResource - HPC Resource
struct to track resource occupiency and jobs

$(TYPEDFIELDS)
"""
mutable struct HPCResource
    "number of nodes"
    nodes::Int64
    "array with job id using that node"
    node_used_by_job::Vector{Int64}
    "the node will be released at time, -1 if already available"
    node_released_at::Vector{Int64}
    "same as node_released_at but sorted"
    node_released_at_sorted::Vector{Int64}
    "Jobs in queue"
    queue::Vector{BatchJob}
    "Jobs currently running"
    executing::Dict{Int64,BatchJob}
    "Jobs finished"
    history::Vector{BatchJob}
    max_nodes_per_job::Int64
    max_time_per_job::Int64
    scheduler_fifo::Bool
    scheduler_backfill::Bool
end

mutable struct Simulation
    id::Int64
    timeunits_per_day::Int64
    last_task_id::Int64
    task_list::Vector{Task}
    last_job_id::Int64
    jobs_list::Vector{BatchJob}
    last_user_id::Int64
    users_list::Vector{User}
    resource::Union{HPCResource, Nothing}
    space::Union{GridSpace, Nothing}
    model::Union{StandardABM, Nothing}
    rng::AbstractRNG
    adf::Union{DataFrame, Nothing}
    mdf::Union{DataFrame, Nothing}
    """
    Extra step by user: after finishing current jobs and 
    before initiating new good spot to create new tasks or 
    something else
    """
    user_extra_step::Union{Function, Nothing}
    model_extra_step::Union{Function, Nothing}
end