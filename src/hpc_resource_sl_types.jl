using Logging
using DataFrames
using Agents
using Random
using Printf
using PrettyPrint
using Dates
using DocStringExtensions

const NodeFeatureId = Int
const ARESType = Int
const ARESModel = Int
const NodeId = Int
const PartitionId = Int
const QoSId = Int
const AccountId = Int
const UserAccountId = Int
const UserId = Int
const JobId = Int
const ResourceId = Int

const TIME_INFINITE::Int = 100*365*24*3600
const GRES_MODEL_ANY::ARESModel = 0

const NO_NEXT_EVENT::Int = 0
const NO_USER_ACCOUNT::UserAccountId = 0
const DATETIME_UNSET::DateTime=DateTime(0,1,1,0,0,0)
# this unset to large date for proper sorting
const DATETIME_UNSET_L::DateTime=DateTime(9999,1,1,0,0,0)
const NOT_USED_BY_JOB::JobId = 0

"
NODE_STATE_UNKNOWN,	/* node's initial state, unknown */
NODE_STATE_DOWN,	/* node in non-usable state */
NODE_STATE_IDLE,	/* node idle and available for use */
NODE_STATE_ALLOCATED,	/* node has been allocated to a job */
NODE_STATE_ERROR,	/* UNUSED - node is in an error state */
NODE_STATE_MIXED,	/* node has a mixed state */
NODE_STATE_FUTURE,	/* node slot reserved for future use */
NODE_STATE_END		/* last entry in table */ 
"
@enum NodeState NODE_STATE_DOWN NODE_STATE_IDLE

"
/* Actual partition states based upon state flags */
#define PARTITION_DOWN		(PARTITION_SUBMIT)
#define PARTITION_UP		(PARTITION_SUBMIT | PARTITION_SCHED)
#define PARTITION_DRAIN		(PARTITION_SCHED)
#define PARTITION_INACTIVE	0x00
"
@enum PartitionState PARTITION_DOWN PARTITION_UP


# 2 if the job can only share nodes with other
#     *   jobs owned by that user,
#     * 1 if job can share nodes with other jobs,
#     * 0 if job needs exclusive access to the node,
#     * or NO_VAL to accept the system default.
#     * SHARED_FORCE to eliminate user control. "
@enum NodeSharing NODE_SHARING_UNSET NODE_SHARING_OFF NODE_SHARING_SAMEUSER NODE_SHARING_ON

@enum JobStatus JOBSTATUS_UNKNOWN JOBSTATUS_INQUEUE JOBSTATUS_RUNNING JOBSTATUS_DONE JOBSTATUS_CANCELED
@enum HPCEventType SUBMIT_JOB

"
Used resources by job
"
#mutable struct JobOnNode

# node_info_t?
# Procs->CPUs?
"""
largely adopted from Slurm's node_info_t
"""
mutable struct ComputeNodeSL
    id::Int
    # char *arch;		" computer architecture "
	# char *bcast_address;	" BcastAddr (optional) "
	# uint16_t boards;        " total number of boards per node  "
	# time_t boot_time;	" time of node boot "
	# char *cluster_name;	" Cluster name ONLY set in federation "

	" number of cores per socket (node_info_t.cores)"
    cores_per_socket::Int
	# uint16_t core_spec_cnt; " number of specialized cores on node "
	# uint32_t cpu_bind;	" Default task binding "
	# uint32_t cpu_load;	" CPU load * 100 "

	" configured count of cpus running on the node "
    cpus::Int
	# uint16_t cpus_efctv;	" count of effective cpus on the node. i.e cpus minus specialized cpus"
	# char *cpu_spec_list;	" node's specialized cpus "
	# acct_gather_energy_t *energy;	 " energy data "
	# ext_sensors_data_t *ext_sensors; " external sensor data "
	# char *extra;		" arbitrary sting "
	# power_mgmt_data_t *power;        " power management data "
	" list of a node's available features "
    features::Vector{NodeFeatureId}
	# char *features_act;	" list of a node's current active features,
	# 			 * Same as "features" if NULL "
	" list of a node's generic resources types"
    gres::Vector{ARESType}
    " list of a node's generic resources models "
    gres_model::Vector{ARESModel}
	# char *gres_drain;	" list of drained GRES "
	# time_t last_busy;	" time node was last busy (i.e. no jobs) "
	# char *mcs_label;	" mcs label if mcs plugin in use "
	# uint64_t mem_spec_limit; " MB memory limit for specialization "
	" node name to slurm "
    name::String
	# uint32_t next_state;	" state after reboot (enum node_states) "
	# char *node_addr;	" communication name (optional) "
	# char *node_hostname;	" node's hostname (optional) "
	# uint32_t ;	" see enum node_states "
    node_state::NodeState
	# char *os;		" operating system currently running "
	# uint32_t owner;		" User allowed to use this node or NO_VAL "
	# char *partitions;	" Comma separated list of partitions containing
	# 			 * this node, NOT supplied by slurmctld, but
	# 			 * populated by scontrol "
	# uint16_t port;		" TCP port number of the slurmd "
	" configured MB of real memory on the node "
    memory::Int
	# char *comment;		" arbitrary comment "
	# char *reason;		" reason for node being DOWN or DRAINING "
	# time_t reason_time;	" Time stamp when reason was set, ignore if
	# 			 * no reason is set. "
	# uint32_t reason_uid;   	" User that set the reason, ignore if
	# 			 * no reason is set. "
	# time_t resume_after;    " automatically resume DOWN or DRAINED node at
	# 		         * this point in time "
	# char *resv_name;        " If node is in a reservation this is
	# 			 * the name of the reservation "
	# dynamic_plugin_data_t *select_nodeinfo;  " opaque data structure,
	# 					  * use
	# 					  * slurm_get_select_nodeinfo()
	# 					  * to access contents "
	# time_t slurmd_start_time;" time of slurmd startup "
	" total number of sockets per node "
    sockets::Int
	#" number of threads per core (node_info_t.threads_per_core)"
    #threads_per_core::Int
	# uint32_t tmp_disk;	" configured MB of total disk in TMP_FS "
	# uint32_t weight;	" arbitrary priority of node for scheduling "
	# char *tres_fmt_str;	" str representing configured TRES on node "
	# char *version;		 " Slurm version number "
    #function ComputeNodeSL(;sockets=,threads=) 

    "Max number of jobs on node, typically equal to cpus"
    job_slots::Int
    "Job on Node, we can have up to"
    jobs_on_node::Vector{JobId}
    jobs_release_time::Vector{DateTime}
    "Order in which job-slots become available, starting from currently available"
    job_slots_avail_order::Vector{Int}
    " free CPUs"
    cpus_free::Int
    cpu_used::Vector{JobId}
    cpu_used_by_job_slots::Vector{Int}
    " free memory in MiB "
    memory_free::Int
    " cpu slots"
    memory_used::Vector{Int}



    " list of GRES in current use => Vector of GRES use status"
    gres_used::Vector{JobId}
    gres_release_time::Vector{DateTime}
    gres_avail_order::Vector{Int}

    ares_type::Vector{ARESType}
    ares_model::Vector{ARESModel}
    ares_total::Vector{Int}
    ares_used::Vector{Int}
    ares_free::Vector{Int}
    #    "work array"
#    gres_used_wa::Vector{Bool}
end


"
partition_info_t
"
mutable struct PartitionSL
    # char *allow_alloc_nodes;" list names of allowed allocating
    #     * nodes "
    # char *allow_accounts;   " comma delimited list of accounts,
    #     * null indicates all "
    # char *allow_groups;	" comma delimited list of groups,
    #     * null indicates all "
    # char *allow_qos;	" comma delimited list of qos,
    #     * null indicates all "
    # char *alternate; 	" name of alternate partition "
    # char *billing_weights_str;" per TRES billing weights string "
    # char *cluster_name;	" Cluster name ONLY set in federation "
    # uint16_t cr_type;	" see CR_* values "
    # uint32_t cpu_bind;	" Default task binding "
    " default MB memory per allocated CPU "
    def_mem_per_cpu::Int64
    # uint32_t default_time;	" minutes, NO_VAL or INFINITE "
    # char *deny_accounts;    " comma delimited list of denied accounts "
    # char *deny_qos;		" comma delimited list of denied qos "
    # uint16_t flags;		" see PART_FLAG_* above "
    # uint32_t grace_time; 	" preemption grace time in seconds "
    # list_t *job_defaults_list; " List of job_defaults_t elements "
    # char *job_defaults_str;	" String of job defaults,
    #     * used only for partition update RPC "
    # uint32_t max_cpus_per_node; " maximum allocated CPUs per node "
    # uint32_t max_cpus_per_socket; " maximum allocated CPUs per socket "
    # uint64_t max_mem_per_cpu; " maximum MB memory per allocated CPU "
    # uint32_t max_nodes;	" per job or INFINITE "
    # uint16_t max_share;	" number of jobs to gang schedule "
    "Max Time, seconds (uint32_t max_time; minutes in Slurm or INFINITE)"
    max_time::Int64
    # uint32_t min_nodes;	" per job "
    " name of the partition "
    name::String
    # int32_t *node_inx;	" list index pairs into node_table:
    #     * start_range_1, end_range_1,
    #     * start_range_2, .., -1  "
    # char *nodes;		" list names of nodes in partition "
    # char *nodesets;		" list of nodesets used by partition "
    "Nodes in the partition"
    node_ids::Vector{NodeId}
    # uint16_t over_time_limit; " job's time limit can be exceeded by this
    #       * number of minutes before cancellation "
    # uint16_t preempt_mode;	" See PREEMPT_MODE_* in slurm/slurm.h "
    " job priority weight factor (uint16_t priority_job_factor;)"
    priority_job_factor::Int
    # " tier for scheduling and preemption (uint16_t priority_tier;)"
    # priority_tier::Int
    # char *qos_char;	        " The partition QOS name "
    # uint16_t resume_timeout; " time required in order to perform a node
    #      * resume operation "
    # uint16_t state_up;	" see PARTITION_ states above "
    "Partition State"
    state::PartitionState
    # uint32_t suspend_time;  " node idle for this long before power save
    #     * mode "
    # uint16_t suspend_timeout; " time required in order to perform a node
    #       * suspend operation "
    # uint32_t total_cpus;	" total number of cpus in the partition "
    # uint32_t total_nodes;	" total number of nodes in the partition "
    # char    *tres_fmt_str;	" str of configured TRES in partition "
end

mutable struct QoSSL
    name::String
    priority::Int
end

mutable struct AccountSL
    name::String
    id::Int
    "Fairshare"
    fairshare::Int
    "Users in this account"
    user_id::Vector{UserId}
end

"
User Account on particular Resource
"
mutable struct UserAccountSL
    id::Int
    name::String
    "global user id"
    user_id::Int
    default_account_id::AccountId
    # max_submit_jobs::Int
    # admin_level
    "User's accounts"
    account_id::Vector{AccountId}
end


"""
a batch job represent a manageble portion of CompTask

Mixture of slurm_job_info_t 

$(TYPEDFIELDS)

"""
mutable struct BatchJobSL
    "Job id - smaller id does not nessesary guaranee ealier submition"
    id::JobId
    "Resource Id"
    resource_id::ResourceId
    "global user id"
    user_id::UserId
    "local resource user id"
    user_account_id::UserAccountId
    " charge to specified account (char *account;)"
    account_id::AccountId

    " name of requested partition, (default in Slurm config) (char *partition;)"
    partition_id::PartitionId
    " Quality of Service (char *qos;)"
    qos_id::QoSId

    cpus::Int64
    cpus_per_node::Int64
    nodes::Int64
    gres_per_node::Vector{ARESType}
    gres_model_per_node::Vector{ARESModel}

    " memory per cpu in MB "
    mem_per_cpu::Int64
    node_sharing::NodeSharing

    " required feature specification, default NONE (char *features;)"
    features::Vector{NodeFeatureId}

    # task::CompTask
    req_walltime::Millisecond
    "negative time means run untill scheduler kick out"
    sim_walltime::Millisecond
    " Planned submit_time, after submittion actual submit_time"
    submit_time::DateTime

    # 
    priority::Int64
    status::JobStatus
    start_time::DateTime
    end_time::DateTime
    walltime::Millisecond
    # scheduled_by::SchedulerType
    "Vector with node ids"
    nodes_list::Vector{Int64}
end

"
Working and temporary arrays 
"
mutable struct JobOnResourceSL
    "nodes which can be used for this job"
    runnable_nodes::Vector{NodeId}
    "nodes which can be used for this job, bool array"
    runnable_nodes_bool::Vector{Bool}
    "Currently runnable nodes, indexing as in runnable_nodes"
    currently_runnable_nodes::Vector{Bool}
    "runnable_nodes availtime"
    runnable_nodes_availtime::Vector{DateTime}
    "temporary array for runnable_nodes calculations"
    gres_counted::Vector{Bool}

    job::BatchJobSL
end

function JobOnResourceSL(job::BatchJobSL)
    JobOnResourceSL(
            Vector{NodeId}(),
            Vector{Bool}(),
            Vector{Bool}(),
            Vector{DateTime}(),
            Vector{Bool}(),
            job)
end

mutable struct HPCEventSL
    when::DateTime
    dt::Millisecond
    event_type::HPCEventType
    event::Union{BatchJobSL}
end

@agent struct UserSL(GridAgent{1})
    name::String
    resource_user_account_id::Vector{UserAccountId}
    next_event::Int
    events_list::Vector{HPCEventSL}
end

mutable struct HPCResourceSL
    id::ResourceId
    name::String
    node::Vector{ComputeNodeSL}
    node_id::Dict{String, NodeId}
    partition::Vector{PartitionSL}
    partition_id::Dict{String, PartitionId}
    default_partition_id::PartitionId
    qos::Vector{QoSSL}
    qos_id::Dict{String, QoSId}
    default_qos_id::QoSId
    account::Vector{AccountSL}
    account_id::Dict{String, AccountId}
    user_account::Vector{UserAccountSL}
    user_account_id::Dict{String, UserAccountId}
    NodeFeatures::Vector{String}
    NodeFeatures_id::Dict{String, NodeFeatureId}

    
    "Jobs in queue"
    queue::Vector{JobOnResourceSL}
    "Jobs currently running"
    executing::Dict{JobId,JobOnResourceSL}
    "Jobs finished"
    history::Vector{BatchJobSL}
    "Job order in queue"
    jobs_order::Vector{Int}

    "Maximal observed GRES in nodes (used for temporary arrays allocations)"
    gres_max::Int

    "allocation/deallocation can be expansive so do it less often"
    cleanup_arrays::Int

    "Freq of tracking of individual allocatable resource, if negative do not track"
    ind_alloc_res_tracking_df_freq::Int
    ind_alloc_res_tracking_df::DataFrame
end


mutable struct SimulationSL
    id::Int64
    steps_per_day::Int64
    timestep::Millisecond
    cur_step::Int
    cur_datetime::DateTime
    init_datetime::DateTime
    # last_task_id::Int64
    # task_list::Vector{CompTask}
    # task_dict::Dict{Int64,CompTask}
    # last_job_id::Int64
    # jobs_list::Vector{BatchJobSimple}
    # jobs_dict::Dict{Int64,BatchJobSimple}
    # last_user_id::Int64
    # users_list::Vector{UserSL}
    # users_dict::Dict{Int64,UserSL}
    # """
    # Extra step by user: after finishing current jobs and 
    # before initiating new good spot to create new tasks or 
    # something else
    # """
    # user_extra_step::Union{Function,Nothing}
    # """
    # executed at the end of model_step!
    # """
    # model_extra_step::Union{Function,Nothing}
    workload_done_check_freq::Int64

    # resources
    resource::Vector{HPCResourceSL}
    resource_id::Dict{String, ResourceId}
    user::Vector{UserSL}
    user_id::Dict{String, UserId}
    next_event::Int
    events_list::Vector{HPCEventSL}

    #
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

    # Allocatable resources look-up
    ARESTypes::Vector{String}
    ARESTypes_id::Dict{String, ARESType}
    ARESModels::Vector{String}
    ARESModels_id::Dict{String, ARESModel}
end
