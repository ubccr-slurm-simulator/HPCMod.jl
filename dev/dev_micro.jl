# using HPCMod
using Logging
using DataFrames
using Agents
using Random
using Printf
using PrettyPrint

const NodeFeatureId = Int
const GRESType = Int
const NodeId = Int
const PartitionId = Int
const QoSId = Int
const AccountId = Int
const UserId = Int

const TIME_INFINITE::Int = 100*365*24*3600

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
# node_info_t?
# Procs->CPUs?
"""
largely adopted from Slurm's node_info_t
"""

mutable struct ComputeNode
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
	" free memory in MiB "
    free_mem::Int
    " free CPUs"
    free_cpus::Int
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
	" list of a node's generic resources "
    gres::Vector{GRESType}
	# char *gres_drain;	" list of drained GRES "
	" list of GRES in current use => Vector of GRES use status"
    gres_used::Vector{Bool};	
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
    real_memory::Int
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
	" number of threads per core (node_info_t.threads_per_core)"
    threads_per_core::Int
	# uint32_t tmp_disk;	" configured MB of total disk in TMP_FS "
	# uint32_t weight;	" arbitrary priority of node for scheduling "
	# char *tres_fmt_str;	" str representing configured TRES on node "
	# char *version;		 " Slurm version number "
    #function ComputeNode(;sockets=,threads=)
    
end



function ComputeNode(
    name::String;
    cores_per_socket::Union{Missing, Int64}=missing,
    cpus::Union{Missing, Int64}=missing,
    features::Vector{Int64}=Vector{Int64}(),
    gres::Vector{Int64}=Vector{Int64}(),
    gres_used::Union{Missing, Vector{Bool}}=missing,
    node_state::NodeState=NODE_STATE_IDLE,
    real_memory::Union{Missing, Int64}=missing,
    sockets::Union{Missing, Int64}=missing,
    threads_per_core::Union{Missing, Int64}=missing,
    free_mem::Union{Missing, Int64}=missing,
    free_cpus::Union{Missing, Int64}=missing,
)
    ismissing(gres_used) && (gres_used = Vector{Bool}())
    ismissing(free_mem) && (free_mem = real_memory)
    ismissing(free_cpus) && (free_cpus = cpus)
    ComputeNode(cores_per_socket, free_mem, free_cpus, cpus, features, gres, gres_used, name, node_state, real_memory, sockets, threads_per_core)
end

"
partition_info_t
"
mutable struct Partition
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

mutable struct QoS
    name::String
    priority::Int
end

mutable struct Account
    name::String
    id::Int
    "Fairshare"
    fairshare::Int
    "Users in this account"
    user_id::Vector{UserId}
end

mutable struct User
    name::String
    id::Int
    default_account_id::AccountId
    # max_submit_jobs::Int
    # admin_level
    "User's accounts"
    account_id::Vector{AccountId}
end


mutable struct HPCResource
    NodeFeatures::Dict{String, NodeFeatureId}
    GRESTypes::Dict{String, GRESType}
    name::String
    node::Vector{ComputeNode}
    node_id::Dict{String, NodeId}
    partition::Vector{Partition}
    partition_id::Dict{String, PartitionId}
    default_partition_id::PartitionId
    qos::Vector{QoS}
    qos_id::Dict{String, QoSId}
    default_qos_id::QoSId
    account::Vector{Account}
    account_id::Dict{String, AccountId}
    user::Vector{User}
    user_id::Dict{String, UserId}

end

function HPCResource(;name::String="HPCResource")
    HPCResource(
        Dict{String, NodeFeatureId}(),
        Dict{String, GRESType}(),
        name,
        Vector{ComputeNode}(), Dict{String, NodeId}(),
        Vector{Partition}(), Dict{String, PartitionId}(), 0,
        Vector{QoS}(), Dict{String, QoSId}(), 0,
        Vector{Account}(), Dict{String, AccountId}(),
        Vector{User}(), Dict{String, UserId}(),
        )
end

"
Get vector of integer ids from the vector of string ids using id dict
"
function get_ids_from_str_ids(str_ids::Vector{String}, id_dict::Dict{String, Int})
    ids = Vector{Int64}()
    for str_id in str_ids
        if !haskey(id_dict, str_id)
            id_dict[str_id] = length(id_dict) + 1
        end
        push!(ids, id_dict[str_id])
    end
    return ids
end

function add_nodes!(
    resource,
    nodesname_list::Vector{String};
    CPUs::Int=12,
    Sockets::Int=2,
    CoresPerSocket::Int=6,
    ThreadsPerCore::Int=1,
    RealMemory::Int=48000,
    Feature::Vector{String}=["IB","CPU-N"],
    Gres::Vector{String}=["GPU","GPU"],
    State::NodeState=NODE_STATE_IDLE
)
    # Features Ids
    features=get_ids_from_str_ids(Feature, resource.NodeFeatures)
    # GRES
    gres=get_ids_from_str_ids(Gres, resource.GRESTypes)

    for name in nodesname_list
        # Check name uniqueness
        haskey(resource.node_id, name) && throw("Node $(name) already exists")
        # Add
        push!(resource.node, ComputeNode( name;
        cores_per_socket=CoresPerSocket,
            cpus=CPUs,
            features=copy(features),
            gres=copy(gres),
            node_state=State,
            real_memory=RealMemory,
            sockets=Sockets,
            threads_per_core=ThreadsPerCore
        ))
        resource.node_id[name] = length(resource.node)
    end
    nothing
end

function add_partition!(
    resource,
    name,
    nodes;
    Default::Bool=false,
    DefMemPerCPU::Int=2800,
    MaxTime::Int=TIME_INFINITE,
    priority_job_factor::Int=0,
    State::PartitionState=PARTITION_UP
)
    haskey(resource.partition_id, name) && throw("Partition $(name) already exists")
    
    push!(resource.partition, Partition(
            DefMemPerCPU,
            MaxTime,
            name,
            sort([resource.node_id[node] for node in nodes]),
            priority_job_factor,
            State
    ))
    resource.partition_id[name] = length(resource.partition)
    Default && (resource.default_partition_id = resource.partition_id[name])
    nothing
end

function add_qos!(resource, name;priority::Int=0)
    haskey(resource.qos_id, name) && throw("QoS $(name) already exists")

    push!(resource.qos, QoS(
        name,
        priority
    ))
    resource.qos_id[name] = length(resource.qos)
    nothing
end

function add_account!(resource, name;fairshare::Int=100)
    haskey(resource.account_id, name) && throw("Account $(name) already exists")

    account_id = length(resource.account) + 1
    push!(resource.account, Account(
        name,
        account_id,
        fairshare,
        Vector{UserId}()
    ))
    resource.account_id[name] = account_id
    nothing
end

function add_user!(
    resource, name, default_account::String;
    accounts::Vector{String}=Vector{String}()
    )
    haskey(resource.user_id, name) && throw("User $(name) already exists")

    accounts_id = sort(unique([[resource.account_id[default_account]]; [resource.account_id[acc] for acc in accounts]]))

    user_id = length(resource.user) + 1
    push!(resource.user, User(
        name,
        user_id,
        resource.account_id[default_account],
        accounts_id
    ))
    resource.user_id[name] = user_id
    nothing
end


#ComputeNode()
# add cluster
# add cluster Name=micro Fairshare=1 QOS=normal,supporters
resource = HPCResource(;name="micro")
# NodeName=DEFAULT RealMemory=48000 Procs=12 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1
# NodeName=n[1-4] Procs=12 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1 Feature=IB,CPU-N
add_nodes!(
    resource,
    map(x->@sprintf("n%d",x), 1:4);
    CPUs=12,
    Sockets=2,
    CoresPerSocket=6,
    ThreadsPerCore=1,
    RealMemory=48000,
    Feature=["IB","CPU-N"]
)
# NodeName=m[1-4] Procs=12 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1 Feature=IB,CPU-M
add_nodes!(
    resource,
    map(x->@sprintf("m%d",x), 1:4);
    CPUs=12,
    Sockets=2,
    CoresPerSocket=6,
    ThreadsPerCore=1,
    RealMemory=48000,
    Feature=["IB","CPU-M"]
)
# NodeName=g1 Procs=12 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1 Gres=gpu:2 Feature=IB,CPU-G
add_nodes!(
    resource,
    ["g1"];
    CPUs=12,
    Sockets=2,
    CoresPerSocket=6,
    ThreadsPerCore=1,
    RealMemory=48000,
    Feature=["IB","CPU-G"],
    Gres=["GPU","GPU"]
)
# NodeName=b1 RealMemory=512000 Procs=12 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1 Feature=IB,CPU-G,BigMem
add_nodes!(
    resource,
    ["b1"];
    CPUs=12,
    Sockets=2,
    CoresPerSocket=6,
    ThreadsPerCore=1,
    RealMemory=512000,
    Feature=["IB","CPU-G","BigMem"]
)


# PartitionName=normal Nodes=n[1-4],m[1-4],g1,b1 Default=YES DefMemPerCPU=2800 MaxTime=INFINITE State=UP
add_partition!(
    resource, "normal",
    [map(x->@sprintf("n%d",x), 1:4); map(x->@sprintf("m%d",x), 1:4); ["g1","b1"]];
    Default=true,
    DefMemPerCPU=2800,
    MaxTime=TIME_INFINITE,
    State=PARTITION_UP
)
# PartitionName=debug Nodes=n[1-2] DefMemPerCPU=2800 MaxTime=INFINITE State=UP
add_partition!(
    resource, "debug",
    ["n1", "n2"];
    Default=true,
    DefMemPerCPU=2800,
    MaxTime=TIME_INFINITE,
    State=PARTITION_UP
)


# add/modify QOS
add_qos!(resource, "normal"; priority=0)
add_qos!(resource, "supporters"; priority=100)
# add QOS Name=supporters Priority=100

# add accounts
add_account!(resource, "account0"; fairshare=100)
add_account!(resource, "account1"; fairshare=100)
add_account!(resource, "account2"; fairshare=100)
# add admin
add_user!(resource, "admin", "account0")
# add users
add_user!(resource, "user1", "account1")
add_user!(resource, "user2", "account1")
add_user!(resource, "user3", "account1")
add_user!(resource, "user4", "account2")
add_user!(resource, "user5", "account2")



pprint(resource)
println("Done")

MyType=Partition

for field in fieldnames(MyType)
    println("$field::Union{Missing,$(fieldtype(MyType, field))}=missing,")
end

for field in fieldnames(MyType)
    print("$field, ")
end
println()
for field in fieldnames(MyType)
    println("$field=$field,")
end
