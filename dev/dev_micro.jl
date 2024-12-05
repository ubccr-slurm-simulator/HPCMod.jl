
using Logging
using DataFrames
using CSV
using Agents
using Random
using Printf
using PrettyPrint
using Dates
using DocStringExtensions
# using DataStructures: OrderedDict
using HPCMod


model = create_model_sl(
    init_datetime=DateTime(2024,11,1,0,0,0)
    )

resource = add_resource!(model; name="micro")

#ComputeNodeSL()
# add cluster
# add cluster Name=micro Fairshare=1 QOS=normal,supporters
#resource = HPCResourceSL(;name="micro")


# NodeName=DEFAULT RealMemory=48000 Procs=12 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1
# NodeName=m[1-4] Procs=12 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1 Feature=IB,CPU-M
add_nodes!(
    model, resource,
    map(x->@sprintf("m%d",x), 1:4);
    sockets=2,
    cores_per_socket=4,
    memory=48000,
    features=["IB","CPU-M"]
)

# NodeName=n[1-4] Procs=16 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1 Feature=IB,CPU-N
add_nodes!(
    model, resource,
    map(x->@sprintf("n%d",x), 1:4);
    sockets=2,
    cores_per_socket=6,
    memory=48000,
    features=["IB","CPU-N"]
)

# NodeName=g1 Procs=12 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1 Gres=gpu:2 Feature=IB,CPU-G
add_nodes!(
    model, resource,
    ["g1"];
    sockets=2,
    cores_per_socket=4,
    memory=48000,
    features=["IB","CPU-G"],
    gres=["GPU","GPU","FPGA"], # FPGA is for testing/debugging purposes
    gres_model=["GPU-Model1","GPU-Model1","FPGA-Model1"]
)
add_nodes!(
    model, resource,
    ["gn1", "gn2"];
    sockets=2,
    cores_per_socket=6,
    memory=128000,
    features=["IB","CPU-N"],
    gres=["GPU","GPU"],
    gres_model=["GPU-Model2","GPU-Model2"]
)

# NodeName=b1 RealMemory=512000 Procs=16 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1 Feature=IB,CPU-G,BigMem
add_nodes!(
    model, resource,
    ["b1"];
    sockets=2,
    cores_per_socket=6,
    memory=512000,
    features=["IB","CPU-N","BigMem"]
)


# PartitionName=normal Nodes=n[1-4],m[1-4],g1,b1 Default=YES DefMemPerCPU=2800 MaxTime=INFINITE State=UP
add_partition!(
    model, resource, "normal",
    [map(x->@sprintf("n%d",x), 1:4); map(x->@sprintf("m%d",x), 1:4); ["g1","b1"]];
    Default=true,
    DefMemPerCPU=2800,
    MaxTime=TIME_INFINITE,
    State=HPCMod.PARTITION_UP
)
# PartitionName=debug Nodes=n[1-2] DefMemPerCPU=2800 MaxTime=INFINITE State=UP
add_partition!(
    model, resource, "debug",
    ["n1", "n2"];
    Default=true,
    DefMemPerCPU=2800,
    MaxTime=TIME_INFINITE,
    State=HPCMod.PARTITION_UP
)


# add/modify QOS
add_qos!(model, resource, "normal"; priority=0)
add_qos!(model, resource, "supporters"; priority=100)
# add QOS Name=supporters Priority=100

# add accounts
add_account!(model, resource, "account0"; fairshare=100)
add_account!(model, resource, "account1"; fairshare=100)
add_account!(model, resource, "account2"; fairshare=100)
# add admin
add_user!(model, resource, "admin", "account0")
# add users
user1 = add_user!(model, resource, "user1", "account1")
user2 = add_user!(model, resource, "user2", "account1")
user3 = add_user!(model, resource, "user3", "account1")
user4 = add_user!(model, resource, "user4", "account2")
user5 = add_user!(model, resource, "user5", "account2")



# NodeName=m[1-4] Procs=12 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1 Feature=IB,CPU-M
# NodeName=n[1-4] Procs=16 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1 Feature=IB,CPU-N
# NodeName=g1 Procs=12 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1 Gres=gpu:2 Feature=IB,CPU-G
# NodeName=gn[1-2] Procs=16 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1 Gres=gpu:2 Feature=IB,CPU-G
# NodeName=b1 RealMemory=512000 Procs=16 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1 Feature=IB,CPU-N,BigMem
# [(i,n.name) for (i,n) in enumerate(resource.node)]
# (1, "m1") Procs=12 Feature=IB,CPU-M memory=48000
# (2, "m2")
# (3, "m3")
# (4, "m4")
# (5, "n1") Procs=16 Feature=IB,CPU-N memory=48000
# (6, "n2")
# (7, "n3")
# (8, "n4")
# (9, "g1") Procs=12 Feature=IB,CPU-G Gres=gpu:GPU-Model1:2 memory=48000
# (10, "gn1") Procs=16 Feature=IB,CPU-N Gres=gpu:GPU-Model2:2 memory=128000
# (11, "gn2")
# (12, "b1") Procs=16 Feature=IB,CPU-N,BigMem  Memory=512000

# 1 node with 12 cores per node
job1001 = add_job!(
    model, resource; dt=0, job_id=1001, sim_walltime=Millisecond(0*1000), user="user5", req_walltime=Millisecond(60*1000), 
    cpus=12, cpus_per_node=12, account="account2", partition="normal", qos="normal")
# 1 node with 1 cores per node with CPU-N
job1002 = add_job!(
    model, resource; dt=1, job_id=1002, sim_walltime=Millisecond(-1*1000), user="user1", req_walltime=Millisecond(60*1000), 
    cpus=1, cpus_per_node=1, account="account1", partition="normal", qos="normal", features=["CPU-N"], priority = 2)
# Any 1 cpu 500GB each
job1003 = add_job!(
    model, resource; dt=2, job_id=1003, sim_walltime=Millisecond(5*1000), user="user4", req_walltime=Millisecond(60*1000), 
    cpus=1, cpus_per_node=1, account="account2", partition="normal", qos="normal", mem_per_cpu=500000, priority = 3)
# Any 2 nodes with 8 cpus per node
job1004 = add_job!(
    model, resource; dt=16, job_id=1004, sim_walltime=Millisecond(21*1000), user="user3", req_walltime=Millisecond(60*1000), 
    cpus=16, cpus_per_node=8, account="account1", partition="normal", qos="normal")
# 12 cpus, large MEM
job1005 = add_job!(
    model, resource; dt=19, job_id=1005, sim_walltime=Millisecond(2*1000), user="user5", req_walltime=Millisecond(60*1000), 
    cpus=8, cpus_per_node=8, account="account2", partition="normal", qos="normal", mem_per_cpu=500000÷8)
# 4 Nodes with 16 cores
job1006 = add_job!(
    model, resource; dt=19, job_id=1006, sim_walltime=Millisecond(9*1000), user="user3", req_walltime=Millisecond(60*1000), 
    cpus=48, cpus_per_node=12, account="account1", partition="normal", qos="normal")
# 2 node of CPU-M
job1007 = add_job!(
    model, resource; dt=19, job_id=1007, sim_walltime=Millisecond(-1*1000), user="user4", req_walltime=Millisecond(60*1000), 
    cpus=16, cpus_per_node=8, account="account2", partition="normal", qos="normal", features=["CPU-M"])
# one GPU 4 cores
job1008 = add_job!(
    model, resource; dt=22, job_id=1008, sim_walltime=Millisecond(0*1000), user="user4", req_walltime=Millisecond(60*1000), 
    cpus=4, cpus_per_node=4, account="account2", partition="normal", qos="normal",
    gres_per_node=["GPU"])
# 8 nodes
job1009 = add_job!(
    model, resource; dt=26, job_id=1009, sim_walltime=Millisecond(2*1000), user="user1", req_walltime=Millisecond(60*1000), 
    cpus=64, cpus_per_node=8, account="account1", partition="normal", qos="normal")
# 1 node CPU-N
job1010 = add_job!(
    model, resource; dt=26, job_id=1010, sim_walltime=Millisecond(0*1000), user="user5", req_walltime=Millisecond(60*1000), 
    cpus=12, cpus_per_node=12, account="account2", partition="normal", qos="normal", features=["CPU-N"])
# 1 node with 2 abitrary GPUS
job1011 = add_job!(
    model, resource; dt=26, job_id=1011, sim_walltime=Millisecond(0*1000), user="user4", req_walltime=Millisecond(60*1000), 
    cpus=4, cpus_per_node=4, account="account2", partition="normal", qos="normal", gres_per_node=["GPU","GPU"])
# 2 nodes with 2 gpu per node
job1012 = add_job!(
    model, resource; dt=32, job_id=1012, sim_walltime=Millisecond(-1*1000), user="user5", req_walltime=Millisecond(60*1000), 
    cpus=12, cpus_per_node=12, account="account2", partition="normal", qos="normal",
    gres_per_node=["GPU","GPU"], gres_model_per_node=["GPU-Model2","GPU-Model2"])
# 
job1013 = add_job!(
    model, resource; dt=36, job_id=1013, sim_walltime=Millisecond(0*1000), user="user2", req_walltime=Millisecond(60*1000), 
    cpus=1, cpus_per_node=1, account="account1", partition="normal", qos="normal", mem_per_cpu=100000)
#
job1014 = add_job!(
    model, resource; dt=36, job_id=1014, sim_walltime=Millisecond(7*1000), user="user5", req_walltime=Millisecond(60*1000), 
    cpus=24, cpus_per_node=12, account="account2", partition="normal", qos="normal", features=["CPU-N"])
#
job1015 = add_job!(
    model, resource; dt=39, job_id=1015, sim_walltime=Millisecond(18*1000), user="user2", req_walltime=Millisecond(60*1000), 
    cpus=4, cpus_per_node=4, account="account1", partition="normal", qos="normal")
# 
job1016 = add_job!(
    model, resource; dt=40, job_id=1016, sim_walltime=Millisecond(25*1000), user="user1", req_walltime=Millisecond(60*1000), 
    cpus=8, cpus_per_node=8, account="account1", partition="normal", qos="normal", 
    gres_per_node=["GPU","GPU"])
#
job1017 = add_job!(
    model, resource; dt=42, job_id=1017, sim_walltime=Millisecond(1*1000), user="user1", req_walltime=Millisecond(60*1000), 
    cpus=48, cpus_per_node=12, account="account1", partition="normal", qos="normal", features=["CPU-N"])
#
job1018 = add_job!(
    model, resource; dt=42, job_id=1018, sim_walltime=Millisecond(0*1000), user="user3", req_walltime=Millisecond(60*1000), 
    cpus=8, cpus_per_node=8, account="account1", partition="normal", qos="normal")
#
job1019 = add_job!(
    model, resource; dt=43, job_id=1019, sim_walltime=Millisecond(34*1000), user="user4", req_walltime=Millisecond(60*1000), 
    cpus=8, cpus_per_node=8, account="account2", partition="normal", qos="normal", gres_per_node=["GPU","GPU"])
#
job1020 = add_job!(
    model, resource; dt=43, job_id=1020, sim_walltime=Millisecond(14*1000), user="user1", req_walltime=Millisecond(60*1000), 
    cpus=1, cpus_per_node=1, account="account1", partition="normal", qos="normal", features=["CPU-N"])

HPCMod.check_resource(model, resource)

#pprint(resource)
HPCMod.show_node_info(model, resource )
#pprint(model.user)
ENV["JULIA_DEBUG"] = "all"


#macro debug2(exs...) @logmsg(Logging.LogLevel(-1000), exs) end



debug_logger = ConsoleLogger(stdout, HPCMod.Debug5)
old_logger = global_logger(debug_logger); 




HPCMod.@debug1 "Test"
HPCMod.@debug4 "Test"
HPCMod.@debug5 "Test"

Logging.logmsg_code()
step!(model, 30);

Logging.min_enabled_level(debug_logger)

global_logger()
Logging.min_enabled_level(current_logger())

Logging.shouldlog()

#delete!(ENV, "JULIA_DEBUG")
#

user1
# @info "Test"
# @debug "Test"

#run!(model; run_till_no_jobs=false, nsteps=5);
abmtime(model)
pprint(abmqueue(model))

#(agent id, event index) to the time 
#run!(model; run_till_no_jobs=true);

#step!(model, 120);
pprint(model[3])


delete!(ENV, "JULIA_DEBUG")

#pprintln(resource)
HPCMod.show_queue(model, resource)
HPCMod.show_history(model, resource)

#check_finished_job!(model, resource)
#model.resource[1].ind_alloc_res_tracking_df
#CSV.write("iares.csv", model.resource[1].ind_alloc_res_tracking_df)
#HPCMod.track_ind_alloc_res!(model, model.resource[1])
#pprintln(resource.executing)

#*":"*GRESModels[mid]

#a=[DateTime(2024,1,1), HPCMod.DATETIME_UNSET_L, DateTime(2023,1,1)]
#p=Vector{Int}()
#sortperm!(p, a, rev=false)

#a[p]


for n in resource.node
    println("$(n.name) $(n.ares_used)")
end



a = Set([11,2,31,4,5])
delete!(a, 11)

gres = "GPU:GPU-Model1:2,FPGA:FPGA-Model1"



collect(allagents(model))