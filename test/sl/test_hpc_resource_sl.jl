using Random
using Agents
using HPCMod
using Printf
using DataFrames
using DataStructures
using Logging

@testset "HPCResourceSL" begin
    sim = HPCMod.gen_micro12()
    resource = sim.resource[1]
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

    # 1 node with 16 cores per node
    job1001 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(0*1000), job_id=1001, sim_walltime=Millisecond(0*1000), user="user5", req_walltime=Millisecond(60*1000), 
        cpus=16, cpus_per_node=16, account="account2", partition="normal", qos="normal"))
    HPCMod.find_runnable_nodes!(sim,resource,job1001)
    @test job1001.runnable_nodes==[5, 6, 7, 8, 10, 11, 12]

    # 1 node with 1 cores per node with CPU-N
    job1002 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(1*1000), job_id=1002, sim_walltime=Millisecond(-1*1000), user="user1", req_walltime=Millisecond(60*1000), 
        cpus=1, cpus_per_node=1, account="account1", partition="normal", qos="normal", features=["CPU-N"], priority = 2))
    HPCMod.find_runnable_nodes!(sim,resource,job1002)
    @test job1002.runnable_nodes==[5, 6, 7, 8, 10, 11, 12]

    # Any 1 cpu 500GB each
    job1003 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(2*1000), job_id=1003, sim_walltime=Millisecond(5*1000), user="user4", req_walltime=Millisecond(60*1000), 
        cpus=1, cpus_per_node=1, account="account2", partition="normal", qos="normal", mem_per_cpu=500000, priority = 3))
    HPCMod.find_runnable_nodes!(sim,resource,job1003)
    @test job1003.runnable_nodes==[12,]

    # Any 2 nodes with 12 cpus per node
    job1004 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(16*1000), job_id=1004, sim_walltime=Millisecond(21*1000), user="user3", req_walltime=Millisecond(60*1000), 
        cpus=24, cpus_per_node=12, account="account1", partition="normal", qos="normal"))
    HPCMod.find_runnable_nodes!(sim,resource,job1004)
    @test job1004.runnable_nodes==[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

    # 12 cpus, large MEM
    job1005 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(19*1000), job_id=1005, sim_walltime=Millisecond(2*1000), user="user5", req_walltime=Millisecond(60*1000), 
        cpus=12, cpus_per_node=12, account="account2", partition="normal", qos="normal", mem_per_cpu=500000÷12))
    HPCMod.find_runnable_nodes!(sim,resource,job1005)
    @test job1005.runnable_nodes==[12,]

    # 4 Nodes with 16 cores
    job1006 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(19*1000), job_id=1006, sim_walltime=Millisecond(9*1000), user="user3", req_walltime=Millisecond(60*1000), 
        cpus=64, cpus_per_node=16, account="account1", partition="normal", qos="normal"))
    HPCMod.find_runnable_nodes!(sim,resource,job1006)
    @test job1006.runnable_nodes==[5, 6, 7, 8, 10, 11, 12]

    # 2 node of CPU-M
    job1007 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(19*1000), job_id=1007, sim_walltime=Millisecond(-1*1000), user="user4", req_walltime=Millisecond(60*1000), 
        cpus=24, cpus_per_node=12, account="account2", partition="normal", qos="normal", features=["CPU-M"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1007)
    @test job1007.runnable_nodes==[1, 2, 3, 4]

    # one GPU 4 cores
    job1008 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(22*1000), job_id=1008, sim_walltime=Millisecond(0*1000), user="user4", req_walltime=Millisecond(60*1000), 
        cpus=4, cpus_per_node=4, account="account2", partition="normal", qos="normal",
        gres_per_node=["GPU"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1008)
    @test job1008.runnable_nodes==[9, 10, 11]

    # 8 nodes
    job1009 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(26*1000), job_id=1009, sim_walltime=Millisecond(2*1000), user="user1", req_walltime=Millisecond(60*1000), 
        cpus=96, cpus_per_node=12, account="account1", partition="normal", qos="normal"))
    HPCMod.find_runnable_nodes!(sim,resource,job1009)
    @test job1009.runnable_nodes==[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

    # 1 node CPU-N
    job1010 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(26*1000), job_id=1010, sim_walltime=Millisecond(0*1000), user="user5", req_walltime=Millisecond(60*1000), 
        cpus=16, cpus_per_node=16, account="account2", partition="normal", qos="normal", features=["CPU-N"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1010)
    @test job1010.runnable_nodes==[5, 6, 7, 8, 10, 11, 12]

    # 1 node with 2 abitrary GPUS
    job1011 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(29*1000), job_id=1011, sim_walltime=Millisecond(0*1000), user="user4", req_walltime=Millisecond(60*1000), 
        cpus=4, cpus_per_node=4, account="account2", partition="normal", qos="normal", gres_per_node=["GPU","GPU"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1011)
    @test job1011.runnable_nodes==[9, 10, 11]

    # 2 nodes with 2 gpu per node
    job1012 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(32*1000), job_id=1012, sim_walltime=Millisecond(-1*1000), user="user5", req_walltime=Millisecond(60*1000), 
        cpus=16, cpus_per_node=16, account="account2", partition="normal", qos="normal",
        gres_per_node=["GPU","GPU"], gres_model_per_node=["GPU-Model2","GPU-Model2"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1012)
    @test job1012.runnable_nodes==[10, 11]

    # 
    job1013 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(36*1000), job_id=1013, sim_walltime=Millisecond(0*1000), user="user2", req_walltime=Millisecond(60*1000), 
        cpus=1, cpus_per_node=1, account="account1", partition="normal", qos="normal", mem_per_cpu=100000))
    HPCMod.find_runnable_nodes!(sim,resource,job1013)
    @test job1013.runnable_nodes==[10, 11, 12]
    #
    job1014 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(36*1000), job_id=1014, sim_walltime=Millisecond(7*1000), user="user5", req_walltime=Millisecond(60*1000), 
        cpus=32, cpus_per_node=16, account="account2", partition="normal", qos="normal", features=["CPU-N"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1014)
    @test job1014.runnable_nodes==[5, 6, 7, 8, 10, 11, 12]
    #
    job1015 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(39*1000), job_id=1015, sim_walltime=Millisecond(18*1000), user="user2", req_walltime=Millisecond(60*1000), 
        cpus=6, cpus_per_node=6, account="account1", partition="normal", qos="normal"))
    HPCMod.find_runnable_nodes!(sim,resource,job1015)
    @test job1015.runnable_nodes==[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    # 
    job1016 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(40*1000), job_id=1016, sim_walltime=Millisecond(25*1000), user="user1", req_walltime=Millisecond(60*1000), 
        cpus=8, cpus_per_node=8, account="account1", partition="normal", qos="normal", 
        gres_per_node=["GPU","GPU"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1016)
    @test job1016.runnable_nodes==[9, 10, 11]
    #
    job1017 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(42*1000), job_id=1017, sim_walltime=Millisecond(1*1000), user="user1", req_walltime=Millisecond(60*1000), 
        cpus=64, cpus_per_node=16, account="account1", partition="normal", qos="normal", features=["CPU-N"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1017)
    @test job1017.runnable_nodes==[5, 6, 7, 8, 10, 11, 12]
    #
    job1018 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(42*1000), job_id=1018, sim_walltime=Millisecond(0*1000), user="user3", req_walltime=Millisecond(60*1000), 
        cpus=12, cpus_per_node=12, account="account1", partition="normal", qos="normal"))
    HPCMod.find_runnable_nodes!(sim,resource,job1018)
    @test job1018.runnable_nodes==[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    #
    job1019 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(43*1000), job_id=1019, sim_walltime=Millisecond(34*1000), user="user4", req_walltime=Millisecond(60*1000), 
        cpus=12, cpus_per_node=12, account="account2", partition="normal", qos="normal", gres_per_node=["GPU","GPU"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1019)
    @test job1019.runnable_nodes==[9, 10, 11]
    #
    job1020 = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(43*1000), job_id=1020, sim_walltime=Millisecond(14*1000), user="user1", req_walltime=Millisecond(60*1000), 
        cpus=1, cpus_per_node=1, account="account1", partition="normal", qos="normal", features=["CPU-N"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1020)
    @test job1020.runnable_nodes==[5, 6, 7, 8, 10, 11, 12]



    # Such job should not be possible
    job1007b = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(19*1000), job_id=1007, sim_walltime=Millisecond(-1*1000), user="user4", req_walltime=Millisecond(60*1000), 
        cpus=32, cpus_per_node=16, account="account2", partition="normal", qos="normal", features=["CPU-M"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1007b)
    @test job1007b.runnable_nodes==[]

    job1007c = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(19*1000), job_id=1007, sim_walltime=Millisecond(-1*1000), user="user4", req_walltime=Millisecond(60*1000), 
        cpus=24, cpus_per_node=12, account="account2", partition="normal", qos="normal", features=["CPU-M"], mem_per_cpu=500000÷12))
    HPCMod.find_runnable_nodes!(sim,resource,job1007c)
    @test job1007c.runnable_nodes==[]

    job1007d = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(19*1000), job_id=1007, sim_walltime=Millisecond(-1*1000), user="user4", req_walltime=Millisecond(60*1000), 
        cpus=240, cpus_per_node=12, account="account2", partition="normal", qos="normal", features=["CPU-M"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1007d)
    @test job1007d.runnable_nodes==[]

    # three GPU 4 cores
    job1008b = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(22*1000), job_id=1008, sim_walltime=Millisecond(0*1000), user="user4", req_walltime=Millisecond(60*1000), 
        cpus=4, cpus_per_node=4, account="account2", partition="normal", qos="normal",
        gres_per_node=["GPU","GPU","GPU"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1008b)
    @test job1008b.runnable_nodes==[]

    job1008b = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(22*1000), job_id=1008, sim_walltime=Millisecond(0*1000), user="user4", req_walltime=Millisecond(60*1000), 
        cpus=4, cpus_per_node=4, account="account2", partition="normal", qos="normal",
        gres_per_node=["GPU","GPU","GPU","GPU"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1008b)
    @test job1008b.runnable_nodes==[]

    job1008b = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(22*1000), job_id=1008, sim_walltime=Millisecond(0*1000), user="user4", req_walltime=Millisecond(60*1000), 
        cpus=4, cpus_per_node=4, account="account2", partition="normal", qos="normal",
        gres_per_node=["GPU","GPU","FPGA"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1008b)
    @test job1008b.runnable_nodes==[9]

    # 1 node CPU-N
    job1011b = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(29*1000), job_id=1011, sim_walltime=Millisecond(0*1000), user="user4", req_walltime=Millisecond(60*1000), 
        cpus=4, cpus_per_node=4, account="account2", partition="normal", qos="normal", 
        gres_per_node=["GPU","GPU"], gres_model_per_node=["GPU-Model1","GPU-Model1"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1011b)
    @test job1011b.runnable_nodes==[9]

    job1011b = HPCMod.JobOnResourceSL(add_job!(
        sim, resource; dt=Millisecond(29*1000), job_id=1011, sim_walltime=Millisecond(0*1000), user="user4", req_walltime=Millisecond(60*1000), 
        cpus=4, cpus_per_node=4, account="account2", partition="normal", qos="normal", 
        gres_per_node=["GPU","GPU"], gres_model_per_node=["GPU-Model2","GPU-Model2"]))
    HPCMod.find_runnable_nodes!(sim,resource,job1011b)
    @test job1011b.runnable_nodes==[10,11]

    HPCMod.check_resource(sim, resource)


    # 
    @test HPCMod.gres_str(sim, resource, resource.node[9].gres, resource.node[9].gres_model) == "GPU:GPU-Model1:2,FPGA:FPGA-Model1"
    @test HPCMod.gres_str(sim, resource, [3,3], Vector{Int}()) == "GPU:2"
    @test HPCMod.gres_str(sim, resource, [3,3], [0,0]) == "GPU:2"
end