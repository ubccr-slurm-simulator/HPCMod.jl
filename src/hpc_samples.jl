"
Generate Simulation of Micro12 cluster
"
function gen_micro12()
    sim = SimulationSL(init_datetime=DateTime(2024,11,1,0,0,0), timestep=Millisecond(1000))

    resource = add_resource!(sim; name="micro")
    #ComputeNodeSL()
    # add cluster
    # add cluster Name=micro Fairshare=1 QOS=normal,supporters
    #resource = HPCResourceSL(;name="micro")


    # NodeName=DEFAULT RealMemory=48000 Procs=12 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1
    # NodeName=m[1-4] Procs=8 Sockets=2 CoresPerSocket=4 ThreadsPerCore=1 Feature=IB,CPU-M
    add_nodes!(
        sim, resource,
        map(x->@sprintf("m%d",x), 1:4);
        sockets=2,
        cores_per_socket=4,
        memory=48000,
        features=["IB","CPU-M"]
    )

    # NodeName=n[1-4] Procs=12 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1 Feature=IB,CPU-N
    add_nodes!(
        sim, resource,
        map(x->@sprintf("n%d",x), 1:4);
        sockets=2,
        cores_per_socket=12,
        memory=48000,
        features=["IB","CPU-N"]
    )

    # NodeName=g1 Procs=12 Sockets=2 CoresPerSocket=6 ThreadsPerCore=1 Gres=gpu:2 Feature=IB,CPU-G
    add_nodes!(
        sim, resource,
        ["g1"];
        sockets=2,
        cores_per_socket=4,
        memory=48000,
        features=["IB","CPU-G"],
        gres=["GPU","GPU","FPGA"], # FPGA is for testing/debugging purposes
        gres_model=["GPU-Model1","GPU-Model1","FPGA-Model1"]
    )
    add_nodes!(
        sim, resource,
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
        sim, resource,
        ["b1"];
        sockets=2,
        cores_per_socket=6,
        memory=512000,
        features=["IB","CPU-N","BigMem"]
    )


    # PartitionName=normal Nodes=n[1-4],m[1-4],g1,b1 Default=YES DefMemPerCPU=2800 MaxTime=INFINITE State=UP
    add_partition!(
        sim, resource, "normal",
        [map(x->@sprintf("n%d",x), 1:4); map(x->@sprintf("m%d",x), 1:4); ["g1","b1"]];
        Default=true,
        DefMemPerCPU=2800,
        MaxTime=TIME_INFINITE,
        State=HPCMod.PARTITION_UP
    )
    # PartitionName=debug Nodes=n[1-2] DefMemPerCPU=2800 MaxTime=INFINITE State=UP
    add_partition!(
        sim, resource, "debug",
        ["n1", "n2"];
        Default=true,
        DefMemPerCPU=2800,
        MaxTime=TIME_INFINITE,
        State=HPCMod.PARTITION_UP
    )


    # add/modify QOS
    add_qos!(sim, resource, "normal"; priority=0)
    add_qos!(sim, resource, "supporters"; priority=100)
    # add QOS Name=supporters Priority=100

    # add accounts
    add_account!(sim, resource, "account0"; fairshare=100)
    add_account!(sim, resource, "account1"; fairshare=100)
    add_account!(sim, resource, "account2"; fairshare=100)
    # add admin
    add_user!(sim, resource, "admin", "account0")
    # add users
    add_user!(sim, resource, "user1", "account1")
    add_user!(sim, resource, "user2", "account1")
    add_user!(sim, resource, "user3", "account1")
    add_user!(sim, resource, "user4", "account2")
    add_user!(sim, resource, "user5", "account2")

    sim
end


