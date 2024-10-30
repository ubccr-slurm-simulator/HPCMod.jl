using Random
using Agents
using HPCMod
using Plots
using DataFrames
using Dates

"""
`function simulate(adaptive_fourth_user=false, thinktime_gamma=false)`

Simulate 10-Node Cluster

* `adaptive_fourth_user` - should the 4th user use AdaptiveFactor strategy
* `thinktime_gamma` - should a gamma distribution used for think time between jobs of same task
"""
function simulate(
	;
	adaptive_fourth_user=false, 
	thinktime_gamma=false, 
	max_time_per_job=24*3, 
	all_adaptive=false,
	rng=Random.Xoshiro(123),
	arrive_same_time=true)
	# Init simulation, seed a random generator
	sim = Simulation(;rng=rng)
	sim.workload_done_check_freq = 1
	# Add HPC resource
	add_resource!(
	    sim; 
	    nodes=10,
	    max_nodes_per_job=6,
	    max_time_per_job,
	    scheduler_backfill=true)
	
	# add four users
	for user_id in 1:4
	    user = User(
	        sim;
	        max_concurrent_tasks=2
	        )
		if thinktime_gamma
	    	user.thinktime_generator=generate_thinktime_gamma
		end

		adaptive_user = false
		if adaptive_fourth_user && user_id==4
			adaptive_user = true
		end

		if all_adaptive
			adaptive_user = true
		end
		
	    for m_task_id in 1:1
	        CompTask(sim, user.id;
	            task_split_schema = adaptive_user ? AdaptiveFactor : UserPreferred,
	            submit_time= arrive_same_time ? 1 : user_id+m_task_id,
	            nodetime=140, nodes_prefered=4, walltime_prefered=48)
	    end
	end
	
	run!(sim; run_till_no_jobs=true);
	
	sim
end

base = simulate(adaptive_fourth_user=false, thinktime_gamma=false);
base24 = simulate(adaptive_fourth_user=false, thinktime_gamma=true, max_time_per_job=24);
adaptive = simulate(adaptive_fourth_user=true, thinktime_gamma=false);
adaptive_gamma = simulate(adaptive_fourth_user=true, thinktime_gamma=true);
adaptive_gamma2 = simulate(adaptive_fourth_user=true, thinktime_gamma=true,rng=Random.Xoshiro(131));
adaptive_all = simulate(all_adaptive=true, thinktime_gamma=true);

#plotly(ticks=:native)
#"A. Prefered User and no Think Time"
#"B. Prefered User and no Think Time, max allowed requested wall-time reduced to 24 hours"
#"C. Fourth Adaptive User and no Think Time"
#"D. Fourth Adaptive User and Gamma Distribution for Think Time"
annotation_pointsize=8
p1=plot_node_util(base, ticks_step=0.5, annotation_pointsize=annotation_pointsize)
title!("A.", titleloc=:left)
p2=plot_node_util(base24, ticks_step=0.5, annotation_pointsize=annotation_pointsize)
title!("B.", titleloc=:left)
p3=plot_node_util(adaptive, ticks_step=0.5, annotation_pointsize=annotation_pointsize)
title!("C.", titleloc=:left)
p4=plot_node_util(adaptive_gamma,ticks_step=0.5, annotation_pointsize=annotation_pointsize)
title!("D.", titleloc=:left)
p5=plot_node_util(adaptive_gamma2,ticks_step=0.5, annotation_pointsize=annotation_pointsize)
title!("E.", titleloc=:left)
p6=plot_node_util(adaptive_all,ticks_step=0.5, annotation_pointsize=annotation_pointsize)
title!("F.", titleloc=:left)

plot(p1,p2,p3,p4,p5,p6,layout = (2, 3),size=(800,600), colorbar = false, left_margin = 5Plots.mm, bottom_margin = 5Plots.mm)

base_arrive_same_time = simulate(arrive_same_time=true, max_time_per_job=24);
adaptive_all_arrive_same_time = simulate(all_adaptive=true, arrive_same_time=true, thinktime_gamma=true, max_time_per_job=24);

p1=plot_node_util(base_arrive_same_time, ticks_step=0.5, annotation_pointsize=annotation_pointsize)
title!("A.", titleloc=:left)
p2=plot_node_util(adaptive_all_arrive_same_time, ticks_step=0.5, annotation_pointsize=annotation_pointsize)
title!("B.", titleloc=:left)

#savefig("node_util2.svg")
#savefig("node_util2.pdf")


