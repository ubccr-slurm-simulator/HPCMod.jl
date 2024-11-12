## Prerequisites

Install Julia

Install Pluto, Julia notebook

```jl
import Pkg
Pkg.add("Pluto")
```

Install dependent libraries

```jl
using Pkg
Pkg.add("CSV")
Pkg.add("DataFrames")
Pkg.add("DataFramesMeta")
```


```jl
import Pkg
Pkg.add("IJulia")

import IJulia

IJulia.notebook()


using Pkg
Pkg.add("BenchmarkTools")
```

To get address

```bash
start_conda
nikolays@bumblebee:~$ conda activate xdmod-notebooks
jupyter notebook list
```


# Run examples

```bash
julia --project=.. ./simple1.jl
```


# Debug logging

```jl
    #init_log_level = Logging.min_enabled_level(Logging.current_logger())
    #Logging.disable_logging(Logging.Debug - 1)
    #Logging.disable_logging
    ENV["JULIA_DEBUG"] = "all"

    @info "ref_5jobs_1user_unordered START"
    @debug "ref_5jobs_1user_unordered (debug)"

    #Logging.disable_logging(init_log_level)
    @info "ref_5jobs_1user_unordered END"
    delete!(ENV, "JULIA_DEBUG")
```



```

    timeunits_per_day = sim.timeunits_per_day
    mdf = @chain sim.mdf begin
        @filter(used_nodes > 0)
        @summarise begin
            mean_used_nodes = mean(used_nodes)
            sd_used_nodes = std(used_nodes)
            max_time = maximum(time)
        end
        @mutate max_days = max_time / !!timeunits_per_day 
    end
```


# Constructor

```jl

MyType=BatchJob

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


```


# REg on Visuals
```
using VisualRegressionTests

@plottest plot_node_util(sim) "simple1_jl.png" !isci() 5e-3
```

```
using Plots
# savefig(plot_node_util(sim), "simple1_jl.png")
```