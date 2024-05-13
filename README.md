# HPCMod - HPC Resources Modeling and Simulation Framework

[![CI](https://github.com/ubccr-slurm-simulator/HPCMod.jl/workflows/CI/badge.svg)](https://github.com/ubccr-slurm-simulator/HPCMod.jl/actions?query=workflow%3ACI)



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