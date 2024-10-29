# HPCMod - HPC Resources Modeling and Simulation Framework

[![CI](https://github.com/ubccr-slurm-simulator/HPCMod.jl/workflows/CI/badge.svg)](https://github.com/ubccr-slurm-simulator/HPCMod.jl/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/ubccr-slurm-simulator/HPCMod.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/ubccr-slurm-simulator/HPCMod.jl)


## Installation

Install [julia](https://julialang.org/) on your system.

Get the HPCMod code:

```bash
# Get code
git clone https://github.com/ubccr-slurm-simulator/HPCMod.jl HPCMod
# Get to HPCMod directory
cd HPCMod
# start julia
julia
```

In the julia-console, run some examples and tests:

```jl
# activate current directory as project
using Pkg
Pkg.activate(".")

# run tests (on first run it would take some time to install and precompile packages)
Pkg.test()

# Run examples
include("./examples/simple_job_trace_replay.jl")

```

Output for `simple_job_trace_replay.jl` example:

```
Simple job-trace_replayExecuting following jobs:
6×4 DataFrame
 Row │ submit_time  user_id  nodes  walltime 
     │ Int64        Int64    Int64  Int64
─────┼───────────────────────────────────────
   1 │           2        1      2         2
   2 │           3        2      2         2
   3 │           4        2      3         3
   4 │           5        2      2         2
   5 │           5        1      2         2
   6 │           6        1      1         3
Without backfiller:
14×5 DataFrame
 Row │ t      N0001  N0002  N0003  N0004 
     │ Int64  Int64  Int64  Int64  Int64
─────┼───────────────────────────────────
   1 │     0      0      0      0      0
   2 │     1      0      0      0      0
   3 │     2      1      1      0      0
   4 │     3      1      1      2      2
   5 │     4      0      0      2      2
   6 │     5      3      3      3      0
   7 │     6      3      3      3      0
   8 │     7      3      3      3      0
   9 │     8      5      5      4      4
  10 │     9      5      5      4      4
  11 │    10      6      0      0      0
  12 │    11      6      0      0      0
  13 │    12      6      0      0      0
  14 │    13      0      0      0      0
With backfiller:
12×5 DataFrame
 Row │ t      N0001  N0002  N0003  N0004 
     │ Int64  Int64  Int64  Int64  Int64
─────┼───────────────────────────────────
   1 │     0      0      0      0      0
   2 │     1      0      0      0      0
   3 │     2      1      1      0      0
   4 │     3      1      1      2      2
   5 │     4      0      0      2      2
   6 │     5      3      3      3      0
   7 │     6      3      3      3      6
   8 │     7      3      3      3      6
   9 │     8      5      5      0      6
  10 │     9      5      5      4      4
  11 │    10      0      0      4      4
  12 │    11      0      0      0      0
The tables above show nodes occupience by job at a given time.
0-means no jobs running on that node, otherwise show job id.
```

For actual work Visual Studio Code, Jupyter Notebook and Pluto Notebook are suggested.

## Exmaples

For examples see [examples](examples/) 

