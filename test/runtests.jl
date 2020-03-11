using Test
using LinkedEventsAnalysis

using CSV
using DataFrames
using SpineBasedRecordLinkage

const outdir = joinpath(pwd(), "output")  # NOTE: pwd() is /path/to/LinkedEventAnalysis.jl/test/

!isdir(outdir) && mkdir(outdir)

function cleanup(somedir::String)
    contents = readdir(somedir)
    for x in contents
        rm(joinpath(somedir, x); recursive=true)
    end
end

# Test sets
cleanup(outdir)
include("test_eventchains.jl")
cleanup(outdir)
