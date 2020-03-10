using Test
using LinkedEventsAnalysis

using CSV
using DataFrames
using SpineBasedRecordLinkage

const outdir = joinpath(pwd(), "output")  # NOTE: pwd() is /path/to/LinkedEventAnalysis.jl/test/

if !isdir(outdir)
    mkdir(outdir)
end

function cleanup()
    contents = readdir(outdir)
    for x in contents
        rm(joinpath(outdir, x); recursive=true)
    end
end

# Test sets
cleanup()
include("test_eventchains.jl")
#cleanup()
