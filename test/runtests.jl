using Test
using LinkedEventsAnalysis

using CSV
using DataFrames
using SpineBasedRecordLinkage

################################################################################
# Functions

function empty_directory!(somedir::String)  # Empty directory but do not delete it
    contents = readdir(somedir)
    for x in contents
        rm(joinpath(somedir, x); recursive=true)
    end
end

function remove_newdirs!(newdirs)
    for newdir in newdirs
        rm(newdir; recursive=true)  # Empty the directory and delete it
        pop!(newdirs, newdir)
    end
end

function remove_newfiles!(newfiles)
    for newfile in newfiles
        !isfile(newfile) && continue
        rm(newfile)
        pop!(newfiles, newfile)
    end
end

################################################################################
# Script

# Define test directories
const lea_testdir  = normpath(joinpath(dirname(pathof(LinkedEventsAnalysis)), "..", "test"))     # /path/to/LinkedEventsAnalysis.jl/test
const sbrl_testdir = normpath(joinpath(dirname(pathof(SpineBasedRecordLinkage)), "..", "test"))  # /path/to/SpineBasedRecordLinkage.jl/test

# Keep track of new files and directories to be deleted after the tests are complete
const newdirs  = Set{String}()
const newfiles = Set{String}()

# Construct output directory if it doesn't already exist
newdir = joinpath(lea_testdir, "output")
if !isdir(newdir)
    mkdir(newdir)
    push!(newdirs, newdir)
end

# Test sets
empty_directory!(joinpath(lea_testdir, "output"))
include("test_eventchains.jl")
#remove_newdirs!(newdirs)
#remove_newfiles!(newfiles)
