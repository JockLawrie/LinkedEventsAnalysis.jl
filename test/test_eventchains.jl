# Create directories for linkage if they don't already exist
required_dirs = ["data", "schema"]  # Already have config and output directories
for d in required_dirs
    isdir(joinpath(lea_testdir, d)) && continue
    mkdir(joinpath(lea_testdir, d))
    push!(newdirs, joinpath(lea_testdir, d))
end

# Populate linkage directories with inputs from SpineBasedRecordLinkage/test

# Config
configfiles = ["link_all_health_service_events.yml"]
for configfile in configfiles
    newfile = joinpath(lea_testdir, "config", configfile)
    cp(joinpath(sbrl_testdir, "config", configfile), newfile; force=true)
    push!(newfiles, newfile)
end

# Schemata
schemafiles = ["spine.yml", "influenza_cases.yml", "emergency_presentations.yml", "hospital_admissions.yml"]
for schemafile in schemafiles
    newfile = joinpath(lea_testdir, "schema", schemafile)
    cp(joinpath(sbrl_testdir, "schema", schemafile), newfile; force=true)
    push!(newfiles, newfile)
end

# Data
datafiles = ["influenza_cases", "emergency_presentations", "hospital_admissions"]
for datafile in datafiles
    newfile = joinpath(lea_testdir, "data", "$(datafile).csv")
    cp(joinpath(sbrl_testdir, "data", "$(datafile).csv"), newfile; force=true)
    push!(newfiles, newfile)
end

# Run linkage
outdir1a = run_linkage(joinpath(lea_testdir, "config", "link_all_health_service_events.yml"))
outdir1a = joinpath(lea_testdir, outdir1a)

# Construct event chains
using YAML

configfile = joinpath(lea_testdir, "config", "event_chains.yml")
d          = YAML.load_file(configfile)
cp(outdir1a, d["linkagedir"]; force=true)
outdir1b   = construct_event_chains(configfile)
