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

# Append :EventId to the input event tables and copy input to linkedtestdir
for datafile in datafiles
    d1 = DataFrame(CSV.File(joinpath(lea_testdir, "data",   "$(datafile).csv")))               # Columns: All columns except EventId
    d2 = DataFrame(CSV.File(joinpath(outdir1a,    "output", "$(datafile)_with_eventid.tsv")))  # Columns: EventId, primarykey...
    d2[:, :EventId] = UInt.(d2[!, :EventId])
    pk = copy(names(d2))
    splice!(pk, findfirst(==(:EventId), pk))
    d3 = join(d1, d2, on=pk, kind=:left)
    newfile = joinpath(lea_testdir, "output", "$(datafile).csv")
    CSV.write(newfile, d3)
    push!(newfiles, newfile)
end
newfile = joinpath(lea_testdir, "output", "links.tsv")
cp(joinpath(outdir1a, "output", "links.tsv"), newfile; force=true)
push!(newfiles, newfile)

# Construct event chains
configfile = joinpath(lea_testdir, "config", "event_chains.yml")
outdir1b   = construct_event_chains(configfile)
