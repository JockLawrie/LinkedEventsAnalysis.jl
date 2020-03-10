# Set directories
thisdir  = pwd()  # /path/to/LinkedEventsAnalysis.jl/test
spinedir = normpath(joinpath(dirname(pathof(SpineBasedRecordLinkage)), ".."))  # /path/to/SpineBasedRecordLinkage.jl

# Run linkage
cd(spinedir)
outdir1a = run_linkage(joinpath(spinedir, "test", "config", "link_all_health_service_events.yml"))
cd(thisdir)

# Copy results to test/output
for filename in readdir(joinpath(outdir1a, "output"))
    cp(joinpath(outdir1a, "output", filename), joinpath(outdir, filename))
end

# Copy input event tables to test/output
filenames = ["influenza_cases.csv", "emergency_presentations.csv", "hospital_admissions.csv"]
for filename in filenames
    cp(joinpath(spinedir, "test", "data", filename), joinpath(outdir, filename))
end

# Append :EventId to the event tables
for rawfile in filenames
    filename = "$(splitext(rawfile)[1])_with_eventid.tsv"
    data0    = DataFrame(CSV.File(rawfile))
    data1    = DataFrame(CSV.File(filename))
    data1[:, :EventId] = UInt.(data1[!, :EventId])
    pk       = copy(names(data1))
    splice!(pk, findfirst(==(:EventId), pk))
    data2    = join(data0, data1, on=pk, kind=:left)
    CSV.write(joinpath(outdir, rawfile), data2)
end

# Construct event chains
configfile = joinpath(pwd(), "config", "event_chains.yml")
outdir1b   = construct_event_chains(configfile)
