# Set directories
linkedtestdir = normpath(joinpath(dirname(pathof(LinkedEventsAnalysis)), "..", "test"))     # /path/to/LinkedEventsAnalysis.jl/test
spinetestdir  = normpath(joinpath(dirname(pathof(SpineBasedRecordLinkage)), "..", "test"))  # /path/to/SpineBasedRecordLinkage.jl/test

# Run linkage
cd(spinetestdir)
outdir1a = run_linkage(joinpath(spinetestdir, "config", "link_all_health_service_events.yml"))
outdir1a = joinpath(spinetestdir, outdir1a)
cd(linkedtestdir)

# Append :EventId to the input event tables and copy input to linkedtestdir
filenames = ["influenza_cases", "emergency_presentations", "hospital_admissions"]
for filename in filenames
    d1 = DataFrame(CSV.File(joinpath(spinetestdir, "data", "$(filename).csv")))             # Columns: All columns except EventId
    d2 = DataFrame(CSV.File(joinpath(outdir1a, "output", "$(filename)_with_eventid.tsv")))  # Columns: EventId, primarykey...
    d2[:, :EventId] = UInt.(d2[!, :EventId])
    pk = copy(names(d2))
    splice!(pk, findfirst(==(:EventId), pk))
    d3 = join(d1, d2, on=pk, kind=:left)
    CSV.write(joinpath(linkedtestdir, "output", "$(filename).csv"), d3)
end
cp(joinpath(outdir1a, "output", "links.tsv"), joinpath(linkedtestdir, "output", "links.tsv"))
cleanup(joinpath(spinetestdir, "output"))

# Construct event chains
configfile = joinpath(pwd(), "config", "event_chains.yml")
outdir1b   = construct_event_chains(configfile)
