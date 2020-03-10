module eventchains

export construct_event_chains

using CSV
using DataFrames
using Dates
using Logging
using YAML

function construct_event_chains(configfile::String)
    @info "$(now()) Configuring event chain construction"
    cfg = ChainsConfig(configfile)

    @info "$(now()) Initialising output directory: $(cfg.output_directory)"
    outdir = cfg.output_directory
    mkdir(outdir)
    mkdir(joinpath(outdir, "input"))
    mkdir(joinpath(outdir, "output"))
    cp(configfile, joinpath(outdir, "input", basename(configfile)))  # Copy config file to d/input

    @info "$(now()) Importing the links table"
    types = Dict(:TableName => String, :EntityId => UInt, :EventId => UInt, :CriteriaId => Int)
    links = DataFrame(CSV.File(cfg.links_table; types=types))
    select!(links, Not(:CriteriaId))  # Drop CriteriaId column

    @info "$(now()) Appending DateTime and EventTag to links table"
    append_tag_and_dttm!(links, cfg)

    @info "$(now()) Appending ChainId to links table"
    chainid2name_duration = append_chainid!(links, cfg)  # chainid => (name, duration)

    @info "$(now()) Constructing event chain definitions"
    x = sort!([(ChainId=k, ChainName=v[1], ChainDuration=v[2]) for (k,v) in chainid2name_duration], by=(x) -> x.ChainId)

    @info "$(now()) Exporting event chain definitions"
    CSV.write(joinpath(outdir, "output", "event_chain_definitions.tsv"), x)

    @info "$(now()) Exporting event chains"
    x = view(links, :, [:ChainId, :EventId])
    CSV.write(joinpath(outdir, "output", "event_chains.tsv"), x)

    @info "$(now()) Finished constructing event chains. Results are stored at:\n    $(outdir)"
    outdir
end

################################################################################
# Config

const registered_units = Dict("day" => Day)

struct ChainsConfig{T <: Period}
    projectname::String
    description::String
    output_directory::String
    links_table::String                 # filename
    event_tables::Dict{String, String}  # tablename => filename
    tags::Dict{String, Symbol}          # tablename => colname
    timestamps::Dict{String, Symbol}    # tablename => colname
    max_time_between_events::T          # Example: Day(30)
end

function ChainsConfig(configfile::String)
    !isfile(configfile) && error("The config file $(configfile) does not exist.")
    d = YAML.load_file(configfile)
    ChainsConfig(d)
end

function ChainsConfig(d::Dict)
    projectname  = d["projectname"]
    description  = d["description"]
    dttm         = "$(round(now(), Second(1)))"
    dttm         = replace(dttm, "-" => ".")
    dttm         = replace(dttm, ":" => ".")
    outdir       = joinpath(d["output_directory"], "eventchains-$(projectname)-$(dttm)")
    links_table  = d["links_table"]
    event_tables = d["event_tables"]
    tags         = Dict{String, Symbol}(tablename => Symbol(tag) for (tablename,tag) in d["tags"])
    timestamps   = Dict{String, Symbol}(tablename => Symbol(tag) for (tablename,tag) in d["timestamps"])
    gap    = d["criteria"]["time_window"]  # Example: "30 days"
    idx    = findfirst(==(" "), gap)
    isnothing(idx) && error("Max time between events is mis-specified. Format should be \"\$n \$units\". E.g., \"30 days\".")
    T = registered_units[lowercase(gap[(idx+1):(idx+3)])]
    n = parse(Int, gap[1:(idx-1)])
    maxgap = T(n)  # Example: Day(30)
    n <= 0 && error("The maximum time between events must be greater than 0 $(T)s.")
    ChainsConfig(projectname, description, outdir, links_table, event_tables, tags, timestamps, maxgap)
end

################################################################################

function append_tag_and_dttm!(links::DataFrame, cfg::ChainsConfig)
    n = size(links, 1)
    links[!, :DateTime] = missings(DateTime, n)
    links[!, :EventTag] = missings(String,   n)
    tablename = ""
    for i = 1:n
        # Update table if necessary
        new_tablename = links[i, :TableName]
        if new_tablename != tablename
            tablename = new_tablename
            eventid2dttm_tag = construct_eventid2dttm_tag(cfg, tablename)
        end

        # Populate new columns of links table
        dttm, tag = eventid2dttm_tag[links[i, :EventId]]
        links[i, :DateTime] = dttm
        links[i, :EventTag] = tag
    end
end

function append_chainid!(links, cfg)
    result = Dict{UInt, Tuple{String, Int}}()  # chainid => (name, duration)
    sort!(links, (:EntityId, :DateTime))
    n = size(links, 1)
    links[!, :ChainId]       = missings(UInt, n)
    links[!, :ChainDuration] = missings(Int,  n)
    chainid       = UInt(0)
    prev_entityid = UInt(0)
    for i = 1:n
        entityid = links[i, :EntityId]
        eventtag = "$(links[i, :TableName]).$(links[i, :EventTag])"
        if entityid != prev_entityid  # First event for this entity...start a new chain
            prev_entityid = entityid
            chainid += 1
            result[chainid]    = (eventtag, 0)
            links[i, :ChainId] = chainid
        else
            dttm = links[i, :DateTime]
            dttm_prev = links[i - 1, :DateTime]
            gap = dttm - dttm_prev  # gap isa T <: Period
            gap = gap.value         # gap isa Int
            if gap <= maxgap  # Event is in the same chain as the previous row
                cid = links[i - 1, :ChainId]
                nm, dur = result[cid]
                nm      = "$(nm) -> $(eventtag)"
                dur    += gap
                links[i, :ChainId] = cid
                result[cid] = (nm, dur)
            else              # Event is the start of a new chain
                chainid += 1
                result[chainid]    = (eventtag, 0)
                links[i, :ChainId] = chainid
            end
        end
    end
    result
end

function construct_eventid2dttm_tag(cfg, tablename::String)
    result       = Dict{UInt, Tuple{DateTime, String}}()
    dttm_colname = cfg.timestamps[tablename]
    tag_colname  = cfg.tags[tablename]
    for row in CSV.Rows(cfg.event_tables[tablename]; use_mmap=true, reusebuffer=true)
        eventid = parse(UInt, getproperty(row, :EventId))
        dttm    = DateTime(getproperty(row, dttm_colname))
        tag     = getproperty(row, tag_colname)
        result[eventid] = (dttm, tag)
    end
    result
end

end
