module eventchains

export construct_event_chains

using CSV
using DataFrames
using Dates
using Logging
using Schemata
using YAML

function construct_event_chains(configfile::String)
    @info "$(now()) Configuring event chain construction"
    cfg = EventChainConfig(configfile)

    @info "$(now()) Initialising output directory: $(cfg.output_directory)"
    outdir = cfg.output_directory
    mkdir(outdir)
    mkdir(joinpath(outdir, "input"))
    mkdir(joinpath(outdir, "output"))
    cp(configfile, joinpath(outdir, "input", basename(configfile)))  # Copy config file to d/input

    @info "$(now()) Importing the links table"
    links = DataFrame(CSV.File(joinpath(cfg.linkagedir, "output", "links.tsv")))
    links[:, :EntityId] = UInt.(links[!, :EntityId])
    links[:, :EventId]  = UInt.(links[!, :EventId])
    select!(links, Not(:CriteriaId))  # Drop CriteriaId column
    sort!(links, [:TableName])

    @info "$(now()) Appending DateTime and EventTag to links table"
    append_tag_and_dttm!(links, cfg)

    @info "$(now()) Appending ChainId to links table"
    chainid2name_duration = append_chainid!(links, cfg.max_time_between_events)  # chainid => (name, duration)

    @info "$(now()) Constructing event chain definitions"
    x = construct_event_chain_definitions(chainid2name_duration, typeof(cfg.max_time_between_events))

    @info "$(now()) Exporting event chain definitions"
    CSV.write(joinpath(outdir, "output", "event_chain_definitions.tsv"), x; delim='\t')

    @info "$(now()) Exporting event chains"
    x = view(links, :, [:ChainId, :EventId])
    CSV.write(joinpath(outdir, "output", "event_chains.tsv"), x; delim='\t')

    @info "$(now()) Finished constructing event chains. Results are stored at:\n    $(outdir)"
    outdir
end

################################################################################
# Config

const registered_timeunits = Dict("day" => Day)

struct EventTableConfig
    datafile::String    # File containing the data of interest. Must have a primary key that matches that used during linkage.
    schemafile::String  # File containing the table's schema.
    tagcolumn::Symbol   # The column containing a brief description of each event. This enables us to easily understand the type of event.
    timestamp::Symbol   # The column containing each event's Date or DateTime}

    function EventTableConfig(datafile, schemafile, tagcolumn, timestamp)
        !isfile(datafile)   && error("Data file does not exist: $(datafile)")
        !isfile(schemafile) && error("Schema file does not exist: $(schemafile)")
        new(datafile, schemafile, tagcolumn, timestamp)
    end
end

EventTableConfig(d::Dict) = EventTableConfig(d["datafile"], d["schemafile"], Symbol(d["tagcolumn"]), Symbol(d["timestamp"]))

struct EventChainConfig{T <: Period}
    projectname::String
    description::String
    max_time_between_events::T  # Example: Day(30)
    output_directory::String
    linkagedir::String    
    event_tables::Dict{String, EventTableConfig}  # tablename => tableconfig

    function EventChainConfig(projectname, description, max_time_between_events, output_directory, linkagedir, event_tables)
        !isdir(dirname(output_directory)) && error("The directory containing the output directory does not exist: $(dirname(output_directory))")
        !isdir(linkagedir) && error("The linkage directory does not exist: $(linkagedir)")
        T = typeof(max_time_between_events)
        new{T}(projectname, description, max_time_between_events, output_directory, linkagedir, event_tables)
    end
end

function EventChainConfig(configfile::String)
    !isfile(configfile) && error("The config file $(configfile) does not exist.")
    d = YAML.load_file(configfile)
    EventChainConfig(d)
end

function EventChainConfig(d::Dict)
    projectname  = d["projectname"]
    description  = d["description"]
    maxgap       = construct_period(d["max_time_between_events"])
    dttm         = "$(round(now(), Second(1)))"
    dttm         = replace(dttm, "-" => ".")
    dttm         = replace(dttm, ":" => ".")
    outdir       = joinpath(d["output_directory"], "eventchains-$(projectname)-$(dttm)")
    linkagedir   = d["linkagedir"]
    event_tables = Dict{String, EventTableConfig}(k => EventTableConfig(v) for (k, v) in d["event_tables"])   
    EventChainConfig(projectname, description, maxgap, outdir, linkagedir, event_tables)
end

#=
"Returns path, corrected for the operating system."
function correctpath(path::String)
    ispath(path) && return path  # Already correct
    path_is_unixlike = !isnothing(findfirst(==('/'), path))
    sep = path_is_unixlike ? '/' : '\\'
    components = split(path, sep)
    Sys.iswindows() && return join(components, '\\')
    join(components, '/')
end
=#

"""
Example: gap = "30 days"
"""
function construct_period(gap::String)
    idx = findfirst(==(' '), gap)
    isnothing(idx) && error("Max time between events is mis-specified. Format should be \"\$n \$units\". E.g., \"30 days\".")
    T   = registered_timeunits[lowercase(gap[(idx+1):(idx+3)])]
    n   = parse(Int, gap[1:(idx-1)])
    n  <= 0 && error("The maximum time between events must be greater than 0 $(T)s.")
    T(n)  # Example: Day(30)
end

################################################################################

function append_tag_and_dttm!(links::DataFrame, cfg::EventChainConfig)
    n = size(links, 1)
    links[!, :DateTime] = missings(DateTime, n)
    links[!, :EventTag] = missings(String,   n)
    tablename = ""
    eventid2dttm_tag = nothing
    for i = 1:n
        # Update table if necessary
        new_tablename = links[i, :TableName]
        if new_tablename != tablename
            tablename = new_tablename
            eventid2dttm_tag = construct_eventid2dttm_tag(cfg.event_tables[tablename], cfg.linkagedir, tablename)
        end

        # Populate new columns of links table
        eventid = links[i, :EventId]
        !haskey(eventid2dttm_tag, eventid) && continue
        dttm, tag = eventid2dttm_tag[eventid]
        links[i, :DateTime] = dttm
        links[i, :EventTag] = tag
    end
end

function append_chainid!(links, maxgap::T) where {T <: Period}
    result = Dict{UInt, Tuple{String, Int}}()  # chainid => (name, duration)
    sort!(links, [:EntityId, :DateTime])
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
            ismissing(dttm) && continue
            dttm_prev = links[i - 1, :DateTime]
            gap = dttm - dttm_prev  # gap isa T <: Period
            if gap <= maxgap  # Event is in the same chain as the previous row
                cid = links[i - 1, :ChainId]
                nm, dur = result[cid]
                nm      = "$(nm) -> $(eventtag)"
                gapdur  = convert(T, gap)  # Convert from milliseconds to typeof(cfg.max_time_between_events)
                dur    += gapdur.value
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

function construct_eventid2dttm_tag(event_table_config::EventTableConfig, linkagedir::String, tablename::String)
    result       = Dict{UInt, Tuple{Union{Missing,DateTime}, Union{Missing,String}}}()
    dttm_colname = event_table_config.timestamp
    tag_colname  = event_table_config.tagcolumn
    tableschema  = readschema(event_table_config.schemafile)
    pk_cols      = tableschema.primarykey
    primarykey   = ["" for colname in pk_cols]
    primarykey2eventid = construct_primarykey2eventid(linkagedir, tablename, pk_cols)  # tuple(primarykey) => eventid
    for row in CSV.Rows(event_table_config.datafile; use_mmap=true, reusebuffer=true)
        populate_primarykey!(primarykey, row, pk_cols)
        pk      = Tuple(primarykey)
        eventid = primarykey2eventid[pk]        
        dttm    = getproperty(row, dttm_colname)
        dttm    = ismissing(dttm) ? missing : DateTime(dttm)
        tag     = getproperty(row, tag_colname)
        result[eventid] = (dttm, tag)
    end
    result
end

function construct_primarykey2eventid(linkagedir::String, tablename::String, pk_cols::Vector{Symbol})
    N          = length(pk_cols)
    result     = Dict{NTuple{N, String}, UInt}()
    datafile   = joinpath(linkagedir, "output", "$(tablename)_primarykey_and_eventid.tsv")
    primarykey = ["" for colname in pk_cols]
    for row in CSV.Rows(datafile; use_mmap=true, reusebuffer=true)
        populate_primarykey!(primarykey, row, pk_cols)
        pk      = Tuple(primarykey)
        eventid = parse(Float64, getproperty(row, :EventId))  # Example: "1.23456789" -> 1.23456789
        eventid = convert(UInt, eventid)
        result[pk] = eventid
    end
    result
end

function populate_primarykey!(primarykey, row, pk_cols)
    for (j, colname) in enumerate(pk_cols)
        primarykey[j] = getproperty(row, colname)
    end
end

function construct_event_chain_definitions(chainid2name_duration, T)
    n = length(chainid2name_duration)
    i = 0
    result = DataFrame(ChainId=Vector{UInt}(undef, n), ChainName=Vector{String}(undef, n))
    dur_units = lowercase(replace("$(T)", "Dates." => ""))  # Dates.Day => day
    durcol = Symbol("duration_$(dur_units)s")
    result[!, durcol] = Vector{Int}(undef, n)
    for (chainid, v) in chainid2name_duration
        i += 1
        result[i, :ChainId]   = chainid
        result[i, :ChainName] = v[1]
        result[i, durcol]     = v[2]
    end
    sort!(result, [:ChainId])
end

end
