using GZip, Serialization, TarIterators, CodecZlib, CSV, DataFrames

# Variable names to retrieve
vx = Set([
    "LOGRECNO",
    "B01001_001",
    "B01001H_001",
    "B01001I_001",
    "B01001B_001",
    "B01001D_001",
])

# Produce a variable description entity for each file.
function varloc(vx)
    vm = Dict{Int,Any}()
    for f in readdir("templates")
        if !occursin("_e.csv.gz", f)
            continue
        end

        idx = split(f, "_")[1][4:end]
        idx = parse(Int, idx)

        GZip.open(joinpath("templates", f)) do io
            for (ii, row) in enumerate(CSV.File(io, header = false))
                if row[1] in vx
                    if !haskey(vm, idx)
                        vm[idx] = []
                    end
                    push!(vm[idx], (name = row[1], description = row[2], pos = ii))
                end
            end
        end
    end
    return vm
end

# Construct a DataFrame from the data accessible via 'io'.  Include all variables
# described in 'vd'.
function retrieve_data(io, vd)

    # The columns to retrieve
    jj = [x.pos for x in vd]

    # Get the data for the relevant variables.  The schema is assumed to be
    # a string (for LOGRECNO) followed by values that are missing or convertible
    # to floating point.
    da = []
    push!(da, String[])
    push!(da, [Union{Float64,Missing}[] for _ = 1:length(jj)-1]...)
    for (ii, row) in enumerate(CSV.File(io, header = false, type = String))
        for (k, j) in enumerate(jj)
            if k == 1
                push!(da[k], row[j])
            else
                push!(da[k], ismissing(row[j]) ? missing : parse(Float64, row[j]))
            end
        end
    end

    da = DataFrame(da, :auto)
    na = [x.name for x in vd]
    rename!(da, na)

    return da
end

# Get the logical record numbers corresponding to census tracts.
function get_tract_logrecs(geo)
    # A tract has a tract number but no block number
    lr = [r.LOGRECNO for r in eachrow(geo) if !ismissing(r.TRACT) && ismissing(r.BLOCK)]
    lrs = Set(lr)
    ii = [i for (i, x) in enumerate(geo.LOGRECNO) if x in lrs]
    return lr, geo[ii, :]
end

# Produce a dataframe for the given state 'st', containing the variables
# described in 'vm'.
function read_state(st, vm)

    # The number of distinct variables in the final result.
    ss = []
    for x in values(vm)
        push!(ss, [y.name for y in x]...)
    end
    nn = length(Set(ss))

    # dd contains the variable information, da contains the data,
    # geo contains geography information
    dxa, geo, logrecs = nothing, nothing, nothing

    open(joinpath("raw", "$(st).tar.gz")) do io

        # Loop over the components of the archive
        cio = GzipDecompressorStream(io)
        ti = TarIterator(cio)
        for (h, io) in ti

            # The first component of the archive is the geography file
            if isnothing(geo)
                geo = CSV.read(io, DataFrame, header = false, type = String)
                geo = geo[:, [2, 5, 10, 11, 14, 15]]
                rename!(geo, ["STATE", "LOGRECNO", "STATENO", "COUNTY", "TRACT", "BLOCK"])
                logrecs, geo = get_tract_logrecs(geo)
                continue
            end

            # Stop iterating once all the variables are obtained.
            if !isnothing(dxa) && (size(dxa, 2) == nn)
                break
            end

            # Not sure what these are but we aren't using them right now
            if startswith(h.path, "m")
                continue
            end

            # Get the sequence number for this file
            idx = h.path[10:12]
            idx = parse(Int, idx)

            # Skip if no relevant variables are in this file
            if !haskey(vm, idx)
                continue
            end

            dx = retrieve_data(io, vm[idx])

            # Restrict to the census tracts
            ii = [i for (i, x) in enumerate(dx[:, :LOGRECNO]) if x in logrecs]
            dx = dx[ii, :]

            if isnothing(dxa)
                dxa = dx
            else
                dxa = leftjoin(dxa, dx, on = :LOGRECNO)
            end
        end
    end

    # Merge in the geographical information and remove variables
    # that we no longer need.
    dxa = leftjoin(dxa, geo, on = :LOGRECNO)
    select!(dxa, Not(:BLOCK))

    dxa[!, :TRACT_FIPS] =
        ["$a$b$c" for (a, b, c) in zip(dxa[:, :STATENO], dxa[:, :COUNTY], dxa[:, :TRACT])]

    return dxa
end

# Loop over the state files
vm = varloc(vx)
for st in readdir("raw")

    if !endswith(st, ".tar.gz")
        continue
    end

    stx = split(st, ".")[1]

    println(st)
    da = read_state(stx, vm)

    s = replace(st, ".tar.gz" => ".csv.gz")
    CSV.write(joinpath("results", s), da)

end
