using Serialization, CSV, GZip, Tables, DataFrames, Printf

tz = GZip.open("crosswalks/tract_zip.jls.gz") do io
	Serialization.deserialize(io)
end


function get_state(st)

	counts = Dict{String,Array{Float64,1}}()

	df = GZip.open(joinpath("results", "$(st).csv.gz")) do io
		CSV.read(io, DataFrame, types=Dict(:TRACT_FIPS=>String))
	end

	for r in Tables.namedtupleiterator(df)
		if !haskey(tz, r.TRACT_FIPS)
			continue
		end
		
		zip = tz[r.TRACT_FIPS]
		if !haskey(counts, zip)
			counts[zip] = [0, 0, 0, 0, 0]
		end

		counts[zip][1] += r.B01001_001
		counts[zip][2] += r.B01001D_001
		counts[zip][3] += r.B01001B_001
		counts[zip][4] += r.B01001I_001
		counts[zip][5] += r.B01001H_001
	end

	return counts
end

out = GZip.open("zip_race.csv.gz", "w")
write(out, "State,Zip,Total,Asian,Black,Hispanic,White\n")

for st in readdir("results")

	if !endswith(st, ".csv.gz")
		continue
	end
	
	stx = split(st, ".")[1]
	println(stx)
	
	counts = get_state(stx)

	for (zip,rec) in counts
		write(out, @sprintf("%s,%s,%.0f,%.0f,%.0f,%.0f,%.0f\n", stx, zip, rec[1], rec[2], rec[3], rec[4], rec[5]))
	end
end

close(out)
