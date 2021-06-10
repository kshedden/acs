using XLSX, GZip, DataFrames, CSV

rm("templates", force=true, recursive=true)
mkpath("templates")

# Download the template file
f = "2019_5yr_Summary_FileTemplates.zip"
c = `wget https://www2.census.gov/programs-surveys/acs/summary_file/2019/data/$f -O templates/$f`
run(c)

cd("templates")

c = `unzip $f`
run(c)

# Process a template for each sequence file.
for f in readdir(".")

	if !startswith(f, "seq")
		continue
	end

	xf = XLSX.readxlsx(f)
	for b in ["e", "m"]
		z = xf[b][:]

		# Create a 2-column dataframe, containing variable name, variable description.
		tz = [String[], String[]]
		for i in 1:length(z[1,:])
		    push!(tz[1], z[1, i])
		    push!(tz[2], z[2, i])
		end
		tz = DataFrame(tz, :auto)

		GZip.open(replace(f, ".xlsx"=>"_$(b).csv.gz"), "w") do io
			CSV.write(io, tz, header=false)
		end
	end

	rm(f)
end

# Clean up
for f in readdir(".")
    if !startswith(f, "seq")
    	rm(f)
    end
end

cd("..")
