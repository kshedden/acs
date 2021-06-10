using Downloads

mkpath("raw")

sn = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida",
      "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine",
      "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska",
      "Nevada", "NewHampshire", "NewJersey", "NewMexico", "NewYork", "NorthCarolina", "NorthDakota", "Ohio",
      "Oklahoma", "Oregon", "Pennsylvania", "RhodeIsland", "SouthCarolina", "SouthDakota", "Tennessee", "Texas",
      "Utah", "Vermont", "Virginia", "Washington", "WestVirginia", "Wisconsin", "Wyoming", "DistrictOfColumbia"]

# Base url for files
ba = "https://www2.census.gov/programs-surveys/acs/summary_file/2019/data/5_year_by_state"

for st in sn

	# Set up an empty tmp directory
	rm("raw/tmp", force=true, recursive=true)
	mkpath("raw/tmp")

	# Download the data for one state
	f = "$(st)_Tracts_Block_Groups_Only.zip"
	print("Downloading $f...")
    Downloads.download("$(ba)/$f", "raw/tmp/$f")
    print("done\n")

	# Unpack the zip archive
	cd("raw/tmp")
	run(`unzip $f`)

	# Remove the zip file
	rm(f)

	# Repack as a tar archive, putting the geography file first
	af = readdir(".")
	gd = [x for x in af if startswith(x, "e") || startswith(x, "m")]
	gc = [x for x in af if startswith(x, "g") && endswith(x, ".csv")]
	@assert length(gc) == 1
	pushfirst!(gd, gc[1])

	c = `tar -cvf ../$(st).tar $gd`
	run(c)
	cd("..")

	# Compress the tar archive
	c = `gzip -f $(st).tar`
	run(c)

	cd("..")

end

rm("raw/tmp", force=true, recursive=true)
