using XLSX, Serialization, GZip

# Download the tract to zip crosswalk from 
#   https://www.huduser.gov/portal/datasets/usps_crosswalk.html
# and put it into a subdirectory called "crosswalks"

xf = XLSX.readxlsx("crosswalks/TRACT_ZIP_032021.xlsx")

cw = xf["TRACT_ZIP_032021"][:]

# Make a map from tracts to zip codes.
tz = Dict{String,String}()
for i = 2:size(cw, 1)
    tz[cw[i, 1]] = cw[i, 2]
end

GZip.open("crosswalks/tract_zip.jls.gz", "w") do io
    Serialization.serialize(io, tz)
end
