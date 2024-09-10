import xarray as xr
import json

xr.set_options(display_max_rows=30)

netcdf_file = 'in_data/IBMA_AUVXPLORE4_d01_20220917_to_20220918_v001.nc'
xrds_example = xr.open_dataset(netcdf_file)

## Save the base attributes 
with open('metadata/global_attributes.json', 'w') as f:
  json.dump(xrds_example.attrs, f, indent=4)

## Load them back from the json file
with open('metadata/global_attributes.json', 'r') as f:
  global_attributes = json.load(f)

print(global_attributes)

time = [0,1,2,3,4,5,6,7,8,9]

xrds = xr.Dataset(
    coords = {
        'TIME': time
    },
    attrs = global_attributes
)
print(xrds)