import xarray as xr
import json
import numpy as np
from datetime import datetime, timedelta 

# Some of the types in the original attributes are not compatible with python types. 
# This converts them to usable types
def convert_attrs_to_serializable(attrs):
    serializable_attrs = {}
    for key, value in attrs.items():
        if isinstance(value, (np.integer, np.floating)):  # Convert numpy int/float types to Python types
            serializable_attrs[key] = value.item()
        else:
            serializable_attrs[key] = value
    return serializable_attrs

xr.set_options(display_max_rows=30)

netcdf_file = 'in_data/IBMA_AUVXPLORE4_d01_20220917_to_20220918_v001.nc'
xrds_example = xr.open_dataset(netcdf_file)

## Save the base attributes 
with open('metadata/global_attributes.json', 'w') as f:
  # Save the base variables attributes
  json.dump(xrds_example.attrs, f, indent=4)

var_dict = {}
# Iterate through all data variables and save attributes
for var_name, var_data in xrds_example.data_vars.items():
  var_dict[var_name] = convert_attrs_to_serializable(var_data.attrs)

coor_dict = {}
for coor_name, coor_data in xrds_example.coords.items():
   coor_dict[coor_name] = coor_data.attrs

print(coor_dict)

## Load them back from the json file
with open('metadata/global_attributes.json', 'r') as f:
  global_attributes = json.load(f)

## Save preset data variables metadata
with open('metadata/var_dict.json', 'w') as f:
  json.dump(var_dict, f, indent=4) 

## Save the preset coordinates
with open('metadata/coor_dict.json', 'w') as f:
   json.dump(coor_dict, f, indent=4)

## Open it up again
with open('metadata/var_dict.json', 'r') as f:
   var_attrs = json.load(f)

# Now lets store those attributes and each existing variables 
start_date = datetime(2063, 9, 1, 10, 30)
delta = timedelta(days=1)
num_dates = 5

datetime_list = [start_date + i * delta for i in range(num_dates)]
depth = []
lat = []
lon = []

temp = [21.5, 23, 18.6, 15.2, 17]
pressure = [300, 400, 500, 600, 900]
xrds = xr.Dataset(
    coords = {
        'TIME': datetime_list,
        'DEPH': depth, 
        'LATITUDE' : lat,
        'LONGITUDE' : lon
    },
    data_vars = 
    {
      var_name: ("time",np.full(len(pressure), np.nan)) for var_name, var_data in var_dict.items()
    },
    attrs = global_attributes
)

for var_name, var_data in var_dict.items():
   xrds[var_name].attrs = var_data

print(var_dict)

for coor_name, coor_data in coor_dict.items():
   xrds[coor_name].attrs = coor_data

print(xrds_example['PSAL'].attrs)

