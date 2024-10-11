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
print(xrds_example)
