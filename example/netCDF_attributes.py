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

netcdf_file = '~/Workspace/pyimclsts/lauv-xplore-5_2024_10_14.nc'
xrds_example = xr.open_dataset(netcdf_file)



print("** PLEASE ENTER CUSTOM GLOBAL ATTRIBUTES **")
emso_facility = input("Type emso facility name to use")
source = input("Type the source of this data: ")
network = input("Type the network associated with this data.")
title = input("Type the title of this data file: ")
summary = input("Type a brief summary of this data file: ")
project = input("Identify the project whose data this belongs to: ")
principal_investigator = input("Identify the principal investigator: ")
principal_investigator_email = input("Identify the previous person email: ")

xrds_example.global_attrs['emso_facility'] = emso_facility
xrds_example.global_attrs['network'] = network
xrds_example.global_attrs['title'] = title
xrds_example.global_attrs['summary'] = summary 
xrds_example.global_attrs['project'] = project
xrds_example.global_attrs['principal_investigator'] = principal_investigator
xrds_example.global_attrs['principal_investigator_email'] = principal_investigator_email

xrds_example.xrds.to_netcdf("lauv-xplore-5_2024_10_14(2).nc")

