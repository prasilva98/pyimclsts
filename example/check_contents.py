import xarray

netcdf_file = '~/Workspace/pyimclsts/lauv-xplore-5_2024_10_14.nc'
neptus_netcdf_file = '~/Workspace/Missions/lauv-xplore-5/20241014/163449_soi_plan/mra/netcdf/pmel-20241015T054841.000-lauv-xplore-5.nc'

dataset = xarray.open_dataset(netcdf_file)

print(dataset.variables)