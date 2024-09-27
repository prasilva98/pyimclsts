import geopandas.geodataframe
import pyimclsts.network as n
import pyimc_generated as pg
import json
import pandas as pd
import xarray as xr
import argparse
import os
import gzip
import shutil
import sys
import geopandas
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from scipy.constants import kilo, G, pi
from shapely import wkt
from shapely.geometry import Point, Polygon 
from datetime import datetime
import numpy as np

# Usefull for applying offset from meters to degrees
c_wgs84_a = 6378137.0
c_wgs84_b = 6356752.3142
c_wgs84_e2 = 0.00669437999013
c_wgs84_ep2 = 0.00673949674228
c_wgs84_f = 0.0033528106647475

csv_delimiter = '\x01'#'; '
json_delimiter = ', '

def computeRN(lat):

    lat_sin = np.sin(lat)
    return (c_wgs84_a / np.sqrt(1 - c_wgs84_e2 * (lat_sin**2)))

def toECEF(lat, lon, hae):

    cos_lat = np.cos(lat)
    sin_lat = np.sin(lat)
    cos_lon = np.cos(lon)
    sin_lon = np.sin(lon)
    
    # Compute the radious of earth at this given latitude
    rn = computeRN(lat)

    x = (rn + hae)*cos_lat*cos_lon
    y = (rn + hae)*cos_lat*sin_lon
    z = ( ( (1.0 - c_wgs84_e2) * rn) + hae) * sin_lat
    return x,y,z   

def fromECEF(x, y, z):

    p = np.sqrt(x**2 + y**2) 
    lon = np.arctan2(y, x)
    theta = np.arctan2(c_wgs84_a * z, p * c_wgs84_b)
    num = z + c_wgs84_ep2 * c_wgs84_b * np.pow(np.sin(theta), 3)
    den = p - c_wgs84_e2 * c_wgs84_a * np.pow(np.cos(theta), 3)
    lat = np.arctan2(num, den)
    hae = p / np.cos(lat) - computeRN(lat)

    return lat, lon, hae 

## Based on path parameter it will search for all possible .Data.lsf.gz files
def gather_log_paths(log_path):

    lsf_files = []
    
    try:
        print("## Gathering all paths to Data files ##")
        for root, dirs, files in os.walk(log_path):

            for file in files: 
                if file.endswith('Data.lsf.gz'):

                    full_path = os.path.join(root)
                    lsf_files.append(full_path)

    except OSError:

        print("Error While Looking for .Data.lsf.gz. {}".format(log_path))
    
    print("List of compressed files gathered: \n {}".format(lsf_files))

    return lsf_files
## Export all log files 
def export_logs(all_logs):
   
    try:
        print("## Exporting all log files ## \n {}".format(all_logs))

        for f in all_logs:
        
                # Open the compressed 
                comp_log = f + '/' + 'Data.lsf.gz'
                uncomp_log =  f + '/' + 'Data.lsf'

                # Check if given log was already decompressed 
                if os.path.isfile(uncomp_log):

                    print("File {} was already decompressed".format(f))
                    continue

                with gzip.open(comp_log, 'rb') as f_in:

                    # Decompress it
                    with open(uncomp_log, 'wb') as f_out:

                        shutil.copyfileobj(f_in, f_out)

    except OSError as e:
        
        print("Not able to read file: {} \n Error: {}".format(e, f))

## Concantenate all those given logs
def concatenate_logs(all_logs):

    print("### Concatenating all Logs ###")
    conc_log = os.curdir + '/' + 'Data.lsf'

    ## Concantenating log
    with open(conc_log, 'wb') as f_out:

        for f in all_logs: 
            data_file = f + '/' + 'Data.lsf'

            with open(data_file, 'rb') as f_in:
                shutil.copyfileobj(f_in, f_out)

# \copy lauvxplore1 from 'output.csv' delimiter E'\x01';
def tolist(msg_or_value) -> list:
    if isinstance(msg_or_value, pg.messages.IMC_message):
        fields = msg_or_value.Attributes.fields
        values = [tolist(getattr(msg_or_value, f)) for f in fields if tolist(getattr(msg_or_value, f)) is not None]
        
        return [[f, v] for (f, v) in zip(fields, values)]
    elif isinstance(msg_or_value, list):
        return [tolist(i) for i in msg_or_value]
    elif isinstance(msg_or_value, int) or isinstance(msg_or_value, float):
        return msg_or_value
    else: # ignore plaintext and rawdata
        return None

class logDataGatherer():
    
    def __init__(self, f : str) -> None:
        '''f is file name'''

        self.file_name = f
        
        ## This will have to be seperated 
        self.datatable = []
        self.temperature = []
        self.estimated_states = []
        self.sound_speed = []
        self.conductivity = []
        self.salinity = []
        self.turbidity = []
        self.chloro = []
        self.name = 'NoName'
        self.cols = []

        ## Usefull for parsing 
        self.sensor_ent = -1
    
    def update_temperature(self, msg, callback):

        time = msg._header.timestamp
        src_ent = msg._header.src_ent
        temp = [time, src_ent, msg.value]
        self.temperature.append(temp)
        
    def update_state(self, msg, callback):
        
        time = msg._header.timestamp
        if self.name == 'NoName':
            self.name = msg._header.src

        # Turn the roll, pitch, yaw into readable degrees
        phi = np.rad2deg(np.arctan2(np.sin(msg.phi), np.cos(msg.phi)))
        theta = np.rad2deg(np.arctan2(np.sin(msg.theta), np.cos(msg.theta)))
        psi = np.rad2deg(np.arctan2(np.sin(msg.psi), np.cos(msg.psi)))

        # Calculate the velocity over ground magniute (dont take into account z axis)
        ground_speed =  [msg.vx, msg.vy]
        ground_speed = np.linalg.norm(ground_speed)

        # Calculate course over ground
        course_ground = np.rad2deg(np.arctan2(msg.vy, msg.vx))
        
        """
        # Add offset to lat and lon values 
        lat = np.rad2deg(msg.lat) + np.rad2deg( (msg.y / earth_radius_meters) )
        lon = np.rad2deg(msg.lon) + np.rad2deg( (msg.x / earth_radius_meters) ) / np.cos(np.deg2rad(lat)) 
        """

        lat = msg.lat 
        lon =  msg.lon 
        hae = msg.height

        north_offset =  msg.x
        east_offset = msg.y
        down_offset = msg.z

        # Only apply offset if there is actually any
        if(north_offset != 0 or east_offset != 0 or down_offset != 0):

            # Translate WGSM coordinates to ECEF so we can add x,y displacement
            x_ecef, y_ecef, z_ecef = toECEF(lat,lon,hae)

            p = np.sqrt(msg.x**2 + msg.y**2)

            phi = np.arctan2(z_ecef, p)

            slon = np.sin(lon)
            clon = np.cos(lon)
            sphi = np.sin(phi)
            cphi = np.sin(phi)
            
            # Add the displacement
            x_ecef = x_ecef + (-slon*east_offset - clon*sphi*north_offset - clon*cphi*down_offset)
            y_ecef = y_ecef + (clon*east_offset - slon*sphi*north_offset -slon*cphi*down_offset)
            z_ecef = z_ecef + (cphi*north_offset - sphi*down_offset)
            
            lat, lon, hae = fromECEF(x_ecef, y_ecef, z_ecef)

        point = [time, np.rad2deg(lat), np.rad2deg(lon), msg.depth, phi, theta, psi, ground_speed, course_ground]
        self.estimated_states.append(point)

    def update_sound_speed(self, msg, callback):

        time = msg._header.timestamp
        sspeed = [time, msg.value]
        self.sound_speed.append(sspeed)

    def update_conductivity(self, msg, callback):

        time = msg._header.timestamp
        src_ent = msg._header.src_ent
        conductivity  = [time, src_ent, msg.value]
        self.conductivity.append(conductivity)

    # Turbidity may or may not exist depending on the vehicle
    def update_turbidity(self, msg, callback):

        time = msg._header.timestamp
        turbidity  = [time, msg.value]
        self.turbidity.append(turbidity)
    
    # Chlorophyll may also exist or not depending on the vehicle
    def update_chloro(self, msg, callback):

        time = msg._header.timestamp
        chloro = [time, msg.value]
        self.chloro.append(chloro)
    
    def update_salinity(self, msg, callback):
        
        time = msg._header.timestamp
        salinity = [time, msg.value]
        self.salinity.append(salinity)

    def create_dataframes(self):

        # Save the variables in a dataframe for easier parsing
        self.df_positions = pd.DataFrame(self.estimated_states, columns=['TIME', 'LATITUDE', 'LONGITUDE', 'DEPH', 'ROLL', 'PCTH', 'HDNG', 'APSA', 'APDA'])
        self.df_positions = self.df_positions.sort_values(by='TIME')

        self.df_temperatures = pd.DataFrame(self.temperature, columns=['TIME','SRC_ENT', 'TEMP'])
        self.df_temperatures = self.df_temperatures.sort_values(by='TIME')

        self.df_conductivity = pd.DataFrame(self.conductivity, columns=['TIME','SRC_ENT', 'CNDC'])
        self.df_conductivity = self.df_conductivity.sort_values(by='TIME')

        self.df_sound_speed = pd.DataFrame(self.sound_speed, columns=['TIME', 'SVEL'])
        self.df_sound_speed = self.df_sound_speed.sort_values(by='TIME')

        self.df_salinity = pd.DataFrame(self.salinity, columns=['TIME', 'PSAL'])
        self.df_salinity = self.df_salinity.sort_values(by='TIME')

        self.df_turbidity = pd.DataFrame(self.turbidity, columns=['TIME', 'TSED'])
        self.df_turbidity = self.df_turbidity.sort_values(by='TIME')

        self.df_chloro =  pd.DataFrame(self.chloro, columns=['TIME', 'CPWC'])
        self.df_chloro = self.df_chloro.sort_values(by='TIME')
 
    def merge_data(self):

        self.cols = ['TIME','LATITUDE', 'LONGITUDE', 'DEPH', 'ROLL', 'PCTH', 'HDNG', 'APSA', 'APDA', 'TEMP', 'CNDC', 'SVEL', 'PSAL']
        
        # Do a sanity check and look for the sensor gathering oceanographic data

        # Also merge data by lowest frequency data which seems to always be the sound speed variable
        if self.df_sound_speed.isnull().all().all():
            print("NO SOUND SPEED FOUND")

        if self.df_conductivity.isnull().all().all():
            print("NO CONDUCTIVITY FOUND")

        else:
            self.sensor_ent = self.df_conductivity.loc[1, 'SRC_ENT']
            self.df_all_data = pd.merge_asof(self.df_sound_speed, self.df_conductivity, on='TIME', 
                                            direction='nearest', suffixes=('_df1', '_df2'))

        if self.sensor_ent != -1:

            if self.df_temperatures.isnull().all().all():

                print("NO TEMMPERATURE FOUND")

            else:

                self.df_temperatures = self.df_temperatures[self.df_temperatures['SRC_ENT'] == self.sensor_ent]
                self.df_temperatures = self.df_temperatures.drop('SRC_ENT', axis=1)
                self.df_all_data = self.df_all_data.drop('SRC_ENT', axis=1)

                self.df_all_data = pd.merge_asof(self.df_all_data, self.df_temperatures, on='TIME', 
                                                direction='nearest', suffixes=('_df1', '_df2') )
        
        else:
            print("NO USABLE ENTITY FOR TEMPERATURE FOUND")

        if self.df_salinity.isnull().all().all():
            print("NO SALINITY FOUND")
        
        else:
            self.df_all_data = pd.merge_asof(self.df_all_data, self.df_salinity, on='TIME',
                                            direction='nearest', suffixes=('_df1', '_df2'))
        
        if self.df_chloro.isnull().all().all():
            print("NO CHLOROPHYLL FOUND")

        else:
            self.df_all_data = pd.merge_asof(self.df_all_data, self.df_chloro, on='TIME',
                                            direction='nearest', suffixes=('_df1', '_df2'))
            self.cols.append('CPWC')
            
        
        if self.df_turbidity.isnull().all().all():
            print("NO TURBIDITY FOUND")

        else:
            self.df_all_data = pd.merge_asof(self.df_all_data, self.df_turbidity, on='TIME',
                                             direction='nearest', suffixes=('_df1', '_df2'))
            self.cols.append('TSED')
        
        if self.df_positions.isnull().all().all():
            print("NO POSITIONS FOUND")

        else:
            self.df_all_data = pd.merge_asof(self.df_all_data, self.df_positions, on='TIME',
                                             direction='nearest', suffixes=('_df1', '_df2'))
        
        # Rearrange positions dataframe for better visibility
        self.df_all_data = self.df_all_data[self.cols]

        # Turn the normal dataframe into geopandas dataframe for easier filtering 
        self.df_all_data = geopandas.GeoDataFrame(self.df_all_data,
                                                geometry = geopandas.points_from_xy(self.df_all_data.LATITUDE, self.df_all_data.LONGITUDE))
        
    def filter_data(self, polygon = False, duration_limit=-1, depth_min = -1):
        
        self.df_all_data.sort_values(by='TIME')

        # Check the duration of the current data gathered
        if duration_limit != -1:

            duration = self.df_all_data['TIME'].max() - self.df_all_data['TIME'].min()
            
            # Duration of log is just too short so csv will not be created 
            if duration < duration_limit:

                return 0

        # If an area (polygon) a filtering should occur      
        if polygon:
            
            print("Checking if trajectory belongs within polygon {}".format(polygon))

            # GeoDataframe now comes in handy to remove points not belonging to the polygon provided
            self.df_all_data = self.df_all_data[self.df_all_data.geometry.within(polygon)]
            
            print(self.df_all_data)
            
        # Extract data above the given depth
        if depth_min != -1:

            print("Checking if depth values are above given limit of {}".format(depth_min))
            
            return -1

    def write_to_file(self):

        if not self.df_all_data.isnull().all().all():  
            
            # Before writing to file let's add some general metadata
            metadata = {
            'system' : self.name,
            'data_created' : datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            'time_coverage_start' : datetime.fromtimestamp(self.df_all_data['TIME'].min()),
            'time_coverage_end' : datetime.fromtimestamp(self.df_all_data['TIME'].max()),
            'geospatial_lat_min' : self.df_all_data['LATITUDE'].min(),
            'geospatial_lat_max' : self.df_all_data['LATITUDE'].max(),
            'geospatial_lon_min' : self.df_all_data['LONGITUDE'].min(),
            'geospatial_lon_max' : self.df_all_data['LONGITUDE'].max(),
            'geospatial_vertical_min' : self.df_all_data['DEPH'].min(),
            'geospatial_vertical_max' : self.df_all_data['DEPH'].max()
            }

            with pd.ExcelWriter(self.file_name, engine='xlsxwriter') as writer:

                print("Writing to {}".format(self.file_name))
                self.df_all_data.to_excel(writer, sheet_name='DATA', index=False)

                workbook = writer.book 
                metadata_sheet = workbook.add_worksheet('METADATA')

                for i, (key, value) in enumerate(metadata.items()):
                    metadata_sheet.write(0, i, key)
                    metadata_sheet.write(1, i, str(value))

        else:

            print("Dataframe {} was empty. NOT WRITING".format(self.file_name))
class netCDFExporter():

    def __init__(self, f:str) -> None:

        '''f is file name'''
        self.file_name = f
        self.data_attrs = {}
        self.coor_attrs = {}
        self.data_attrs = {}

        # Check for the metadata in the excel file
        with pd.ExcelFile(self.file_name, engine='openpyxl') as writer:
            
            # If it exists replace needed specific metadata
            if 'METADATA' in writer.sheet_names:
                
                self.data_df = pd.read_excel(writer, sheet_name='DATA')
                self.metadata_df = pd.read_excel(writer, sheet_name='METADATA')

            else:     
                print("Excel file has no Metadata")
                sys.exit()
        
        # Create a base xarray dataset
        self.xrds = xr.Dataset()
        # Load global attributes from json
        with open('metadata/global_attributes.json', 'r') as f:
            self.global_attrs = json.load(f)
        # Load coordinates attributes from json
        with open('metadata/var_dict.json', 'r') as f:
            self.coor_attrs = json.load(f)
        # Load data attributes from json
        with open('metadata/var_dict.json', 'r') as f:
            self.data_attrs = json.load(f)
        # Save it in the dataset attrbutes
        #self.xrds.attrs = global_attrs
    
                
    def replace_json_metadata(self):
        
        if not 'data_created' in self.global_attrs:

            print("Something is wrong with the format of the global_dict.json file")
        
        else: 
            
            self.system_name = self.metadata_df['system_name']
            self.global_attrs['data_created'] = self.metadata_df['data_created'].iloc[0]
            self.global_attrs['time_coverage_start'] = self.metadata_df['time_coverage_start'].iloc[0]
            self.global_attrs['time_coverage_end'] = self.metadata_df['time_coverage_end'].isloc[0]
            self.global_attrs['geospatial_lat_min'] = self.metadata_df['geospatial_lat_min'].isloc[0]
            self.global_attrs['geospatial_lat_max'] = self.metadata_df['geospatial_lat_max'].isloc[0]
            self.global_attrs['geospatial_lon_min'] = self.metadata_df['geospatial_lon_min'].isloc[0]
            self.global_attrs['geospatial_lon_max'] = self.metadata_df['geospatial_lon_max'].isloc[0]
            self.global_attrs['geospatial_vertical_min'] = self.metadata_df['geospatial_vertical_min'].isloc[0]
            self.global_attrs['geospatial_vertical_max'] = self.metadata_df['geospatial_vertical_max'].isloc[0]

    def build_netCDF(self):

        self.xrds.assign_coords(
            {
                'TIME' : ('TIME', pd.to_datetime(self.data_df['TIME'].unique(), unit='s') ),
                'DEPH' : ('DEPH', self.data_df['DEPH'].unique().astype(np.float64) ),
                'LATITUDE' : ('LATITUDE', self.data_df['LATITUDE'].unique().astype(np.float32)),
                'LONGITUDE' : ('LONGITUDE', self.data_df['LONGITUDE'].unique().astype(np.float32) )
            }
        )
        
        for data in self.data_df.columns:

            self.xrds[str(data)] = ('TIME', self.data_df[str(data)])

        print(self.xrds)

   
if __name__ == '__main__':

    # Parser for command line
    parser = argparse.ArgumentParser(description="Process arguments for Concatenation Script")

    # Minimum time argument
    parser.add_argument('-t','--min_time', type=int, default=2,
                        help="Minimum length of log (in min) to be used. Preset is 2 min")
    # Path to the mission argument
    parser.add_argument('-p', '--mission_path', type=str, default=os.getcwd(),
                        help="Specify path to the actual logs. Preset is your current location")
    # Start Time
    parser.add_argument('-s', '--start_time', type=str,
                        help="Logs should be after this daytime. Specify in HHMMSS format (ex: -s 130599). If empty logs the whole days are used.")
    
    # Area in the form of a polygon
    parser.add_argument('-a', '--area_polygon', type=float, nargs='+', default=False, 
                        help = "Input a list of points to define an area. Rules: \n" + 
                        "A polygon is required so at least 3 points should be entered. \n" +
                        "A odd number of points will result in an error" 
                        "Ex: lat1 lon1 lat2 lon2 lat3 lon3")
    
    # Add a boolean flag to force th deletion of data files
    parser.add_argument('--force', action='store_true', help="Call option if you want all previous data files to be deleted")
    
    # Parse the argument and save it 
    args = parser.parse_args()
    min_time= args.min_time
    mission_path = args.mission_path
    start_time = args.start_time
    points =  args.area_polygon 
    force = args.force
    
    if points:

        if (len(points) % 2 == 1):

            print("Number of points is odd. Please insert an even number of points")
            sys.exit()

        if (len(points)/2 < 4):

            print("Number of points is not enough to define a polygon. Please enter at least 4 points")
            sys.exit()
        
        point_tupple = []

        for i in range(0, len(points), 2):
            
            point_tupple.append( (points[i], points[i + 1]) )
        
        polygon = Polygon(point_tupple)
    else:
        polygon = points

    if start_time:
        if len(start_time) != 6:
            sys.exit()

    ## Find all Data.lsf.gz
    compressed_files_path = gather_log_paths(mission_path)
    compressed_files_path.sort()

    ## Decompress them 
    export_logs(compressed_files_path)

    # If the data files already exist, remove them
    checkable_files = []

    if(force):

        for path in compressed_files_path: 
            
            if os.path.isfile(path + '/Data.xlsx'):
                os.remove(path + '/Data.xlsx')

            checkable_files.append(path)

    # else, only go through data files without data xlsx
    else: 
        for path in compressed_files_path:

            if not os.path.isfile(path + '/Data.xlsx'):
                checkable_files.append(path)

    rejected_files = []

    ## Get needed data into xlsv file
    for path in checkable_files:
        
        logData = logDataGatherer(path + '/Data.xlsx')
        src_file = path + '/Data.lsf'

        try:

            # Connect to the actual file
            sub = n.subscriber(n.file_interface(input = src_file), use_mp=True)
            print("\nExporting {} to xlsv file".format(src_file))

            # Subscribe to specific variables and provide sub with a callback function
            sub.subscribe_async(logData.update_state, msg_id =pg.messages.EstimatedState)
            sub.subscribe_async(logData.update_temperature, msg_id =pg.messages.Temperature)
            sub.subscribe_async(logData.update_sound_speed, msg_id=pg.messages.SoundSpeed)
            sub.subscribe_async(logData.update_conductivity, msg_id=pg.messages.Conductivity)
            sub.subscribe_async(logData.update_salinity, msg_id=pg.messages.Salinity)
            sub.subscribe_async(logData.update_turbidity, msg_id=pg.messages.Turbidity)
            sub.subscribe_async(logData.update_chloro, msg_id=pg.messages.Chlorophyll)

            # Run the even loop (This is asyncio witchcraft)
            sub.run()

            # Go through the Entity info and check for the vehicle name
            for key in sub._peers.keys():
                if 'lauv' in key:
                    print("Log is coming from vehicle {} ".format(key))
                    logData.name = key
            
            if not 'lauv' in logData.name:
                print("No Vehicle found in EntityList.")
                print("Skipping this Log")
                continue

            # Create dataframes based on data collected from file
            logData.create_dataframes()
            # Merge that data into a single dataframe
            logData.merge_data()
            # Parse that data
            logData.filter_data(polygon)
            # Actually write to a csv file
            logData.write_to_file()

        except Exception as e:
            
            print("Something went wrong with {} \n ERROR: {}".format(src_file, e))
            rejected_files.append(path)

    if rejected_files:
        print("For whatever reason these files were not used: {}".format(rejected_files))

    # Remove rejected files from original file list
    for path in rejected_files:
        compressed_files_path.remove(path)

    # Now we concatenate all of the created excel files into a single one
    concat_data = geopandas.GeoDataFrame(logData.cols)

    for index, path in enumerate(compressed_files_path):
        
        print("Concatenating file: {}".format(path))

        logData = path + '/Data.xlsx'

        all_data = pd.read_excel(logData, sheet_name='DATA')
        concat_data = pd.concat([concat_data, all_data])

        if index == len(compressed_files_path) - 1:
            
            metadata_df = pd.read_excel(logData, sheet_name='METADATA')
            system_name = metadata_df['system'].iloc[0]

    concat_data.sort_values(by='TIME')

    with pd.ExcelWriter(os.getcwd() + '/' + '' +'data.xlsx', engine='xlsxwriter') as writer:

        # Before writing to file let's add some general metadata
        metadata = {
        'system' : system_name,
        'data_created' : datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        'time_coverage_start' : datetime.fromtimestamp(concat_data['TIME'].min()),
        'time_coverage_end' : datetime.fromtimestamp(concat_data['TIME'].max()),
        'geospatial_lat_min' : concat_data['LATITUDE'].min(),
        'geospatial_lat_max' : concat_data['LATITUDE'].max(),
        'geospatial_lon_min' : concat_data['LONGITUDE'].min(),
        'geospatial_lon_max' : concat_data['LONGITUDE'].max(),
        'geospatial_vertical_min' : concat_data['DEPH'].min(),
        'geospatial_vertical_max' : concat_data['DEPH'].max()
        }

        concat_data.to_excel(writer, sheet_name='DATA', index=False)
        
        workbook = writer.book 
        metadata_sheet = workbook.add_worksheet('METADATA')

        for i, (key, value) in enumerate(metadata.items()):
            metadata_sheet.write(0, i, key)
            metadata_sheet.write(1, i, str(value))

    concat_data['geometry'] = concat_data['geometry'].apply(wkt.loads)
    concat_data = geopandas.GeoDataFrame(concat_data, geometry='geometry')

    """
    plt.plot(concat_data['LONGITUDE'], concat_data['LATITUDE'], marker='None', linestyle='-', color='b')
    decimal_format = ticker.FormatStrFormatter('%.4f')

    plt.gca().yaxis.set_major_formatter(decimal_format)
    plt.gca().xaxis.set_major_formatter(decimal_format)
    plt.show()
    """

    # Now we create the actual netCDF file based on the name of the system

    netCDF = netCDFExporter(os.getcwd() + '/data.xlsx')
    netCDF.replace_json_metadata()
    netCDF.build_netCDF()
        
