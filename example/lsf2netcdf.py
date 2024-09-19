import pyimclsts.network as n
import pyimc_generated as pg
import json
import pandas as pd
import numpy as np
import xarray as xr
import argparse
import os
import gzip
import shutil
import sys

csv_delimiter = '\x01'#'; '
json_delimiter = ', '

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
        
class table():
    
    def __init__(self, f : str) -> None:
        '''f is file name'''

        self.file_name = f
        # erases the file if it exists, creates it otherwise
        with open(f, 'w'):
            pass
        
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

        ## Usefull for parsing 
        self.sensor_ent = -1
    
    def get_system_name(self, msg, callback):
        
        if self.name == 'NoName':
            self.name = msg.sys_name

    def update_temperature(self, msg, callback):

        time = msg._header.timestamp
        src_ent = msg._header.src_ent
        temp = [time, src_ent, msg.value]
        self.temperature.append(temp)
        
    def update_state(self, msg, callback):
        
        time = msg._header.timestamp

        # Turn the roll, pitch, yaw into readable degrees
        phi = np.rad2deg(np.arctan2(np.sin(msg.phi), np.cos(msg.phi)))
        theta = np.rad2deg(np.arctan2(np.sin(msg.theta), np.cos(msg.theta)))
        psi = np.rad2deg(np.arctan2(np.sin(msg.psi), np.cos(msg.psi)))

        # Calculate the velocity over ground magniute (dont take into account z axis)
        ground_speed =  [msg.vx, msg.vy]
        ground_speed = np.linalg.norm(ground_speed)

        # Calculate course over ground
        course_ground = np.rad2deg(np.arctan2(msg.vy, msg.vx))

        point = [time, msg.lat, msg.lon, msg.depth, phi, theta, psi, ground_speed, course_ground]
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

    
    def merge_data(self):

        cols = ['TIME','LATITUDE', 'LONGITUDE', 'DEPH', 'ROLL', 'PCTH', 'HDNG', 'APSA', 'APDA', 'TEMP', 'CNDC', 'SVEL', 'PSAL']
        
        # Do a sanity check and look for the sensor gathering oceanographic data
        # Also merge data by lowest frequency data 

        if self.df_sound_speed.isnull().all().all():
            print("No Sound Speed values found in {}".format(self.file_name))
  
        if self.df_conductivity.isnull().all().all():
            print("No Conductity values found in {}".format(self.file_name))

        else:
            self.sensor_ent = self.df_conductivity.loc[1, 'SRC_ENT']
            self.df_all_data = pd.merge_asof(self.df_sound_speed, self.df_conductivity, on='TIME', 
                                            direction='nearest', suffixes=('_df1', '_df2'))

        if self.sensor_ent != -1:

            if self.df_temperatures.isnull().all().all():

                print("No Temperature values found in {}".format(self.file_name))

            else:

                self.df_temperatures = self.df_temperatures[self.df_temperatures['SRC_ENT'] == self.sensor_ent]
                self.df_temperatures = self.df_temperatures.drop('SRC_ENT', axis=1)
                self.df_all_data = self.df_all_data.drop('SRC_ENT', axis=1)

                self.df_all_data = pd.merge_asof(self.df_all_data, self.df_temperatures, on='TIME', 
                                                direction='nearest', suffixes=('_df1', '_df2') )
        
        else:
            print("No usable entity found for temperature data on {}".format(self.file_name))

        if self.df_salinity.isnull().all().all():
            print("No Salinity values found in {}".format(self.file_name))
        
        else:
            self.df_all_data = pd.merge_asof(self.df_all_data, self.df_salinity, on='TIME',
                                            direction='nearest', suffixes=('_df1', '_df2'))
        
        if self.df_chloro.isnull().all().all():
            print("No Chlorophyll values found in {}".format(self.file_name))

        else:
            self.df_all_data = pd.merge_asof(self.df_all_data, self.df_chloro, on='TIME',
                                            direction='nearest', suffixes=('_df1', '_df2'))
            cols.append('CPWC')
            
        
        if self.df_turbidity.isnull().all().all():
            print("No Turbidity values found in {}".format(self.file_name))

        else:
            self.df_all_data = pd.merge_asof(self.df_all_data, self.df_turbidity, on='TIME',
                                             direction='nearest', suffixes=('_df1', '_df2'))
            cols.append('TSED')
        
        if self.df_positions.isnull().all().all():
            print("No Positions values found in {}".format(self.file_name))

        else:
            self.df_all_data = pd.merge_asof(self.df_all_data, self.df_positions, on='TIME',
                                             direction='nearest', suffixes=('_df1', '_df2'))
        
        # Rearrange positions dataframe for better visibility
        self.df_all_data = self.df_all_data[cols]
        print(self.df_all_data)

        
    def write_to_file(self):
        
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

        self.merge_data()

        with pd.ExcelWriter(self.file_name, engine='xlsxwriter') as writer:

            self.df_all_data.to_excel(writer, sheet_name='DATA')


            
class cdfFile():

    def __init__(self, f : str) -> None:

        '''f is file name'''
        self.file_name = f
        self.data_attrs = {}
        self.coor_attrs = {}
        self.data_attrs = {}
        # erases the file if it exists, creates it otherwise
        with open(f, 'w'):
            pass
        
        # Create a base xarray dataset
        self.xrds = xr.Dataset
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
    
    def start_saving_data():

        print("Start saving data")


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
    # List of Plan IDs
    parser.add_argument('-id', '--list_ids', nargs='+',
                        help="List of plan_ids to use Example (-id cmd plan_id). If empty use all of them")

    # Parse the argument and save it 
    args = parser.parse_args()
    min_time= args.min_time
    mission_path = args.mission_path
    start_time = args.start_time
    id_list = args.list_ids

    if start_time:
        if len(start_time) != 6:
            sys.exit()

    ## Find all Data.lsf.gz
    compressed_files_path = gather_log_paths(mission_path)

    compressed_files_path.sort()

    ## Decompress them 
    export_logs(compressed_files_path)
    
    ## Get needed data into xlsv file
    for path in compressed_files_path:
        
        csv_file = table(path + '/Data.xlsx')
        src_file = path + '/Data.lsf'
        #csv_file = table('in_data/output.xlsx')
        #src_file = 'in_data/Data.lsf'
        
        try:

            # Connect to the actual file
            sub = n.subscriber(n.file_interface(input = src_file), use_mp=True)
            print("Exporting {} to xlsv file".format(src_file))

            # Subscribe to specific variables and provide sub with a callback function
            sub.subscribe_async(csv_file.update_state, msg_id =pg.messages.EstimatedState)
            sub.subscribe_async(csv_file.update_temperature, msg_id =pg.messages.Temperature)
            sub.subscribe_async(csv_file.update_sound_speed, msg_id=pg.messages.SoundSpeed)
            sub.subscribe_async(csv_file.update_conductivity, msg_id=pg.messages.Conductivity)
            sub.subscribe_async(csv_file.update_salinity, msg_id=pg.messages.Salinity)
            sub.subscribe_async(csv_file.update_turbidity, msg_id=pg.messages.Turbidity)
            sub.subscribe_async(csv_file.update_chloro, msg_id=pg.messages.Chlorophyll)

            # Run the even loop (This is asyncio witchcraft)
            sub.run()

            # Save all the info into csv files for later parsing
            csv_file.write_to_file()

        except KeyError as e:
            print("Something went wrong with {} \n ERROR: {}".format(src_file, e))


