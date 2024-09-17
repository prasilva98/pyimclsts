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
        print("## Concatenation Start ##")
        for root, dirs, files in os.walk(log_path):

            for file in files: 
                if file.endswith('Data.lsf.gz'):

                    full_path = os.path.join(root)
                    lsf_files.append(full_path)

    except OSError:

        print("Error While Looking for .Data.lsf.gz. {}".format(log_path))

    return lsf_files
## Export all log files 
def export_logs(all_logs):

        
    try:
        print("## Exporting all log files ## \n {}".format(all_logs))
        for f in all_logs:
                # Open the compressed 
                comp_log = f + '/' + 'Data.lsf.gz'
                uncomp_log =  f + '/' + 'Data.lsf'
                print(comp_log)

                with gzip.open(comp_log, 'rb') as f_in:

                    f_in.read(1)
                    print("{} Sucessfully Opened ".format(os.path.basename(comp_log)))

                    # Decompress it
                    with gzip.open(uncomp_log, 'wb') as f_out:

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
## Parse all of those logs
def parse_logs(all_logs):

    print("Parsing the Log and Creating csf File")

    for f in all_logs:

        f_in = f + '/' + 'Data.lsf'
        src_file = f + '/' + 'output.csv'
        w = table(src_file)

        sub = n.subscriber(n.file_interface(input = src_file), use_mp=True)

        sub.subscribe_async(w.writetotable, msg_id =pg.messages.Temperature, src='lauv-noptilus-1', src_ent=None)
        sub.subscribe_async(w.update_state, msg_id =pg.messages.EstimatedState, src='lauv-noptilus-1', src_ent=None)

        sub.run()

        positions = pd.DataFrame(w.estimated_states, columns=['lat', 'lon', 'depth', 'timestamp'])
        values = pd.DataFrame(w.datatable, columns=['timestamp', 'message', 'src', 'src_ent','field', 'value'])

    
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

        self.datatable = []
        self.estimated_states = []
            
    def writetotable(self, msg, callback) -> str:
        
        time = msg._header.timestamp
        message_abbrev = msg.Attributes.abbrev
        src = msg._header.src
        src_ent = msg._header.src_ent
        
        data = tolist(msg)
        data = [[time, message_abbrev, src, src_ent, *d] for d in data] # i don't think it expects a list with more than 1 item.
        self.datatable += data

    def update_state(self, msg, callback):
        time = msg._header.timestamp
            
        point = [msg.lat, msg.lon, msg.depth, time]
        
        self.estimated_states.append(point)

    def write_to_file(self):
    
        positions = pd.DataFrame(self.estimated_states, columns=['lat', 'lon', 'depth', 'timestamp'])
        values = pd.DataFrame(self.datatable, columns=['timestamp', 'message', 'src', 'src_ent','field', 'value'])

        values.to_csv(self.file_name)
        positions.to_csv(self.file_name)
        
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
        with open('metadata/global_attributes', 'r') as f:
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

    csv_file = cdfFile()
    csv_file = table('output.csv')

    src_file = 'lauv_xplore_1_20170813_full.lsf'
    sub = n.subscriber(n.file_interface(input = src_file), use_mp=True)

    csv_file.write_to_file()


