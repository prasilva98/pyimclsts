## If you already have all the data required to generate a netCDF file on a CSV
## You can use this script instead of exporting everything out of the log files from the vehicles 

from example.netCDF.utils import *
from example.netCDF.core import *
from datetime import datetime 
from scipy.constants import kilo, G, pi
from shapely import wkt
from shapely.geometry import Point, Polygon 
import pyimclsts.network as n
import pyimc_generated as pg
import argparse
import os
import sys
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import plotly.express as px

file_path = '~/Workspace/pyimclsts/outdata/lauv-xplore-5_2024_10_14'

# Now we create the actual netCDF file based on the name of the system
netCDF = netCDFExporter(file_path)
netCDF.build_netCDF()
netCDF.replace_json_metadata()
netCDF.to_netCDF()