## Shalehaven Main AFE Scripts
## Developed by Michael Tanner

# Imports - SHEM Scripts
import shalehavenscripts.afe as afe

# Imports - General
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import warnings

# disable `SettingWithCopyWarning
pd.options.mode.chained_assignment = None  # default='warn'

# disable future and user warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

# path to AFE data
pathToAfe = os.getenv("AFE_PATH")

print("Begin Shalehaven AFE ETL Process")

load_dotenv()  # load enviroment variables

afeData = afe.combineAfeData(pathToAfe)

print("Shalehaven AFE ETL Process Complete")