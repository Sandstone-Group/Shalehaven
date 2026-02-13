## Shalehaven Main AFE Scripts
## Developed by Michael Tanner

# Imports - SHEM Scripts
import shalehavenscripts.los as los

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
pathToAfe2025 = os.getenv("SHALEHAVEN_AFE_2025_PATH")
pathToJib = os.getenv("SHALEHAVEN_JIB_PATH")


runAfe = False # set to true to run the AFE ETL process, set to false to skip the AFE ETL process
runJib = False # set to true to run the JIB ETL process, set to false to skip the JIB ETL process

print("Begin Shalehaven AFE ETL Process")

load_dotenv()  # load enviroment variables

# if runAFE is true, run the AFE ETL process
if runAfe:
    afeData = los.combineAfeData(pathToAfe2025)
else:
    print("Skipping AFE ETL Process")

# if runJib is true, run the JIB ETL process
if runJib:
    jibData = los.combineJibData(pathToJib)
else:
    print("Skipping JIB ETL Process")
    


print("Shalehaven AFE ETL Process Complete")