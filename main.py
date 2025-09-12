## Shalehaven Main Scripts
## Developed by Michael Tanner

# Imports - SHEM Scripts
import shalehavenscripts.production as production
import shalehavenscripts.combocurve as combocurve

# Imports - General
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv

print("Begin Shalehaven ETL Process")

load_dotenv()  # load enviroment variables

# Paths to data
pathToAdmiralData = os.getenv("SHALEHAVEN_ADMIRAL_PATH")
pathToHuntData = os.getenv("SHALEHAVEN_HUNT_PATH")

# Get & Format Production Data
admiralPermianProductionData = production.admiralPermianProductionData(pathToAdmiralData)
huntOilProductionData = production.huntOilProductionData(pathToHuntData)

x = 5
