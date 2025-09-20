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
sandstoneComboCurveServiceAccount = os.getenv("SANDSTONE_COMBOCURVE_API_SEC_CODE")
sandstoneComboCurveApiKey = os.getenv("SANDSTONE_COMBOCURVE_API_KEY_PASS")

print("Begin Shalehaven ETL Process")

load_dotenv()  # load enviroment variables

# Paths to data
pathToAdmiralData = os.getenv("SHALEHAVEN_ADMIRAL_PATH")
pathToHuntData = os.getenv("SHALEHAVEN_HUNT_PATH")

# Get Wells From ComboCurve and Split by Operator
wells = combocurve.getWellsFromComboCurve(sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey)
huntWells = wells[wells['currentOperator'] == 'HUNT OIL COMPANY']
admiralWells = wells[wells['currentOperator'] == 'ADMIRAL PERMIAN OPERATING LLC']
aethonWells = wells[wells['currentOperator'] == 'AETHON ENERGY OPERATING LLC']
allWells = pd.concat([huntWells, admiralWells, aethonWells]) # merge huntWells with admiralWells and aethonWells

# Get & Format Production Data
admiralPermianProductionData = production.admiralPermianProductionData(pathToAdmiralData)
huntOilProductionData = production.huntOilProductionData(pathToHuntData,huntWells)

# Put Production Data to ComboCurve
combocurve.putDataComboCurve(admiralPermianProductionData,sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey)
combocurve.putDataComboCurve(huntOilProductionData,sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey)

# Get Daily Productions from ComboCurve for Shalehaven
dailyProductions = combocurve.getDailyProductionsFromComboCurve(sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey, allWells)

print("End Shalehaven ETL Process")
