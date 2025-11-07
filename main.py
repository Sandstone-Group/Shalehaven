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
import warnings

# disable `SettingWithCopyWarning
pd.options.mode.chained_assignment = None  # default='warn'

# disable future and user warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

sandstoneComboCurveServiceAccount = os.getenv("SANDSTONE_COMBOCURVE_API_SEC_CODE")
sandstoneComboCurveApiKey = os.getenv("SANDSTONE_COMBOCURVE_API_KEY_PASS")
shalehavenProjectId = os.getenv("SHALEHAVEN_PROJECT_ID")
shalehavenForcastIdUpdatedTypeCurve = os.getenv("SHALEHAVEN_FORCAST_ID_UPDATED_TYPE_CURVE")
shalehavenForcastIdOriginalTypeCurve = os.getenv("SHALEHAVEN_FORCAST_ID_ORIGINAL_TYPE_CURVE")

print("Begin Shalehaven ETL Process")

load_dotenv()  # load enviroment variables

# Paths to data
pathToAdmiralData = os.getenv("SHALEHAVEN_ADMIRAL_PATH")
pathToHuntData = os.getenv("SHALEHAVEN_HUNT_PATH")
pathToAethonData = os.getenv("SHALEHAVEN_AETHON_PATH")
pathToDevonData = os.getenv("SHALEHAVEN_DEVON_PATH")
pathToDatabase = os.getenv("SANDSTONE_DATABASE_PATH")

# Get Wells From ComboCurve and Split by Operator
wells = combocurve.getWellsFromComboCurve(sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey)
huntWells = wells[wells['currentOperator'] == 'HUNT OIL COMPANY']
admiralWells = wells[wells['currentOperator'] == 'ADMIRAL PERMIAN OPERATING LLC']
aethonWells = wells[wells['currentOperator'] == 'AETHON ENERGY OPERATING LLC']
devonWells = wells[wells['currentOperator'] == 'DEVON ENERGY PRODUCTION COMPANY LP']
fundWells = pd.concat([huntWells, admiralWells, aethonWells, devonWells]) # merge huntWells with admiralWells, devonWells and aethonWells

# print fundWells to database
fundWells.to_excel(os.path.join(pathToDatabase, r"fundWells.xlsx"))
wells.to_excel(os.path.join(pathToDatabase, r"allWells.xlsx"))

# Get & Format Production Data
admiralPermianProductionData = production.admiralPermianProductionData(pathToAdmiralData)
huntOilProductionData = production.huntOilProductionData(pathToHuntData,huntWells)
aethonProductionData = production.aethonProductionData(pathToAethonData)
devonProductionData = production.devonProductionData(pathToDevonData)

# Put Production Data to ComboCurve
combocurve.putDataComboCurve(admiralPermianProductionData,sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey)
combocurve.putDataComboCurve(huntOilProductionData,sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey)
combocurve.putDataComboCurve(aethonProductionData,sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey)
combocurve.putDataComboCurve(devonProductionData,sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey)

# Get Daily Productions from ComboCurve for Shalehaven
dailyProductions = combocurve.getDailyProductionFromComboCurve(sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey, fundWells)

# Get Updated and Original Type Curves from ComboCurve for Shalehaven LP 2024
updatedTypeCurves = combocurve.getDailyForecastFromComboCurve(sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey, shalehavenProjectId, shalehavenForcastIdUpdatedTypeCurve, fundWells)
originalTypeCurves = combocurve.getDailyForecastFromComboCurve(sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey, shalehavenProjectId, shalehavenForcastIdOriginalTypeCurve, fundWells)

# Merge Type Curves Updated and Orginal
mergedUpdatedTypeCurves = production.mergeProductionWithTypeCurves(dailyProductions,updatedTypeCurves, originalTypeCurves, fundWells)

# Cumulative Summaries
cumulativeProduction = production.cumulativeProduction(mergedUpdatedTypeCurves)

print("End Shalehaven ETL Process")
