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
shalehavenProjectId = os.getenv("SHALEHAVEN_PROJECT_ID")
shalehavenForcastIdUpdatedTypeCurve = os.getenv("SHALEHAVEN_FORCAST_ID_UPDATED_TYPE_CURVE")
shalehavenForcastIdOriginalTypeCurve = os.getenv("SHALEHAVEN_FORCAST_ID_ORIGINAL_TYPE_CURVE")

print("Begin Shalehaven ETL Process")

load_dotenv()  # load enviroment variables

# Paths to data
pathToAdmiralData = os.getenv("SHALEHAVEN_ADMIRAL_PATH")
pathToHuntData = os.getenv("SHALEHAVEN_HUNT_PATH")
pathToAethonData = os.getenv("SHALEHAVEN_AETHON_PATH")

# Get Wells From ComboCurve and Split by Operator
wells = combocurve.getWellsFromComboCurve(sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey)
huntWells = wells[wells['currentOperator'] == 'HUNT OIL COMPANY']
admiralWells = wells[wells['currentOperator'] == 'ADMIRAL PERMIAN OPERATING LLC']
aethonWells = wells[wells['currentOperator'] == 'AETHON ENERGY OPERATING LLC']
allWells = pd.concat([huntWells, admiralWells, aethonWells]) # merge huntWells with admiralWells and aethonWells

# print allWells to database
allWells.to_excel(r"C:\Users\Michael Tanner\OneDrive - Sandstone Group\Clients - Documents\# Shalehaven Partners\# Production\database\wells.xlsx")
wells.to_excel(r"C:\Users\Michael Tanner\OneDrive - Sandstone Group\Clients - Documents\# Shalehaven Partners\# Production\database\allWells.xlsx")

# Get & Format Production Data
admiralPermianProductionData = production.admiralPermianProductionData(pathToAdmiralData)
huntOilProductionData = production.huntOilProductionData(pathToHuntData,huntWells)
aethonProductionData = production.aethonProductionData(pathToAethonData)

# Put Production Data to ComboCurve
combocurve.putDataComboCurve(admiralPermianProductionData,sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey)
combocurve.putDataComboCurve(huntOilProductionData,sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey)
combocurve.putDataComboCurve(aethonProductionData,sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey)

# Get Daily Productions from ComboCurve for Shalehaven
dailyProductions = combocurve.getDailyProductionFromComboCurve(sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey, allWells)

# Get Updated and Original Type Curves from ComboCurve for Shalehaven LP 2024
updatedTypeCurves = combocurve.getDailyForecastFromComboCurve(sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey, shalehavenProjectId, shalehavenForcastIdUpdatedTypeCurve, allWells)
originalTypeCurves = combocurve.getDailyForecastFromComboCurve(sandstoneComboCurveServiceAccount,sandstoneComboCurveApiKey, shalehavenProjectId, shalehavenForcastIdOriginalTypeCurve, allWells)



# Merge Type Curves Updated and Orginal
mergedUpdatedTypeCurves = production.mergeProductionWithTypeCurves(dailyProductions,updatedTypeCurves, originalTypeCurves, allWells)

# Cumulative Summaries
cumulativeProduction = production.cumulativeProduction(mergedUpdatedTypeCurves)

print("End Shalehaven ETL Process")
