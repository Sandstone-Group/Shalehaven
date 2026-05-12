## Shalehaven Main Production Model
## Developed by Michael Tanner

# Imports - SHEM Scripts
import shalehavenscripts.production as production
import shalehavenscripts.combocurve as combocurve
import shalehavenscripts.dealsheet as dealsheet
import shalehavenscripts.novi as novi

# Imports - General
import pandas as pd
import numpy as np
import os
import sys
from dotenv import load_dotenv
import warnings

# disable `SettingWithCopyWarning
pd.options.mode.chained_assignment = None  # default='warn'

# disable future and user warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

print("Begin Shalehaven ETL Process")

load_dotenv()  # load enviroment variables

runMode = input("Run deal pipeline only or full script? Enter D for deal pipeline only, F for full script: ").strip().upper()
while runMode not in ("D", "F"):
    runMode = input("Please enter D for deal pipeline only or F for full script: ").strip().upper()

sandstoneComboCurveServiceAccount = os.getenv("SANDSTONE_COMBOCURVE_API_SEC_CODE")
sandstoneComboCurveApiKey = os.getenv("SANDSTONE_COMBOCURVE_API_KEY_PASS")
shalehavenProjectId = os.getenv("SHALEHAVEN_PROJECT_ID")
shalehavenForcastIdUpdatedTypeCurve = os.getenv("SHALEHAVEN_FORCAST_ID_UPDATED_TYPE_CURVE")
shalehavenForcastIdOriginalTypeCurve = os.getenv("SHALEHAVEN_FORCAST_ID_ORIGINAL_TYPE_CURVE")

# Paths to data
pathToAdmiralData = os.getenv("SHALEHAVEN_ADMIRAL_PATH")
pathToHuntData = os.getenv("SHALEHAVEN_HUNT_PATH")
pathToAethonData = os.getenv("SHALEHAVEN_AETHON_PATH")
pathToDevonData = os.getenv("SHALEHAVEN_DEVON_PATH")
pathToCopData = os.getenv("SHALEHAVEN_COP_PATH")
pathToSpurData = os.getenv("SHALEHAVEN_SPUR_PATH")
pathToBallardData = os.getenv("SHALEHAVEN_BALLARD_PATH")
pathToKrakenData = os.getenv("SHALEHAVEN_KRAKEN_PATH")
pathToEogData = os.getenv("SHALEHAVEN_EOG_PATH")
pathToMonthlyPDSData = os.getenv("SHALEHAVEN_MONTHLY_PDS_PATH")
pathToDatabase = os.getenv("SHALEHAVEN_DATABASE_PATH")
pathToDealSheet = os.getenv("SHALEHAVEN_DEAL_SHEET_PATH")

if runMode == "D":
    dealsheet.buildDealPipeline(pathToDealSheet) # runs updating the deal sheet
    print("End Shalehaven Deal Pipeline Process")
    sys.exit(0)

novi.checkNoviDbStatus() # checks Novi API bulk download, then replaces data if new DB as dropped

dealsheet.buildDealPipeline(pathToDealSheet) # runs updating the deal sheet

# Build a single ComboCurve client (pooled session, retries, env-driven auth)
ccClient = combocurve.ComboCurveClient.from_env()

# Get Wells From ComboCurve and Split by Operator
wells = combocurve.getWellsFromComboCurve(ccClient)
huntWells = wells[wells['currentOperator'] == 'HUNT OIL COMPANY']
admiralWells = wells[wells['currentOperator'] == 'ADMIRAL PERMIAN OPERATING LLC']
aethonWells = wells[wells['currentOperator'] == 'AETHON ENERGY OPERATING LLC']
devonWells = wells[wells['currentOperator'] == 'DEVON ENERGY PRODUCTION COMPANY LP']
copWells = wells[wells['currentOperator'] == 'COG OPERATING LLC']
spurWells = wells[wells['currentOperator'] == 'Spur Energy Partners LLC']
ballardWells = wells[wells['currentOperator'] == 'Ballard Petroleum']
krakenWells = wells[wells['currentOperator'] == 'Kraken Operating, LLC']
eogWells = wells[wells['currentOperator'] == 'EOG RESOURCES INC']
fundWells = pd.concat([huntWells, admiralWells, aethonWells, devonWells, copWells, spurWells, ballardWells, krakenWells, eogWells]) # merge huntWells with admiralWells, devonWells, aethonWells, copWells, spurWells, ballardWells, krakenWells, and eogWells

# print fundWells to database
fundWells.to_excel(os.path.join(pathToDatabase, r"fundWells.xlsx"))
wells.to_excel(os.path.join(pathToDatabase, r"allWells.xlsx"))

# Get & Format Production Data
admiralPermianProductionData = production.admiralPermianProductionData(pathToAdmiralData)
huntOilProductionData = production.huntOilProductionData(pathToHuntData,huntWells)
aethonProductionData = production.aethonProductionData(pathToAethonData)
devonProductionData = production.devonProductionData(pathToDevonData)
copProductionData = production.copProductionData(pathToCopData)
spurWellMapping = dict(zip(spurWells['wellName'], spurWells['chosenID']))
spurProductionData = production.spurProductionData(pathToSpurData, spurWellMapping)
ballardProductionData = production.ballardProductionData(pathToBallardData)
krakenWellMapping = dict(zip(krakenWells['wellName'], krakenWells['chosenID']))
krakenProductionData = production.krakenProductionData(pathToKrakenData, krakenWellMapping)
eogProductionData = production.eogProductionData(pathToEogData)
monthlyPds = production.pdsMonthlyData(pathToMonthlyPDSData)

# Put Production Data to ComboCurve
combocurve.putDataComboCurveDaily(ccClient, admiralPermianProductionData, operator="Admiral Permian")
combocurve.putDataComboCurveDaily(ccClient, huntOilProductionData, operator="Hunt Oil")
combocurve.putDataComboCurveDaily(ccClient, aethonProductionData, operator="Aethon")
combocurve.putDataComboCurveDaily(ccClient, devonProductionData, operator="Devon")
combocurve.putDataComboCurveDaily(ccClient, copProductionData, operator="ConocoPhillips")
combocurve.putDataComboCurveDaily(ccClient, spurProductionData, operator="Spur")
combocurve.putDataComboCurveDaily(ccClient, ballardProductionData, operator="Ballard")
combocurve.putDataComboCurveDaily(ccClient, krakenProductionData, operator="Kraken")
combocurve.putDataComboCurveDaily(ccClient, eogProductionData, operator="EOG")
combocurve.putDataComboCurveMonthly(ccClient, monthlyPds)

# Get Daily Productions from ComboCurve for Shalehaven
dailyProductions = combocurve.getDailyProductionFromComboCurve(ccClient, fundWells, pathToDatabase)

# Get Updated and Original Type Curves from ComboCurve for Shalehaven LP 2024
updatedTypeCurves = combocurve.getDailyForecastFromComboCurve(ccClient, shalehavenProjectId, shalehavenForcastIdUpdatedTypeCurve, fundWells)
originalTypeCurves = combocurve.getDailyForecastFromComboCurve(ccClient, shalehavenProjectId, shalehavenForcastIdOriginalTypeCurve, fundWells)

# Merge Type Curves Updated and Orginal
mergedUpdatedTypeCurves = production.mergeProductionWithTypeCurves(dailyProductions,updatedTypeCurves, originalTypeCurves, fundWells, pathToDatabase)

# Cumulative Summaries
cumulativeProduction = production.cumulativeProduction(mergedUpdatedTypeCurves, pathToDatabase)

print("End Shalehaven ETL Process")
