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

load_dotenv()  # load enviroment variables

# path to AFE data
pathToAfe2025 = os.getenv("SHALEHAVEN_AFE_2025_PATH")
pathToAfe2024 = os.getenv("SHALEHAVEN_AFE_2024_PATH")
pathToJib = os.getenv("SHALEHAVEN_JIB_PATH")
pathToRevenue = os.getenv("SHALEHAVEN_REVENUE_PATH")
pathToDatabase = os.getenv("SHALEHAVEN_DATABASE_PATH")
pathToAfeMaster = os.getenv("SHALEHAVEN_AFE_MASTER_PATH")
pathToJibMaster = os.getenv("SHALEHAVEN_JIB_MASTER_PATH")


runAfe = True # set to true to run the AFE ETL process, set to false to skip the AFE ETL process
runJib = True # set to true to run the JIB ETL process, set to false to skip the JIB ETL process
runRevenue = True # set to true to run the Revenue ETL process, set to false to skip the Revenue ETL process
runAfeActual = True # set to true to run the AFE vs Actual reconciliation, set to false to skip
print("Begin Shalehaven LOS ETL Process")

# if runAFE is true, run the AFE ETL process
if runAfe:
    print("Running AFE ETL Process")
    afeData2025 = los.combineAfeData(pathToAfe2025)
    afeData2024 = los.combineAfeData(pathToAfe2024)
    # combine afeData2025 and afeData2024 into a single dataframe called afeData
    afeData = pd.concat([afeData2025, afeData2024], ignore_index=True)
    # save afeData to database
    afeData.to_excel(os.path.join(pathToDatabase, r"afe_data.xlsx"), index=False)
else:
    print("Skipping AFE ETL Process")

# if runJib is true, run the JIB ETL process
if runJib:
    print("Running JIB ETL Process")
    jibData = los.combineJibData(pathToJib)
else:
    print("Skipping JIB ETL Process")
    
# if runRevenue is true, run the Revenue ETL process
if runRevenue:
    print("Running Revenue ETL Process")
    revenueData = los.combineRevenueData(pathToRevenue)
else:
    print("Skipping Revenue ETL Process")

# get afeData, jibData, and revenueData from database
afeData = pd.read_excel(os.path.join(pathToDatabase, r"afe_data.xlsx"))
jibData = pd.read_excel(os.path.join(pathToDatabase, r"jib_data.xlsx"))
revenueData = pd.read_excel(os.path.join(pathToDatabase, r"revenue_data.xlsx"))

# reconcile AFE vs JIB actuals and export Power BI-ready workbook
if runAfeActual:
    print("Running AFE vs Actual Reconciliation")
    los.generateAfeActualReport(pathToAfeMaster, pathToJibMaster, pathToDatabase)
else:
    print("Skipping AFE vs Actual Reconciliation")

print("Shalehaven LOS ETL Process Complete")