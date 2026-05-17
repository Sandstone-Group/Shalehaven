## Shalehaven Main Production Model
## Developed by Michael Tanner

# Imports - SHEM Scripts
import shalehavenscripts.production as production
import shalehavenscripts.combocurve as combocurve
import shalehavenscripts.dealsheet as dealsheet
import shalehavenscripts.novi as novi
import shalehavenscripts.tech as tech

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

# ─── Production-report email distribution ───
# Flip to False when ready to send to the full distribution list.
# To add a recipient: drop their address into .env as SHALEHAVEN_<NAME>_EMAIL,
# then append the env-var name to PROD_REPORT_RECIPIENT_KEYS.
PROD_REPORT_TEST_MODE = False
PROD_REPORT_RECIPIENT_KEYS = [
    "SHALEHAVEN_MICHAEL_EMAIL",
    "SHALEHAVEN_GRAHAM_EMAIL",
    "SHALEHAVEN_NATHAN_EMAIL",
]
PROD_REPORT_TEST_RECIPIENT_KEYS = ["SHALEHAVEN_MICHAEL_EMAIL"]

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
pathToProdPdf = os.getenv("SHALEHAVEN_PROD_PDF_PATH")

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
combocurve.putDataComboCurveDaily(ccClient, eogProductionData, operator="EOG Resources")
combocurve.putDataComboCurveMonthly(ccClient, monthlyPds)

# Get Daily Productions from ComboCurve for Shalehaven
dailyProductions = combocurve.getDailyProductionFromComboCurve(ccClient, fundWells, pathToDatabase)

# Get Updated and Original Type Curves from ComboCurve for Shalehaven LP 2024
updatedTypeCurves = combocurve.getDailyForecastFromComboCurve(ccClient, shalehavenProjectId, shalehavenForcastIdUpdatedTypeCurve, fundWells)
originalTypeCurves = combocurve.getDailyForecastFromComboCurve(ccClient, shalehavenProjectId, shalehavenForcastIdOriginalTypeCurve, fundWells)

# 7-day net production PDF (by fund → operator → well; forecast = original TC, Admiral Permian falls back to updated TC)
prodReport = production.buildSevenDayNetProductionPdf(
    dailyProductions, fundWells, pathToProdPdf,
    originalTypeCurves=originalTypeCurves,
    updatedTypeCurves=updatedTypeCurves,
)
prodPdfPath = prodReport['pdfPath']
prodKpis = prodReport['kpis']

# Email the rendered PDF to the distribution list (or just Michael in test mode)
_recipientKeys = PROD_REPORT_TEST_RECIPIENT_KEYS if PROD_REPORT_TEST_MODE else PROD_REPORT_RECIPIENT_KEYS
_recipients = [v for v in (os.getenv(k) for k in _recipientKeys) if v]
if _recipients:
    _today = pd.Timestamp.now().strftime("%Y-%m-%d")
    _kpiLines = []
    for _fund in ("2024 LP", "2025 LP"):
        _k = prodKpis.get(_fund, {'oil_avg_bbl_d': 0.0, 'gas_avg_mcf_d': 0.0})
        _kpiLines.append(
            f"  {_fund}:  Net Oil {_k['oil_avg_bbl_d']:,.1f} bbl/d   "
            f"|   Net Gas {_k['gas_avg_mcf_d']:,.1f} Mcf/d"
        )
    tech.sendEmail(
        to=_recipients,
        subject=f"Shalehaven Daily Production Report — {_today}",
        body=(
            f"Shalehaven daily production report for {_today} attached.\n\n"
            f"7-day average net production (per fund):\n"
            + "\n".join(_kpiLines) + "\n\n"
            f"Full report includes 30-day net actual vs forecast roll-up by fund.\n\n"
            f"Generated automatically by the Shalehaven ETL pipeline."
        ),
        attachments=[prodPdfPath],
    )
    print(f"Sent production report to {len(_recipients)} recipient(s)")
else:
    print("Skipping production report email: no recipient addresses found in environment")

# Merge Type Curves Updated and Orginal
mergedUpdatedTypeCurves = production.mergeProductionWithTypeCurves(dailyProductions,updatedTypeCurves, originalTypeCurves, fundWells, pathToDatabase)

# Cumulative Summaries
cumulativeProduction = production.cumulativeProduction(mergedUpdatedTypeCurves, pathToDatabase)

print("End Shalehaven ETL Process")
