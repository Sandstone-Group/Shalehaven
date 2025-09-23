import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os

"""
    
Script to import production data and format for ComboCurve upload. For Admiral Permian wells in 2024 LP portfolio.    
    
"""

def admiralPermianProductionData(pathToData):

    print("Getting Admiral Permian Production Data")

    load_dotenv()  # load enviroment variables
    
    # Update path to include the last file in the directory based on time modified
    pathToData = max([os.path.join(pathToData, f) for f in os.listdir(pathToData)], key=os.path.getmtime)
    
    data = pd.read_excel(pathToData)    
    
    # remove dashes in API, add 4 trailing zeros at the end to make 14 characters and convert to string
    data['API'] = data['API'].str.replace('-','') + '0000'
    data['API'] = data['API'].astype(str)
    
    # drop all rows with "nan" API
    data = data[data['API'] != "nan"]
    
    
    # keep only columns: Date, API, Oil, Gas, Water
    data = data[['Date', 'API', 'Oil Prod', 'Gas Prod', 'Water Prod']]
    
    # add new column to data called 'dataSource' and set all values to "other" 
    data['dataSource'] = "other"
    
    columnsComboCurve = [
        "date",
        "chosenID",
        "oil",
        "gas",
        "water",
        "dataSource",
    ]
    
    data.columns = columnsComboCurve
    
    return data

def huntOilProductionData(pathToData, huntWells):
    
    print("Getting Hunt Oil Production Data")
    
    load_dotenv()  # load enviroment variables
    
    #drop all columns from huntWells except for 'wellName' and 'chosenID'
    huntWells = huntWells[['wellName','chosenID']]
    
    # Update path to include the last file in the directory based on time modified
    pathToData = max([os.path.join(pathToData, f) for f in os.listdir(pathToData)], key=os.path.getmtime)
    
    # read in excel data
    data = pd.read_excel(pathToData)
    
    # put chosenID from huntWells into data based on wellName
    for i in range(len(huntWells)):
        wellName = huntWells.iloc[i]['wellName']
        chosenId = huntWells.iloc[i]['chosenID']
        for j in range(len(data)):
            dataWellName = data.iloc[j]['LEASE']
            if dataWellName == wellName:
                data["API"] = data["API"].astype(str)
                data.loc[j, 'API'] = chosenId
    
    data = data[['D_DATE', 'API', 'OIL_BBLS', 'GAS_MCF', 'WATER_BBLS']]
    
    # add new column to data called 'dataSource' and set all values to "other" 
    data['dataSource'] = "other"
    
    columnsComboCurve = [
        "date",
        "chosenID",
        "oil",
        "gas",
        "water",
        "dataSource",
    ]
    
    data.columns = columnsComboCurve

    return data

"""
    
    Merge daily production data with updated and original type curves from ComboCurve.

"""

def mergeProductionWithTypeCurves(dailyprod, updated, original, wellList):
    
    print("Begin Merging dailyprod with  Orginal and Updated Type Curves")
    
    # ensure date columns are datetime64[ns]
    dailyprod['date'] = pd.to_datetime(dailyprod['date'], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True).dt.tz_localize(None)
    updated['date'] = pd.to_datetime(updated['date'], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True).dt.tz_localize(None)
    original['date'] = pd.to_datetime(original['date'], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True).dt.tz_localize(None)

    # Merge dailyprod with updated and original type curves on date and well but keep all rows from updated type curve - if no daily production data, fill with NaN
    mergedData = pd.merge(updated, dailyprod, how='left', on=['date', 'well'], suffixes=('', '_dailyprod'))
    mergedData = pd.merge(mergedData, original, how='left', on=['date', 'well'], suffixes=('', '_original'))

    # change columns names from oil, gas, water to oil_updated, gas_updated, water_updated
    mergedData = mergedData.rename(columns={
        'oil': 'oil_updated',
        'gas': 'gas_updated',
        'water': 'water_updated'
    })

    # any _original columns that are NaN should be filled with ""
    mergedData['oil_original'] = mergedData['oil_original'].fillna("")
    mergedData['gas_original'] = mergedData['gas_original'].fillna("")
    mergedData['water_original'] = mergedData['water_original'].fillna("")
    # any _updated columns that are NaN should be filled with ""
    mergedData['oil_dailyprod'] = mergedData['oil_dailyprod'].fillna("")
    mergedData['gas_dailyprod'] = mergedData['gas_dailyprod'].fillna("")
    mergedData['water_dailyprod'] = mergedData['water_dailyprod'].fillna("")

    # sort by well and date
    mergedData = mergedData.sort_values(by=['wellName', 'date'])
    
    # drop wellName_original and API_original columns
    mergedData = mergedData.drop(columns=['wellName_original', 'API_original', 'wellName_dailyprod', 'API_dailyprod'])
    
    # drop index
    mergedData = mergedData.reset_index(drop=True)
    
    mergedData.to_excel(r"C:\Users\Michael Tanner\OneDrive - Sandstone Group\Clients - Documents\# Shalehaven Partners\# Production\database\daily_merge.xlsx")

    print("Finished Merging Original and Updated Type Curves")


    return mergedData