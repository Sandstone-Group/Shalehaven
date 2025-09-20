import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os

"""
    
Calculate the first 12 month oil production per foot of lateral.
    
"""

def tweleveMonthOilPerFt(dataOil,dataHeader):
    
    ## keep Well Name, Chosen ID, 
    
    x= 5
    
    return x

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