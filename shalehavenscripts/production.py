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
    
    return data

def huntOilProductionData(pathToData):
    
    load_dotenv()  # load enviroment variables
    
    # Update path to include the last file in the directory based on time modified
    pathToData = max([os.path.join(pathToData, f) for f in os.listdir(pathToData)], key=os.path.getmtime)
    
    data = pd.read_excel(pathToData)    
    
    x = 5
    
    return data