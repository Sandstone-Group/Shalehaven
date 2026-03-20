import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os

pathToDatabase = os.getenv("SHALEHAVEN_DATABASE_PATH")

"""
This program goes into every folder within the AFE folder and reads the excel files and combines into a single dataframe. It then writes that dataframe to an excel file in the database folder.  It also adds two columns at the end of each row with the (1) name of the folder and (2) the company code found in the database under company_code 

"""

def combineAfeData(pathToAfe):
    afeData = pd.DataFrame() # create empty dataframe to store combined data
    companyCodes = pd.read_excel(os.path.join(pathToDatabase, "company_code.xlsx")) # read company codes from database
    # get the year from the file path - it is the last 4 characters of the file path
    year = pathToAfe[-4:]
    for folder in os.listdir(pathToAfe): # loop through each folder in the AFE folder
        folderPath = os.path.join(pathToAfe, folder) # get the path to the current folder
        
        if os.path.isdir(folderPath): # check if the current path is a directory
            for file in os.listdir(folderPath): # loop through each file in the current folder
                filePath = os.path.join(folderPath, file) # get the path to the current file
                
                if file.endswith('.xlsx'): # check if the current file is a excel file
                    df = pd.read_excel(filePath) # read the excel file into a dataframe
                    df['Folder'] = folder  # add a column with the folder name
                    df['Fund'] = year  # add a column with the year
                    operatorList = companyCodes['Operator Name'].tolist() # get the list of operators from the company codes dataframe
                    # find the name that matches the folder name in column "Owner JIB Code" and add that code to a new column called "Company Code"
                    for operator in operatorList:
                        if operator in folder:
                            companyCode = companyCodes[companyCodes['Operator Name'] == operator]['Owner JIB Code'].values[0]
                            df['Company Code'] = companyCode
                            break
                    afeData = pd.concat([afeData, df], ignore_index=True) # concatenate the current dataframe with the combined dataframe

    return afeData

"""
 This mergers all JIB's from all subfolders within JIB folder 
  
"""
def combineJibData(pathToJib):
    jibData = pd.DataFrame() # create empty dataframe to store combined data
    for folder in os.listdir(pathToJib): # loop through each folder in the JIB folder
        folderPath = os.path.join(pathToJib, folder) # get the path to the current folder
        
        if os.path.isdir(folderPath): # check if the current path is a directory
            for file in os.listdir(folderPath): # loop through each file in the current folder
                filePath = os.path.join(folderPath, file) # get the path to the current file
                
                if file.endswith('.xlsx'): # check if the current file is a excel file
                    df = pd.read_excel(filePath) # read the excel file into a dataframe
                    jibData = pd.concat([jibData, df], ignore_index=True) # concatenate the current dataframe with the combined dataframe
    
    # print jibData to database using name "jib_data.xlsx"
    jibData.to_excel(os.path.join(pathToDatabase, r"jib_data.xlsx"), index=False)
    
    return jibData

"""
 This mergers all Revenue from all subfolders within Revenue folder 
  
"""
def combineRevenueData(pathToRevenue):
    revenueData = pd.DataFrame() # create empty dataframe to store combined data
    for folder in os.listdir(pathToRevenue): # loop through each folder in the Revenue folder
        folderPath = os.path.join(pathToRevenue, folder) # get the path to the current folder
        
        if os.path.isdir(folderPath): # check if the current path is a directory
            for file in os.listdir(folderPath): # loop through each file in the current folder
                filePath = os.path.join(folderPath, file) # get the path to the current file
                
                if file.endswith('.xlsx'): # check if the current file is a excel file
                    df = pd.read_excel(filePath) # read the excel file into a dataframe
                    revenueData = pd.concat([revenueData, df], ignore_index=True) # concatenate the current dataframe with the combined dataframe
    
    # print revenueData to database using name "revenue_data.xlsx"
    revenueData.to_excel(os.path.join(pathToDatabase, r"revenue_data.xlsx"), index=False)
    
    return revenueData

"""

Formats Rev and JIB data into single dataframe to be used for consolidated LOS reporting

"""

def formatLosData(jibData, revenueData):

    masterHeaders = [
        "Invoice Date",
        "Date",
        "Operator",
        "Owner Name",
        "Owner Number",
        "Invoice Number",
        "Property Name",
        "API Number",
        "AFE Description",
        "Product Bucket",
        "Description",
        "Price",
        "Gross Volume",
        "Gross Cost",
        "Net Cost",
        "Line Detail"
    ]


    data = revenueData

    # create empty dataframe with masterHeaders as columns
    losData = pd.DataFrame(columns=masterHeaders)
    # place headers in revenueData into losData based on <header mapping> in the correct columns and fill in the rest of the columns with null values
    headerMapping = {
        "Invoice Date": "Check Date",
        "Date": "Prod Date",
        "Operator": "Operator",
        "Owner Name": "Owner Name",
        "Owner Number": "Owner Number",
        "Invoice Number": "Check Number",
        "Property Name": "Property Name",
        "API Number": "API Number",
    }

    x= 5
    return x