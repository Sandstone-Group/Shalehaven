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
    
    for folder in os.listdir(pathToAfe): # loop through each folder in the AFE folder
        folderPath = os.path.join(pathToAfe, folder) # get the path to the current folder
        
        if os.path.isdir(folderPath): # check if the current path is a directory
            for file in os.listdir(folderPath): # loop through each file in the current folder
                filePath = os.path.join(folderPath, file) # get the path to the current file
                
                if file.endswith('.xlsx'): # check if the current file is a excel file
                    df = pd.read_excel(filePath) # read the excel file into a dataframe
                    df['Folder'] = folder  # add a column with the folder name
                    operatorList = companyCodes['Operator Name'].tolist() # get the list of operators from the company codes dataframe
                    # find the name that matches the folder name in column "Owner JIB Code" and add that code to a new column called "Company Code"
                    for operator in operatorList:
                        if operator in folder:
                            companyCode = companyCodes[companyCodes['Operator Name'] == operator]['Owner JIB Code'].values[0]
                            df['Company Code'] = companyCode
                            break
                    afeData = pd.concat([afeData, df], ignore_index=True) # concatenate the current dataframe with the combined dataframe
    
    # print afeData to database using name "afe_data.xlsx"
    afeData.to_excel(os.path.join(pathToDatabase, r"afe_data.xlsx"), index=False)
    
    return afeData