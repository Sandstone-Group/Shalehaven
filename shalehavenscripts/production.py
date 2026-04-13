import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
import re

pathToDatabase = os.getenv("SANDSTONE_DATABASE_PATH")

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

Get Aethon Production Data

"""

def aethonProductionData(pathToData):
    
    print("Getting Aethon Production Data")

    load_dotenv()  # load enviroment variables
    
    # Update path to include the last file in the directory based on time modified
    pathToData = max([os.path.join(pathToData, f) for f in os.listdir(pathToData)], key=os.path.getmtime)
    
    data = pd.read_csv(pathToData) 
    
    data['API'] = data['API'].astype(str)
    
    # drop operatorID rows that are not 9724
    data = data[data['OperatorID'] == 9724]
    
    data = data[['Production Date', 'API', 'Oil Production', 'Gas Production', 'Water Production']]
    
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

Get Devon Production Data - PDS

"""

def devonProductionData(pathToData):
    
    print("Getting Devon Production Data")

    load_dotenv()  # load enviroment variables
    
    # Update path to include the last file in the directory based on time modified
    pathToData = max([os.path.join(pathToData, f) for f in os.listdir(pathToData)], key=os.path.getmtime)
    
    ### get file name
    name = os.path.basename(pathToData)
    # if starts with "PDSWDX"
    if name.startswith("PDSWDX"):
        data = pd.read_csv(pathToData) 
        
        # drop last row
        data = data[:-1]
        
        data['API'] = data['API'].astype(str)
        #drop last two characters from API
        data['API'] = data['API'].str[:-2]
        # add two more trailing zeros to API
        data['API'] = data['API'] + '00'

        data = data[['Prod Date', 'API', 'Oil Prod', 'Gas Prod', 'Water Prod']]
        
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
    else:
        
        data = pd.read_csv(pathToData) 
    
        data['API'] = data['API'].astype(str)
        
        # drop operatorID rows that are not 9724
        data = data[data['OperatorID'] == 1014]
        
        data = data[['Production Date', 'API', 'Oil Production', 'Gas Production', 'Water Production']]
        
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

Get COP Production Data - PDS

"""

def copProductionData(pathToData):
    print("Getting ConocoPhillips Production Data")

    load_dotenv()  # load enviroment variables
    
    # Update path to include the last file in the directory based on time modified
    pathToData = max([os.path.join(pathToData, f) for f in os.listdir(pathToData)], key=os.path.getmtime)
    
    ### get file name
    name = os.path.basename(pathToData)
    # if starts with "PDSWDX"
    if name.startswith("PDSWDX"):
        data = pd.read_csv(pathToData) 
        
        # drop last row
        data = data[:-1]
        
        data['API'] = data['API'].astype(str)
        #drop last two characters from API
        data['API'] = data['API'].str[:-2]
        # add two more trailing zeros to API
        data['API'] = data['API'] + '00'

        data = data[['PRODDATE', 'API', 'OIL PROD', 'GAS PROD', 'WATER PROD']]
        
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
    else:
        
        data = pd.read_csv(pathToData) 
    
        data['API'] = data['API'].astype(str)
        
        # drop operatorID rows that are not 9724
        data = data[data['OperatorID'] == 1014]
        
        data = data[['Production Date', 'API', 'Oil Production', 'Gas Production', 'Water Production']]
        
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

Load Spur Energy Data from ProdView excel to ComboCurve Monthly Format

"""

def spurProductionData(pathToData, wellMapping):

    print("Getting Spur Energy Production Data")

    load_dotenv()  # load enviroment variables

    # Update path to include the last file in the directory based on time modified
    pathToData = max([os.path.join(pathToData, f) for f in os.listdir(pathToData)], key=os.path.getmtime)

    data = pd.read_excel(pathToData, header=1)

    rows = []
    current_chosen_id = None

    mapping_keys = list(wellMapping.keys())

    for _, row in data.iterrows():
        cell = row['Unit Name/Date']
        if isinstance(cell, str) and cell.strip().upper().startswith("FRIESIAN"):
            well_name = cell.strip()
            if well_name in wellMapping:
                current_chosen_id = wellMapping[well_name]
            else:
                # extract trailing well number (e.g. "01H" -> 1, "#001H" -> 1)
                excel_num = re.search(r'#?0*(\d+)[Hh]\s*$', well_name)
                matched = False
                if excel_num:
                    target = int(excel_num.group(1))
                    for key in mapping_keys:
                        key_num = re.search(r'#?0*(\d+)[Hh]\s*$', key)
                        if key_num and int(key_num.group(1)) == target:
                            print(f"  Matched '{well_name}' -> '{key}'")
                            current_chosen_id = wellMapping[key]
                            matched = True
                            break
                if not matched:
                    print(f"  Warning: No match found for '{well_name}' in wellMapping")
                    current_chosen_id = None
        elif current_chosen_id is not None:
            rows.append({
                'date': pd.to_datetime(cell.strip()),
                'chosenID': current_chosen_id,
                'oil': row['Oil Production (bbl)'],
                'gas': row['Gas Production (MCF)'],
                'water': row['Water Production (bbl)'],
                'dataSource': 'other',
            })

    data = pd.DataFrame(rows, columns=['date', 'chosenID', 'oil', 'gas', 'water', 'dataSource'])
    
    return data

"""

Convert Ballard Petroleum Production Data from ProdView excel to ComboCurve Monthly Format

"""

def ballardProductionData(pathToData):
    
    print("Getting Ballard Petroleum Production Data")
    
    load_dotenv()  # load enviroment variables
    
    # Update path to include the last file in the directory based on time modified
    pathToData = max([os.path.join(pathToData, f) for f in os.listdir(pathToData)], key=os.path.getmtime)
    
    data = pd.read_excel(pathToData) # read in excel data
    
    data = data[['RecordDate', 'API10', 'EstimatedOilProductionBBLS', 'EstimatedGasProductionMCF', 'EstimatedWaterProductionBBLS']] 
    
    # format API to be 14 characters by dropping leading zeros and adding trailing zeros if necessary, then convert to string
    data['API'] = data['API10'].astype(str).str.lstrip('0').str.replace('.0', '', regex=False) + '0000'
    data['API'] = data['API'].astype(str)
    # add new column to data called 'dataSource' and set all values to "other"
    data['dataSource'] = "other"

    # reorder columns to match ComboCurve format
    data = data[['RecordDate', 'API', 'EstimatedOilProductionBBLS', 'EstimatedGasProductionMCF', 'EstimatedWaterProductionBBLS', 'dataSource']]

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

Gets Kraken Resources Production Data from  excel and converts to ComboCurve Monthly Format

"""

def krakenProductionData(pathToData, wellMapping):

    print("Getting Kraken Resources Production Data")

    load_dotenv()

    # Update path to include the last file in the directory based on time modified
    pathToData = max([os.path.join(pathToData, f) for f in os.listdir(pathToData)], key=os.path.getmtime)

    raw = pd.read_excel(pathToData, sheet_name="actual-size", header=None)

    # Row 2 has well names at columns 1, 4, 7, 10, ... (each well spans 3 cols: oil, gas, water)
    # Row 3 onward is data; column 0 is RecordDate
    well_row = raw.iloc[2]
    data_rows = raw.iloc[4:].reset_index(drop=True)

    rows = []
    # Walk the well name row in steps of 3 starting at col 1
    well_cols = [(i, str(well_row.iloc[i])) for i in range(1, len(well_row), 3)
                 if pd.notna(well_row.iloc[i]) and "Total" not in str(well_row.iloc[i])]

    for col_idx, well_name in well_cols:
        well_name_upper = well_name.strip().upper()

        # Skip Delores — not our well
        if "DELORES" in well_name_upper:
            print(f"  Skipping {well_name} (not our well)")
            continue

        # Match to ComboCurve chosenID via wellMapping
        # Normalize: strip whitespace, remove '#', case-insensitive
        def _normalize(s):
            return s.strip().upper().replace("#", "")

        chosen_id = wellMapping.get(well_name.strip())
        if chosen_id is None:
            norm = _normalize(well_name)
            for k, v in wellMapping.items():
                if _normalize(k) == norm:
                    chosen_id = v
                    break
        if chosen_id is None:
            print(f"  Warning: No match found for '{well_name}' in wellMapping, skipping")
            continue

        for _, row in data_rows.iterrows():
            date_val = row.iloc[0]
            if pd.isna(date_val):
                continue
            try:
                parsed_date = pd.to_datetime(date_val)
            except (ValueError, TypeError):
                continue
            oil = row.iloc[col_idx]
            gas = row.iloc[col_idx + 1]
            water = row.iloc[col_idx + 2]
            rows.append({
                'date': parsed_date,
                'chosenID': chosen_id,
                'oil': oil,
                'gas': gas,
                'water': water,
                'dataSource': 'other',
            })

    data = pd.DataFrame(rows, columns=['date', 'chosenID', 'oil', 'gas', 'water', 'dataSource'])
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=15)
    data = data[data['date'] >= cutoff]
    print(f"  Parsed {len(data)} production rows (last 15 days) for {len(well_cols) - 1} wells")

    return data



"""
    
    Merge daily production data with updated and original type curves from ComboCurve.

"""

def mergeProductionWithTypeCurves(dailyprod, updated, original, wellList, pathToDatabase):
    
    print("Begin Merging dailyprod with Orginal and Updated Type Curves")
    
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

    mergedData.to_excel(os.path.join(pathToDatabase, r"daily_merge.xlsx"))

    print("Finished Merging Original and Updated Type Curves")


    return mergedData

"""
    
    Create cumulative production from daily production data, updated type curves, and original type curves.  The data should have a index column (1 through n) then date, well, oil, gas, water columns for daily production, updated type curves, and original type curve cumulative.    
    
"""

def cumulativeProduction(data, pathToDatabase):
    
    print("Begin Creating Cumulative Production Data")
    
    # create new dataframe to hold cumulative production data
    cumulativeData = pd.DataFrame(columns=["day", "well", "wellName", "API", "oil_dailyprod_cum", "gas_dailyprod_cum", "water_dailyprod_cum", "oil_updated_cum", "gas_updated_cum", "water_updated_cum", "oil_original_cum", "gas_original_cum", "water_original_cum"])

    # get unique wells from data
    uniqueWells = data['well'].unique()
    
    # split dataframe into each well
    for well in uniqueWells:
        wellData = data[data['well'] == well]
        
        ## count the number of rows in wellData[wellData['oil_dailyprod'] != ""]
        numProductionDays = len(wellData[wellData['oil_dailyprod'] != ""])
    
        # if oil_dailyprod, gas_dailyprod, water_dailyprod are "", replace with 0
        wellData['oil_dailyprod'] = wellData['oil_dailyprod'].replace("", 0).astype(float)
        wellData['gas_dailyprod'] = wellData['gas_dailyprod'].replace("", 0).astype(float)
        wellData['water_dailyprod'] = wellData['water_dailyprod'].replace("", 0).astype(float)
        # if oil_updated, gas_updated, water_updated are "", replace with 0
        wellData['oil_updated'] = wellData['oil_updated'].replace("", 0).astype(float)
        wellData['gas_updated'] = wellData['gas_updated'].replace("", 0).astype(float)
        wellData['water_updated'] = wellData['water_updated'].replace("", 0).astype(float)
        # if oil_original, gas_original, water_original are "", replace with 0
        wellData['oil_original'] = wellData['oil_original'].replace("", 0).astype(float)
        wellData['gas_original'] = wellData['gas_original'].replace("", 0).astype(float)
        wellData['water_original'] = wellData['water_original'].replace("", 0).astype(float)

        # create cumulative sum for oil_dailyprod, gas_dailyprod, water_dailyprod
        wellData['oil_dailyprod_cum'] = wellData['oil_dailyprod'].cumsum()
        wellData['gas_dailyprod_cum'] = wellData['gas_dailyprod'].cumsum()
        wellData['water_dailyprod_cum'] = wellData['water_dailyprod'].cumsum()
        
        # create cumulative sum for oil_updated, gas_updated, water_updated
        wellData['oil_updated_cum'] = wellData['oil_updated'].cumsum()
        wellData['gas_updated_cum'] = wellData['gas_updated'].cumsum()
        wellData['water_updated_cum'] = wellData['water_updated'].cumsum()
        
        # create cumulative sum for oil_original, gas_original, water_original
        wellData['oil_original_cum'] = wellData['oil_original'].cumsum()
        wellData['gas_original_cum'] = wellData['gas_original'].cumsum()
        wellData['water_original_cum'] = wellData['water_original'].cumsum()
        
        # if a list starts with 0's, delete zeros and shift the list so that the first non-zero value is at day 1
        wellData['oil_dailyprod_cum'] = wellData['oil_dailyprod_cum'].where(wellData['oil_dailyprod_cum'] != 0).ffill().fillna(0)
        wellData['gas_dailyprod_cum'] = wellData['gas_dailyprod_cum'].where(wellData['gas_dailyprod_cum'] != 0).ffill().fillna(0)
        wellData['water_dailyprod_cum'] = wellData['water_dailyprod_cum'].where(wellData['water_dailyprod_cum'] != 0).ffill().fillna(0)
        wellData['oil_updated_cum'] = wellData['oil_updated_cum'].where(wellData['oil_updated_cum'] != 0).ffill().fillna(0)
        wellData['gas_updated_cum'] = wellData['gas_updated_cum'].where(wellData['gas_updated_cum'] != 0).ffill().fillna(0)
        wellData['water_updated_cum'] = wellData['water_updated_cum'].where(wellData['water_updated_cum'] != 0).ffill().fillna(0)
        wellData['oil_original_cum'] = wellData['oil_original_cum'].where(wellData['oil_original_cum'] != 0).ffill().fillna(0)
        wellData['gas_original_cum'] = wellData['gas_original_cum'].where(wellData['gas_original_cum'] != 0).ffill().fillna(0).fillna(0)
        wellData['water_original_cum'] = wellData['water_original_cum'].where(wellData['water_original_cum'] != 0).ffill().fillna(0)
        
        # shift list so that if there are 0's to begin with, the production is shifted so the first day of production is day 1
        wellData['oil_dailyprod_cum'] = wellData['oil_dailyprod_cum'].shift(fill_value=0)
        wellData['gas_dailyprod_cum'] = wellData['gas_dailyprod_cum'].shift(fill_value=0)
        wellData['water_dailyprod_cum'] = wellData['water_dailyprod_cum'].shift(fill_value=0)
        wellData['oil_updated_cum'] = wellData['oil_updated_cum'].shift(fill_value=0)
        wellData['gas_updated_cum'] = wellData['gas_updated_cum'].shift(fill_value=0)
        wellData['water_updated_cum'] = wellData['water_updated_cum'].shift(fill_value=0)
        wellData['oil_original_cum'] = wellData['oil_original_cum'].shift(fill_value=0)
        wellData['gas_original_cum'] = wellData['gas_original_cum'].shift(fill_value=0)
        wellData['water_original_cum'] = wellData['water_original_cum'].shift(fill_value=0)

        # create a list for day number starting at 0 to n
        wellData['day'] = np.arange(len(wellData))
        
        # for oil_dailyprod_cum, gas_dailyprod_cum, water_dailyprod_cum, if day number is greater than numProductionDays, set cumulative production to ""
        wellData.loc[wellData['day'] >= numProductionDays, 'oil_dailyprod_cum'] = ""
        wellData.loc[wellData['day'] >= numProductionDays, 'gas_dailyprod_cum'] = ""
        wellData.loc[wellData['day'] >= numProductionDays, 'water_dailyprod cum'] = ""
        
        # select only necessary columns
        wellCumulativeData = wellData[["day", "well", "wellName", "API", "oil_dailyprod_cum", "gas_dailyprod_cum", "water_dailyprod_cum", "oil_updated_cum", "gas_updated_cum", "water_updated_cum", "oil_original_cum", "gas_original_cum", "water_original_cum"]]
        
        # append to cumulativeData dataframe
        cumulativeData = pd.concat([cumulativeData, wellCumulativeData], ignore_index=True)
 
    # print cumulativeData to excel for review
    cumulativeData.to_excel(os.path.join(pathToDatabase, r"cumulative_production.xlsx"))
     
    print("Finished Creating Cumulative Production Data")

    return cumulativeData





"""
Convert Monthly PDS to ComboCurve Monthly Format    
    
"""

def pdsMonthlyData(pathToData):
    
    print("Getting Devon Production Data")

    load_dotenv()  # load enviroment variables
    
    # Update path to include the last file in the directory based on time modified
    pathToData = max([os.path.join(pathToData, f) for f in os.listdir(pathToData)], key=os.path.getmtime)
    
    data = pd.read_csv(pathToData)
    
    data['API'] = data['API'].astype(str)
    #drop last two characters from API
    data['API'] = data['API'].str[:-2]
    # add two more trailing zeros to API
    data['API'] = data['API'] + '00'
    # convert 'Production Date' to datetime format YYYY-MM-DD
    data['Production Date'] = pd.to_datetime(data['Production Date'], format="%m/%d/%Y").dt.strftime("%Y-%m-%d")

    data = data[['Production Date', 'API', 'Oil Production', 'Gas Production', 'Water Production']]
    
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