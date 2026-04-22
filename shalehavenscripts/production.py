import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
import re

load_dotenv()

COMBOCURVE_COLUMNS = ["date", "chosenID", "oil", "gas", "water", "dataSource"]

_WELL_NUM_RE = re.compile(r'#?0*(\d+)[Hh]\s*$')


def _latest_file_in_dir(path):
    return max(
        (os.path.join(path, f) for f in os.listdir(path)),
        key=os.path.getmtime,
    )


"""

Script to import production data and format for ComboCurve upload. For Admiral Permian wells in 2024 LP portfolio.

"""

def admiralPermianProductionData(pathToData):

    print("Getting Admiral Permian Production Data")

    pathToData = _latest_file_in_dir(pathToData)

    data = pd.read_excel(pathToData)

    # API comes in with dashes; strip them and pad to 14 chars
    data['API'] = data['API'].str.replace('-', '') + '0000'
    data['API'] = data['API'].astype(str)
    data = data[data['API'] != "nan"]

    data = data[['Date', 'API', 'Oil Prod', 'Gas Prod', 'Water Prod']]
    data['dataSource'] = "other"
    data.columns = COMBOCURVE_COLUMNS

    return data

def huntOilProductionData(pathToData, huntWells):

    print("Getting Hunt Oil Production Data")

    pathToData = _latest_file_in_dir(pathToData)

    data = pd.read_excel(pathToData)

    nameToChosenId = dict(zip(huntWells['wellName'], huntWells['chosenID']))
    data['API'] = data['API'].astype(str)
    data['API'] = data['LEASE'].map(nameToChosenId).fillna(data['API'])

    data = data[['D_DATE', 'API', 'OIL_BBLS', 'GAS_MCF', 'WATER_BBLS']]
    data['dataSource'] = "other"
    data.columns = COMBOCURVE_COLUMNS

    return data

"""

Get Aethon Production Data

"""

def aethonProductionData(pathToData):

    print("Getting Aethon Production Data")

    pathToData = _latest_file_in_dir(pathToData)

    data = pd.read_csv(pathToData)
    data['API'] = data['API'].astype(str)
    data = data[data['OperatorID'] == 9724]

    data = data[['Production Date', 'API', 'Oil Production', 'Gas Production', 'Water Production']]
    data['dataSource'] = "other"
    data.columns = COMBOCURVE_COLUMNS

    return data

"""

Get Devon Production Data - PDS

"""

def devonProductionData(pathToData):

    print("Getting Devon Production Data")

    pathToData = _latest_file_in_dir(pathToData)
    name = os.path.basename(pathToData)

    if name.startswith("PDSWDX"):
        data = pd.read_csv(pathToData)
        data = data[:-1]
        data['API'] = data['API'].astype(str).str[:-2] + '00'
        data = data[['Prod Date', 'API', 'Oil Prod', 'Gas Prod', 'Water Prod']]
    else:
        data = pd.read_csv(pathToData)
        data['API'] = data['API'].astype(str)
        data = data[data['OperatorID'] == 1014]
        data = data[['Production Date', 'API', 'Oil Production', 'Gas Production', 'Water Production']]

    data['dataSource'] = "other"
    data.columns = COMBOCURVE_COLUMNS

    return data

"""

Get COP Production Data - PDS

"""

def copProductionData(pathToData):
    print("Getting ConocoPhillips Production Data")

    pathToData = _latest_file_in_dir(pathToData)
    name = os.path.basename(pathToData)

    if name.startswith("PDSWDX"):
        data = pd.read_csv(pathToData)
        data = data[:-1]
        data['API'] = data['API'].astype(str).str[:-2] + '00'
        data = data[['PRODDATE', 'API', 'OIL PROD', 'GAS PROD', 'WATER PROD']]
    else:
        data = pd.read_csv(pathToData)
        data['API'] = data['API'].astype(str)
        # NOTE: both devonProductionData and copProductionData filter OperatorID==1014;
        # one of these is likely wrong — verify against the PDS operator ID for COP.
        data = data[data['OperatorID'] == 1014]
        data = data[['Production Date', 'API', 'Oil Production', 'Gas Production', 'Water Production']]

    data['dataSource'] = "other"
    data.columns = COMBOCURVE_COLUMNS

    return data

"""

Load Spur Energy Data from ProdView excel to ComboCurve Monthly Format

"""

def spurProductionData(pathToData, wellMapping):

    print("Getting Spur Energy Production Data")

    pathToData = _latest_file_in_dir(pathToData)

    data = pd.read_excel(pathToData, header=1)

    # Pre-index well mapping by trailing well-number (e.g. "01H" → 1) so header
    # rows with different zero-padding still match.
    keyByWellNum = {}
    for key in wellMapping:
        m = _WELL_NUM_RE.search(key)
        if m:
            keyByWellNum[int(m.group(1))] = key

    rows = []
    current_chosen_id = None

    for _, row in data.iterrows():
        cell = row['Unit Name/Date']
        if isinstance(cell, str) and cell.strip().upper().startswith("FRIESIAN"):
            well_name = cell.strip()
            if well_name in wellMapping:
                current_chosen_id = wellMapping[well_name]
            else:
                excel_num = _WELL_NUM_RE.search(well_name)
                matched_key = keyByWellNum.get(int(excel_num.group(1))) if excel_num else None
                if matched_key:
                    print(f"  Matched '{well_name}' -> '{matched_key}'")
                    current_chosen_id = wellMapping[matched_key]
                else:
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

    return pd.DataFrame(rows, columns=COMBOCURVE_COLUMNS)

"""

Convert Ballard Petroleum Production Data from ProdView excel to ComboCurve Monthly Format

"""

def ballardProductionData(pathToData):

    print("Getting Ballard Petroleum Production Data")

    pathToData = _latest_file_in_dir(pathToData)

    data = pd.read_excel(pathToData)

    data = data[['RecordDate', 'API10', 'EstimatedOilProductionBBLS', 'EstimatedGasProductionMCF', 'EstimatedWaterProductionBBLS']]

    # API10 arrives as float (trailing .0); strip leading zeros and '.0', then pad to 14 chars
    data['API'] = data['API10'].astype(str).str.lstrip('0').str.replace('.0', '', regex=False) + '0000'
    data['dataSource'] = "other"

    data = data[['RecordDate', 'API', 'EstimatedOilProductionBBLS', 'EstimatedGasProductionMCF', 'EstimatedWaterProductionBBLS', 'dataSource']]
    data.columns = COMBOCURVE_COLUMNS

    return data

"""

Gets Kraken Resources Production Data from  excel and converts to ComboCurve Monthly Format

"""

def _normalizeWellName(s):
    return s.strip().upper().replace("#", "")


def krakenProductionData(pathToData, wellMapping):

    print("Getting Kraken Resources Production Data")

    pathToData = _latest_file_in_dir(pathToData)

    raw = pd.read_excel(pathToData, sheet_name="actual-size", header=None)

    # Row 2 has well names at columns 1, 4, 7, 10, ... (each well spans 3 cols: oil, gas, water)
    # Row 3 onward is data; column 0 is RecordDate
    well_row = raw.iloc[2]
    data_rows = raw.iloc[4:].reset_index(drop=True)

    wellMappingNorm = {_normalizeWellName(k): v for k, v in wellMapping.items()}

    well_cols = [(i, str(well_row.iloc[i])) for i in range(1, len(well_row), 3)
                 if pd.notna(well_row.iloc[i]) and "Total" not in str(well_row.iloc[i])]

    rows = []
    for col_idx, well_name in well_cols:
        if "DELORES" in well_name.upper():
            print(f"  Skipping {well_name} (not our well)")
            continue

        chosen_id = wellMapping.get(well_name.strip()) or wellMappingNorm.get(_normalizeWellName(well_name))
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
            rows.append({
                'date': parsed_date,
                'chosenID': chosen_id,
                'oil': row.iloc[col_idx],
                'gas': row.iloc[col_idx + 1],
                'water': row.iloc[col_idx + 2],
                'dataSource': 'other',
            })

    data = pd.DataFrame(rows, columns=COMBOCURVE_COLUMNS)
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=15)
    data = data[data['date'] >= cutoff]
    print(f"  Parsed {len(data)} production rows (last 15 days) for {len(well_cols) - 1} wells")

    return data



"""
    
    Merge daily production data with updated and original type curves from ComboCurve.

"""

def mergeProductionWithTypeCurves(dailyprod, updated, original, wellList, pathToDatabase):

    print("Begin Merging dailyprod with Orginal and Updated Type Curves")

    for df in (dailyprod, updated, original):
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True).dt.tz_localize(None)

    mergedData = pd.merge(updated, dailyprod, how='left', on=['date', 'well'], suffixes=('', '_dailyprod'))
    mergedData = pd.merge(mergedData, original, how='left', on=['date', 'well'], suffixes=('', '_original'))

    mergedData = mergedData.rename(columns={
        'oil': 'oil_updated',
        'gas': 'gas_updated',
        'water': 'water_updated',
    })

    for col in ['oil_original', 'gas_original', 'water_original',
                'oil_dailyprod', 'gas_dailyprod', 'water_dailyprod']:
        mergedData[col] = mergedData[col].fillna("")

    mergedData = (
        mergedData
        .sort_values(by=['wellName', 'date'])
        .drop(columns=['wellName_original', 'API_original', 'wellName_dailyprod', 'API_dailyprod'])
        .reset_index(drop=True)
    )

    mergedData.to_excel(os.path.join(pathToDatabase, r"daily_merge.xlsx"))

    print("Finished Merging Original and Updated Type Curves")

    return mergedData

"""
    
    Create cumulative production from daily production data, updated type curves, and original type curves.  The data should have a index column (1 through n) then date, well, oil, gas, water columns for daily production, updated type curves, and original type curve cumulative.    
    
"""

def cumulativeProduction(data, pathToDatabase):

    print("Begin Creating Cumulative Production Data")

    sourceCols = [
        'oil_dailyprod', 'gas_dailyprod', 'water_dailyprod',
        'oil_updated', 'gas_updated', 'water_updated',
        'oil_original', 'gas_original', 'water_original',
    ]
    cumCols = [f"{c}_cum" for c in sourceCols]
    outputCols = (
        ["day", "well", "wellName", "API"]
        + [f"{c}_dailyprod_cum" for c in ("oil", "gas", "water")]
        + [f"{c}_updated_cum" for c in ("oil", "gas", "water")]
        + [f"{c}_original_cum" for c in ("oil", "gas", "water")]
    )

    frames = []
    for well in data['well'].unique():
        wellData = data[data['well'] == well].copy()

        numProductionDays = len(wellData[wellData['oil_dailyprod'] != ""])

        for col in sourceCols:
            wellData[col] = wellData[col].replace("", 0).astype(float)

        for src, cum in zip(sourceCols, cumCols):
            series = wellData[src].cumsum()
            # Trailing zero-runs inherit the last non-zero cum; leading zeros stay 0.
            series = series.where(series != 0).ffill().fillna(0)
            wellData[cum] = series.shift(fill_value=0)

        wellData['day'] = np.arange(len(wellData))

        # Beyond the last day with actual production, blank out dailyprod cum columns.
        pastProduction = wellData['day'] >= numProductionDays
        for cum in ('oil_dailyprod_cum', 'gas_dailyprod_cum', 'water_dailyprod_cum'):
            wellData.loc[pastProduction, cum] = ""

        frames.append(wellData[outputCols])

    cumulativeData = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=outputCols)

    cumulativeData.to_excel(os.path.join(pathToDatabase, r"cumulative_production.xlsx"))

    print("Finished Creating Cumulative Production Data")

    return cumulativeData





"""
Convert Monthly PDS to ComboCurve Monthly Format    
    
"""

def pdsMonthlyData(pathToData):

    print("Getting PDS Monthly Production Data")

    pathToData = _latest_file_in_dir(pathToData)

    data = pd.read_csv(pathToData)

    data['API'] = data['API'].astype(str).str[:-2] + '00'
    data['Production Date'] = pd.to_datetime(data['Production Date'], format="%m/%d/%Y").dt.strftime("%Y-%m-%d")

    data = data[['Production Date', 'API', 'Oil Production', 'Gas Production', 'Water Production']]
    data['dataSource'] = "other"
    data.columns = COMBOCURVE_COLUMNS

    return data