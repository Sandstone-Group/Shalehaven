import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()
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


"""
Generates a Profit & Loss statement dataset in long format for Power BI.
Mirrors the structure of the LOS Scope Excel template:
  Production (Oil, Gas, BOE)
  Revenue (Oil, Gas, NGL, Total)
  Deductions
  Operating Expense
  Free Cash Flow
  CAPEX
  Net Cash Flow
  Cumulative Cash Flow
"""

# categorize JIB Major Descriptions into CAPEX vs Operating Expense
OPEX_DESCRIPTIONS = [
    "Lease Operating Expenses",
    "Lease Operating Expe",
    "Operating Expense",
]

CAPEX_DESCRIPTIONS = [
    "Intangible Drilling",
    "Intangible Completion",
    "Intangible Completio",
    "Intangible Facility",
    "Intangible Facilty CWW",
    "Intangible Capital Well Work",
    "Intangible Facility Costs",
    "Tangible Drilling",
    "Tangible Drilling Co",
    "Tangible Completion",
    "Tangible Facility",
    "Tangible Facility Co",
    "Tangible Facility Costs",
    "Tangible Capital Well Work",
    "AFE Expenditures",
    "Comp Generated Copas Overhead",
    "INTANGIBLE DRILLING COSTS",
    "INTANGIBLE DRILLING COST",
    "INTANGIBLE COMPLETION COSTS",
    "INTANGIBLE COMPL COST",
    "INTANGIBLE CONSTRUCTION COSTS",
    "INTANGIBLE FACILITY COST",
    "TANGIBLE DRILLING COSTS",
    "TANGIBLE DRILLING COST",
    "TANGIBLE COMPLETION COSTS",
    "TANGIBLE COMPL COST",
    "TANGIBLE CONSTRUCTION COSTS",
    "TANGIBLE FACILITY COST",
    "EXPLORATORY/APPRAISAL DRILL",
    "EXPLORATORY/APPRAISAL COMPLETE",
    "EXPLORE/APPRAISAL PRODUCTION FACILITIES",
]


OPERATOR_NAME_MAP = {
    "ADAMAS ENERGY LLC": "AETHON ENERGY OPERATING LLC",
    "DEVON ENERGY PROD CO, L.P.": "DEVON ENERGY PRODUCTION COMPANY, L.P.",
}

WELL_NAME_MAP = {
    "ARKLANDFED021443723XNH": "ARK LAND FED 02-144372-3XNH",
}


def generatePnlData(jibData, revenueData):
    rows = []

    # --- Revenue Data (monthly by operator & well) ---
    rev = revenueData.copy()
    rev["Prod Date"] = pd.to_datetime(rev["Prod Date"])
    rev["Month"] = rev["Prod Date"].dt.to_period("M").dt.to_timestamp()
    rev["Operator"] = rev["Operator Name"].replace(OPERATOR_NAME_MAP)
    rev["Well Name"] = rev["Property Description"]

    # classify products into Oil, Gas, NGL buckets
    product_map = {
        "Oil": ["Oil"],
        "Gas": ["Gas", "Residue Gas"],
        "NGL": ["Natural Gas Liquids", "Plant Products"],
    }
    rev["Product Bucket"] = rev["Product Description"].map(
        {prod: bucket for bucket, prods in product_map.items() for prod in prods}
    )
    rev["Product Bucket"] = rev["Product Bucket"].fillna("Other")

    grp_rev = ["Month", "Operator", "Well Name"]

    # production volumes by product bucket per month per operator per well
    vol = rev.groupby(grp_rev + ["Product Bucket"])["Owner Gross Volume"].sum().reset_index()
    for _, r in vol.iterrows():
        if r["Product Bucket"] == "Oil":
            rows.append({"Date": r["Month"], "Operator": r["Operator"], "Well Name": r["Well Name"], "Category": "Production", "Line Item": "Oil Production", "Expense Category": "Oil", "Sort Order": 1, "Value": r["Owner Gross Volume"]})
        elif r["Product Bucket"] == "Gas":
            rows.append({"Date": r["Month"], "Operator": r["Operator"], "Well Name": r["Well Name"], "Category": "Production", "Line Item": "Gas Production", "Expense Category": "Gas", "Sort Order": 2, "Value": r["Owner Gross Volume"]})

    # BOE = Oil + Gas/6 per operator per well
    boe = rev.groupby(grp_rev).apply(
        lambda g: g.loc[g["Product Bucket"] == "Oil", "Owner Gross Volume"].sum()
        + g.loc[g["Product Bucket"] == "Gas", "Owner Gross Volume"].sum() / 6
    ).reset_index(name="BOE")
    for _, r in boe.iterrows():
        rows.append({"Date": r["Month"], "Operator": r["Operator"], "Well Name": r["Well Name"], "Category": "Production", "Line Item": "BOE", "Expense Category": "", "Sort Order": 3, "Value": r["BOE"]})

    # revenue by product bucket per operator per well
    rev_by_product = rev.groupby(grp_rev + ["Product Bucket"])["Owner Gross Value"].sum().reset_index()
    sort_map = {"Oil": 5, "Gas": 6, "NGL": 7, "Other": 8}
    for _, r in rev_by_product.iterrows():
        rows.append({
            "Date": r["Month"],
            "Operator": r["Operator"],
            "Well Name": r["Well Name"],
            "Category": "Total Revenue",
            "Line Item": f"{r['Product Bucket']} Revenue",
            "Expense Category": r["Product Bucket"],
            "Sort Order": sort_map.get(r["Product Bucket"], 8),
            "Value": r["Owner Gross Value"],
        })


    # deductions per operator per well
    rev["Total Deductions"] = rev["Owner Gross Taxes"].fillna(0) + rev["Owner Gross Deducts"].fillna(0)
    deductions = rev.groupby(grp_rev)["Total Deductions"].sum().reset_index()
    for _, r in deductions.iterrows():
        rows.append({"Date": r["Month"], "Operator": r["Operator"], "Well Name": r["Well Name"], "Category": "Deductions", "Line Item": "Deductions", "Expense Category": "", "Sort Order": 10, "Value": r["Total Deductions"]})

    # net revenue per operator per well (Total Revenue + Deductions)
    net_rev = rev.groupby(grp_rev).agg(
        total_rev=("Owner Gross Value", "sum"),
        total_ded=("Total Deductions", "sum"),
    ).reset_index()
    net_rev["Net Revenue"] = net_rev["total_rev"] + net_rev["total_ded"]
    for _, r in net_rev.iterrows():
        rows.append({"Date": r["Month"], "Operator": r["Operator"], "Well Name": r["Well Name"], "Category": "Net Revenue", "Line Item": "Net Revenue", "Expense Category": "", "Sort Order": 11, "Value": r["Net Revenue"]})

    # --- JIB Data (monthly by operator & well) ---
    jib = jibData.copy()
    jib["Activity Month"] = pd.to_datetime(jib["Activity Month"])
    jib["Invoice Date"] = pd.to_datetime(jib["Invoice Date"])
    # fall back to Invoice Date when Activity Month is missing
    jib["Activity Month"] = jib["Activity Month"].fillna(jib["Invoice Date"])
    jib["Month"] = jib["Activity Month"].dt.to_period("M").dt.to_timestamp()
    jib["Operator"] = jib["Operator"].replace(OPERATOR_NAME_MAP)
    jib["Well Name"] = jib["Property Name"]

    grp_jib = ["Month", "Operator", "Well Name"]

    # operating expense per operator per well with expense category
    opex_mask = jib["Major Description"].isin(OPEX_DESCRIPTIONS)
    opex = jib[opex_mask].groupby(grp_jib + ["Minor Description"])["Net Expense"].sum().reset_index()
    for _, r in opex.iterrows():
        rows.append({"Date": r["Month"], "Operator": r["Operator"], "Well Name": r["Well Name"], "Category": "OPEX", "Line Item": "Operating Expense", "Expense Category": r["Minor Description"], "Sort Order": 12, "Value": -abs(r["Net Expense"])})

    # CAPEX per operator per well with expense category
    capex_mask = jib["Major Description"].isin(CAPEX_DESCRIPTIONS)
    capex = jib[capex_mask].groupby(grp_jib + ["Major Description"])["Net Expense"].sum().reset_index()
    for _, r in capex.iterrows():
        rows.append({"Date": r["Month"], "Operator": r["Operator"], "Well Name": r["Well Name"], "Category": "CAPEX", "Line Item": "CAPEX", "Expense Category": r["Major Description"], "Sort Order": 16, "Value": -abs(r["Net Expense"])})

    # build dataframe
    pnl = pd.DataFrame(rows)

    # compute Free Cash Flow and Net Cash Flow per month per operator
    monthly = pnl.groupby(["Date", "Operator", "Line Item"])["Value"].sum().reset_index()
    month_operator_pairs = pnl[["Date", "Operator"]].drop_duplicates()

    fcf_rows = []
    ncf_rows = []
    for _, pair in month_operator_pairs.iterrows():
        month, operator = pair["Date"], pair["Operator"]
        m = monthly[(monthly["Date"] == month) & (monthly["Operator"] == operator)]
        net_revenue = m.loc[m["Line Item"] == "Net Revenue", "Value"].sum()
        total_opex = m.loc[m["Line Item"] == "Operating Expense", "Value"].sum()
        total_capex = m.loc[m["Line Item"] == "CAPEX", "Value"].sum()

        ebitda = net_revenue + total_opex
        fcf = ebitda + total_capex

        fcf_rows.append({"Date": month, "Operator": operator, "Well Name": "", "Category": "EBITDA", "Line Item": "EBITDA", "Expense Category": "", "Sort Order": 14, "Value": ebitda})
        ncf_rows.append({"Date": month, "Operator": operator, "Well Name": "", "Category": "Free Cash Flow", "Line Item": "Free Cash Flow", "Expense Category": "", "Sort Order": 18, "Value": fcf})

    pnl = pd.concat([pnl, pd.DataFrame(fcf_rows), pd.DataFrame(ncf_rows)], ignore_index=True)

    # normalize well names - strip everything after the first comma, then apply name map
    pnl["Well Name"] = pnl["Well Name"].fillna("").str.split(",").str[0].str.strip()
    pnl["Well Name"] = pnl["Well Name"].replace(WELL_NAME_MAP)

    # add Category Sort for Power BI sorting (one value per section)
    category_sort_map = {
        "Production": 1,
        "Total Revenue": 2,
        "Deductions": 3,
        "Net Revenue": 4,
        "OPEX": 5,
        "EBITDA": 6,
        "CAPEX": 7,
        "Free Cash Flow": 8,
    }
    pnl["Category Sort"] = pnl["Category"].map(category_sort_map)

    # sort for readability
    pnl = pnl.sort_values(["Operator", "Date", "Category Sort", "Sort Order"]).reset_index(drop=True)

    return pnl