import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
import re
import difflib

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


"""
AFE vs Actuals reconciliation.

Reads the AFE master and JIB master workbooks, joins on the well (not the
AFE number), and writes a Power BI–ready workbook with a tidy Facts sheet
and a per-well Dimensions sheet.

Entry point: generateAfeActualReport(afeMasterPath, jibMasterPath, outputDir).
All three args default to SHALEHAVEN_AFE_MASTER_PATH, SHALEHAVEN_JIB_MASTER_PATH,
and SHALEHAVEN_DATABASE_PATH respectively.
"""

# CTB wells in this set are left as standalone rows (not allocated across
# siblings). Add entries when a CTB represents a legitimately separate scope.
CTB_ALLOCATION_SKIPLIST = {"AGGIE CTB"}

# Manual AFE rollups. Key = source Well Name (matched case/punct-insensitive).
# Non-empty target list: split 1/N across those specific AFE Well Names.
# Empty list: split across ALL OTHER wells in the same Folder as the source.
AFE_PROPERTY_ALIASES = {
    "ALL": [],  # ConocoPhillips - split 1/N across every other well in folder
}

# Manual JIB rollups. Key = source JIB Property Name; value = list of AFE
# Well Names to re-parent to. 1 target => 1-to-1 move; N targets => 1/N split.
JIB_PROPERTY_ALIASES = {
    "NICHOLS-TRULSON/PALERMO CTB": ["Nichols-Trulson 156-90-10-14H-1"],
    "PALERMO 3-31 CTB": ["Palermo 156-90-3-31H-2"],
    "PALERMO 31 CTB": ["Palermo 156-90-3-31H-3"],
    "PALERMO 3-31H / NICHOLS-TRULSON LL": [
        "Nichols-Trulson 156-90-10-14H-1",
        "Palermo 156-90-3-31H-2",
        "Palermo 156-90-3-31H-3",
    ],
    "PALERMO 3-31H / NICHOLS-TRULSON LL PAD": [
        "Nichols-Trulson 156-90-10-14H-1",
        "Palermo 156-90-3-31H-2",
        "Palermo 156-90-3-31H-3",
    ],
}

# Manual JIB AFE Number -> AFE Well Name map. For JIB rows whose Op AFE
# doesn't appear in the AFE master (so neither the AFE-key lookup nor any
# other rule can find a well), add an entry here to re-parent the row onto
# a real AFE well. Key is the normalized AFE Key (post prefix/suffix strip
# and trailing-zero normalization); value is the exact AFE Well Name.
# Keys support both integer-like "24032" and decimal-suffixed "171651.01".
JIB_AFE_TO_WELL = {
    "AZUL1H-1": "Clase Azul 1H",
    "AZUL1H-2": "Clase Azul 1H",
    "AZUL2H-1": "Clase Azul 2H",
    "AZUL2H-2": "Clase Azul 2H",
    # "171651.01": "Some Well Name",
    # "24032": "Some Well Name",
    # "24033": "Some Well Name",
    # "24034": "Some Well Name",
}

# Manual Property-Key merges. Use when fuzzy reconciliation leaves the same
# well split across two keys (typically an H-suffix mismatch like
# "Clase Azul 1" vs "Clase Azul 1H"). Each entry rewrites every row on both
# AFE and JIB sides with Property Key == `from_key` to the canonical key
# and display name.
#   `key`:  target Property Key (uppercase, alphanumeric only)
#   `name`: canonical Well/Property Name used for display (Property Name and
#           Property Name (Normalized) are both rebuilt from this)
PROPERTY_KEY_ALIASES = {
    "CLASEAZUL1":  {"key": "CLASEAZUL1H", "name": "Clase Azul 1H"},
    "CLASEAZUL2":  {"key": "CLASEAZUL2H", "name": "Clase Azul 2H"},
}

# Manual JIB Operator (legal name) -> clean Project name. Applied as the
# final pass of the operator map, so these entries win over any automatic
# match from the three-pass heuristic. Use when the automatic map picks a
# wrong (or no) folder for a given legal operator.
OPERATOR_TO_PROJECT = {
    "AETHON ENERGY OPERATING LLC": "Aethon Energy",
    "ADAMAS ENERGY LLC":           "Aethon Energy",  # Shiloh wells
}

# Generic tokens excluded when finding distinctive name tokens for the
# fuzzy operator-pair scope.
_GENERIC_OP_TOKENS = {
    "LLC", "LP", "INC", "CO", "CORP", "CORPORATION", "COMPANY",
    "ENERGY", "OIL", "GAS", "PERMIAN", "EAGLE", "HOLDINGS", "HOLDING",
    "OPERATING", "RESOURCES", "PRODUCTION", "PETROLEUM", "EXPLORATION",
    "PARTNERS", "MIDSTREAM", "ROCKIES", "BASIN", "FORD",
}


def normalizeAfeKey(val):
    """Strip prefixes (100*, XX-, MM-) and iteratively strip trailing
    .CAP/.CMP/.DRL tokens. Returns None for blank/nan."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    for prefix in ("100*", "XX-", "MM-"):
        if s.startswith(prefix):
            s = s[len(prefix):]
    while True:
        stripped = False
        for suffix in (".CAP", ".CMP", ".DRL"):
            if s.upper().endswith(suffix):
                s = s[: -len(suffix)]
                stripped = True
                break
        if not stripped:
            break
    s = s.strip()
    return s or None


def normalizePropertyKey(val):
    """Aggressive key: split on first comma, uppercase, drop non-alnum."""
    if pd.isna(val):
        return None
    s = str(val).split(",")[0].strip().upper()
    s = re.sub(r"[^A-Z0-9]", "", s)
    return s or None


def normalizePropertyName(val):
    """Less aggressive: uppercase + collapse whitespace. For Power BI slicer."""
    if pd.isna(val):
        return None
    s = re.sub(r"\s+", " ", str(val).strip().upper())
    return s or None


def normalizeOwnerName(val):
    """Uppercase, commas->spaces, collapse whitespace. Collapses near-duplicates."""
    if pd.isna(val):
        return None
    s = str(val).replace(",", " ").upper()
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


_JIB_TAX_RE = re.compile(r"\b(INTANGIBLE|TANGIBLE)\b", re.IGNORECASE)

# Tax fallback when neither INTANGIBLE nor TANGIBLE appears in the Major
# Description. Applied in order; first match wins. Rationale per O&G
# convention: expensed items (LOE, overhead, exploration) = Intangible;
# capitalized items with salvage value (facilities, equipment, current
# assets) = Tangible.
_JIB_TAX_FALLBACKS = (
    ("EQUIPMENT",            "Tangible"),
    ("FACILITIES",           "Tangible"),
    ("FACILITY",             "Tangible"),
    ("OTHER CURRENT ASSETS", "Tangible"),
    ("LEASE OPERATING",      "Intangible"),
    ("LEASE OP EXPENSES",    "Intangible"),
    ("LEASE OP",             "Intangible"),
    ("LEASE OPERATIONS",     "Intangible"),
    ("OPERATING EXPENSE",    "Intangible"),
    ("OVERHEAD",             "Intangible"),
    ("EXPLORATORY",          "Intangible"),
    ("EXPLORE",              "Intangible"),
    ("AFE EXPENDITURES",     "Intangible"),
    # "SAFE" in operator JIB files = safety/environmental expense (not
    # acronym), always OPEX -> Intangible. Exact-match only so we don't
    # accidentally catch future codes that happen to contain the substring.
    ("SAFE",                 "Intangible"),
)

# Order matters: more specific categories first (CWW before FACIL so
# "Facilty CWW" resolves to Capital Well Work, not Facility). Operating
# Expense variants fold into Overhead — the user prefers one bucket for
# all non-CAPEX cost, not two.
_JIB_CATEGORY_MAP = (
    ("CWW",                  "Capital Well Work"),
    ("CAPITAL WELL",         "Capital Well Work"),
    ("DRILL",                "Drilling"),
    ("COMPL",                "Completion"),
    ("FACIL",                "Facility"),
    ("CONSTRUCT",            "Facility"),
    ("OVERHEAD",             "Overhead"),
    ("LEASE OPERATING",      "Overhead"),
    ("LEASE OP",             "Overhead"),
    ("LEASE OPERATIONS",     "Overhead"),
    ("OPERATING EXPENSE",    "Overhead"),
    ("PRODUCTION",           "Production"),
    ("EQUIPMENT",            "Equipment"),
)

# When Major Description doesn't pin down a Category (e.g. Hunt Oil's generic
# "AFE Expenditures"), fall back to Minor Description — it carries the real
# cost classification. Matched substring -> Category. Order matters: more
# specific patterns first.
_JIB_MINOR_CATEGORY_MAP = (
    # Capital Well Work first (so "workover rig" doesn't match the Drilling block)
    ("WORKOVER",             "Capital Well Work"),
    ("WRKOVR",               "Capital Well Work"),
    # Drilling
    ("DOWNHOLE",             "Drilling"),
    ("DWNHOLE",              "Drilling"),
    ("DOWN HOLE",            "Drilling"),
    ("RIG MOB",              "Drilling"),
    ("RIG DEMOB",            "Drilling"),
    ("MUD ",                 "Drilling"),
    ("DRILL",                "Drilling"),
    ("DRLLNG",               "Drilling"),
    ("CHEMICAL",             "Drilling"),
    # Completion (COMPL catches truncations like "Compltn")
    ("WIRELINE",             "Completion"),
    ("WIRELNE",              "Completion"),
    ("CASING",               "Completion"),
    ("CEMENT",               "Completion"),
    ("PERFORAT",             "Completion"),
    ("COMPLET",              "Completion"),
    ("COMPL",                "Completion"),
    ("FRAC",                 "Completion"),
    ("STIMULAT",             "Completion"),
    ("TUBING",               "Completion"),
    # Facility (more specific before less specific)
    ("WELLHEAD",             "Facility"),
    ("WLLHD",                "Facility"),
    ("WELL CNSTR",           "Facility"),
    ("FACILITY",             "Facility"),
    ("FACILT",               "Facility"),
    ("FACIL",                "Facility"),
    ("CONSTRUCT",            "Facility"),
    ("CNSTR",                "Facility"),
    ("ROAD",                 "Facility"),
    ("VALVE",                "Facility"),
    ("FITTING",              "Facility"),
    ("FITTNG",               "Facility"),
    ("INSTRUMENT",           "Facility"),
    ("TANK BATTERY",         "Facility"),
    ("TANK",                 "Facility"),
    ("BATTERY",              "Facility"),
    ("WELDING",              "Facility"),
    ("PIPELINE",             "Facility"),
    ("ELECTRIC",             "Facility"),
    # Land
    ("RIGHTS OF WAY",        "Land"),
    ("EASEMENT",             "Land"),
    ("PERMIT",               "Land"),
    ("BROKERAGE",            "Land"),
    ("SURVEY",               "Land"),
    ("TITLE",                "Land"),
    ("LAND",                 "Land"),
    ("LEASE",                "Land"),
    # Overhead — includes everything that used to be "Operating Expense"
    # plus travel, generic labor/services/fees, and anything else ambiguous.
    ("OVERHEAD",             "Overhead"),
    ("ADMIN",                "Overhead"),
    ("CONSULTANT",           "Overhead"),
    ("CONSULTING",           "Overhead"),
    ("SUBSCRIPT",            "Overhead"),
    ("EMPLOYEE",             "Overhead"),
    ("CLIENT",               "Overhead"),
    ("PRINTED",              "Overhead"),
    ("ENVIRONMENT",          "Overhead"),
    ("TRANSPORT",            "Overhead"),
    ("WASTE",                "Overhead"),
    ("WATER",                "Overhead"),
    ("FUEL",                 "Overhead"),
    ("SAFETY",               "Overhead"),
    ("EHS",                  "Overhead"),
    ("AIRFARE",              "Overhead"),
    ("HOTEL",                "Overhead"),
    ("LODGING",              "Overhead"),
    ("TRAVEL",               "Overhead"),
    ("AUTO ",                "Overhead"),
    ("MEETING",              "Overhead"),
    ("BOOKS",                "Overhead"),
    ("CONTRACT LABOR",       "Overhead"),
    ("CO LABR",              "Overhead"),
    ("INSPECTION",           "Overhead"),
    ("FEES",                 "Overhead"),
    ("MISCELLANEOUS",        "Overhead"),
    ("MISC ",                "Overhead"),
)


def parseJibMajorDescription(major, minor=None):
    """Derive (Tax, Category) from JIB Major Description, using Minor
    Description as Category fallback. Handles casings, spellings, OPEX
    variants, and Hunt Oil's generic `AFE Expenditures` label whose real
    category signal lives in Minor Description:
      ('Intangible Drilling', any)          -> ('Intangible', 'Drilling')
      ('TANGIBLE CONSTRUCTION', any)        -> ('Tangible',   'Facility')
      ('Intangible Facilty CWW', any)       -> ('Intangible', 'Capital Well Work')
      ('Lease Operating Expenses', any)     -> ('Intangible', 'Operating Expense')
      ('AFE Expenditures', 'Drllng Fluids') -> ('Intangible', 'Drilling')
      ('AFE Expenditures', 'Administrative OH') -> ('Intangible', 'Overhead')
      ('Cash Call' / 'Revenue', any)        -> (None, None)  # not an expense
    """
    if _isBlank(major):
        return (None, None)
    s = str(major).upper()
    taxMatch = _JIB_TAX_RE.search(s)
    tax = taxMatch.group(1).title() if taxMatch else None
    if tax is None:
        for needle, fallbackTax in _JIB_TAX_FALLBACKS:
            if needle in s:
                tax = fallbackTax
                break
    category = None
    for needle, label in _JIB_CATEGORY_MAP:
        if needle in s:
            category = label
            break
    if category is None and not _isBlank(minor):
        minorUpper = str(minor).upper()
        for needle, label in _JIB_MINOR_CATEGORY_MAP:
            if needle in minorUpper:
                category = label
                break
    # Anything tax-tagged but still unclassified is Overhead per operator
    # convention. Cash Call / Revenue (tax=None) legitimately stay None.
    if category is None and tax is not None:
        category = "Overhead"
    return (tax, category)


def _normalizeOperatorKey(val):
    """Normalize an operator legal name for OPERATOR_TO_PROJECT lookup.
    Uppercase; commas -> spaces (so 'ENERGY, LLC' stays two tokens);
    periods stripped entirely (so 'L.L.C.' collapses to 'LLC', not 'L L C');
    whitespace collapsed. Makes matches tolerant to comma/period variations."""
    if _isBlank(val):
        return ""
    s = str(val).upper().replace(",", " ").replace(".", "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _distinctiveTokens(name):
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return set()
    return {
        t for t in re.split(r"[^A-Z0-9]+", str(name).upper())
        if len(t) >= 4 and t not in _GENERIC_OP_TOKENS
    }


def buildOperatorMap(afe, jib):
    """Four-pass legal_name -> clean_folder map. Passes 1-3 are automatic
    heuristics (shared Property Key, shared AFE Key, title-case prefix);
    pass 4 applies OPERATOR_TO_PROJECT overrides and is the final word."""
    operatorMap = {}

    # Pass 1: shared Property Key
    afeFolderByKey = (
        afe.dropna(subset=["Property Key", "Folder"])
           .groupby("Property Key")["Folder"].first()
    )
    jibOpByKey = (
        jib.dropna(subset=["Property Key", "Operator"])
           .groupby("Property Key")["Operator"].first()
    )
    for key in set(afeFolderByKey.index) & set(jibOpByKey.index):
        legal = jibOpByKey[key]
        clean = afeFolderByKey[key]
        if legal and clean:
            operatorMap.setdefault(legal, clean)

    # Pass 2: shared AFE Key
    afeFolderByAfe = (
        afe.dropna(subset=["AFE Key", "Folder"])
           .groupby("AFE Key")["Folder"].first()
    )
    jibOpByAfe = (
        jib.dropna(subset=["AFE Key", "Operator"])
           .groupby("AFE Key")["Operator"].first()
    )
    for key in set(afeFolderByAfe.index) & set(jibOpByAfe.index):
        legal = jibOpByAfe[key]
        clean = afeFolderByAfe[key]
        if legal and clean and legal not in operatorMap:
            operatorMap[legal] = clean

    # Pass 3: title-case prefix fallback (longest match wins)
    folders = sorted(afe["Folder"].dropna().unique().tolist(), key=len, reverse=True)
    for op in jib["Operator"].dropna().unique():
        if op in operatorMap:
            continue
        opTitle = str(op).title()
        for folder in folders:
            if opTitle.startswith(folder):
                operatorMap[op] = folder
                break

    # Pass 4: manual OPERATOR_TO_PROJECT overrides (win over everything above)
    # Match JIB operators against override keys using a normalized form
    # (uppercase, commas->spaces, collapsed whitespace) so variations like
    # "AETHON ENERGY OPERATING LLC" vs "AETHON ENERGY OPERATING, LLC" match.
    if OPERATOR_TO_PROJECT:
        normalizedOverrides = {
            _normalizeOperatorKey(k): v for k, v in OPERATOR_TO_PROJECT.items()
        }
        for op in jib["Operator"].dropna().unique():
            normalized = _normalizeOperatorKey(op)
            if normalized and normalized in normalizedOverrides:
                operatorMap[op] = normalizedOverrides[normalized]

    return operatorMap


def fuzzyWellReconcile(afe, jib, operatorMap):
    """Recover unmatched wells within operator pairs that share a distinctive
    token. Returns {jibPropertyKey: afePropertyKey} aliases at ratio >= 0.80."""
    aliases = {}
    folders = afe["Folder"].dropna().unique().tolist()
    folderTokens = {f: _distinctiveTokens(f) for f in folders}
    allOps = jib["Operator"].dropna().unique().tolist()

    candidatePairs = []
    for op in allOps:
        opTokens = _distinctiveTokens(op)
        if not opTokens:
            continue
        for folder, ftokens in folderTokens.items():
            if opTokens & ftokens:
                candidatePairs.append((op, folder))

    for op, folder in candidatePairs:
        jibWells = jib[jib["Operator"] == op]["Property Key"].dropna().unique().tolist()
        afeWellsInFolder = set(afe[afe["Folder"] == folder]["Property Key"].dropna().unique())
        jibAllKeys = set(jib["Property Key"].dropna().unique())

        unmatchedJib = [w for w in jibWells if w not in afeWellsInFolder]
        unmatchedAfe = [w for w in afeWellsInFolder if w not in jibAllKeys]
        if not unmatchedJib or not unmatchedAfe:
            continue

        candidates = []
        for jw in unmatchedJib:
            for aw in unmatchedAfe:
                ratio = difflib.SequenceMatcher(None, jw, aw).ratio()
                if ratio >= 0.80:
                    candidates.append((ratio, jw, aw))
        candidates.sort(reverse=True)

        usedJib, usedAfe = set(), set()
        localAliases = {}
        for ratio, jw, aw in candidates:
            if jw in usedJib or aw in usedAfe:
                continue
            usedJib.add(jw)
            usedAfe.add(aw)
            localAliases[jw] = aw

        if localAliases:
            operatorMap[op] = folder
            for jw, aw in localAliases.items():
                aliases[jw] = aw

    return aliases


def _applyPropertyKeyAliases(df, nameCol):
    """Merge divergent Property Keys onto a canonical identity. For every row
    whose Property Key matches a key in PROPERTY_KEY_ALIASES, rewrite the
    Property Key, the source-specific name column (Well Name or Property
    Name), and Property Name (Normalized) to the canonical values."""
    if "Property Key" not in df.columns or not PROPERTY_KEY_ALIASES:
        return df
    for badKey, target in PROPERTY_KEY_ALIASES.items():
        mask = df["Property Key"] == badKey
        if not mask.any():
            continue
        df.loc[mask, "Property Key"] = target["key"]
        if nameCol in df.columns:
            df.loc[mask, nameCol] = target["name"]
        if "Property Name (Normalized)" in df.columns:
            df.loc[mask, "Property Name (Normalized)"] = normalizePropertyName(target["name"])
    return df


def _applyCtbAllocation(afe):
    """Split CTB AFE rows across sibling wells sharing the prefix. Conservation
    invariant: total AFE cost unchanged — only redistributed."""
    toAdd = []
    toDrop = []
    for idx, row in afe.iterrows():
        well = row.get("Well Name")
        if pd.isna(well):
            continue
        wellStr = str(well).strip()
        if not wellStr.upper().endswith(" CTB"):
            continue
        if wellStr.upper() in CTB_ALLOCATION_SKIPLIST:
            continue
        prefix = wellStr[:-4].strip() + " "
        folder = row.get("Folder")
        siblingMask = (
            (afe["Folder"] == folder)
            & (afe["Well Name"].astype(str).str.startswith(prefix, na=False))
            & (afe.index != idx)
        )
        siblings = afe.loc[siblingMask, "Well Name"].dropna().unique().tolist()
        siblings = [s for s in siblings if not str(s).strip().upper().endswith(" CTB")]
        if not siblings:
            continue
        n = len(siblings)
        gross = row.get("Gross Cost") or 0
        net = row.get("Net Cost") or 0
        for sib in siblings:
            newRow = row.copy()
            newRow["Well Name"] = sib
            newRow["Property Key"] = normalizePropertyKey(sib)
            newRow["Property Name (Normalized)"] = normalizePropertyName(sib)
            newRow["Gross Cost"] = gross / n
            newRow["Net Cost"] = net / n
            origDesc = str(row.get("Description") or "").strip()
            newRow["Description"] = f"[1/{n} of {wellStr}] {origDesc}".strip()
            toAdd.append(newRow)
        toDrop.append(idx)

    if toDrop:
        afe = afe.drop(index=toDrop)
    if toAdd:
        afe = pd.concat([afe, pd.DataFrame(toAdd)], ignore_index=True)
    return afe.reset_index(drop=True)


def _applyAfeRollups(afe):
    """Apply AFE_PROPERTY_ALIASES. Non-empty target list = 1/N across those
    AFE Well Names; empty = 1/N across all other wells in the same Folder."""
    toAdd = []
    toDrop = []
    for src, targets in AFE_PROPERTY_ALIASES.items():
        srcKey = normalizePropertyKey(src)
        matches = afe[afe["Property Key"] == srcKey]
        if matches.empty:
            continue
        for idx, row in matches.iterrows():
            folder = row.get("Folder")
            if targets:
                targetWells = list(targets)
            else:
                targetWells = (
                    afe.loc[(afe["Folder"] == folder) & (afe.index != idx), "Well Name"]
                       .dropna().unique().tolist()
                )
                targetWells = [t for t in targetWells if normalizePropertyKey(t) != srcKey]
            if not targetWells:
                continue
            n = len(targetWells)
            gross = row.get("Gross Cost") or 0
            net = row.get("Net Cost") or 0
            for tw in targetWells:
                newRow = row.copy()
                newRow["Well Name"] = tw
                newRow["Property Key"] = normalizePropertyKey(tw)
                newRow["Property Name (Normalized)"] = normalizePropertyName(tw)
                newRow["Gross Cost"] = gross / n
                newRow["Net Cost"] = net / n
                origDesc = str(row.get("Description") or "").strip()
                newRow["Description"] = f"[1/{n} of {row.get('Well Name', src)}] {origDesc}".strip()
                toAdd.append(newRow)
            toDrop.append(idx)
    if toDrop:
        afe = afe.drop(index=toDrop)
    if toAdd:
        afe = pd.concat([afe, pd.DataFrame(toAdd)], ignore_index=True)
    return afe.reset_index(drop=True)


def _applyJibRollups(jib, afe):
    """Apply JIB_PROPERTY_ALIASES. Re-parents JIB rows onto real AFE wells,
    inheriting their Property Key / Property Name / Property Name (Normalized)."""
    afeWellLookup = {}
    for _, r in afe.iterrows():
        name = r.get("Well Name")
        if pd.isna(name):
            continue
        afeWellLookup[str(name)] = {
            "Property Key": r.get("Property Key"),
            "Property Name (Normalized)": r.get("Property Name (Normalized)"),
        }

    toAdd = []
    toDrop = []
    for src, targets in JIB_PROPERTY_ALIASES.items():
        srcKey = normalizePropertyKey(src)
        matches = jib[jib["Property Key"] == srcKey]
        if matches.empty:
            continue
        valid = [t for t in targets if t in afeWellLookup]
        if not valid:
            continue
        n = len(valid)
        for idx, row in matches.iterrows():
            gross = row.get("Gross Invoiced") or 0
            net = row.get("Net Expense") or 0
            for tw in valid:
                info = afeWellLookup[tw]
                newRow = row.copy()
                newRow["Property Name"] = tw
                newRow["Property Key"] = info["Property Key"]
                newRow["Property Name (Normalized)"] = info["Property Name (Normalized)"]
                newRow["Gross Invoiced"] = gross / n
                newRow["Net Expense"] = net / n
                tag = f"[from {src}]" if n == 1 else f"[1/{n} from {src}]"
                origNote = str(row.get("Detail Line Notation") or "").strip()
                newRow["Detail Line Notation"] = f"{tag} {origNote}".strip()
                toAdd.append(newRow)
            toDrop.append(idx)

    if toDrop:
        jib = jib.drop(index=toDrop)
    if toAdd:
        jib = pd.concat([jib, pd.DataFrame(toAdd)], ignore_index=True)
    return jib.reset_index(drop=True)


# Truly invisible Unicode chars — NBSP, zero-width space / joiner / non-joiner,
# word-joiner, BOM, Mongolian vowel separator, LRM/RLM, etc. Regular ASCII
# whitespace is intentionally NOT in this class: stripping it would destroy
# multi-word values like "AETHON ENERGY OPERATING LLC" or "Drilly Idol A15501LH".
# Leading/trailing whitespace is handled separately by .strip() below.
_INVISIBLE_RE = re.compile(
    r"[\u00a0\u180e\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]"
)


def _stripInvisible(v):
    """Strip Unicode invisible chars (NBSP, ZWSP, BOM, etc.) but preserve
    regular whitespace. Leading/trailing whitespace is stripped.
    Returns '' for None/NaN."""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    try:
        return _INVISIBLE_RE.sub("", str(v)).strip()
    except Exception:
        return ""


def _isBlank(v):
    """True for None, NaN, pd.NA/NaT, or any string that — after stripping
    ASCII + invisible Unicode whitespace — is '', 'nan', 'none', '<na>', or 'nat'."""
    s = _stripInvisible(v).lower()
    return s in ("", "nan", "none", "<na>", "nat")


def _firstNonBlank(series):
    """First value in a Series/iterable that passes _isBlank, else None."""
    for v in series:
        if not _isBlank(v):
            return v
    return None


def _stampIdentity(df, idx, nameCol, name):
    """Write the well-identity triplet onto a single row: nameCol = name,
    Property Key = normalizePropertyKey(name), Property Name (Normalized) =
    normalizePropertyName(name). Used by backfills and last-mile fallbacks."""
    df.at[idx, nameCol] = name
    df.at[idx, "Property Key"] = normalizePropertyKey(name)
    df.at[idx, "Property Name (Normalized)"] = normalizePropertyName(name)


def _stampFallbackIdentity(df, nameCol, unmatchedPrefix, unknownLabel):
    """Last-mile guarantee: no row reaches Facts with a blank nameCol.
    For every row with a blank nameCol, pick the best available fallback:
    `({unmatchedPrefix} <raw AFE>)` > `(Key <property key>)` > `{unknownLabel}`,
    then stamp the well-identity triplet."""
    if nameCol not in df.columns:
        return df
    for idx in df.index:
        if not _isBlank(df.at[idx, nameCol]):
            continue
        raw = df.at[idx, "AFE Key Raw"] if "AFE Key Raw" in df.columns else None
        key = df.at[idx, "Property Key"] if "Property Key" in df.columns else None
        if not _isBlank(raw):
            label = f"({unmatchedPrefix} {_stripInvisible(raw)})"
        elif not _isBlank(key):
            label = f"(Key {_stripInvisible(key)})"
        else:
            label = f"({unknownLabel})"
        _stampIdentity(df, idx, nameCol, label)
    return df


def _backfillAfeWellNames(afe):
    """AFE rows with blank Well Name but a present AFE Number get a synthetic
    `(AFE-only <raw>)` label so Power BI never shows a blank Property slicer."""
    if "Well Name" not in afe.columns:
        return afe
    for idx in afe.index:
        if not _isBlank(afe.at[idx, "Well Name"]):
            continue
        raw = afe.at[idx, "AFE Key Raw"] if "AFE Key Raw" in afe.columns else None
        if _isBlank(raw):
            continue
        _stampIdentity(afe, idx, "Well Name", f"(AFE-only {_stripInvisible(raw)})")
    return afe


def _buildAfeLookup(afe, keyCol):
    """Build {keyCol_value: {Property Name, Property Key, Property Name
    (Normalized)}} for AFE rows where both keyCol and Well Name are
    non-blank. First occurrence wins on duplicate keys."""
    clean = afe.dropna(subset=[keyCol, "Well Name"])
    dedup = clean.drop_duplicates(subset=[keyCol], keep="first")
    lookup = {}
    for keyVal, name, pk, pn in zip(
        dedup[keyCol], dedup["Well Name"],
        dedup["Property Key"], dedup["Property Name (Normalized)"]
    ):
        if _isBlank(keyVal) or _isBlank(name):
            continue
        lookup[keyVal] = {
            "Property Name": name,
            "Property Key": pk,
            "Property Name (Normalized)": pn,
        }
    return lookup


def _backfillJibPropertyNames(jib, afe):
    """Two-purpose pass over JIB rows:

      A. UNCONDITIONAL manual override — any JIB row whose AFE Key matches
         JIB_AFE_TO_WELL is re-parented onto the target AFE well, regardless
         of whether its Property Name was blank. Escape hatch for source
         typos like `CLAUSE AZUL #1H` vs `Clase Azul 1H`.

      B. Blank-only backfill — for rows still blank after (A), try the AFE
         master via AFE Key, then fall back to a synthetic `(Unmatched AFE
         <raw>)` label."""
    if "Property Name" not in jib.columns:
        return jib

    afeByKey = _buildAfeLookup(afe, "AFE Key")
    afeByWellName = _buildAfeLookup(afe, "Well Name")

    def _applyInfo(idx, info):
        jib.at[idx, "Property Name"] = info["Property Name"]
        jib.at[idx, "Property Key"] = info["Property Key"]
        jib.at[idx, "Property Name (Normalized)"] = info["Property Name (Normalized)"]

    for idx in jib.index:
        key = jib.at[idx, "AFE Key"] if "AFE Key" in jib.columns else None

        if not _isBlank(key) and key in JIB_AFE_TO_WELL:
            target = JIB_AFE_TO_WELL[key]
            info = afeByWellName.get(target)
            if info:
                _applyInfo(idx, info)
            else:
                _stampIdentity(jib, idx, "Property Name", target)
            continue

        if not _isBlank(jib.at[idx, "Property Name"]):
            continue

        if not _isBlank(key) and key in afeByKey:
            _applyInfo(idx, afeByKey[key])
            continue

        raw = jib.at[idx, "AFE Key Raw"] if "AFE Key Raw" in jib.columns else None
        if _isBlank(raw):
            continue
        _stampIdentity(jib, idx, "Property Name", f"(Unmatched AFE {_stripInvisible(raw)})")
    return jib


def _cleanStringColumns(df, cols):
    """Normalize string columns: cells that are blank (including invisible
    Unicode whitespace) -> NaN; retained values are stripped of leading/
    trailing invisible chars. Vectorized via .map for ~50K row frames."""
    for col in cols:
        if col not in df.columns:
            continue
        series = df[col].astype("object")
        stripped = series.map(_stripInvisible)
        df[col] = stripped.where(stripped.map(lambda s: s.lower() not in ("", "nan", "none", "<na>", "nat")), np.nan)
    return df


def generateAfeActualReport(afeMasterPath=None, jibMasterPath=None, outputDir=None):
    """Reconcile AFE (projected) vs JIB (actual) costs; write afe_actual.xlsx.

    Joins on the well (aggressive Property Key), not on AFE Number, because a
    single well has multiple AFE numbers across its lifecycle (DRL/CMP/CAP/FC).

    Args default to SHALEHAVEN_AFE_MASTER_PATH, SHALEHAVEN_JIB_MASTER_PATH,
    SHALEHAVEN_DATABASE_PATH. Returns (factsDf, dimensionsDf).
    """
    afeMasterPath = afeMasterPath or os.getenv("SHALEHAVEN_AFE_MASTER_PATH")
    jibMasterPath = jibMasterPath or os.getenv("SHALEHAVEN_JIB_MASTER_PATH")
    outputDir = outputDir or os.getenv("SHALEHAVEN_DATABASE_PATH")
    missing = [
        n for n, v in [
            ("SHALEHAVEN_AFE_MASTER_PATH", afeMasterPath),
            ("SHALEHAVEN_JIB_MASTER_PATH", jibMasterPath),
            ("SHALEHAVEN_DATABASE_PATH", outputDir),
        ] if not v
    ]
    if missing:
        raise RuntimeError(
            "generateAfeActualReport missing required config: " + ", ".join(missing)
        )

    afe = pd.read_excel(afeMasterPath)
    afe = afe.loc[:, ~afe.columns.astype(str).str.startswith("Unnamed:")]

    jib = pd.read_excel(jibMasterPath)

    afe = _cleanStringColumns(afe, [
        "AFE Number", "Well Name", "Bucketing", "Tax", "Description",
        "Folder", "Fund", "Company Code",
    ])
    jib = _cleanStringColumns(jib, [
        "Operator", "Owner Name", "Op AFE", "Property Name",
        "Major Description", "Minor Description", "AFE Description",
        "Detail Line Notation", "Property Code", "Invoice Number",
    ])

    # drop opex JIB rows (no Op AFE number = out of scope) and fully-blank AFE rows
    jib = jib[jib["Op AFE"].notna()].reset_index(drop=True)
    afe = afe[~(afe["AFE Number"].isna() & afe["Well Name"].isna())].reset_index(drop=True)

    afe["AFE Key Raw"] = afe["AFE Number"]
    afe["AFE Key"] = afe["AFE Number"].apply(normalizeAfeKey)
    jib["AFE Key Raw"] = jib["Op AFE"]
    jib["AFE Key"] = jib["Op AFE"].apply(normalizeAfeKey)

    afe["Property Key"] = afe["Well Name"].apply(normalizePropertyKey)
    afe["Property Name (Normalized)"] = afe["Well Name"].apply(normalizePropertyName)
    jib["Property Key"] = jib["Property Name"].apply(normalizePropertyKey)
    jib["Property Name (Normalized)"] = jib["Property Name"].apply(normalizePropertyName)

    if "Owner Name" in jib.columns:
        jib["Owner Name"] = jib["Owner Name"].apply(normalizeOwnerName)

    afe = _backfillAfeWellNames(afe)
    jib = _backfillJibPropertyNames(jib, afe)

    operatorMap = buildOperatorMap(afe, jib)

    aliases = fuzzyWellReconcile(afe, jib, operatorMap)
    if aliases:
        jib["Property Key"] = jib["Property Key"].replace(aliases)

    afe = _applyPropertyKeyAliases(afe, "Well Name")
    jib = _applyPropertyKeyAliases(jib, "Property Name")

    afe = _applyCtbAllocation(afe)
    afe = _applyAfeRollups(afe)
    jib = _applyJibRollups(jib, afe)

    afe["Project"] = afe["Folder"]
    jib["Project"] = jib["Operator"].map(operatorMap).fillna(jib["Operator"])

    afe = _stampFallbackIdentity(afe, "Well Name", "AFE-only", "Unknown AFE")
    jib = _stampFallbackIdentity(jib, "Property Name", "Unmatched AFE", "Unknown JIB")

    # Canonicalize Property Name per Property Key so Power BI slicers don't
    # double-count wells spelled differently on the AFE vs JIB side (e.g.
    # "Chuck Fed 22-31-18SH" vs "CHUCK FED 22-31-18 SH"). AFE wins if present.
    afeCanonical = (
        afe.groupby("Property Key")["Well Name"].agg(_firstNonBlank).dropna()
    )
    jibCanonical = (
        jib.groupby("Property Key")["Property Name"].agg(_firstNonBlank).dropna()
    )
    canonicalName = {**jibCanonical.to_dict(), **afeCanonical.to_dict()}
    canonicalNorm = {k: normalizePropertyName(v) for k, v in canonicalName.items()}
    afe["Well Name"] = afe["Property Key"].map(canonicalName).fillna(afe["Well Name"])
    afe["Property Name (Normalized)"] = (
        afe["Property Key"].map(canonicalNorm).fillna(afe["Property Name (Normalized)"])
    )
    jib["Property Name"] = jib["Property Key"].map(canonicalName).fillna(jib["Property Name"])
    jib["Property Name (Normalized)"] = (
        jib["Property Key"].map(canonicalNorm).fillna(jib["Property Name (Normalized)"])
    )

    # Has Both: true when THIS WELL has sources on both sides
    afeKeys = set(afe["Property Key"].dropna().unique())
    jibKeys = set(jib["Property Key"].dropna().unique())
    bothKeys = afeKeys & jibKeys

    # Inherit Owner Name from JIB -> AFE by Property Key, so slicers can
    # filter AFE budgets alongside JIB actuals. When a well has multiple
    # owners on the JIB side, pick the one with the largest absolute Net
    # Expense (owner with the most economic stake).
    ownerByKey = {}
    if "Owner Name" in jib.columns and "Net Expense" in jib.columns:
        jibOwner = jib.dropna(subset=["Property Key", "Owner Name"]).copy()
        if not jibOwner.empty:
            jibOwner["_absNet"] = jibOwner["Net Expense"].abs().fillna(0)
            ranked = (
                jibOwner.groupby(["Property Key", "Owner Name"])["_absNet"].sum()
                        .reset_index()
                        .sort_values(["Property Key", "_absNet"], ascending=[True, False])
                        .drop_duplicates("Property Key")
            )
            ownerByKey = dict(zip(ranked["Property Key"], ranked["Owner Name"]))

    # JIB Major Description encodes Tax + Category as one string; split it
    # so both sides have the same three-column structure. Minor Description
    # is passed as Category fallback (Hunt Oil's generic "AFE Expenditures"
    # Major Description has no Category keyword, but the Minor does).
    jibMajor = jib.get("Major Description", pd.Series([None] * len(jib)))
    jibMinor = jib.get("Minor Description", pd.Series([None] * len(jib)))
    jibParsed = pd.Series(
        [parseJibMajorDescription(m, n) for m, n in zip(jibMajor, jibMinor)],
        index=jib.index,
    )
    jibTax = jibParsed.apply(lambda t: t[0])
    jibCategory = jibParsed.apply(lambda t: t[1])

    afeFacts = pd.DataFrame({
        "Source": "AFE",
        "AFE Number": afe["AFE Key"],
        "AFE Number (Raw)": afe["AFE Key Raw"],
        "Project": afe["Project"],
        "Operator (Legal)": pd.Series([pd.NA] * len(afe)),
        "Property Name": afe["Well Name"],
        "Property Name (Normalized)": afe["Property Name (Normalized)"],
        "Owner Name": afe["Property Key"].map(ownerByKey),
        "Tax": afe.get("Tax"),
        "Category": afe.get("Bucketing"),
        "Sub Category": afe.get("Description"),
        "Description": afe.get("Description"),
        "Gross Amount": afe.get("Gross Cost"),
        "Working Interest": afe.get("Working Interest"),
        "Net Amount": afe.get("Net Cost"),
        "Invoice Date": pd.Series([pd.NaT] * len(afe)),
        "Activity Month": pd.Series([pd.NaT] * len(afe)),
        "_Property Key": afe["Property Key"],
    })

    jibFacts = pd.DataFrame({
        "Source": "JIB",
        "AFE Number": jib["AFE Key"],
        "AFE Number (Raw)": jib["AFE Key Raw"],
        "Project": jib["Project"],
        "Operator (Legal)": jib.get("Operator"),
        "Property Name": jib["Property Name"],
        "Property Name (Normalized)": jib["Property Name (Normalized)"],
        "Owner Name": jib.get("Owner Name"),
        "Tax": jibTax,
        "Category": jibCategory,
        "Sub Category": jib.get("Minor Description"),
        "Description": jib.get("AFE Description"),
        "Gross Amount": jib.get("Gross Invoiced"),
        "Working Interest": jib.get("Working Interest"),
        "Net Amount": jib.get("Net Expense"),
        "Invoice Date": pd.to_datetime(jib.get("Invoice Date"), errors="coerce"),
        "Activity Month": pd.to_datetime(jib.get("Activity Month"), errors="coerce"),
        "_Property Key": jib["Property Key"],
    })

    facts = pd.concat([afeFacts, jibFacts], ignore_index=True)

    # split amounts by source so Power BI can aggregate with plain SUM
    isAfe = facts["Source"].eq("AFE")
    isJib = facts["Source"].eq("JIB")
    facts["AFE Gross Amount"] = facts["Gross Amount"].where(isAfe, 0.0)
    facts["AFE Net Amount"] = facts["Net Amount"].where(isAfe, 0.0)
    facts["Actual Gross Amount"] = facts["Gross Amount"].where(isJib, 0.0)
    facts["Actual Net Amount"] = facts["Net Amount"].where(isJib, 0.0)

    facts["Has Both AFE and JIB"] = facts["_Property Key"].isin(bothKeys)
    facts = facts.drop(columns=["_Property Key"])

    colOrder = [
        "Source", "AFE Number", "AFE Number (Raw)", "Project", "Operator (Legal)",
        "Property Name", "Property Name (Normalized)", "Owner Name",
        "Tax", "Category", "Sub Category", "Description",
        "Gross Amount", "Working Interest", "Net Amount",
        "AFE Gross Amount", "AFE Net Amount",
        "Actual Gross Amount", "Actual Net Amount",
        "Invoice Date", "Activity Month",
        "Has Both AFE and JIB",
    ]
    facts = facts[colOrder]

    # Dimensions sheet: one row per well
    dimRows = []
    for key in sorted({k for k in (afeKeys | jibKeys) if k}):
        a = afe[afe["Property Key"] == key]
        j = jib[jib["Property Key"] == key]
        # Mirror the old precedence exactly: AFE side is primary when present,
        # JIB only used when AFE is empty. After the backfill + last-mile
        # safety pass, these columns are guaranteed non-blank so no fallback
        # chain is needed — just read straight from the primary side.
        primary = a if not a.empty else j
        nameCol = "Well Name" if not a.empty else "Property Name"
        propName = _firstNonBlank(primary[nameCol])
        propNorm = _firstNonBlank(primary["Property Name (Normalized)"])
        project = _firstNonBlank(primary["Project"])
        operatorLegal = (
            _firstNonBlank(j["Operator"])
            if (not j.empty and "Operator" in j.columns) else None
        )
        owners = (
            ", ".join(sorted(j["Owner Name"].dropna().unique().tolist()))
            if (not j.empty and "Owner Name" in j.columns) else ""
        )
        afeKeysSeen = sorted(set(
            a["AFE Key"].dropna().unique().tolist()
            + j["AFE Key"].dropna().unique().tolist()
        ))
        dimRows.append({
            "Property Key": key,
            "Project": project,
            "Operator (Legal)": operatorLegal,
            "Property Name": propName,
            "Property Name (Normalized)": propNorm,
            "Owner Name": owners,
            "AFE Numbers": ", ".join(afeKeysSeen),
        })
    dimensions = (
        pd.DataFrame(dimRows)
          .sort_values(["Project", "Property Name"], na_position="last")
          .reset_index(drop=True)
    )

    outPath = os.path.join(outputDir, "afe_actual.xlsx")
    with pd.ExcelWriter(outPath, engine="openpyxl") as writer:
        facts.to_excel(writer, sheet_name="Facts", index=False)
        dimensions.to_excel(writer, sheet_name="Dimensions", index=False)

    return facts, dimensions