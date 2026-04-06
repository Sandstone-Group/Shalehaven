## Novi API Functions - Authentication, Permit Lookup, Offset Well Search, EUR Forecasting, and Data Export
## Developed by Michael Tanner

import requests
import os
import math
import difflib
import pandas as pd


BASE_URL = "https://insight.novilabs.com/api/"


## Read in AFE Summary from Excel file
## Expects columns: API Number, County, State, Landing Zone, Well Name (for Texas wells)
def readAFESummary(pathToFile):

    afeData = pd.read_excel(pathToFile)

    return afeData


## Authenticate with Novi Labs API using environment variables NOVI_USERNAME and NOVI_PASSWORD
def authNovi():
    username = os.environ.get("NOVI_USERNAME")
    password = os.environ.get("NOVI_PASSWORD")

    session = requests.Session()

    response = session.post(BASE_URL + "v2/sessions", json={"email": username, "password": password})

    response.raise_for_status()

    token = response.json()["authentication_token"]

    return token


## Retrieve offset wells within 3 miles of permit locations, filtered by formation
## Uses a bounding box around permit lat/long to query well_details endpoint
## Formation name is pulled from AFE Summary "Landing Zone" column and uppercased for Novi
def getWells(token, permitData, afeData, scope="us-horizontals"):

    # Calculate bounding box around the permit locations
    # 1 degree latitude ~ 69 miles, 1 degree longitude ~ 69 * cos(lat) miles
    miles = float(input("Enter search radius in miles: ").strip())
    lat_offset = miles / 69.0
    avg_lat = permitData["Latitude"].mean()
    lon_offset = miles / (69.0 * abs(math.cos(math.radians(avg_lat))))

    # Expand permit min/max by offset to create the search boundary
    min_lat = permitData["Latitude"].min() - lat_offset
    max_lat = permitData["Latitude"].max() + lat_offset
    min_lon = permitData["Longitude"].min() - lon_offset
    max_lon = permitData["Longitude"].max() + lon_offset

    # Pull all unique formations from AFE Summary and uppercase to match Novi format
    formations = afeData["Landing Zone"].dropna().str.upper().unique().tolist()

    print(f"Searching for wells within {miles} miles of permit locations, Formations: {formations}")
    print(f"Bounding box: Lat [{min_lat:.4f}, {max_lat:.4f}], Lon [{min_lon:.4f}, {max_lon:.4f}]")

    # Query bulk endpoint with bounding box and formation filter
    params = {
        "authentication_token": token,
        "scope": scope,
        "q[MPLatitude_gteq]": min_lat,
        "q[MPLatitude_lteq]": max_lat,
        "q[MPLongitude_gteq]": min_lon,
        "q[MPLongitude_lteq]": max_lon,
        "q[Formation_in][]": formations,
    }

    print("Fetching offset wells via bulk endpoint...")

    for attempt in range(1, 6):
        try:
            response = requests.get(BASE_URL + "v3/bulk.json", params=params, timeout=600)
            response.raise_for_status()
            break
        except requests.exceptions.ReadTimeout:
            print(f"Request timed out (attempt {attempt}/5), retrying...")
            if attempt == 5:
                raise

    all_wells = response.json()

    print(f"Done. Retrieved {len(all_wells)} wells total.")
    return pd.DataFrame(all_wells)


## Retrieve well permits from Novi based on AFE Summary data
## Texas wells: searches by API10 + County + State (Texas permits don't use standard ID field)
## Non-Texas wells: searches by ID (API Number) + County + State
## Falls back to BHL coordinates when SHL Latitude/Longitude are missing (common for Texas permits)
def getWellPermits(token, afeData, scope="us-horizontals"):

    all_permits = []

    # Loop through each well in the AFE Summary
    for index, row in afeData.iterrows():
        api_number = row["API Number"]
        county = row["County"]
        state = row["State"]
        well_name = row.get("Well Name", "")
        is_texas = str(state).strip().lower() == "texas"

        # Build query params based on state - Texas uses API10, others use ID
        if is_texas:
            # Texas permits don't populate the standard ID field, so we use API10 instead
            print(f"\nWell {index + 1}/{len(afeData)}: Texas well - searching permits by API10")
            print(f"  API10: {api_number}, {county} County, {state}")

            params = {
                "authentication_token": token,
                "scope": scope,
                "q[API10_eq]": api_number,
                "q[County_eq]": county,
                "q[State_eq]": state,
            }
        else:
            # Non-Texas states use standard ID field for permit lookup
            print(f"\nWell {index + 1}/{len(afeData)}: Searching for permits - ID {api_number}, {county} County, {state}")

            params = {
                "authentication_token": token,
                "scope": scope,
                "q[ID_eq]": api_number,
                "q[County_eq]": county,
                "q[State_eq]": state,
            }

        # Paginate through permit results for this well
        page = 1
        well_permits = []

        while True:
            params["page"] = page
            print(f"Fetching page {page}...")

            # Retry up to 5 times on timeout
            for attempt in range(1, 6):
                try:
                    response = requests.get(BASE_URL + "v3/well_permits.json", params=params, timeout=300)
                    response.raise_for_status()
                    break
                except requests.exceptions.ReadTimeout:
                    print(f"Request timed out (attempt {attempt}/5), retrying...")
                    if attempt == 5:
                        raise

            data = response.json()

            if not data:
                break

            well_permits.extend(data)
            print(f"Page {page} returned {len(data)} permits ({len(well_permits)} total so far)")

            if len(data) < 100:
                break

            page += 1

        all_permits.extend(well_permits)

    print(f"Done. Retrieved {len(all_permits)} well permits total.")
    permitDf = pd.DataFrame(all_permits)

    # Texas permits often have null Lat/Lon but BHL coordinates are populated - use those as fallback
    if "Latitude" in permitDf.columns and "BHLLatitude" in permitDf.columns:
        permitDf["Latitude"] = permitDf["Latitude"].fillna(permitDf["BHLLatitude"])
    if "Longitude" in permitDf.columns and "BHLLongitude" in permitDf.columns:
        permitDf["Longitude"] = permitDf["Longitude"].fillna(permitDf["BHLLongitude"])

    return permitDf


## Retrieve EUR forecasts for offset wells by summing all forecast years (up to 50 years)
## Batches all API10s into a single request using q[API10_in][] filter
## Appends Oil EUR, Gas EUR, Water EUR columns to the offset well DataFrame
def getNoviYearlyForecast(token, offsetData, scope="us-horizontals"):

    api10_list = offsetData["API10"].tolist()
    print(f"Fetching yearly forecasts for {len(api10_list)} wells in batch...")

    params = {
        "authentication_token": token,
        "scope": scope,
        "q[API10_in][]": api10_list,
    }

    # Paginate through all results
    all_data = []
    page = 1

    while True:
        params["page"] = page

        for attempt in range(1, 4):
            try:
                response = requests.get(BASE_URL + "v3/forecast_well_years.json", params=params, timeout=300)
                response.raise_for_status()
                break
            except requests.exceptions.ReadTimeout:
                print(f"  Request timed out (attempt {attempt}/3), retrying...")
                if attempt == 3:
                    raise

        data = response.json()

        if not data:
            break

        all_data.extend(data)
        print(f"  Page {page}: {len(data)} records ({len(all_data)} total)")

        if len(data) < 100:
            break

        page += 1

    # Group by API10 and sum EUR
    eur_map = {}
    for record in all_data:
        api10 = record.get("API10")
        if api10 not in eur_map:
            eur_map[api10] = {"oil": 0, "gas": 0, "water": 0}
        eur_map[api10]["oil"] += (record.get("OilPerYear", 0) or 0)
        eur_map[api10]["gas"] += (record.get("GasPerYear", 0) or 0)
        eur_map[api10]["water"] += (record.get("WaterPerYear", 0) or 0)

    # Append EUR columns to offset well data
    offsetData = offsetData.copy()
    offsetData["Oil EUR"] = offsetData["API10"].map(lambda x: eur_map.get(x, {}).get("oil", 0))
    offsetData["Gas EUR"] = offsetData["API10"].map(lambda x: eur_map.get(x, {}).get("gas", 0))
    offsetData["Water EUR"] = offsetData["API10"].map(lambda x: eur_map.get(x, {}).get("water", 0))

    print(f"Done. Retrieved forecast data for {len(eur_map)} of {len(api10_list)} wells.")
    return offsetData


## Retrieve monthly forecast volumes for all wells in forecastData
## Uses the bulk export endpoint for a single fast download, then filters to matching API10s
def getNoviMonthlyForecast(token, forecastData, scope="us-horizontals"):

    api10_list = forecastData["API10"].tolist()
    api10_set = set(api10_list)
    print(f"Fetching monthly forecasts for {len(api10_list)} wells via bulk endpoint...")

    params = {
        "authentication_token": token,
        "scope": scope,
        "q[API10_in][]": api10_list,
    }

    for attempt in range(1, 6):
        try:
            response = requests.get(BASE_URL + "v3/bulk.json", params=params, timeout=600)
            response.raise_for_status()
            break
        except requests.exceptions.ReadTimeout:
            print(f"  Request timed out (attempt {attempt}/5), retrying...")
            if attempt == 5:
                raise

    data = response.json()
    print(f"Bulk endpoint returned {len(data)} records.")

    # Filter to only our wells
    monthlyDf = pd.DataFrame(data)
    if not monthlyDf.empty and "API10" in monthlyDf.columns:
        monthlyDf = monthlyDf[monthlyDf["API10"].isin(api10_set)]
        print(f"Filtered to {len(monthlyDf)} records matching {len(api10_set)} wells.")

    return monthlyDf


## Export header data and monthly forecast data to Excel files in the same directory as the AFE Summary
## headerData_{DSU Name}.xlsx - offset wells with EUR
## forecastData_{DSU Name}.xlsx - monthly forecast volumes
def printData(forecastData, monthlyForecastData, pathToAfeSummary):
    # Parse DSU name from AFE Summary filename (everything after the "-")
    afeDir = os.path.dirname(pathToAfeSummary)
    afeFilename = os.path.splitext(os.path.basename(pathToAfeSummary))[0]
    dsuName = afeFilename.split("-", 1)[1].strip() if "-" in afeFilename else afeFilename

    # Create "Data" subfolder next to AFE Summary (or use existing one)
    outputDir = os.path.join(afeDir, "Data")
    os.makedirs(outputDir, exist_ok=True)

    # Export header data (offset wells with EUR)
    headerPath = os.path.join(outputDir, f"headerData_{dsuName}.xlsx")
    forecastData.to_excel(headerPath, index=False)
    print(f"Exported {len(forecastData)} offset wells with EUR to {headerPath}")

    # Export monthly forecast data
    forecastPath = os.path.join(outputDir, f"forecastData_{dsuName}.xlsx")
    monthlyForecastData.to_excel(forecastPath, index=False)
    print(f"Exported {len(monthlyForecastData)} monthly forecast rows to {forecastPath}")
