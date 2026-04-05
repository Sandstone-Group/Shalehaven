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

    # Calculate bounding box: 3 miles around the permit locations
    # 1 degree latitude ~ 69 miles, 1 degree longitude ~ 69 * cos(lat) miles
    miles = 3
    lat_offset = miles / 69.0
    avg_lat = permitData["Latitude"].mean()
    lon_offset = miles / (69.0 * abs(math.cos(math.radians(avg_lat))))

    # Expand permit min/max by offset to create the search boundary
    min_lat = permitData["Latitude"].min() - lat_offset
    max_lat = permitData["Latitude"].max() + lat_offset
    min_lon = permitData["Longitude"].min() - lon_offset
    max_lon = permitData["Longitude"].max() + lon_offset

    # Pull formation from AFE Summary and uppercase to match Novi format
    formation = afeData["Landing Zone"].iloc[0].upper()

    print(f"Searching for wells within 3 miles of permit locations, Formation: {formation}")
    print(f"Bounding box: Lat [{min_lat:.4f}, {max_lat:.4f}], Lon [{min_lon:.4f}, {max_lon:.4f}]")

    # Query well_details using SHL coordinates and formation filter
    params = {
        "authentication_token": token,
        "scope": scope,
        "q[SHLLatitude_gteq]": min_lat,
        "q[SHLLatitude_lteq]": max_lat,
        "q[SHLLongitude_gteq]": min_lon,
        "q[SHLLongitude_lteq]": max_lon,
        "q[Formation_eq]": formation,
    }

    # Paginate through results, 100 wells per page
    all_wells = []
    page = 1

    while True:
        params["page"] = page
        print(f"Fetching page {page}...")

        # Retry up to 5 times on timeout
        for attempt in range(1, 6):
            try:
                response = requests.get(BASE_URL + "v3/well_details.json", params=params, timeout=300)
                response.raise_for_status()
                break
            except requests.exceptions.ReadTimeout:
                print(f"Request timed out (attempt {attempt}/5), retrying...")
                if attempt == 5:
                    raise

        data = response.json()

        if not data:
            break

        all_wells.extend(data)
        print(f"Page {page} returned {len(data)} wells ({len(all_wells)} total so far)")

        # Less than 100 results means we've hit the last page
        if len(data) < 100:
            break

        page += 1

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
    pd.set_option('future.no_silent_downcasting', True)
    if "Latitude" in permitDf.columns and "BHLLatitude" in permitDf.columns:
        permitDf["Latitude"] = permitDf["Latitude"].fillna(permitDf["BHLLatitude"])
    if "Longitude" in permitDf.columns and "BHLLongitude" in permitDf.columns:
        permitDf["Longitude"] = permitDf["Longitude"].fillna(permitDf["BHLLongitude"])

    return permitDf


## Retrieve EUR forecasts for offset wells by summing all forecast years (up to 50 years)
## Queries forecast_well_years endpoint per well and sums OilPerYear, GasPerYear, WaterPerYear
## Appends Oil EUR, Gas EUR, Water EUR columns to the offset well DataFrame
def getWellForecast(token, offsetData, scope="us-horizontals"):

    # Collect EUR values for each well
    oil_eur = []
    gas_eur = []
    water_eur = []

    # Loop through each offset well and query its forecast
    for index, row in offsetData.iterrows():
        well_id = row["API10"]
        well_name = row.get("WellName", well_id)

        print(f"\nWell {index + 1}/{len(offsetData)}: Fetching forecast months for {well_name} (API10: {well_id})")

        params = {
            "authentication_token": token,
            "scope": scope,
            "q[API10_eq]": well_id,
        }

        print(f"  Querying well forecast data...")

        # Retry up to 3 times on timeout
        for attempt in range(1, 4):
            try:
                response = requests.get(BASE_URL + "v3/forecast_well_years.json", params=params, timeout=120)
                response.raise_for_status()
                break
            except requests.exceptions.ReadTimeout:
                print(f"  Request timed out (attempt {attempt}/3), retrying...")
                if attempt == 3:
                    raise

        data = response.json()

        # Sum across all forecast years (up to 50) to get total EUR
        if data:
            oil_sum = sum((year.get("OilPerYear", 0) or 0) for year in data)
            gas_sum = sum((year.get("GasPerYear", 0) or 0) for year in data)
            water_sum = sum((year.get("WaterPerYear", 0) or 0) for year in data)
            print(f"  Summed {len(data)} forecast years")
            print(f"  EUR - Oil: {oil_sum:,.0f}, Gas: {gas_sum:,.0f}, Water: {water_sum:,.0f}")
        else:
            oil_sum = 0
            gas_sum = 0
            water_sum = 0
            print(f"  No forecast data found.")

        oil_eur.append(oil_sum)
        gas_eur.append(gas_sum)
        water_eur.append(water_sum)

    # Append EUR columns to offset well data
    offsetData = offsetData.copy()
    offsetData["Oil EUR"] = oil_eur
    offsetData["Gas EUR"] = gas_eur
    offsetData["Water EUR"] = water_eur

    print(f"\nDone. Retrieved forecast data for {len(offsetData)} wells.")
    return offsetData


## Export offset well data with EUR to Excel file in the same directory as the AFE Summary
## Filename is headerData_{DSU Name}.xlsx where DSU Name is parsed from the AFE Summary filename after the "-"
def printHeaderData(forecastData, pathToAfeSummary):
    # Parse DSU name from AFE Summary filename (everything after the "-")
    outputDir = os.path.dirname(pathToAfeSummary)
    afeFilename = os.path.splitext(os.path.basename(pathToAfeSummary))[0]
    dsuName = afeFilename.split("-", 1)[1].strip() if "-" in afeFilename else afeFilename

    # Write to Excel in the same folder as the AFE Summary
    outputPath = os.path.join(outputDir, f"headerData_{dsuName}.xlsx")
    forecastData.to_excel(outputPath, index=False)
    print(f"Exported {len(forecastData)} offset wells with EUR to {outputPath}")
