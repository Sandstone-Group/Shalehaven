## This file contains functions for authenticating with the Novi API and retrieving data.
## Developed by Michael Tanner

import requests
import os
import math
import pandas as pd


BASE_URL = "https://insight.novilabs.com/api/"

## Read in AFE Summary from Excel file
def readAFESummary(pathToFile):
    
    afeData = pd.read_excel(pathToFile)
    
    return afeData


## Novi API Authentication and Data Retrieval Functions
def authNovi():
    username = os.environ.get("NOVI_USERNAME")
    password = os.environ.get("NOVI_PASSWORD")

    session = requests.Session()

    response = session.post(BASE_URL + "v2/sessions", json={"email": username, "password": password})
    
    response.raise_for_status()

    token = response.json()["authentication_token"]

    return token


# Function to retrieve wells within 5 miles of permit locations
def getWells(token, permitData, afeData, scope="us-horizontals"):

    # Calculate bounding box: 5 miles around the permit locations
    # 1 degree latitude ≈ 69 miles, 1 degree longitude ≈ 69 * cos(lat) miles
    miles = 5
    lat_offset = miles / 69.0
    avg_lat = permitData["Latitude"].mean()
    lon_offset = miles / (69.0 * abs(math.cos(math.radians(avg_lat))))

    min_lat = permitData["Latitude"].min() - lat_offset
    max_lat = permitData["Latitude"].max() + lat_offset
    min_lon = permitData["Longitude"].min() - lon_offset
    max_lon = permitData["Longitude"].max() + lon_offset

    formation = afeData["Landing Zone"].iloc[0]

    print(f"Searching for wells within 5 miles of permit locations, Formation: {formation}")
    print(f"Bounding box: Lat [{min_lat:.4f}, {max_lat:.4f}], Lon [{min_lon:.4f}, {max_lon:.4f}]")

    params = {
        "authentication_token": token,
        "scope": scope,
        "q[SHLLatitude_gteq]": min_lat,
        "q[SHLLatitude_lteq]": max_lat,
        "q[SHLLongitude_gteq]": min_lon,
        "q[SHLLongitude_lteq]": max_lon,
        "q[Formation_eq]": formation,
    }

    all_wells = []
    page = 1

    while True:
        params["page"] = page
        print(f"Fetching page {page}...")

        for attempt in range(1, 4):
            try:
                response = requests.get(BASE_URL + "v3/wells.json", params=params, timeout=120)
                response.raise_for_status()
                break
            except requests.exceptions.ReadTimeout:
                print(f"Request timed out (attempt {attempt}/3), retrying...")
                if attempt == 3:
                    raise

        data = response.json()

        if not data:
            break

        all_wells.extend(data)
        print(f"Page {page} returned {len(data)} wells ({len(all_wells)} total so far)")

        # If fewer results than a full page, we've reached the end
        if len(data) < 100:
            break

        page += 1

    print(f"Done. Retrieved {len(all_wells)} wells total.")
    return pd.DataFrame(all_wells)


# Function to retrieve well permits based on AFE Summary data
def getWellPermits(token, afeData, scope="us-horizontals"):

    all_permits = []

    for index, row in afeData.iterrows():
        api_number = row["API Number"]
        county = row["County"]
        state = row["State"]

        print(f"\nWell {index + 1}/{len(afeData)}: Searching for permits - ID {api_number}, {county} County, {state}")

        params = {
            "authentication_token": token,
            "scope": scope,
            "q[ID_eq]": api_number,
            "q[County_eq]": county,
            "q[State_eq]": state,
        }

        page = 1

        while True:
            params["page"] = page
            print(f"Fetching page {page}...")

            for attempt in range(1, 4):
                try:
                    response = requests.get(BASE_URL + "v3/well_permits.json", params=params, timeout=120)
                    response.raise_for_status()
                    break
                except requests.exceptions.ReadTimeout:
                    print(f"Request timed out (attempt {attempt}/3), retrying...")
                    if attempt == 3:
                        raise

            data = response.json()

            if not data:
                break

            all_permits.extend(data)
            print(f"Page {page} returned {len(data)} permits ({len(all_permits)} total so far)")

            if len(data) < 100:
                break

        page += 1

    print(f"Done. Retrieved {len(all_permits)} well permits total.")
    return pd.DataFrame(all_permits)

# Function to retrieve forecast well months and sum EUR for offset wells
def getWellForecast(token, offsetData, scope="us-horizontals"):

    oil_eur = []
    gas_eur = []
    water_eur = []

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

        if data:
            oil_sum = data[0].get("OilPerYear", 0) or 0
            gas_sum = data[0].get("GasPerYear", 0) or 0
            water_sum = data[0].get("WaterPerYear", 0) or 0
            print(f"  EUR - Oil: {oil_sum:,.0f}, Gas: {gas_sum:,.0f}, Water: {water_sum:,.0f}")
        else:
            oil_sum = 0
            gas_sum = 0
            water_sum = 0
            print(f"  No forecast data found.")

        oil_eur.append(oil_sum)
        gas_eur.append(gas_sum)
        water_eur.append(water_sum)

    offsetData = offsetData.copy()
    offsetData["Oil EUR"] = oil_eur
    offsetData["Gas EUR"] = gas_eur
    offsetData["Water EUR"] = water_eur

    print(f"\nDone. Retrieved forecast data for {len(offsetData)} wells.")
    return offsetData
