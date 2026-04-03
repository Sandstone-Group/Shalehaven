## This file contains functions for authenticating with the Novi API and retrieving data.
## Developed by Michael Tanner

import requests
import os
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


# Function to retrieve wells based on AFE Summary data
def getWells(token, afeData, scope="us-horizontals"):
   
   # pull out Township, Range, County, State, and Landing Zone from AFE Summary data
    township = afeData["Township"].iloc[0]
    range = afeData["Range"].iloc[0]
    county = afeData["County"].iloc[0]
    state = afeData["State"].iloc[0]
    formation = afeData["Landing Zone"].iloc[0]

    params = {
        "authentication_token": token,
        "scope": scope,
        "q[Township_eq]": township,
        "q[Range_eq]": range,
        "q[County_eq]": county,
        "q[State_eq]": state,
        "q[Formation_eq]": formation,
    }

    all_wells = []
    page = 1

    while True:
        params["page"] = page
        response = requests.get(BASE_URL + "v3/wells.json", params=params)
        response.raise_for_status()

        data = response.json()

        if not data:
            break

        all_wells.extend(data)

        # If fewer results than a full page, we've reached the end
        if len(data) < 100:
            break

        page += 1

    return pd.DataFrame(all_wells)


