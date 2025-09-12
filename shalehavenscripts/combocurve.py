import pandas as pd
import numpy as np
import requests as requests
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from combocurve_api_v1 import ComboCurveAuth

"""

    Script to put excel data into ComboCurve - production-ready

"""


def putJoynWellProductionData(data, serviceAccount, comboCurveApi, daysToLookback):
    
    load_dotenv()  # load enviroment variables
    
    print("Start upsert of daily well production data for update records from excel")

    # connect to service account
    service_account = serviceAccount
    # set API Key from enviroment variable
    api_key = comboCurveApi
    # specific Python ComboCurve authentication
    combocurve_auth = ComboCurveAuth(service_account, api_key)

    # helps when uploading to ComboCurve to check for length of data (can only send 20,000 data points at a time)
    print("Length of Total Asset Production: " + str(len(data)))
    
    columnsComboCurve = [
        "date",
        "chosenID",
        "oil",
        "gas",
        "water",
        "dataSource",
    ]
    
    cleanComboCurveData = pd.DataFrame(columns=columnsComboCurve)
    
    # drop all rows with chosenId = 123456789
    cleanComboCurveData = cleanComboCurveData[cleanComboCurveData["chosenID"] != "123456789"]
    ##drop index column
    cleanComboCurveData = cleanComboCurveData.reset_index(drop=True)

    ## conver date to YYYY-MM-DD
    cleanComboCurveData["date"] = pd.to_datetime(cleanComboCurveData["date"])
    cleanComboCurveData["date"] = cleanComboCurveData["date"].dt.strftime("%Y-%m-%d")
    ## convert date to string
    cleanComboCurveData["date"] = cleanComboCurveData["date"].astype(str)      
    ## convert API to string
    cleanComboCurveData["chosenID"] = cleanComboCurveData["chosenID"].astype(str)
    
    
    totalAssetProductionJson = cleanComboCurveData.to_json(
        orient="records"
    )  # converts to internal json format
    
    # loads json into format that can be sent to ComboCurve
    cleanTotalAssetProduction = json.loads(totalAssetProductionJson)

    # prints length as final check (should be less than 20,000)
    print("Length of Sliced Data: " + str(len(cleanTotalAssetProduction)))

    # sets url to daily production for combo curve for daily production
    url = "https://api.combocurve.com/v1/daily-productions"
    auth_headers = combocurve_auth.get_auth_headers()  # authenticates ComboCurve

    # put request to ComboCurve
    response = requests.put(url, headers=auth_headers, json=cleanTotalAssetProduction)

    responseCode = response.status_code  # sets response code to the current state
    responseText = response.text  # sets response text to the current state

    print("Response Code: " + str(responseCode))  # prints response code

    if (
        "successCount" in responseText
    ):  # checks if the response text contains successCount
        # finds the index of successCount
        # prints the successCount and the number of data points sent
        indexOfSuccessFail = responseText.index("successCount")
        text = responseText[indexOfSuccessFail:]
        print(text)

    print(
        "Finished PUT "
        + str(len(cleanTotalAssetProduction))
        + " Rows of New Production Data to ComboCurve from JOYN"
    )

    return text