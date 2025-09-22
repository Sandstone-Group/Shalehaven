import pandas as pd
import numpy as np
import requests as requests
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from combocurve_api_v1 import ComboCurveAuth, ServiceAccount

"""

    Script to put excel data into ComboCurve - production-ready

"""


def putDataComboCurve(data, serviceAccount, comboCurveApi):
    
    load_dotenv()  # load enviroment variables
    
    print("Start upsert of daily well production data for update records from excel")

    # connect to service account
    service_account = ServiceAccount.from_file(serviceAccount)
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
    
    cleanComboCurveData = data
    
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

"""
    
    Getting Wells from ComboCurve (company list) and filtering by Shalehaven - production-ready
    
"""

def getWellsFromComboCurve(serviceAccount, comboCurveApi):
    
    load_dotenv()  # load enviroment variables
    
    print("Getting Shalehaven Wells from ComboCurve")

    # connect to service account
    service_account = ServiceAccount.from_file(serviceAccount)
    # set API Key from enviroment variable
    api_key = comboCurveApi
    # specific Python ComboCurve authentication
    combocurve_auth = ComboCurveAuth(service_account, api_key)

    url = "https://api.combocurve.com/v1/wells?take=200"
    auth_headers = combocurve_auth.get_auth_headers()  # authenticates ComboCurve

    response = requests.get(url, headers=auth_headers)

    responseCode = response.status_code  # sets response code to the current state
    responseText = response.text  # sets response text to the current state
    
    if responseCode != 200:
        print("Error: Unable to fetch wells from ComboCurve")
    else:
        print("Successfully fetched Shalehaven wells from ComboCurve")

    wellsData = json.loads(responseText)
    
    wellsDataDf = pd.DataFrame(wellsData)
    
    # drop wells not in company "Shalehaven Asset Management"
    wellsDataDf = wellsDataDf[wellsDataDf["customString0"] == "Shalehaven Asset Management"]
    
    return wellsDataDf

"""

    Getting Daily Productions from ComboCurve for Shalehaven - production-ready    
    
"""

def getDailyProductionFromComboCurve(serviceAccount, comboCurveApi, wellList):
    
    load_dotenv()  # load enviroment variables
    
    print("Getting Daily Productions from ComboCurve")

    # connect to service account
    service_account = ServiceAccount.from_file(serviceAccount)
    # set API Key from enviroment variable
    api_key = comboCurveApi
    # specific Python ComboCurve authentication
    combocurve_auth = ComboCurveAuth(service_account, api_key)
    
    # paginate through all daily productions 1000 at a time
    all_daily_productions = []
    take = 1000
    skip = 0
    while True:
        url = f"https://api.combocurve.com/v1/daily-productions?take={take}&skip={skip}"
        auth_headers = combocurve_auth.get_auth_headers()  # authenticates ComboCurve

        response = requests.get(url, headers=auth_headers)

        responseCode = response.status_code  # sets response code to the current state
        responseText = response.text  # sets response text to the current state
        
        if responseCode != 200:
            print("Error: Unable to fetch daily productions from ComboCurve")
            break
        else:
            print(f"Successfully fetched {take} daily productions from ComboCurve (skip={skip})")

        dailyProductionsData = json.loads(responseText)
        
        if not dailyProductionsData:
            break
        
        all_daily_productions.extend(dailyProductionsData)
        skip += take
    
    dailyProductionsDf = pd.DataFrame(all_daily_productions)

    # filter dailyProductionsDf by wellList chosenID
    dailyProductionsDf = dailyProductionsDf[dailyProductionsDf["well"].isin(wellList["id"])]
    
    # add wellName and API column to dailyProductionsDf by matching well in dailyProductionsDf with id in wellList not using merge
    dailyProductionsDf["wellName"] = dailyProductionsDf["well"].map(wellList.set_index("id")["wellName"])
    dailyProductionsDf["API"] = dailyProductionsDf["well"].map(wellList.set_index("id")["chosenID"])
    
    # drop createdAt, updatedAt columns
    dailyProductionsDf = dailyProductionsDf.drop(columns=["createdAt", "updatedAt"])

    # reset index
    dailyProductionsDf = dailyProductionsDf.reset_index(drop=True)
    
    print("Finished Getting Daily Productions from ComboCurve")
    
    return dailyProductionsDf

"""
  
  Get Daily Forecast From ComboCurve - production-ready  
    
"""
 
def getDailyForecastFromComboCurve(serviceAccount, comboCurveApi, projectId, forecastId):
    
    load_dotenv()  # load enviroment variables
    
    print("Getting Daily Forecast from ComboCurve")

    # connect to service account
    service_account = ServiceAccount.from_file(serviceAccount)
    # set API Key from enviroment variable
    api_key = comboCurveApi
    # specific Python ComboCurve authentication
    combocurve_auth = ComboCurveAuth(service_account, api_key)
    
    # paginate through all daily forecasts 200 at a time
    all_daily_forecasts = []
    take = 200  # max take is 200
    skip = 0
    while True:
        url = f"https://api.combocurve.com/v1/forecast-daily-volumes?project={projectId}&forecast={forecastId}&take={take}&skip={skip}"
        auth_headers = combocurve_auth.get_auth_headers()  # authenticates ComboCurve

        response = requests.get(url, headers=auth_headers)

        responseCode = response.status_code  # sets response code to the current state
        responseText = response.text  # sets response text to the current state
        
        if responseCode != 200:
            print("Error: Unable to fetch daily forecasts from ComboCurve")
            break
        else:
            print(f"Successfully fetched {take} daily forecasts from ComboCurve (skip={skip})")

        dailyForecastData = json.loads(responseText)
        
        if not dailyForecastData:
            break
        
        all_daily_forecasts.extend(dailyForecastData)
        skip += take

    
    dailyForecastDataDf = pd.DataFrame(all_daily_forecasts)
    
    cleanDailyForecastDataDf = pd.DataFrame()

    # loop through each row in dailyForecastDataDf["phases"] and unpack the phases (in a json format) into separate rows and add to cleanDailyForecastDataDf
    for i in range(len(dailyForecastDataDf)):
        phases = dailyForecastDataDf["phases"][i]
        for j in range(len(phases)):
            phase = phases[j]
            phase

    x = 5
    
    return dailyForecastDataDf