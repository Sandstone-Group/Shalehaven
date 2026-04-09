import pandas as pd
import numpy as np
import requests as requests
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from combocurve_api_v1 import ComboCurveAuth, ServiceAccount
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os


"""

    ComboCurve client wrapper - pooled session, retries, paginated reads.

"""

class ComboCurveClient:
    """Thin wrapper around ComboCurve auth + a pooled, retrying requests Session."""

    BASE_URL = "https://api.combocurve.com"

    def __init__(self, service_account_path, api_key, base_url=None):
        self._auth = ComboCurveAuth(
            ServiceAccount.from_file(service_account_path),
            api_key,
        )
        self.base_url = base_url or self.BASE_URL
        self.session = self._build_session()

    @classmethod
    def from_env(cls,
                 service_account_var="SANDSTONE_COMBOCURVE_API_SEC_CODE",
                 api_key_var="SANDSTONE_COMBOCURVE_API_KEY_PASS",
                 base_url=None):
        """Build a client from environment variables (loads .env automatically)."""
        load_dotenv()
        service_account_path = os.getenv(service_account_var)
        api_key = os.getenv(api_key_var)
        if not service_account_path or not api_key:
            raise RuntimeError(
                f"ComboCurveClient.from_env: missing {service_account_var} or {api_key_var}"
            )
        return cls(service_account_path, api_key, base_url=base_url)

    @staticmethod
    def _build_session():
        s = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1.0,                       # 1s, 2s, 4s, 8s, 16s
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "HEAD", "PUT", "POST", "PATCH", "DELETE"),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        s.mount("https://", adapter)
        return s

    def _headers(self):
        # CC tokens are short-lived; refetch each call so the SDK handles refresh
        return self._auth.get_auth_headers()

    def request(self, method, path, **kwargs):
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        kwargs.setdefault("timeout", 60)
        resp = self.session.request(method, url, headers=self._headers(), **kwargs)
        resp.raise_for_status()
        return resp

    def get(self, path, **kw):    return self.request("GET", path, **kw)
    def put(self, path, **kw):    return self.request("PUT", path, **kw)
    def post(self, path, **kw):   return self.request("POST", path, **kw)
    def patch(self, path, **kw):  return self.request("PATCH", path, **kw)
    def delete(self, path, **kw): return self.request("DELETE", path, **kw)

    def paginate(self, path, params=None, take=1000):
        """Yield items from a paginated CC list endpoint."""
        params = dict(params or {})
        params["take"] = take
        skip = 0
        while True:
            params["skip"] = skip
            batch = self.get(path, params=params).json()
            if not batch:
                return
            yield from batch
            if len(batch) < take:
                return
            skip += take


"""

    Script to put daily excel data into ComboCurve - production-ready

"""

def putDataComboCurveDaily(client, data):

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

    response = client.put("/v1/daily-productions", json=cleanTotalAssetProduction)

    responseJson = response.json()
    successCount = responseJson.get("successCount", 0)
    failedCount = responseJson.get("failedCount", 0)
    text = f"Success: {successCount} Failed: {failedCount}"
    print(text)
    if failedCount > 0:
        print("Errors: " + str(responseJson.get("results", []))[:500])

    print(
        "Finished PUT "
        + str(len(cleanTotalAssetProduction))
        + " Rows of New Production Data to ComboCurve from JOYN"
    )

    return text

"""

    Script to put monthly excel data into ComboCurve - production-ready

"""

def putDataComboCurveMonthly(client, data):

    totalAssetProductionJson = data.to_json(
        orient="records"
    )  # converts to internal json format

    # loads json into format that can be sent to ComboCurve
    cleanTotalAssetProduction = json.loads(totalAssetProductionJson)

    response = client.put("/v1/monthly-productions", json=cleanTotalAssetProduction)

    responseJson = response.json()
    successCount = responseJson.get("successCount", 0)
    failedCount = responseJson.get("failedCount", 0)
    text = f"Success: {successCount} Failed: {failedCount}"
    print(text)
    if failedCount > 0:
        print("Errors: " + str(responseJson.get("results", []))[:500])

    print(
        "Finished PUT "
        + str(len(cleanTotalAssetProduction))
        + " Rows of New Production Data to ComboCurve from JOYN"
    )

    return text


"""

    Getting Wells from ComboCurve (company list) and filtering by Shalehaven - production-ready

"""

def getWellsFromComboCurve(client):

    print("Getting Shalehaven Wells from ComboCurve")

    wellsData = list(client.paginate("/v1/wells", take=200))

    print("Successfully fetched Shalehaven wells from ComboCurve")

    wellsDataDf = pd.DataFrame(wellsData)

    # drop wells not in company "Shalehaven Asset Management"
    wellsDataDf = wellsDataDf[wellsDataDf["customString0"] == "Shalehaven Asset Management"]

    return wellsDataDf

"""

    Getting Daily Productions from ComboCurve for Shalehaven - production-ready

"""

def getDailyProductionFromComboCurve(client, wellList, pathToDatabase):

    print("Getting Daily Productions from ComboCurve")

    all_daily_productions = list(client.paginate("/v1/daily-productions", take=1000))

    print(f"Successfully fetched {len(all_daily_productions)} daily productions from ComboCurve")

    dailyProductionsDf = pd.DataFrame(all_daily_productions)

    # filter dailyProductionsDf by wellList chosenID
    dailyProductionsDf = dailyProductionsDf[dailyProductionsDf["well"].isin(wellList["id"])]

    # add wellName and API column to dailyProductionsDf by matching well in dailyProductionsDf with id in wellList not using merge
    dailyProductionsDf["wellName"] = dailyProductionsDf["well"].map(wellList.set_index("id")["wellName"])
    dailyProductionsDf["API"] = dailyProductionsDf["well"].map(wellList.set_index("id")["chosenID"])

    # drop createdAt, updatedAt columns
    dailyProductionsDf = dailyProductionsDf.drop(columns=["createdAt", "updatedAt"])

    dailyProductionsDf.to_excel(os.path.join(pathToDatabase, r"daily_production.xlsx"))


    print("Finished Getting Daily Productions from ComboCurve")

    return dailyProductionsDf

"""

  Get Daily Forecast From ComboCurve - production-ready

"""

def getDailyForecastFromComboCurve(client, projectId, forecastId, wellList):

    print("Getting Daily Forecast from ComboCurve")

    all_daily_forecasts = list(client.paginate(
        "/v1/forecast-daily-volumes",
        params={"project": projectId, "forecast": forecastId},
        take=200,  # max take is 200
    ))

    print(f"Successfully fetched {len(all_daily_forecasts)} daily forecasts from ComboCurve")

    # Initialize lists to store DataFrame data
    wells = []
    dates = []
    phases = []
    volumes = []

    for entry in all_daily_forecasts:
        wellId = entry["well"]
        for phaseData in entry["phases"]:
            phase = phaseData["phase"]
            series = phaseData["series"][0] # Assuming best series
            startDate = datetime.strptime(series["startDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
            endDate = datetime.strptime(series["endDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
            volumeList = series["volumes"]

            # generate dates from startDate to endDate
            currentDate = startDate
            dayIndex = 0
            while currentDate <= endDate:
                wells.append(wellId)
                dates.append(currentDate)
                phases.append(phase)
                # assigned volume if avaiable, else 0
                volume = volumeList[dayIndex] if dayIndex < len(volumeList) else 0
                volumes.append(volume)

                currentDate += timedelta(days=1)
                dayIndex += 1

    # Create DataFrame from lists
    dailyForecastDf = pd.DataFrame({
        "well": wells,
        "date": dates,
        "phase": phases,
        "volume": volumes
    })

    # add wellName and API column by matching wellId with wellList
    dailyForecastDf["wellName"] = dailyForecastDf["well"].map(wellList.set_index("id")["wellName"])
    dailyForecastDf["API"] = dailyForecastDf["well"].map(wellList.set_index("id")["chosenID"])

    # Pivot the DataFrame to have separate columns for oil, gas, and water, well, date, wellName, API
    pivotDailyForecast = dailyForecastDf.pivot_table(
        index=["date", "well", "wellName", "API"],
        columns=["phase"],
        values="volume",
        fill_value=0
    ).reset_index()

    # Rename columns to match requested format
    pivotDailyForecast.columns.name = None  # Remove the pivot_table column name
    pivotDailyForecast = pivotDailyForecast.rename(columns={
        "oil": "oil",
        "gas": "gas",
        "water": "water"
    })

    # Reorder columns to match requested order
    pivotDailyForecast = pivotDailyForecast[["date", "well", "oil", "gas", "water", "wellName", "API"]]

    print("Finished Getting Daily Forecast from ComboCurve")

    return pivotDailyForecast
