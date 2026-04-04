# Shalehaven Scripts

Python toolkit for oil & gas investment analysis and operations at **Shalehaven Partners** — supporting tax-advantaged, non-operated oil & gas projects in proven basins. Developed by Michael Tanner. For questions or contributions, contact [Michael Tanner](mailto:dev@shalehaven.com).

Process geospatial constraints, run production models, and evaluate economics to inform drilling decisions and investor returns.

## Core Scripts

- **`main_los.py`**  
  Profit and loss analysis.

- **`main_prod.py`**  
  Production forecasting and operational analytics.  

- **`main_model.py`**  
  Core SHP modeling pipeline — authenticates with Novi, prompts the user for an AFE Summary file path, retrieves well permits and offset wells within 5 miles, and fetches production forecasts (EUR) for each offset.
 
## Package Modules (`shalehavenscripts/`)

- **`los.py`** — LOS calculations
  - `combineAfeData(pathToAfe)` — Combines AFE Excel files from subfolders into a single dataframe
    - `pathToAfe` (string) — file path to the AFE folder
  - `combineJibData(pathToJib)` — Merges all JIB Excel files into `jib_data.xlsx`
    - `pathToJib` (string) — file path to the JIB folder
  - `combineRevenueData(pathToRevenue)` — Merges all Revenue Excel files into `revenue_data.xlsx`
    - `pathToRevenue` (string) — file path to the Revenue folder
  - `formatLosData(jibData, revenueData)` — Formats revenue and JIB data into a consolidated LOS dataframe
    - `jibData` (DataFrame) — combined JIB data from `combineJibData()`
    - `revenueData` (DataFrame) — combined revenue data from `combineRevenueData()`
  - `generatePnlData(jibData, revenueData)` — Generates P&L dataset in long format for Power BI
    - `jibData` (DataFrame) — combined JIB data from `combineJibData()`
    - `revenueData` (DataFrame) — combined revenue data from `combineRevenueData()`

- **`novi.py`** — Novi Labs API client for authentication, permit lookup, offset well search, and forecasting
  - `readAFESummary(pathToFile)` — Reads an AFE Summary Excel file into a DataFrame
    - `pathToFile` (string) — file path to the AFE Summary Excel file (must include "Landing Zone", "API Number", "County", and "State" columns)
  - `authNovi()` — Authenticates with the Novi Labs API using environment variables
    - No parameters (uses `NOVI_USERNAME` and `NOVI_PASSWORD` env vars)
  - `getWellPermits(token, afeData, scope="us-horizontals")` — Retrieves well permits from Novi based on AFE Summary rows (API Number, County, State)
    - `token` (string) — authentication token from `authNovi()`
    - `afeData` (DataFrame) — AFE Summary data from `readAFESummary()`
    - `scope` (string, optional) — Novi API well scope (default `"us-horizontals"`)
  - `getWells(token, permitData, afeData, scope="us-horizontals")` — Finds horizontal wells within a 5-mile bounding box of permit locations, filtered by landing zone formation
    - `token` (string) — authentication token from `authNovi()`
    - `permitData` (DataFrame) — permit data with Latitude/Longitude from `getWellPermits()`
    - `afeData` (DataFrame) — AFE Summary data (used for Landing Zone filter)
    - `scope` (string, optional) — Novi API well scope (default `"us-horizontals"`)
  - `getWellForecast(token, offsetData, scope="us-horizontals")` — Retrieves forecast EUR (Oil, Gas, Water) for each offset well via `forecast_well_years` endpoint
    - `token` (string) — authentication token from `authNovi()`
    - `offsetData` (DataFrame) — offset wells from `getWells()`
    - `scope` (string, optional) — Novi API well scope (default `"us-horizontals"`)

- **`production.py`** — Production data processing
  - `admiralPermianProductionData(pathToData)` — Imports and formats Admiral Permian well production data
    - `pathToData` (string) — file path to the Admiral Permian data directory
  - `huntOilProductionData(pathToData, huntWells)` — Processes Hunt Oil production data
    - `pathToData` (string) — file path to the Hunt Oil data directory
    - `huntWells` (DataFrame) — well list with `wellName` and `chosenID` columns
  - `aethonProductionData(pathToData)` — Extracts Aethon Energy production data from CSV
    - `pathToData` (string) — file path to the Aethon data directory
  - `devonProductionData(pathToData)` — Handles Devon Energy production data from PDS files
    - `pathToData` (string) — file path to the Devon data directory
  - `copProductionData(pathToData)` — Processes ConocoPhillips production data from PDS files
    - `pathToData` (string) — file path to the ConocoPhillips data directory
  - `spurProductionData(pathToData, wellMapping)` — Loads Spur Energy production data from ProdView Excel
    - `pathToData` (string) — file path to the Spur Energy data directory
    - `wellMapping` (dict) — dictionary mapping well names to chosenIDs
  - `ballardProductionData(pathToData)` — Converts Ballard Petroleum production data from Excel to ComboCurve format, formatting API10 to 14-character chosenID
    - `pathToData` (string) — file path to the Ballard Petroleum data directory
  - `mergeProductionWithTypeCurves(dailyprod, updated, original, wellList, pathToDatabase)` — Merges daily production with type curves from ComboCurve
    - `dailyprod` (DataFrame) — daily production data
    - `updated` (DataFrame) — updated type curve forecast data
    - `original` (DataFrame) — original type curve forecast data
    - `wellList` (DataFrame) — well list with `id`, `wellName`, and `chosenID` columns
    - `pathToDatabase` (string) — file path to the database output directory
  - `cumulativeProduction(data, pathToDatabase)` — Calculates cumulative production from daily data
    - `data` (DataFrame) — merged production and type curve data from `mergeProductionWithTypeCurves()`
    - `pathToDatabase` (string) — file path to the database output directory
  - `pdsMonthlyData(pathToData)` — Converts monthly PDS data to ComboCurve monthly format
    - `pathToData` (string) — file path to the PDS monthly data directory

- **`combocurve.py`** — Combo/hybrid type curve generation
  - `putDataComboCurveDaily(data, serviceAccount, comboCurveApi)` — Uploads daily production data to ComboCurve API
    - `data` (DataFrame) — daily production data with date, chosenID, oil, gas, water, dataSource columns
    - `serviceAccount` (string) — file path to ComboCurve service account JSON
    - `comboCurveApi` (string) — ComboCurve API key
  - `putDataComboCurveMonthly(data, serviceAccount, comboCurveApi)` — Uploads monthly production data to ComboCurve API
    - `data` (DataFrame) — monthly production data with date, chosenID, oil, gas, water, dataSource columns
    - `serviceAccount` (string) — file path to ComboCurve service account JSON
    - `comboCurveApi` (string) — ComboCurve API key
  - `getWellsFromComboCurve(serviceAccount, comboCurveApi)` — Fetches Shalehaven wells from ComboCurve
    - `serviceAccount` (string) — file path to ComboCurve service account JSON
    - `comboCurveApi` (string) — ComboCurve API key
  - `getDailyProductionFromComboCurve(serviceAccount, comboCurveApi, wellList, pathToDatabase)` — Retrieves daily production data from ComboCurve
    - `serviceAccount` (string) — file path to ComboCurve service account JSON
    - `comboCurveApi` (string) — ComboCurve API key
    - `wellList` (DataFrame) — well list with `id`, `wellName`, and `chosenID` columns
    - `pathToDatabase` (string) — file path to the database output directory
  - `getDailyForecastFromComboCurve(serviceAccount, comboCurveApi, projectId, forecastId, wellList)` — Fetches daily forecast volumes from ComboCurve
    - `serviceAccount` (string) — file path to ComboCurve service account JSON
    - `comboCurveApi` (string) — ComboCurve API key
    - `projectId` (string) — ComboCurve project ID
    - `forecastId` (string) — ComboCurve forecast ID
    - `wellList` (DataFrame) — well list with `id`, `wellName`, and `chosenID` columns

- **`afeleaks.py`** — AFE Leaks API client for well cost, production, and financial data
  - In progress

## Quick Start

```bash
# Example usage (parameters defined inside scripts)
python main_los.py
python main_model.py
python main_prod.py

