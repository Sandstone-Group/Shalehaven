# Shalehaven Scripts

Python toolkit for oil & gas investment analysis and operations at **Shalehaven Partners** — supporting tax-advantaged, non-operated oil & gas projects in proven basins. Developed by Michael Tanner. For questions or contributions, contact [Michael Tanner](mailto:dev@shalehaven.com).

Process geospatial constraints, run production models, and evaluate economics to inform drilling decisions and investor returns.

## Core Scripts

- **`main_los.py`**  
  P/L Analysis - in progress.

- **`main_prod.py`**  
  Production ETL pipeline — pulls Shalehaven wells from ComboCurve, processes operator-specific production (Admiral Permian, Hunt Oil, Aethon, Devon, ConocoPhillips, Spur, Ballard, monthly PDS), pushes daily/monthly volumes back to ComboCurve, then merges actuals with updated and original type curves and exports cumulative summaries.

- **`main_model.py`**  
  Core SHP modeling pipeline — authenticates with Novi, prompts the user for an AFE Summary file path, whether to run forecasts & production export, and whether to run operator analysis. Retrieves well permits (or resolves unpermitted wells via BLM PLSS T/R/S centroid) and offset wells within a user-specified search radius from the local Novi bulk export. When forecasts are enabled, fetches yearly EUR + monthly forecasts + historical monthly production for each offset and exports to Excel. Always pulls subsurface petrophysics + wellbore trajectories and renders a multi-page PDF with DSU section overlays, lettered permit locations, and the nearest offset well names. When operator analysis is enabled, generates a separate PDF with completion design trends, production performance, spacing impact, frac type analysis, and peer comparison charts.

- **`main_analysis.py`**  
  Standalone operator analysis pipeline — reads an AFE Summary, pulls the operator's wells and all peer operators within 5 miles from the local Novi bulk export, and generates a multi-page PDF with completion design trends, production performance by vintage, spacing impact, frac type classification (slickwater vs gel/hybrid from FracFocus), and peer comparison charts. Can be run independently or triggered from `main_model.py`.
 
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

- **`novi.py`** — Novi Labs client + local bulk export pipeline. Most downstream functions read from the local bulk export at `D:\novi` (configurable via `NOVI_BULK_DATA_PATH`) instead of paginating the API. Run `runNoviBulk()` once per export refresh to populate it.
  - `readAFESummary(pathToFile)` — Reads an AFE Summary Excel file into a DataFrame
    - `pathToFile` (string) — file path to the AFE Summary Excel file (must include "Landing Zone", "API Number", "County", and "State" columns)
  - `authNovi()` — Authenticates with the Novi Labs API using environment variables
    - No parameters (uses `NOVI_USERNAME` and `NOVI_PASSWORD` env vars)
  - `runNoviBulk(envPath, outputDir=None)` — Standalone entrypoint that loads creds from `.env`, authenticates, and downloads + extracts the full Novi bulk export
    - `envPath` (string, optional) — path to the `.env` file (default `C:\Users\Michael Tanner\code\.env`)
    - `outputDir` (string, optional) — destination root for the bulk export (default `NOVI_BULK_DATA_PATH` env var or `D:\novi`)
  - `noviBulk(token, scope="us-horizontals", outputDir=None)` — Hits the `/v3/bulk.json` endpoint, streams the database + shapefile zips to disk with progress logging, extracts everything, and writes a `manifest.json` for cache lookup. Skips re-download if the export date hasn't changed
    - `token` (string) — authentication token from `authNovi()`
    - `scope` (string, optional) — Novi API well scope (default `"us-horizontals"`)
    - `outputDir` (string, optional) — destination root for the bulk export (default `NOVI_BULK_DATA_PATH` env var or `D:\novi`)
  - `getNoviBulkPaths(outputDir=None)` — Resolves the local bulk export manifest and returns a dict with the extract directory, Database directory, export date, and a name → path map for every TSV (e.g. `WellDetails`, `WellPermits`, `WellMonths`, `ForecastWellYears`, `ForecastWellMonths`, `Subsurface`, `WellboreLocations`)
    - `outputDir` (string, optional) — bulk export root (default `NOVI_BULK_DATA_PATH` env var or `D:\novi`)
  - `getWellPermits(token, afeData, scope="us-horizontals")` — Looks up permits from the local `WellPermits.tsv` (Texas wells by `API10`, all others by `ID`). When no local match exists (e.g. unpermitted wells), resolves the well's Township/Range/Section via BLM PLSS to get a section centroid as the well location. Backfills null `Latitude`/`Longitude` from `BHLLatitude`/`BHLLongitude`. Accepts both full state names ("Ohio") and abbreviations ("OH") in the AFE
    - `token` (string) — authentication token from `authNovi()`
    - `afeData` (DataFrame) — AFE Summary data from `readAFESummary()`
    - `scope` (string, optional) — Novi API well scope (default `"us-horizontals"`)
  - `getWells(token, permitData, afeData, scope="us-horizontals")` — Prompts for a search radius (miles), builds a bounding box around the permit locations, then filters the local `WellDetails.tsv` by bbox + AFE landing zone formations + `FirstProductionYear >= 2018`
    - `token` (string) — authentication token from `authNovi()`
    - `permitData` (DataFrame) — permit data with Latitude/Longitude from `getWellPermits()`
    - `afeData` (DataFrame) — AFE Summary data (used for Landing Zone filter)
    - `scope` (string, optional) — Novi API well scope (default `"us-horizontals"`)
  - `getNoviYearlyForecast(token, offsetData, scope="us-horizontals")` — Filters `ForecastWellYears.tsv` (chunked) by API10, sums Oil/Gas/Water per well, and appends `Oil EUR`, `Gas EUR`, `Water EUR` columns to the offset DataFrame
    - `token` (string) — kept for backward compat, unused (reads local TSV)
    - `offsetData` (DataFrame) — offset wells from `getWells()`
    - `scope` (string, optional) — Novi API well scope (default `"us-horizontals"`)
  - `getNoviMonthlyForecast(token, forecastData, scope="us-horizontals")` — Streams `ForecastWellMonths.tsv` (~12 GB) chunked, returns monthly forecast rows for the offset wells
    - `token` (string) — kept for backward compat, unused (reads local TSV)
    - `forecastData` (DataFrame) — offset wells with EUR from `getNoviYearlyForecast()`
    - `scope` (string, optional) — Novi API well scope (default `"us-horizontals"`)
  - `getNoviMonthlyProduction(token, offsetData, scope="us-horizontals")` — Streams `WellMonths.tsv` (~4.7 GB) chunked, returns historical actual monthly production for the offset wells
    - `token` (string) — kept for backward compat, unused (reads local TSV)
    - `offsetData` (DataFrame) — offset wells from `getWells()`
    - `scope` (string, optional) — Novi API well scope (default `"us-horizontals"`)
  - `getNoviSubsurface(token, offsetData, scope="us-horizontals")` — Loads `Subsurface.tsv` and inner-merges on `(API10, Formation)` so each well gets exactly the petrophysical row matching its reported zone. Wells with no matching zone are dropped (logged)
    - `token` (string) — kept for backward compat, unused (reads local TSV)
    - `offsetData` (DataFrame) — offset wells from `getWells()`
    - `scope` (string, optional) — Novi API well scope (default `"us-horizontals"`)
  - `getNoviWellboreLocations(token, offsetData, scope="us-horizontals")` — Streams `WellboreLocations.tsv` (~472 MB) chunked, returns ~15 lateral path points per well sorted by `Path` for trajectory rendering
    - `token` (string) — kept for backward compat, unused (reads local TSV)
    - `offsetData` (DataFrame) — offset wells from `getWells()`
    - `scope` (string, optional) — Novi API well scope (default `"us-horizontals"`)
  - `plotSubsurfaceHeatMaps(subsurfaceData, pathToAfeSummary, parameters=None, permitData=None, wellboreLocationsData=None, offsetData=None, labelNearestN=20, afeData=None)` — Builds a multi-page PDF of interpolated subsurface heat maps with Census TIGER state/county basemaps, the Novi `All.shp` operator-acreage underlay, BLM PLSS township/section overlays (cached on disk; warns when offset wells touch Texas since BLM PLSS doesn't cover it), DSU section boxes derived from the AFE T/R/S, lettered permit locations, and the nearest *N* offset well names
    - `subsurfaceData` (DataFrame) — formation-aware subsurface rows from `getNoviSubsurface()`
    - `pathToAfeSummary` (string) — file path to the AFE Summary (used for output naming + DSU parsing)
    - `parameters` (list, optional) — subsurface columns to plot (default `["TVD", "TOC_Avg", "SW_Avg", "Porosity_Avg", "Permeability_Avg", "Thickness_Avg", "VClay_Avg", "Brittleness_Avg"]`)
    - `permitData` (DataFrame, optional) — permit locations from `getWellPermits()` for lettered overlays
    - `wellboreLocationsData` (DataFrame, optional) — wellbore trajectory points from `getNoviWellboreLocations()`
    - `offsetData` (DataFrame, optional) — offset wells from `getWells()` for nearest-well labels
    - `labelNearestN` (int, optional) — number of nearest offset wells to label per permit (default `20`)
    - `afeData` (DataFrame, optional) — AFE Summary data for parsing T/R/S and drawing DSU section boxes
  - `printData(forecastData, monthlyForecastData, monthlyProductionData, pathToAfeSummary)` — Exports header data (offsets with EUR), monthly forecast, and historical monthly production to Excel files in a `Data/` subfolder next to the AFE Summary, named after the DSU parsed from the AFE filename
    - `forecastData` (DataFrame) — offset wells with EUR from `getNoviYearlyForecast()`
    - `monthlyForecastData` (DataFrame) — monthly forecast rows from `getNoviMonthlyForecast()`
    - `monthlyProductionData` (DataFrame) — historical monthly production from `getNoviMonthlyProduction()`
    - `pathToAfeSummary` (string) — file path to the AFE Summary (used for output naming)

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
  - In progress — currently a stub holding the base URL and `AFE_LEAKS_API_KEY` env wiring. Live well-cost / production / financial queries are exposed today through the `afeleaks` MCP server.

## Quick Start

```bash
# Example usage (parameters defined inside scripts)
python main_los.py
python main_model.py
python main_prod.py
python main_analysis.py
```

