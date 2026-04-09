## Shalehaven Main Model Scripts
## Developed by Michael Tanner

# Imports - SHEM Scripts
import shalehavenscripts.novi as novi
import shalehavenscripts.combocurve as combocurve
import shalehavenscripts.production as production

token = novi.authNovi()

print("Successfully Authenticated with Novi Token")

# Read in AFE Summary
pathToAfeSummary = input("Enter the path to the AFE Summary file: ").strip().strip('"').strip("'")
runForecasts = input("Run forecasts & production export? (Y/N): ").strip().upper() == "Y"

afeData = novi.readAFESummary(pathToAfeSummary) # This should be the AFE Summary file provided by the user, containing at least the "Landing Zone" column.
permitData = novi.getWellPermits(token, afeData) # This function retrieves well permits from the Novi API based on the landing zone specified in the AFE Summary. It returns a DataFrame with permit locations (latitude and longitude) that will be used to find nearby wells.

offsetData = novi.getWells(token, permitData, afeData) # This function retrieves wells from the Novi API that are within a 5-mile radius of the permit locations. It uses the bounding box method to filter wells based on their latitude and longitude. The resulting DataFrame contains information about the nearby wells, including their production data.

if runForecasts:
    forecastData = novi.getNoviYearlyForecast(token, offsetData) # This function retrieves production forecasts for the nearby wells identified in the previous step. It uses the well IDs from the offsetData DataFrame to query the Novi API and returns a DataFrame with forecasted production data for each well.
    monthlyForecastData = novi.getNoviMonthlyForecast(token, forecastData) # Retrieve monthly forecast volumes for offset wells
    monthlyProductionData = novi.getNoviMonthlyProduction(token, offsetData) # Retrieve historical monthly production for offset wells from local bulk export
    novi.printData(forecastData, monthlyForecastData, monthlyProductionData, pathToAfeSummary) # Export header data, monthly forecast, and monthly production to Excel

subsurfaceData = novi.getNoviSubsurface(token, offsetData) # Retrieve subsurface petrophysical data for offset wells (formation-aware)
wellboreLocationsData = novi.getNoviWellboreLocations(token, offsetData) # Retrieve lateral path points for offset wells
novi.plotSubsurfaceHeatMaps(subsurfaceData, pathToAfeSummary, permitData=permitData, wellboreLocationsData=wellboreLocationsData, offsetData=offsetData, afeData=afeData) # PDF heat maps with DSU section boxes (from AFE T/R/S) + lettered permits + nearest offset well names

print("SHP Modeling Pipeline Completed Successfully")