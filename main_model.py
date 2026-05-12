## Shalehaven Main Model Scripts
## Developed by Michael Tanner

import re

# Imports - SHEM Scripts
import shalehavenscripts.novi as novi
import shalehavenscripts.economics as economics
import shalehavenscripts.production as production

# ============================================================================
# All interactive inputs gathered upfront — paste/answer everything in one shot
# then walk away while the pipeline runs.
# ============================================================================

pathToAfeSummary = input("Enter the path to the AFE Summary file: ").strip().strip('"').strip("'")
runForecasts = input("Run forecasts & production export? (Y/N): ").strip().upper() == "Y"
runAnalysis = input("Run operator analysis? (Y/N): ").strip().upper() == "Y"

print("\nOffset well search mode:")
print("  [R] Radius around AFE permits (formation + vintage filter)")
print("  [A] Paste specific API10 list (no formation/vintage filter)")
modeInput = input("Choose [R/A] (default R): ").strip().upper()
searchMode = "api_list" if modeInput == "A" else "radius"

apiList = None
radiusMiles = None
if searchMode == "api_list":
    print("\nPaste API10 numbers (comma, space, or newline-separated). End with a blank line:")
    pastedLines = []
    while True:
        try:
            line = input()
        except EOFError:
            break
        if not line.strip():
            break
        pastedLines.append(line)
    raw = " ".join(pastedLines)
    tokens = [t for t in re.split(r"[,\s;]+", raw) if t.strip()]
    apiCandidates = []
    for t in tokens:
        digits = re.sub(r"\D", "", t)
        if len(digits) >= 10:
            apiCandidates.append(digits[:10])
    apiList = sorted(set(apiCandidates))
else:
    radiusMiles = float(input("Enter search radius in miles: ").strip())

basinCode = None
cashPromote = None
carry = None
if runForecasts:
    basinCode = input("Which Basin: ").strip()
    cashPromoteRaw = input("Cash Promote (% or decimal, blank=0): ").strip()
    cashPromote = float(cashPromoteRaw) if cashPromoteRaw else 0.0
    if cashPromote > 1.0:
        cashPromote /= 100.0
    carryRaw = input("Carry Through Tanks (% or decimal, blank=0): ").strip()
    carry = float(carryRaw) if carryRaw else 0.0
    if carry > 1.0:
        carry /= 100.0

# ============================================================================
# Pipeline — no further prompts beyond this point
# ============================================================================

token = novi.authNovi()
print("Successfully Authenticated with Novi Token")

afeData = novi.readAFESummary(pathToAfeSummary) # AFE Summary file — must include Landing Zone, Operator, State, NRI, WI, Net AFE columns.
permitData = novi.getWellPermits(token, afeData) # Well permits from Novi for landing zone(s) in the AFE Summary.
offsetData = novi.getWells(token, permitData, afeData, searchMode=searchMode, apiList=apiList, radiusMiles=radiusMiles) # Offset wells from local Novi bulk export (radius or pasted API10 list).

monthlyForecastData = None
if runForecasts:
    forecastData = novi.getNoviYearlyForecast(token, offsetData) # Yearly EUR forecast per offset well.
    monthlyForecastData = novi.getNoviMonthlyForecast(token, forecastData) # Monthly forecast volumes per offset well.
    monthlyProductionData = novi.getNoviMonthlyProduction(token, offsetData) # Historical monthly production per offset well.
    novi.printData(forecastData, monthlyForecastData, monthlyProductionData, pathToAfeSummary) # Excel exports to Data/ folder.

subsurfaceData = novi.getNoviSubsurface(token, offsetData) # Subsurface petrophysical data per offset well (formation-aware).
wellboreLocationsData = novi.getNoviWellboreLocations(token, offsetData) # Lateral path points per offset well.
monthlyForecastBuckets = novi.plotSubsurfaceHeatMapsHTML(subsurfaceData, pathToAfeSummary, permitData=permitData, wellboreLocationsData=wellboreLocationsData, offsetData=offsetData, afeData=afeData, monthlyForecastData=monthlyForecastData) # Interactive HTML heat maps (opens in browser); returns monthly P10/P50/P90 buckets.

if runForecasts:
    economics.runAfeEconomics(afeData, monthlyForecastBuckets, pathToAfeSummary, basinCode=basinCode, cashPromote=cashPromote, carry=carry)

if runAnalysis:
    analysisData = novi.getOperatorAnalysisData(afeData)
    peerData = novi.getPeerAnalysisData(afeData)
    novi.plotOperatorAnalysisHTML(analysisData, pathToAfeSummary, peerData=peerData)
    print("Operator Analysis Completed Successfully")

print("SHP Modeling Pipeline Completed Successfully")
