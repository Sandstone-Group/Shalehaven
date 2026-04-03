## Shalehaven Main Model Scripts
## Developed by Michael Tanner

# Imports - SHEM Scripts
import shalehavenscripts.novi as novi
import shalehavenscripts.combocurve as combocurve
import shalehavenscripts.production as production

token = novi.authNovi()

print("Successfully Authenticated with Novi Token:")

pathToAfeSummary = "C:\\Users\\MichaelTanner\\OneDrive - Shalehaven Energy & Asset Management LLC\\A&D - Documents\\# 2026 LP\\Bone Springs - Eddy County Mewbourne\\AFE Summary - Milkshake.xlsx"

# Read in AFE Summary

afeData = novi.readAFESummary(pathToAfeSummary)

offsetData = novi.getWells(token, afeData)

x = 5