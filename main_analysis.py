## Shalehaven Operator Analysis
## Developed by Michael Tanner

import shalehavenscripts.novi as novi

# Read in AFE Summary
pathToAfeSummary = input("Enter the path to the AFE Summary file: ").strip().strip('"').strip("'")
afeData = novi.readAFESummary(pathToAfeSummary)

# Pull operator wells from bulk export (WellDetails + WellSpacing)
analysisData = novi.getOperatorAnalysisData(afeData)

# Pull all operators within 5 miles for peer comparison
peerData = novi.getPeerAnalysisData(afeData)

# Generate operator analysis PDF (completion trends, production performance, spacing impact, peer comparison)
novi.plotOperatorAnalysis(analysisData, pathToAfeSummary, peerData=peerData)

# Generate interactive HTML version (same charts, Plotly-based)
novi.plotOperatorAnalysisHTML(analysisData, pathToAfeSummary, peerData=peerData)

print("Operator Analysis Pipeline Completed Successfully")
