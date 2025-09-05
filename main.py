## Shalehaven Main Scripts
## Developed by Michael Tanner

import shalehavenscripts.production as production

import pandas as pd
import numpy as np

pathProduction = r"C:\Users\Michael Tanner\OneDrive - Sandstone Group\Clients - Documents\# Shalehaven Partners\# A&D\Wolfcamp A - Pecos County MBX Operating\Data\mbx drill baby drill E_W orentation-20250826161203344-production.csv"
pathHeader = r"C:\Users\Michael Tanner\OneDrive - Sandstone Group\Clients - Documents\# Shalehaven Partners\# A&D\Wolfcamp A - Pecos County MBX Operating\Data\mbx drill baby drill E_W orentation-20250826161203344-header.csv"

dataOil = pd.read_csv(pathProduction)
dataHeader = pd.read_csv(pathHeader)

twelveMonthOilPerFt = production.tweleveMonthOilPerFt(dataOil,dataHeader)