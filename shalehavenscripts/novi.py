## Novi API Functions - Authentication, Permit Lookup, Offset Well Search, EUR Forecasting, and Data Export
## Developed by Michael Tanner

import requests
import os
import math
import difflib
import re
import pandas as pd
import numpy as np


BASE_URL = "https://insight.novilabs.com/api/"

STATE_ABBR_MAP = {
    "new mexico": "NM", "colorado": "CO", "wyoming": "WY", "north dakota": "ND",
    "montana": "MT", "ohio": "OH", "pennsylvania": "PA", "texas": "TX",
    "south dakota": "SD", "oklahoma": "OK", "kansas": "KS", "louisiana": "LA",
    "california": "CA", "utah": "UT", "alaska": "AK",
}
# Reverse: "TX" -> "Texas" (Novi WellPermits/WellDetails store full state names)
STATE_FULL_MAP = {abbr: name.title() for name, abbr in STATE_ABBR_MAP.items()}
# Also accept abbreviations directly (AFE files sometimes use "OH" instead of "Ohio")
STATE_ABBR_MAP.update({v.lower(): v for v in STATE_ABBR_MAP.values()})


## Normalize an AFE state value ("TX" or "Texas") to Novi's full-name form ("Texas").
def _normalizeStateFull(state):
    s = str(state).strip()
    if not s:
        return s
    if s in STATE_FULL_MAP.values():
        return s
    return STATE_FULL_MAP.get(s.upper(), s.title())


## Strip a trailing " County" / " Parish" suffix — Novi stores bare county names ("Martin", not "Martin County").
def _normalizeCounty(county):
    s = str(county).strip()
    for suffix in (" County", " Parish"):
        if s.lower().endswith(suffix.lower()):
            s = s[: -len(suffix)].strip()
    return s


# Alias -> canonical so downstream fetch + plot treat geologically-equivalent zones as one.
FORMATION_ALIASES = {
    "JO MILL": "LOWER SPRABERRY SAND",
    "CODELL SANDSTONE": "CODELL",
    "CODELL SAND": "CODELL",
}
_FORMATION_GROUPS = {}
for _alias, _canon in FORMATION_ALIASES.items():
    _FORMATION_GROUPS.setdefault(_canon, {_canon}).add(_alias)


def _canonicalFormation(name):
    if pd.isna(name):
        return name
    s = str(name).strip().upper()
    return FORMATION_ALIASES.get(s, s)


def _expandFormations(names):
    result = set()
    for n in names:
        canon = _canonicalFormation(n)
        if canon is None:
            continue
        result.update(_FORMATION_GROUPS.get(canon, {canon}))
    return sorted(result)


## Linear regression with R²; returns (slope, intercept, r2, label) ready for plot legends.
def _linearFitWithR2(x, y):
    import numpy as np
    slope, intercept = np.polyfit(x, y, 1)
    ss_res = ((y - (slope * x + intercept)) ** 2).sum()
    ss_tot = ((y - y.mean()) ** 2).sum()
    r2 = 1 - ss_res / ss_tot if ss_tot else float("nan")
    label = f"y={slope:,.2f}x+{intercept:,.0f}  R²={r2:.2f}"
    return slope, intercept, r2, label


## One-row DataFrame matching the synthetic-permit schema used by getWellPermits fallbacks.
def _syntheticPermitRow(lat, lon, well_name, county, state, api10):
    return pd.DataFrame([{
        "Latitude": float(lat),
        "Longitude": float(lon),
        "WellName": str(well_name),
        "County": county,
        "State": state,
        "API10": api10 if api10 else None,
    }])


# Midland + Delaware (Permian) zones — gas is uneconomic here, so EUR analysis should track oil.
PERMIAN_FORMATION_PATTERNS = (
    "WOLFCAMP", "SPRABERRY", "JO MILL", "BONE SPRING",
    "AVALON", "DEAN", "CLEAR FORK", "LEONARD",
)


def _isPermianOilBasin(formations):
    if not formations:
        return False
    for fm in formations:
        s = str(fm).upper()
        if any(p in s for p in PERMIAN_FORMATION_PATTERNS):
            return True
    return False


PERMIAN_BASIN_PATTERNS = (
    "PERMIAN",
    "MIDLAND",
    "DELAWARE",
    "CENTRAL BASIN PLATFORM",
)


def _isPermianBasin(data):
    if data is None:
        return False

    values = []
    if isinstance(data, pd.DataFrame):
        for col in ("Basin", "Subbasin"):
            if col in data.columns:
                values.extend(data[col].dropna().astype(str).tolist())
    else:
        values = [str(v) for v in data if pd.notna(v)]

    for value in values:
        s = value.upper()
        if any(p in s for p in PERMIAN_BASIN_PATTERNS):
            return True
    return False


def _productionHeatMapParamsForAfe(afeData=None):
    return ["Cum12MOil", "Cum12MGas", "Cum24MOil", "Cum24MGas"]


def _primaryProductionStream(afeData=None):
    if afeData is None or afeData.empty:
        return "oil"
    target_cols = [
        c for c in afeData.columns
        if str(c).strip().lower().replace("_", " ") in {
            "primary target",
            "primarytarget",
            "target",
            "primary commodity",
            "commodity",
        }
    ]
    if not target_cols:
        return "oil"

    targets = afeData[target_cols[0]].dropna().astype(str).str.upper()
    oil_count = targets.str.contains("OIL").sum()
    gas_count = targets.str.contains("GAS").sum()
    if gas_count > oil_count:
        return "gas"
    return "oil"


def _subsurfaceParamLabel(param):
    labels = {
        "TOC_Avg": "TOC Avg",
        "SW_Avg": "Water Saturation Avg",
        "Porosity_Avg": "Porosity Avg",
        "Permeability_Avg": "Permeability Avg",
        "Thickness_Avg": "Thickness Avg",
        "VClay_Avg": "VClay Avg",
        "Brittleness_Avg": "Brittleness Avg",
        "Cum12MOil": "Cum 12 MBO",
        "Cum24MOil": "Cum 24 MBO",
        "Cum12MGas": "Cum 12 Gas",
        "Cum24MGas": "Cum 24 Gas",
        "EUR50YROil": "EUR 50yr Oil",
        "EUR50YRGas": "EUR 50yr Gas",
    }
    return labels.get(param, param)


GEO_DRIVER_PARAMS = [
    "TOC_Avg",
    "SW_Avg",
    "Porosity_Avg",
    "Permeability_Avg",
    "Thickness_Avg",
    "VClay_Avg",
    "Brittleness_Avg",
]


def _geoDriverProductionParam(afeData=None, basinData=None):
    if _isPermianBasin(basinData):
        primary_is_oil = True
    else:
        primary_is_oil = _primaryProductionStream(afeData) != "gas"

    candidates = (
        ["Cum24MOil", "Cum12MOil", "EUR50YROil"]
        if primary_is_oil
        else ["Cum24MGas", "Cum12MGas", "EUR50YRGas"]
    )

    # Data-aware fallback: pick the first candidate with >= 4 non-null values.
    # Brand-new wells (common in api_list mode) have no Cum12M/24M yet — falling
    # back to EUR50YR gives the regression a usable y-axis since Novi populates
    # the 50-year EUR from the forecast as soon as the well exists.
    if isinstance(basinData, pd.DataFrame) and not basinData.empty:
        for col in candidates:
            if col in basinData.columns and basinData[col].notna().sum() >= 4:
                return col
    return candidates[0]


def _geoDriverEstimateTargets(afeData=None):
    primary = _primaryProductionStream(afeData)
    eur_col = "EUR50YRGas" if primary == "gas" else "EUR50YROil"
    return ["Cum24MOil", "Cum24MGas", eur_col]


def _resolveAfeLateralLength(afeData=None):
    candidates = []
    if afeData is None or afeData.empty:
        return None
    for col in afeData.columns:
        col_norm = str(col).strip().lower().replace("_", " ")
        if col_norm == "lateral length" or ("lateral" in col_norm and "length" in col_norm):
            vals = pd.to_numeric(afeData[col], errors="coerce")
            vals = vals[vals > 0]
            if not vals.empty:
                candidates.extend(vals.tolist())
    if not candidates:
        return None
    adjusted = float(pd.Series(candidates).mean()) - 500.0
    return adjusted if adjusted > 0 else None


def _geoDriverResponseSeries(df, response_param, afeData=None):
    response = pd.to_numeric(df[response_param], errors="coerce")
    afe_lateral_length = _resolveAfeLateralLength(afeData)
    if "LateralLength" not in df.columns:
        return response, _subsurfaceParamLabel(response_param), _subsurfaceParamLabel(response_param), None

    offset_lateral_length = pd.to_numeric(df["LateralLength"], errors="coerce").replace(0, float("nan"))
    response_per_ft = response / offset_lateral_length
    response_label = f"{_subsurfaceParamLabel(response_param)} / ft"
    estimate_label = _subsurfaceParamLabel(response_param) if afe_lateral_length else response_label
    return response_per_ft, response_label, estimate_label, afe_lateral_length


def _geoDriverEstimateForTarget(df, driver, target_param, afe_lateral_length):
    if target_param not in df.columns or "LateralLength" not in df.columns:
        return None
    x = pd.to_numeric(df[driver], errors="coerce")
    target = pd.to_numeric(df[target_param], errors="coerce")
    lateral = pd.to_numeric(df["LateralLength"], errors="coerce").replace(0, float("nan"))
    y = target / lateral
    valid = x.notna() & y.notna()
    if valid.sum() < 4:
        return None
    x_valid = x[valid]
    y_valid = y[valid]
    if x_valid.nunique() <= 1:
        return None
    slope, intercept = np.polyfit(x_valid, y_valid, 1)
    x10, x50, x90 = np.percentile(x_valid, [10, 50, 90])
    est10 = slope * x10 + intercept
    est50 = slope * x50 + intercept
    est90 = slope * x90 + intercept
    if afe_lateral_length:
        est10 *= afe_lateral_length
        est50 *= afe_lateral_length
        est90 *= afe_lateral_length
    return {
        "label": _subsurfaceParamLabel(target_param),
        "p10": max(est10, est90),
        "p50": est50,
        "p90": min(est10, est90),
    }


## Lateral-normalize peer monthly volumes to the AFE lateral so cross-peer aggregation is apples-to-apples
def _normalizePeerMonthlyVolumes(monthly_df, peer_lateral_map, afe_lateral_length):
    if monthly_df is None or monthly_df.empty or not afe_lateral_length:
        return pd.DataFrame()
    needed = {"API10", "MonthsOnProduction", "OilPerMonth", "GasPerMonth", "WaterPerMonth"}
    if not needed.issubset(monthly_df.columns):
        return pd.DataFrame()
    df = monthly_df[list(needed)].copy()
    df["API10"] = df["API10"].astype("string")
    df["LL"] = df["API10"].map(peer_lateral_map)
    df = df[df["LL"].notna() & (df["LL"] > 0)]
    if df.empty:
        return pd.DataFrame()
    scaler = afe_lateral_length / df["LL"]
    df["OilNorm"] = pd.to_numeric(df["OilPerMonth"], errors="coerce") * scaler
    df["GasNorm"] = pd.to_numeric(df["GasPerMonth"], errors="coerce") * scaler
    df["WaterNorm"] = pd.to_numeric(df["WaterPerMonth"], errors="coerce") * scaler
    df["MonthsOnProduction"] = pd.to_numeric(df["MonthsOnProduction"], errors="coerce")
    df = df[df["MonthsOnProduction"].notna() & (df["MonthsOnProduction"] >= 1)]
    df["MonthsOnProduction"] = df["MonthsOnProduction"].astype(int)
    return df[["API10", "MonthsOnProduction", "OilNorm", "GasNorm", "WaterNorm"]]


## Outlier-only despike for percentile curves.
## Removes single-month V-blips (e.g. month 4 = 12K when month 3 = 21K and month 5 = 17K) caused
## by peer-cohort rotation, WITHOUT smoothing the genuine decline shape. For each interior month
## we compare the value to the centered-3 median; if it deviates by more than thresholdPct it's
## replaced with the linear interpolation of its two neighbors. Non-outliers are left untouched,
## so the steep flush-production peak in early months is preserved.
def _despikePercentileCurves(agg, thresholdPct=0.20):
    if agg is None:
        return agg
    keys = ("oil_p10", "oil_p50", "oil_p90",
            "gas_p10", "gas_p50", "gas_p90",
            "water_p10", "water_p50", "water_p90")
    for k in keys:
        if k not in agg:
            continue
        vals = list(agg[k])
        n = len(vals)
        if n < 3:
            continue
        out = list(vals)
        for i in range(1, n - 1):
            prev_v, this_v, next_v = vals[i - 1], vals[i], vals[i + 1]
            local_median = sorted((prev_v, this_v, next_v))[1]
            if local_median <= 0:
                continue
            if abs(this_v - local_median) / local_median > thresholdPct:
                out[i] = (prev_v + next_v) / 2.0
        agg[k] = out
    return agg


## Aggregate per-peer normalized monthly volumes into P10/P50/P90 across peers at each month index
## Months with fewer than `min_peers` reporting are dropped (matches geo-driver min-n=4 convention)
def _aggregatePeerMonthlyPercentiles(normalized_df, min_peers=4):
    if normalized_df is None or normalized_df.empty:
        return None
    rows = []
    for month, g in normalized_df.groupby("MonthsOnProduction"):
        n_oil = int(g["OilNorm"].notna().sum())
        n_gas = int(g["GasNorm"].notna().sum())
        n_water = int(g["WaterNorm"].notna().sum())
        n_min = min(n_oil, n_gas, n_water)
        if n_min < min_peers:
            continue
        rows.append({
            "month": int(month),
            "n": n_min,
            "oil_p10": float(g["OilNorm"].quantile(0.10)),
            "oil_p50": float(g["OilNorm"].quantile(0.50)),
            "oil_p90": float(g["OilNorm"].quantile(0.90)),
            "gas_p10": float(g["GasNorm"].quantile(0.10)),
            "gas_p50": float(g["GasNorm"].quantile(0.50)),
            "gas_p90": float(g["GasNorm"].quantile(0.90)),
            "water_p10": float(g["WaterNorm"].quantile(0.10)),
            "water_p50": float(g["WaterNorm"].quantile(0.50)),
            "water_p90": float(g["WaterNorm"].quantile(0.90)),
        })
    if not rows:
        return None
    rows.sort(key=lambda r: r["month"])
    out = {
        "months": [r["month"] for r in rows],
        "n": [r["n"] for r in rows],
    }
    for key in ("oil_p10", "oil_p50", "oil_p90",
                "gas_p10", "gas_p50", "gas_p90",
                "water_p10", "water_p50", "water_p90"):
        out[key] = [r[key] for r in rows]
    return out


## R²-weighted geo-driver multiplier: weighted mean of (driver P50 EUR estimate / peer-median EUR at AFE lateral).
## Quantifies how the AFE's rock properties push EUR vs. the peer-median baseline. Applied uniformly to all
## phase/percentile curves so peer GOR/WOR ratios are preserved.
def _computeGeoDriverMultiplier(drivers, df_subset, response_param, afe_lateral_length):
    if not drivers or not afe_lateral_length:
        return 1.0, None, 0.0
    if response_param not in df_subset.columns or "LateralLength" not in df_subset.columns:
        return 1.0, None, 0.0
    response = pd.to_numeric(df_subset[response_param], errors="coerce")
    lateral = pd.to_numeric(df_subset["LateralLength"], errors="coerce").replace(0, float("nan"))
    per_ft = (response / lateral).dropna()
    if per_ft.empty:
        return 1.0, None, 0.0
    baseline_per_ft = float(per_ft.median())
    baseline_eur = baseline_per_ft * afe_lateral_length
    if baseline_eur <= 0:
        return 1.0, None, 0.0
    weighted_sum = 0.0
    weight_total = 0.0
    for d in drivers:
        est_p50 = d.get("est_p50")
        r = d.get("r")
        if est_p50 is None or r is None:
            continue
        r2 = float(r) * float(r)
        weighted_sum += r2 * (float(est_p50) / baseline_eur)
        weight_total += r2
    if weight_total <= 0:
        return 1.0, baseline_eur, 0.0
    return weighted_sum / weight_total, baseline_eur, weight_total


## Build P10/P50/P90 monthly oil/gas/water curves for one peer subset, scaled by R²-weighted geo-driver multiplier.
## Returns None when monthly forecast data is missing, AFE lateral is unresolvable, or no peers in the subset have forecasts.
def _buildMonthlyForecastBucket(df_subset, monthly_df, drivers, afeData):
    if monthly_df is None or monthly_df.empty or df_subset is None or df_subset.empty:
        return None
    afe_lateral_length = _resolveAfeLateralLength(afeData)
    if not afe_lateral_length:
        return None
    if "API10" not in df_subset.columns or "LateralLength" not in df_subset.columns:
        return None
    ll_map = (
        df_subset.dropna(subset=["API10", "LateralLength"])
        .assign(API10=lambda d: d["API10"].astype("string"))
        .groupby("API10")["LateralLength"]
        .mean()
        .to_dict()
    )
    ll_map = {k: float(v) for k, v in ll_map.items() if v and float(v) > 0}
    if not ll_map:
        return None
    peer_monthly = monthly_df[monthly_df["API10"].astype("string").isin(ll_map.keys())]
    if peer_monthly.empty:
        return None
    normalized = _normalizePeerMonthlyVolumes(peer_monthly, ll_map, afe_lateral_length)
    agg = _aggregatePeerMonthlyPercentiles(normalized)
    if agg is None:
        return None
    agg = _despikePercentileCurves(agg, thresholdPct=0.20)
    response_param = _geoDriverProductionParam(afeData, df_subset)
    multiplier, baseline_eur, _ = _computeGeoDriverMultiplier(
        drivers, df_subset, response_param, afe_lateral_length
    )
    for key in ("oil_p10", "oil_p50", "oil_p90",
                "gas_p10", "gas_p50", "gas_p90",
                "water_p10", "water_p50", "water_p90"):
        agg[key] = [v * multiplier for v in agg[key]]
    summary = {}
    for phase in ("oil", "gas", "water"):
        for pct in ("p10", "p50", "p90"):
            summary[f"{phase}_{pct}_cum"] = float(sum(agg[f"{phase}_{pct}"]))
    agg["summary"] = summary
    agg["multiplier"] = float(multiplier)
    agg["baseline_eur"] = float(baseline_eur) if baseline_eur is not None else None
    agg["response_param"] = response_param
    agg["response_label"] = _subsurfaceParamLabel(response_param) if response_param else None
    agg["afe_lateral_length"] = float(afe_lateral_length)
    agg["peer_count"] = int(len(ll_map))
    agg["peer_count_with_forecast"] = int(normalized["API10"].nunique())
    return agg


## Parse township/range string like "18S", "T18S", "18 South", "T-18-S" into (number, direction)
## Returns (zero-padded 3-digit string, direction letter) or (None, None) on failure
def _parse_tr(s, kind):
    if not s:
        return None, None
    s_up = str(s).upper()
    m = re.search(r"(\d+)", s_up)
    if not m:
        return None, None
    num = m.group(1).zfill(3)
    valid = ("N", "S") if kind == "twp" else ("E", "W")
    d = next((ch for ch in s_up if ch in valid), None)
    return num, d


## Query BLM PLSS for specific section(s) by state/township/range/section and return the
## centroid (lat, lon) of the union of matched section polygons.
## Supports comma-separated sections (e.g. "9,10"). Caches results to D:\novi\basemaps\plss\.
## Returns (lat, lon) tuple or None if the section can't be found.
def _fetchSectionCentroid(state_abbr, township_str, range_str, section_str, outputDir=None):
    import json
    import time
    import urllib.request
    import urllib.error
    import urllib.parse
    from shapely.geometry import shape
    from shapely.ops import unary_union

    if outputDir is None:
        outputDir = os.environ.get("NOVI_BULK_DATA_PATH", r"D:\novi")

    plssDir = os.path.join(outputDir, "basemaps", "plss")
    os.makedirs(plssDir, exist_ok=True)

    BLM_BASE = "https://gis.blm.gov/arcgis/rest/services/Cadastral/BLM_Natl_PLSS_CadNSDI/MapServer"

    twp_no, twp_dir = _parse_tr(township_str, "twp")
    rng_no, rng_dir = _parse_tr(range_str, "rng")

    if not all([twp_no, twp_dir, rng_no, rng_dir]):
        print(f"  Could not parse T/R: twp={township_str!r} rng={range_str!r}")
        return None

    try:
        sec_nums = [s.strip().zfill(2) for s in str(section_str).split(",") if s.strip()]
    except Exception:
        sec_nums = []
    if not sec_nums:
        print(f"  Could not parse section: {section_str!r}")
        return None

    # Cache by (state, T, R, sections) so repeated runs skip the BLM query
    cache_key = f"{state_abbr}_{twp_no}{twp_dir}_{rng_no}{rng_dir}_{'_'.join(sec_nums)}"
    cache_path = os.path.join(plssDir, f"centroid_{cache_key}.json")

    if os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            cached = json.load(f)
        return (cached["lat"], cached["lon"])

    def _blm_query(url):
        for attempt in range(3):
            try:
                with urllib.request.urlopen(url, timeout=30) as r:
                    return json.loads(r.read())
            except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
                if attempt < 2:
                    time.sleep(1.5 * (attempt + 1))
                else:
                    raise

    # Step 1: find the township PLSSID
    twp_where = (
        f"STATEABBR='{state_abbr}' AND "
        f"TWNSHPNO='{twp_no}' AND TWNSHPDIR='{twp_dir}' AND "
        f"RANGENO='{rng_no}' AND RANGEDIR='{rng_dir}'"
    )
    twp_url = (
        f"{BLM_BASE}/1/query"
        f"?where={urllib.parse.quote(twp_where)}"
        f"&outFields=PLSSID&f=json&returnGeometry=false"
    )

    try:
        twp_data = _blm_query(twp_url)
    except Exception as e:
        print(f"  BLM township query failed: {e}")
        return None

    twp_features = twp_data.get("features", [])
    if not twp_features:
        print(f"  No BLM township found for {state_abbr} T{twp_no}{twp_dir} R{rng_no}{rng_dir}")
        return None

    plssids = [f["attributes"]["PLSSID"] for f in twp_features]

    # Step 2: query sections by PLSSID + section number
    plssid_in = ",".join(f"'{p}'" for p in plssids)
    sec_in = ",".join(f"'{s}'" for s in sec_nums)
    sec_where = f"PLSSID IN ({plssid_in}) AND FRSTDIVNO IN ({sec_in})"
    sec_url = (
        f"{BLM_BASE}/2/query"
        f"?where={urllib.parse.quote(sec_where)}"
        f"&outFields=PLSSID,FRSTDIVNO&f=geojson&outSR=4326&returnGeometry=true"
    )

    try:
        sec_data = _blm_query(sec_url)
    except Exception as e:
        print(f"  BLM section query failed: {e}")
        return None

    sec_features = sec_data.get("features", [])
    if not sec_features:
        print(f"  No BLM sections found for T{twp_no}{twp_dir} R{rng_no}{rng_dir} sec {sec_nums}")
        return None

    # Centroid of the union of all matched sections
    geometries = [shape(f["geometry"]) for f in sec_features]
    union = unary_union(geometries)
    centroid = union.centroid

    result = {"lat": centroid.y, "lon": centroid.x}
    with open(cache_path, "w") as f:
        json.dump(result, f)

    return (centroid.y, centroid.x)


## Read in AFE Summary from Excel file
## Expects columns: API Number, County, State, Landing Zone, Well Name (for Texas wells)
def readAFESummary(pathToFile):

    afeData = pd.read_excel(pathToFile)

    return afeData


## Authenticate with Novi Labs API using environment variables NOVI_USERNAME and NOVI_PASSWORD
def authNovi():
    username = os.environ.get("NOVI_USERNAME")
    password = os.environ.get("NOVI_PASSWORD")

    session = requests.Session()

    response = session.post(BASE_URL + "v2/sessions", json={"email": username, "password": password})

    response.raise_for_status()

    token = response.json()["authentication_token"]

    return token


## Retrieve offset wells from local Novi bulk export (D:\novi) — no API call.
## Two modes:
##   [R] Radius — bounding box around AFE permits, filtered by Landing Zone formation + FirstProductionYear >= vintageCutoff
##   [A] API10 list — user pastes specific API10 numbers; no formation/vintage filter
## Search mode + vintage cutoff are stashed on the returned DataFrame's .attrs so downstream
## HTML renderers can surface them (subsurface heat map, economics) and so radius/api-list runs
## can coexist for the same DSU.
## token and scope are kept for backward compat with main_model.py but unused.
def getWells(token, permitData, afeData, scope="us-horizontals", searchMode=None, apiList=None,
             radiusMiles=None, vintageCutoff=2018):

    # searchMode / apiList / radiusMiles can be passed in (collected upfront in main_model.py) or prompted here for backward compat.
    if searchMode is None:
        print("\nOffset well search mode:")
        print("  [R] Radius around AFE permits (formation + vintage filter)")
        print("  [A] Paste specific API10 list (no formation/vintage filter)")
        mode_input = input("Choose [R/A] (default R): ").strip().upper()
        searchMode = "api_list" if mode_input == "A" else "radius"

    # Load WellDetails from local bulk export (shared by both modes)
    paths = getNoviBulkPaths()
    wellDetailsPath = paths["tsv"]["WellDetails"]

    # API10 is varchar(32) per Novi schema — pin as string so downstream filters/joins
    # against ForecastWellMonths/Years (also pinned to string) match correctly
    wells = pd.read_csv(wellDetailsPath, sep="\t", low_memory=False, dtype={"API10": "string"})
    print(f"  Loaded {len(wells):,} total wells from bulk export")

    if searchMode == "api_list":
        if apiList is None:
            print("\nPaste API10 numbers (comma, space, or newline-separated). End with a blank line:")
            lines = []
            while True:
                try:
                    line = input()
                except EOFError:
                    break
                if not line.strip():
                    break
                lines.append(line)
            raw = " ".join(lines)
            # Split on whitespace/commas/semicolons; strip non-digits per token; truncate to API10.
            # Accepts pasted API12/API14, dashed forms ("42-475-12345"), or bare API10.
            tokens = [t for t in re.split(r"[,\s;]+", raw) if t.strip()]
            api_list = []
            for t in tokens:
                digits = re.sub(r"\D", "", t)
                if len(digits) >= 10:
                    api_list.append(digits[:10])
            api_list_unique = sorted(set(api_list))
        else:
            api_list_unique = sorted(set(apiList))
        print(f"  Got {len(api_list_unique)} unique API10 values from input")

        filtered = wells[wells["API10"].isin(api_list_unique)].copy()
        found_set = set(filtered["API10"].dropna().astype(str).tolist())
        missing = sorted(set(api_list_unique) - found_set)
        if missing:
            preview = ", ".join(missing[:10]) + ("..." if len(missing) > 10 else "")
            print(f"  WARNING: {len(missing)} API10s not found in bulk export: {preview}")
        print(f"Done. Retrieved {len(filtered):,} wells matching API10 list.")
    else:
        # Radius mode — bounding box around permit locations
        # 1 degree latitude ~ 69 miles, 1 degree longitude ~ 69 * cos(lat) miles
        miles = float(radiusMiles) if radiusMiles is not None else float(input("Enter search radius in miles: ").strip())
        lat_offset = miles / 69.0
        avg_lat = permitData["Latitude"].mean()
        lon_offset = miles / (69.0 * abs(math.cos(math.radians(avg_lat))))

        min_lat = permitData["Latitude"].min() - lat_offset
        max_lat = permitData["Latitude"].max() + lat_offset
        min_lon = permitData["Longitude"].min() - lon_offset
        max_lon = permitData["Longitude"].max() + lon_offset

        # Pull all unique formations from AFE Summary and uppercase to match Novi format.
        # Expand aliases so geologically-equivalent zones (e.g. JO MILL <-> LOWER SPRABERRY SAND)
        # are pulled from Novi together regardless of which name the AFE used.
        formations = _expandFormations(afeData["Landing Zone"].dropna().str.upper().unique().tolist())

        print(f"Searching for wells within {miles} miles of permit locations, Formations: {formations}")
        print(f"Bounding box: Lat [{min_lat:.4f}, {max_lat:.4f}], Lon [{min_lon:.4f}, {max_lon:.4f}]")

        # FirstProductionYear >= vintageCutoff keeps modern completions; null years (never produced)
        # are excluded automatically since NaN >= N evaluates to False.
        print(f"Vintage cutoff: FirstProductionYear >= {vintageCutoff}")
        mask = (
            (wells["MPLatitude"] >= min_lat)
            & (wells["MPLatitude"] <= max_lat)
            & (wells["MPLongitude"] >= min_lon)
            & (wells["MPLongitude"] <= max_lon)
            & (wells["Formation"].isin(formations))
            & (wells["FirstProductionYear"] >= vintageCutoff)
        )
        filtered = wells[mask].copy()
        print(f"Done. Retrieved {len(filtered):,} wells matching criteria.")

    # Tag mode + filter context so downstream HTML renderers can surface them.
    # vintageCutoff is meaningful only for radius mode; api_list mode stashes None.
    filtered.attrs["searchMode"] = searchMode
    filtered.attrs["vintageCutoff"] = vintageCutoff if searchMode == "radius" else None
    return filtered


## Retrieve well permits from Novi for each AFE row
## Looks up permits from the local bulk export (D:\novi). Texas wells matched by API10,
## non-Texas matched by ID. If no permit row exists (well already spudded and dropped from
## WellPermits), falls back to WellDetails surface-hole coords. If WellDetails also misses
## (truly unpermitted), resolves the well's Township/Range/Section via BLM PLSS centroid.
def getWellPermits(token, afeData, scope="us-horizontals"):

    # Load local permits TSV once (~130 MB) — single read for the whole AFE list
    paths = getNoviBulkPaths()
    permitsPath = paths["tsv"]["WellPermits"]
    localPermits = pd.read_csv(
        permitsPath,
        sep="\t",
        low_memory=False,
        dtype={"API10": "string", "ID": "string"},
    )
    print(f"  Loaded {len(localPermits):,} local permits")

    # WellDetails is lazy-loaded only if we hit a spud-fallback case (~130 MB).
    # Indexed by (API10, State) so per-AFE-row lookup is O(1) instead of O(N) scan.
    wellDetails = None

    all_permits = []
    spud_fallback_count = 0
    trs_fallback_count = 0

    # Loop through each well in the AFE Summary
    for index, row in afeData.iterrows():
        api_number = str(row["API Number"])
        # Normalize to Novi's storage form: full state name ("Texas") and bare county ("Martin")
        state = _normalizeStateFull(row["State"])
        county = _normalizeCounty(row["County"])
        is_texas = state.lower() == "texas"

        # Try local bulk export first — Texas matches on API10, others match on ID
        if is_texas:
            print(f"\nWell {index + 1}/{len(afeData)}: Texas well — local lookup by API10")
            print(f"  API10: {api_number}, {county} County, {state}")
            local_match = localPermits[
                (localPermits["API10"] == api_number)
                & (localPermits["County"] == county)
                & (localPermits["State"] == state)
            ]
        else:
            print(f"\nWell {index + 1}/{len(afeData)}: Local lookup — ID {api_number}, {county} County, {state}")
            local_match = localPermits[
                (localPermits["ID"] == api_number)
                & (localPermits["County"] == county)
                & (localPermits["State"] == state)
            ]

        if len(local_match) > 0:
            print(f"  Local hit: {len(local_match)} permits")
            all_permits.append(local_match)
            continue

        # Permit miss — well may already be spudded (Novi drops spud wells from WellPermits).
        # Look up WellDetails by API10 and use the surface-hole location if present.
        if wellDetails is None:
            raw = pd.read_csv(
                paths["tsv"]["WellDetails"],
                sep="\t",
                low_memory=False,
                dtype={"API10": "string"},
                usecols=["API10", "WellName", "State", "County",
                         "SHLLatitude", "SHLLongitude",
                         "BHLLatitude", "BHLLongitude", "SpudDate"],
            )
            wellDetails = raw.drop_duplicates(subset=["API10", "State"]).set_index(["API10", "State"])
            print(f"  Loaded {len(raw):,} WellDetails rows for spud fallback")

        wd_row = None
        try:
            wd_row = wellDetails.loc[(api_number, state)]
        except KeyError:
            pass
        if wd_row is not None:
            shl_lat, shl_lon = wd_row.get("SHLLatitude"), wd_row.get("SHLLongitude")
            if pd.isna(shl_lat) or pd.isna(shl_lon):
                shl_lat, shl_lon = wd_row.get("BHLLatitude"), wd_row.get("BHLLongitude")
            if pd.notna(shl_lat) and pd.notna(shl_lon):
                all_permits.append(_syntheticPermitRow(
                    shl_lat, shl_lon,
                    wd_row.get("WellName") or f"Well {index + 1}",
                    county, state, api_number,
                ))
                spud_fallback_count += 1
                print(f"  Spud well found in WellDetails (SpudDate={wd_row.get('SpudDate')}), placed at SHL: ({float(shl_lat):.6f}, {float(shl_lon):.6f})")
                continue

        # No permit and no WellDetails row — resolve from AFE Township/Range/Section via BLM PLSS
        print(f"  No local match — resolving location from T/R/S")
        twp_str = str(row.get("Township", "")).strip()
        rng_str = str(row.get("Range", "")).strip()
        sec_str = str(row.get("Section", "")).strip()
        state_abbr = STATE_ABBR_MAP.get(str(state).strip().lower())

        if twp_str and rng_str and sec_str and state_abbr:
            centroid = _fetchSectionCentroid(state_abbr, twp_str, rng_str, sec_str)
            if centroid:
                lat, lon = centroid
                all_permits.append(_syntheticPermitRow(
                    lat, lon,
                    str(row.get("Well Name", f"Well {index + 1}")).strip(),
                    county, state, api_number,
                ))
                trs_fallback_count += 1
                print(f"  Placed at PLSS section centroid: ({lat:.6f}, {lon:.6f})")
            else:
                print(f"  Could not resolve T/R/S to PLSS section — well will be missing from analysis")
        else:
            print(f"  No T/R/S data in AFE — well will be missing from analysis")

    # Concatenate all matches (mix of local permit DataFrames + synthetic T/R/S rows)
    if all_permits:
        permitDf = pd.concat(all_permits, ignore_index=True)
    else:
        permitDf = pd.DataFrame()

    print(f"\nDone. Retrieved {len(permitDf)} well permits total ({spud_fallback_count} spud wells via WellDetails, {trs_fallback_count} unpermitted wells via T/R/S).")

    # Texas permits often have null Lat/Lon but BHL coordinates are populated - use those as fallback
    if "Latitude" in permitDf.columns and "BHLLatitude" in permitDf.columns:
        permitDf["Latitude"] = permitDf["Latitude"].fillna(permitDf["BHLLatitude"])
    if "Longitude" in permitDf.columns and "BHLLongitude" in permitDf.columns:
        permitDf["Longitude"] = permitDf["Longitude"].fillna(permitDf["BHLLongitude"])

    return permitDf


## Retrieve EUR forecasts for offset wells by summing all forecast years
## Reads from local Novi bulk export — chunked even though ForecastWellYears.tsv is only 1.5 GB
## token and scope are kept for backward compat with main_model.py but unused.
## Appends Oil EUR, Gas EUR, Water EUR columns to the offset well DataFrame
def getNoviYearlyForecast(token, offsetData, scope="us-horizontals"):

    api10_set = set(offsetData["API10"].astype("string").tolist())
    print(f"Filtering yearly forecasts for {len(api10_set):,} wells from local bulk export...")

    paths = getNoviBulkPaths()
    yearlyPath = paths["tsv"]["ForecastWellYears"]

    chunk_size = 1_000_000
    matched_chunks = []
    rows_scanned = 0

    for i, chunk in enumerate(
        pd.read_csv(
            yearlyPath,
            sep="\t",
            chunksize=chunk_size,
            low_memory=False,
            dtype={"API10": "string"},
        ),
        1,
    ):
        rows_scanned += len(chunk)
        hits = chunk[chunk["API10"].isin(api10_set)]
        if len(hits):
            matched_chunks.append(hits)
        print(f"  Chunk {i}: scanned {rows_scanned:,} rows, matched {sum(len(c) for c in matched_chunks):,} so far")

    all_data = pd.concat(matched_chunks, ignore_index=True) if matched_chunks else pd.DataFrame()

    # Group by API10 and sum EUR (vectorized replacement for the old per-record loop)
    if len(all_data):
        eur = all_data.groupby("API10").agg(
            oil=("OilPerYear", "sum"),
            gas=("GasPerYear", "sum"),
            water=("WaterPerYear", "sum"),
        )
    else:
        eur = pd.DataFrame(columns=["oil", "gas", "water"])

    # Append EUR columns to offset well data
    offsetData = offsetData.copy()
    offsetData["API10"] = offsetData["API10"].astype("string")  # ensure dtype match for .map
    if len(eur):
        offsetData["Oil EUR"] = offsetData["API10"].map(eur["oil"]).fillna(0)
        offsetData["Gas EUR"] = offsetData["API10"].map(eur["gas"]).fillna(0)
        offsetData["Water EUR"] = offsetData["API10"].map(eur["water"]).fillna(0)
    else:
        offsetData["Oil EUR"] = 0
        offsetData["Gas EUR"] = 0
        offsetData["Water EUR"] = 0

    print(f"Done. Retrieved forecast data for {len(eur)} of {len(api10_set)} wells.")
    return offsetData


## Retrieve monthly forecast volumes for all wells in forecastData
## Reads from local Novi bulk export — chunked because ForecastWellMonths.tsv is ~12 GB
## token and scope are kept for backward compat with main_model.py but unused.
def getNoviMonthlyForecast(token, forecastData, scope="us-horizontals"):

    # API10 is varchar(32) per Novi schema — coerce filter set to string so .isin() matches
    api10_set = set(forecastData["API10"].astype("string").tolist())
    print(f"Filtering monthly forecasts for {len(api10_set):,} wells from local bulk export...")

    paths = getNoviBulkPaths()
    monthlyPath = paths["tsv"]["ForecastWellMonths"]

    chunk_size = 1_000_000
    matched_chunks = []
    rows_scanned = 0

    for i, chunk in enumerate(
        pd.read_csv(
            monthlyPath,
            sep="\t",
            chunksize=chunk_size,
            low_memory=False,
            dtype={"API10": "string"},
        ),
        1,
    ):
        rows_scanned += len(chunk)
        hits = chunk[chunk["API10"].isin(api10_set)]
        if len(hits):
            matched_chunks.append(hits)
        print(f"  Chunk {i}: scanned {rows_scanned:,} rows, matched {sum(len(c) for c in matched_chunks):,} so far")

    if matched_chunks:
        monthlyDf = pd.concat(matched_chunks, ignore_index=True)
    else:
        monthlyDf = pd.DataFrame()

    unique_wells = monthlyDf["API10"].nunique() if len(monthlyDf) else 0
    print(f"Done. Retrieved {len(monthlyDf):,} monthly forecast rows for {unique_wells} wells.")
    return monthlyDf


## Download the full Novi bulk export to local disk (default D:\novi) and extract all files
## Caches by export date via manifest.json — re-running skips download if the export hasn't changed
## Returns the path to the extracted directory; downstream functions should read TSVs from there
def noviBulk(token, scope="us-horizontals", outputDir=None):
    import json
    import zipfile
    from datetime import datetime

    if outputDir is None:
        outputDir = os.environ.get("NOVI_BULK_DATA_PATH", r"D:\novi")

    os.makedirs(outputDir, exist_ok=True)

    # Step 1: hit bulk endpoint to get the download URL + export metadata
    print(f"Requesting bulk export metadata from Novi (scope={scope})...")
    params = {
        "authentication_token": token,
        "scope": scope,
    }

    response = requests.get(BASE_URL + "v3/bulk.json", params=params, timeout=300)
    response.raise_for_status()
    meta = response.json()
    print(f"Bulk endpoint returned {len(meta) if isinstance(meta, list) else 1} entry(ies).")

    # Response is a list of export entries — one per tier/basin combo. Take the first.
    if isinstance(meta, list):
        if not meta:
            raise RuntimeError("Bulk endpoint returned empty list")
        entry = meta[0]
    else:
        entry = meta

    database_url = entry.get("URL")
    shapefile_url = entry.get("ShapefileURL")
    export_date = entry.get("ExportDate") or datetime.now().strftime("%Y-%m-%d")
    tier = entry.get("Tier", "unknown")

    if not database_url:
        raise RuntimeError(f"No URL field in bulk response entry: {entry}")

    print(f"  Tier:          {tier}")
    print(f"  Export date:   {export_date}")
    print(f"  Database zip:  {database_url}")
    print(f"  Shapefile zip: {shapefile_url}")

    # Sanitize export date for filenames (Windows-safe)
    export_date_str = str(export_date).split(".")[0].replace(":", "-").replace("/", "-").replace("T", "_")

    # Cache check
    extractDir = os.path.join(outputDir, f"extracted_{export_date_str}")
    manifestPath = os.path.join(outputDir, "manifest.json")

    if os.path.exists(extractDir) and os.path.exists(manifestPath):
        with open(manifestPath, "r") as f:
            existing = json.load(f)
        if existing.get("export_date") == export_date_str:
            print(f"Export {export_date_str} already cached at {extractDir}, skipping download.")
            return extractDir

    # Remove previous extract and zip files before downloading new export
    if os.path.exists(manifestPath):
        with open(manifestPath, "r") as f:
            oldManifest = json.load(f)
        oldExtractDir = oldManifest.get("extract_dir")
        if oldExtractDir and os.path.isdir(oldExtractDir) and oldExtractDir != extractDir:
            import shutil
            print(f"Removing old export at {oldExtractDir}...")
            shutil.rmtree(oldExtractDir)
        # Clean up old zip files
        for fname in os.listdir(outputDir):
            if fname.startswith("novi_bulk_") and fname.endswith(".zip"):
                oldZip = os.path.join(outputDir, fname)
                print(f"Removing old zip {fname}...")
                os.remove(oldZip)

    os.makedirs(extractDir, exist_ok=True)

    # Helper: stream a URL to disk with progress logging, then extract
    def _download_and_extract(url, label):
        if not url:
            print(f"  ({label}: no URL, skipping)")
            return []
        zipPath = os.path.join(outputDir, f"novi_bulk_{label}_{export_date_str}.zip")
        print(f"\nDownloading {label} to {zipPath}...")
        with requests.get(url, stream=True, timeout=3600) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            downloaded = 0
            chunk_size = 1024 * 1024  # 1 MB
            last_logged_pct = -5
            with open(zipPath, "wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = int(downloaded * 100 / total)
                        if pct >= last_logged_pct + 5:
                            print(f"  [{label}] {downloaded / (1024**3):.2f} / {total / (1024**3):.2f} GB ({pct}%)")
                            last_logged_pct = pct
                    elif downloaded % (256 * 1024 * 1024) < chunk_size:
                        print(f"  [{label}] {downloaded / (1024**3):.2f} GB downloaded")
        print(f"{label} download complete ({os.path.getsize(zipPath) / (1024**3):.2f} GB). Extracting...")
        with zipfile.ZipFile(zipPath, "r") as z:
            members = z.namelist()
            print(f"  {len(members)} files in {label} archive")
            for i, name in enumerate(members, 1):
                z.extract(name, extractDir)
                if i % 5 == 0 or i == len(members):
                    print(f"  [{label}] Extracted {i}/{len(members)}: {name}")
        return members

    db_members = _download_and_extract(database_url, "database")
    shp_members = _download_and_extract(shapefile_url, "shapefiles")

    # Write manifest for caching + downstream lookup
    manifest = {
        "export_date": export_date_str,
        "raw_export_date": export_date,
        "tier": tier,
        "downloaded_at": datetime.now().isoformat(),
        "extract_dir": extractDir,
        "scope": scope,
        "database_files": db_members,
        "shapefile_files": shp_members,
        "stats": entry.get("Stats"),
    }
    with open(manifestPath, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"\nDone. Extracted {len(db_members)} database + {len(shp_members)} shapefile files to {extractDir}")
    return extractDir


## Resolve paths to the locally extracted Novi bulk export
## Reads manifest.json from NOVI_BULK_DATA_PATH (default D:\novi) and returns a dict
## with the Database directory + every TSV path keyed by stem name (e.g. "WellDetails")
## Raises FileNotFoundError if no bulk export has been downloaded yet
def getNoviBulkPaths(outputDir=None):
    import json

    if outputDir is None:
        outputDir = os.environ.get("NOVI_BULK_DATA_PATH", r"D:\novi")

    manifestPath = os.path.join(outputDir, "manifest.json")
    if not os.path.exists(manifestPath):
        raise FileNotFoundError(
            f"No Novi bulk manifest at {manifestPath}. Run runNoviBulk() first."
        )

    with open(manifestPath, "r") as f:
        manifest = json.load(f)

    extractDir = manifest["extract_dir"]
    databaseDir = os.path.join(extractDir, "Database")

    if not os.path.isdir(databaseDir):
        raise FileNotFoundError(
            f"Bulk manifest points to {extractDir} but Database folder is missing. "
            f"Re-run runNoviBulk() to repair."
        )

    # Build a name -> full path map for every TSV in Database/
    tsvPaths = {}
    for fname in os.listdir(databaseDir):
        if fname.endswith(".tsv"):
            stem = fname[:-4]
            tsvPaths[stem] = os.path.join(databaseDir, fname)

    return {
        "extract_dir": extractDir,
        "database_dir": databaseDir,
        "export_date": manifest.get("export_date"),
        "tsv": tsvPaths,
    }


## Check if the Novi bulk export needs refreshing
## Hits the bulk endpoint for metadata, compares ExportDate against local manifest
## Only downloads if a newer export is available on the server
def checkNoviDbStatus(envPath=r"C:\Users\Michael Tanner\code\.env", outputDir=None, force=False):
    import json
    from datetime import datetime

    # Only check/download on Mondays — Novi publishes weekly (unless forced)
    if not force and datetime.today().weekday() != 0:
        print(f"Skipping Novi bulk status check (today is {datetime.today().strftime('%A')}, only runs Monday).")
        return

    if outputDir is None:
        outputDir = os.environ.get("NOVI_BULK_DATA_PATH", r"D:\novi")

    # Load env vars for auth
    if os.path.exists(envPath):
        with open(envPath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

    # Auth and fetch remote export metadata
    print("Checking Novi bulk export status...")
    token = authNovi()
    params = {"authentication_token": token, "scope": "us-horizontals"}
    response = requests.get(BASE_URL + "v3/bulk.json", params=params, timeout=60)
    response.raise_for_status()
    meta = response.json()
    entry = meta[0] if isinstance(meta, list) else meta
    remoteExportDate = str(entry.get("ExportDate", ""))

    print(f"  Remote ExportDate: {remoteExportDate}")

    # Compare against local manifest
    manifestPath = os.path.join(outputDir, "manifest.json")
    localExportDate = None
    if os.path.exists(manifestPath):
        with open(manifestPath, "r") as f:
            noviMeta = json.load(f)
        localExportDate = str(noviMeta.get("raw_export_date", ""))
        print(f"  Local  ExportDate: {localExportDate}")

    if localExportDate == remoteExportDate:
        print("Novi bulk export is up to date, skipping re-download.")
        return

    print(f"New Novi export available ({localExportDate or 'none'} → {remoteExportDate}) — downloading...")
    extractDir = noviBulk(token, outputDir=outputDir)
    print(f"\nDone. Updated bulk export at: {extractDir}")


## Standalone entrypoint for downloading + extracting the Novi bulk export
## Loads NOVI_USERNAME / NOVI_PASSWORD from C:\Users\Michael Tanner\code\.env (no dotenv dependency)
## Then authenticates and runs noviBulk() to pull everything to D:\novi
def runNoviBulk(envPath=r"C:\Users\Michael Tanner\code\.env", outputDir=None):
    if not os.path.exists(envPath):
        raise FileNotFoundError(f".env not found at {envPath}")

    with open(envPath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

    print("Authenticating with Novi...")
    token = authNovi()
    print("Authenticated. Starting bulk download...")
    extractDir = noviBulk(token, outputDir=outputDir)
    print(f"\nALL DONE. Extracted bulk export to: {extractDir}")
    return extractDir


## Retrieve historical actual monthly production for all offset wells from the local bulk export
## Reads WellMonths.tsv (~4.7 GB) chunked because of size, filters by API10 set
## token and scope are kept for backward compat / consistency with the other novi functions but unused.
def getNoviMonthlyProduction(token, offsetData, scope="us-horizontals"):

    api10_set = set(offsetData["API10"].astype("string").tolist())
    print(f"Filtering historical monthly production for {len(api10_set):,} wells from local bulk export...")

    paths = getNoviBulkPaths()
    productionPath = paths["tsv"]["WellMonths"]

    chunk_size = 1_000_000
    matched_chunks = []
    rows_scanned = 0

    for i, chunk in enumerate(
        pd.read_csv(
            productionPath,
            sep="\t",
            chunksize=chunk_size,
            low_memory=False,
            dtype={"API10": "string"},
        ),
        1,
    ):
        rows_scanned += len(chunk)
        hits = chunk[chunk["API10"].isin(api10_set)]
        if len(hits):
            matched_chunks.append(hits)
        print(f"  Chunk {i}: scanned {rows_scanned:,} rows, matched {sum(len(c) for c in matched_chunks):,} so far")

    if matched_chunks:
        productionDf = pd.concat(matched_chunks, ignore_index=True)
    else:
        productionDf = pd.DataFrame()

    unique_wells = productionDf["API10"].nunique() if len(productionDf) else 0
    print(f"Done. Retrieved {len(productionDf):,} monthly production rows for {unique_wells} wells.")
    return productionDf


## Retrieve subsurface petrophysical data for offset wells from local bulk export
## Inner-merges on (API10, Formation) so each well gets exactly one row matching its reported formation.
## Wells whose Formation doesn't match any Subsurface row for that API10 are dropped (logged).
## Also joins 12-month and 24-month cumulative production from WellDetails.tsv so production heatmaps render alongside the petrophysical ones.
## token and scope are kept for backward compat / consistency with the other novi functions but unused.
def getNoviSubsurface(token, offsetData, scope="us-horizontals"):

    api10_set = set(offsetData["API10"].astype("string").tolist())
    print(f"Filtering subsurface data for {len(api10_set):,} wells from local bulk export...")

    paths = getNoviBulkPaths()
    subsurfacePath = paths["tsv"]["Subsurface"]

    subsurface = pd.read_csv(subsurfacePath, sep="\t", low_memory=False, dtype={"API10": "string"})
    print(f"  Loaded {len(subsurface):,} total subsurface rows")

    # Filter to offset wells first (cheap)
    subsurface = subsurface[subsurface["API10"].isin(api10_set)]
    print(f"  {len(subsurface):,} rows match offset well API10s")

    # Formation-aware inner merge — keeps only the subsurface row where (API10, Formation)
    # matches offsetData. Each well ends up with exactly one subsurface row.
    keys = offsetData[["API10", "Formation"]].copy()
    keys["API10"] = keys["API10"].astype("string")
    merged = subsurface.merge(keys, on=["API10", "Formation"], how="inner")
    merged = merged.drop_duplicates(subset=["API10"], keep="first")  # safety if duplicate zone rows exist

    # Join 12-month and 24-month cumulative BOE from WellDetails for production heatmaps.
    # Left-join: wells without 12/24M production (recent vintage) keep petrophysical rows but get NaN
    # on the production columns — the heatmap loop already skips parameters with too few valid points.
    wellDetailsPath = paths["tsv"].get("WellDetails")
    if wellDetailsPath and os.path.exists(wellDetailsPath):
        production_cols = [
            "Cum12MOil", "Cum24MOil",
            "Cum12MGas", "Cum24MGas",
            "EUR50YROil", "EUR50YRGas",
            "LateralLength",
        ]
        wd = pd.read_csv(
            wellDetailsPath,
            sep="\t",
            low_memory=False,
            usecols=["API10"] + production_cols,
            dtype={"API10": "string"},
        )
        wd = wd[wd["API10"].isin(api10_set)]
        merged = merged.merge(wd, on="API10", how="left")
        counts = ", ".join(f"{c} ({int(merged[c].notna().sum()):,} wells)" for c in production_cols)
        print(f"  Joined production heatmap columns from WellDetails: {counts}")
    else:
        print("  WellDetails.tsv not found — production heatmaps will be skipped")

    matched = merged["API10"].nunique()
    total = len(api10_set)
    print(f"Done. {matched:,} of {total:,} offset wells matched to a subsurface zone ({matched/total*100:.1f}%).")
    if matched < total:
        dropped = total - matched
        print(f"  {dropped:,} wells had no subsurface row matching their reported Formation — excluded from heat maps.")
    return merged


## Retrieve wellbore trajectory points (lat/lon path) for offset wells from local bulk export
## Reads WellboreLocations.tsv (~472 MB) chunked, filters by API10 set
## Each well returns ~15 points sequentially numbered by `Path` — use that to draw the trajectory
## token and scope are kept for backward compat / consistency but unused.
def getNoviWellboreLocations(token, offsetData, scope="us-horizontals"):

    api10_set = set(offsetData["API10"].astype("string").tolist())
    print(f"Filtering wellbore locations for {len(api10_set):,} wells from local bulk export...")

    paths = getNoviBulkPaths()
    wbPath = paths["tsv"]["WellboreLocations"]

    chunk_size = 1_000_000
    matched_chunks = []
    rows_scanned = 0

    for i, chunk in enumerate(
        pd.read_csv(
            wbPath,
            sep="\t",
            chunksize=chunk_size,
            low_memory=False,
            dtype={"API10": "string"},
        ),
        1,
    ):
        rows_scanned += len(chunk)
        hits = chunk[chunk["API10"].isin(api10_set)]
        if len(hits):
            matched_chunks.append(hits)
        print(f"  Chunk {i}: scanned {rows_scanned:,} rows, matched {sum(len(c) for c in matched_chunks):,} so far")

    if matched_chunks:
        wbDf = pd.concat(matched_chunks, ignore_index=True)
        wbDf = wbDf.sort_values(["API10", "Path"]).reset_index(drop=True)
    else:
        wbDf = pd.DataFrame()

    unique_wells = wbDf["API10"].nunique() if len(wbDf) else 0
    print(f"Done. Retrieved {len(wbDf):,} wellbore points for {unique_wells} wells.")
    return wbDf


## Ensure Census TIGER state + county basemaps are downloaded and extracted
## Caches to {NOVI_BULK_DATA_PATH}\basemaps\ — one-time download (~13 MB total)
## Returns paths to the extracted .shp files as a dict
def _ensureBasemaps(outputDir=None):
    import json
    import zipfile
    import urllib.request

    if outputDir is None:
        outputDir = os.environ.get("NOVI_BULK_DATA_PATH", r"D:\novi")

    basemapDir = os.path.join(outputDir, "basemaps")
    os.makedirs(basemapDir, exist_ok=True)

    sources = {
        "state": {
            "url": "https://www2.census.gov/geo/tiger/GENZ2023/shp/cb_2023_us_state_500k.zip",
            "zip": os.path.join(basemapDir, "cb_2023_us_state_500k.zip"),
            "extract": os.path.join(basemapDir, "cb_2023_us_state_500k"),
            "shp": os.path.join(basemapDir, "cb_2023_us_state_500k", "cb_2023_us_state_500k.shp"),
        },
        "county": {
            "url": "https://www2.census.gov/geo/tiger/GENZ2023/shp/cb_2023_us_county_500k.zip",
            "zip": os.path.join(basemapDir, "cb_2023_us_county_500k.zip"),
            "extract": os.path.join(basemapDir, "cb_2023_us_county_500k"),
            "shp": os.path.join(basemapDir, "cb_2023_us_county_500k", "cb_2023_us_county_500k.shp"),
        },
    }

    for layer, info in sources.items():
        if os.path.exists(info["shp"]):
            continue
        print(f"Downloading {layer} basemap from Census TIGER...")
        urllib.request.urlretrieve(info["url"], info["zip"])
        os.makedirs(info["extract"], exist_ok=True)
        with zipfile.ZipFile(info["zip"], "r") as z:
            z.extractall(info["extract"])
        print(f"  Extracted {layer} basemap to {info['extract']}")

    return {layer: info["shp"] for layer, info in sources.items()}


## Fetch BLM National PLSS CadNSDI v2 township + section layers for a bounding box
## Queries the BLM ArcGIS REST service on demand, paginates if needed, caches the GeoJSON
## response to D:\novi\basemaps\plss\ keyed by rounded bbox so repeated runs hit local disk.
## Returns (townships_gdf, sections_gdf). Either may be empty if the bbox is in a non-PLSS
## area (Texas, Pennsylvania, ocean) or if the BLM service is unreachable.
def _fetchPlssLayers(lon_min, lat_min, lon_max, lat_max, outputDir=None):
    import json
    import time
    import urllib.request
    import urllib.error
    import geopandas as gpd
    import pandas as pd

    if outputDir is None:
        outputDir = os.environ.get("NOVI_BULK_DATA_PATH", r"D:\novi")

    plssDir = os.path.join(outputDir, "basemaps", "plss")
    os.makedirs(plssDir, exist_ok=True)

    # Round bbox to 0.05° for cache stability — small AFE shifts still hit cache
    def _round(x): return round(x * 20) / 20
    rl_min, rt_min = _round(lon_min), _round(lat_min)
    rl_max, rt_max = _round(lon_max), _round(lat_max)
    bbox_str = f"{rl_min:.2f}_{rt_min:.2f}_{rl_max:.2f}_{rt_max:.2f}"

    BASE = "https://gis.blm.gov/arcgis/rest/services/Cadastral/BLM_Natl_PLSS_CadNSDI/MapServer"
    PAGE_SIZE = 1000
    MAX_PAGES = 20

    def _fetch_page(layer_id, bx_min, by_min, bx_max, by_max, offset):
        """Fetch one page of features from BLM. Retries up to 3 times on transient errors."""
        url = (
            f"{BASE}/{layer_id}/query"
            f"?where=1=1"
            f"&geometry={bx_min},{by_min},{bx_max},{by_max}"
            f"&geometryType=esriGeometryEnvelope"
            f"&inSR=4326&outSR=4326"
            f"&outFields=*"
            f"&f=geojson"
            f"&resultRecordCount={PAGE_SIZE}"
            f"&resultOffset={offset}"
        )
        last_err = None
        for attempt in range(3):
            try:
                with urllib.request.urlopen(url, timeout=60) as r:
                    return json.loads(r.read())
            except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
                last_err = e
                if attempt < 2:
                    time.sleep(1.5 * (attempt + 1))
        raise last_err

    def _fetch_bbox(layer_id, bx_min, by_min, bx_max, by_max):
        """Fetch all features in one bbox with pagination. Returns list or raises on failure."""
        all_features = []
        offset = 0
        for _ in range(MAX_PAGES):
            data = _fetch_page(layer_id, bx_min, by_min, bx_max, by_max, offset)
            page_features = data.get("features", [])
            if not page_features:
                break
            all_features.extend(page_features)
            if len(page_features) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        return all_features

    def _fetch_bbox_with_split(layer_id, label, bx_min, by_min, bx_max, by_max, depth=0):
        """Fetch a bbox; if BLM returns an error (e.g. 500 on dense layers), split into
        4 quadrants and recurse. Sections are denser than townships and sometimes hit
        BLM's per-request limit on wide bboxes — splitting tiles around it."""
        try:
            return _fetch_bbox(layer_id, bx_min, by_min, bx_max, by_max)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            if depth >= 3:
                print(f"  PLSS {label}: tile {bx_min:.2f},{by_min:.2f},{bx_max:.2f},{by_max:.2f} failed after split ({e})")
                return []
            print(f"  PLSS {label}: bbox failed ({e}), splitting into 4 quadrants")
            mx = (bx_min + bx_max) / 2
            my = (by_min + by_max) / 2
            out = []
            out += _fetch_bbox_with_split(layer_id, label, bx_min, by_min, mx, my, depth + 1)
            out += _fetch_bbox_with_split(layer_id, label, mx, by_min, bx_max, my, depth + 1)
            out += _fetch_bbox_with_split(layer_id, label, bx_min, my, mx, by_max, depth + 1)
            out += _fetch_bbox_with_split(layer_id, label, mx, my, bx_max, by_max, depth + 1)
            return out

    def _fetch_layer(layer_id, label):
        cachePath = os.path.join(plssDir, f"plss_{label}_{bbox_str}.geojson")
        if os.path.exists(cachePath):
            try:
                gdf = gpd.read_file(cachePath)
                if len(gdf) > 0:
                    print(f"  PLSS {label}: {len(gdf)} features (cache hit)")
                    return gdf
                else:
                    print(f"  PLSS {label}: cache file empty, refetching")
            except Exception as e:
                print(f"  PLSS {label}: cache file corrupt ({e}), refetching")

        all_features = _fetch_bbox_with_split(layer_id, label, rl_min, rt_min, rl_max, rt_max)

        if not all_features:
            print(f"  PLSS {label}: 0 features in bbox (likely non-PLSS area or fetch failed)")
            return gpd.GeoDataFrame()

        # De-dupe features that may appear in multiple split tiles (overlap on tile borders)
        seen = set()
        unique = []
        for f in all_features:
            fid = f.get("id") or f.get("properties", {}).get("OBJECTID")
            if fid is None:
                unique.append(f)
                continue
            if fid in seen:
                continue
            seen.add(fid)
            unique.append(f)

        fc = {"type": "FeatureCollection", "features": unique}
        with open(cachePath, "w") as f:
            json.dump(fc, f)

        gdf = gpd.read_file(cachePath)
        print(f"  PLSS {label}: {len(gdf)} features (fetched + cached)")
        return gdf

    print(f"Fetching BLM PLSS layers for bbox [{rl_min}, {rt_min}, {rl_max}, {rt_max}]...")
    townships = _fetch_layer(1, "townships")
    sections = _fetch_layer(2, "sections")
    return townships, sections


## Generate an interactive HTML page of interpolated heat maps for subsurface parameters
## Uses Plotly.js (loaded from CDN) for zoomable, hoverable contour maps with well overlays.
## scipy.interpolate.griddata (linear) interpolates point values onto a regular grid;
## All.shp operator acreage + Census state/county outlines render as a basemap.
## Optionally overlays offset well lateral paths (from getNoviWellboreLocations) and AFE
## permit locations with labels (from getWellPermits). Open the file in any browser.
## Output: subsurface_heatmaps_{DSU Name}.html in the AFE Data/ folder.
def plotSubsurfaceHeatMapsHTML(subsurfaceData, pathToAfeSummary, parameters=None, permitData=None,
                                wellboreLocationsData=None, offsetData=None, labelNearestN=20, afeData=None,
                                monthlyForecastData=None):
    from scipy.interpolate import griddata
    import numpy as np
    import json as _json
    import webbrowser

    if parameters is None:
        parameters = [
            "TVD", "TOC_Avg", "SW_Avg", "Porosity_Avg",
            "Permeability_Avg", "Thickness_Avg", "VClay_Avg", "Brittleness_Avg",
        ] + _productionHeatMapParamsForAfe(afeData)

    afeDir = os.path.dirname(pathToAfeSummary)
    afeFilename = os.path.splitext(os.path.basename(pathToAfeSummary))[0]
    dsuName = afeFilename.split("-", 1)[1].strip() if "-" in afeFilename else afeFilename
    outputDir = os.path.join(afeDir, "Data")
    os.makedirs(outputDir, exist_ok=True)
    # Suffix when getWells ran in api_list mode so radius and api-list runs coexist
    searchMode = offsetData.attrs.get("searchMode") if offsetData is not None and hasattr(offsetData, "attrs") else None
    vintageCutoff = offsetData.attrs.get("vintageCutoff") if offsetData is not None and hasattr(offsetData, "attrs") else None
    suffix = "_apilist" if searchMode == "api_list" else ""
    htmlPath = os.path.join(outputDir, f"subsurface_heatmaps_{dsuName}{suffix}.html")

    # Formation + vintage cutoff for the header banner so the analyst knows what filter built the cohort.
    afe_formations_for_header = []
    if afeData is not None and "Landing Zone" in afeData.columns:
        afe_formations_for_header = sorted(afeData["Landing Zone"].dropna().astype(str).str.strip().str.upper().unique().tolist())
    formation_label = ", ".join(afe_formations_for_header) if afe_formations_for_header else "—"
    if searchMode == "api_list":
        vintage_label = "API10 list (no vintage filter)"
    elif vintageCutoff is not None:
        vintage_label = f"FirstProductionYear ≥ {int(vintageCutoff)}"
    else:
        vintage_label = "—"

    if subsurfaceData is None or subsurfaceData.empty:
        print("No subsurface data — skipping HTML heat map export.")
        return

    df = subsurfaceData.dropna(subset=["Latitude", "Longitude"]).copy()
    if df.empty:
        print("No subsurface rows with valid Lat/Lon — skipping HTML heat map export.")
        return

    # Collapse formation aliases so geologically-equivalent zones (e.g. JO MILL <-> LOWER SPRABERRY SAND) share one page
    if "Formation" in df.columns:
        upper = df["Formation"].astype("string").str.strip().str.upper()
        df["Formation"] = upper.map(FORMATION_ALIASES).fillna(upper)

    # Interpolation grid
    margin = 0.05
    lon_min, lon_max = df["Longitude"].min() - margin, df["Longitude"].max() + margin
    lat_min, lat_max = df["Latitude"].min() - margin, df["Latitude"].max() + margin
    grid_lon, grid_lat = np.meshgrid(
        np.linspace(lon_min, lon_max, 200),
        np.linspace(lat_min, lat_max, 200),
    )
    lon_list = np.linspace(lon_min, lon_max, 200).tolist()
    lat_list = np.linspace(lat_min, lat_max, 200).tolist()
    points = df[["Longitude", "Latitude"]].values

    # Build api10 lookups from offsetData
    api10_to_name = {}
    api10_to_first_prod = {}
    api10_to_fallback_pos = {}
    if offsetData is not None and not offsetData.empty:
        for _, r in offsetData.iterrows():
            api10 = str(r["API10"]) if pd.notna(r.get("API10")) else None
            if not api10:
                continue
            if "WellName" in offsetData.columns and pd.notna(r.get("WellName")):
                api10_to_name[api10] = str(r["WellName"])
            if "FirstProductionYear" in offsetData.columns and pd.notna(r.get("FirstProductionYear")):
                try:
                    api10_to_first_prod[api10] = int(r["FirstProductionYear"])
                except (ValueError, TypeError):
                    pass
            if "MPLatitude" in offsetData.columns and "MPLongitude" in offsetData.columns \
                    and pd.notna(r.get("MPLatitude")) and pd.notna(r.get("MPLongitude")):
                api10_to_fallback_pos[api10] = (float(r["MPLongitude"]), float(r["MPLatitude"]))

    # Wellbore trajectories
    wb_groups = None
    wb_midpoints = {}
    wb_heels = {}
    if wellboreLocationsData is not None and not wellboreLocationsData.empty:
        wb_clean = wellboreLocationsData.dropna(subset=["Latitude", "Longitude"])
        wb_groups = list(wb_clean.sort_values(["API10", "Path"]).groupby("API10"))
        for api10, group in wb_groups:
            mid = len(group) // 2
            wb_midpoints[api10] = (group["Longitude"].iloc[mid], group["Latitude"].iloc[mid])
            wb_heels[api10] = (group["Longitude"].iloc[0], group["Latitude"].iloc[0])

    # Numbered wells by distance from permit centroid
    numbered_wells = []
    api10_to_number = {}
    permit_pts = None
    if permitData is not None and not permitData.empty:
        permit_pts = permitData.dropna(subset=["Latitude", "Longitude"])
    if permit_pts is not None and not permit_pts.empty and api10_to_name:
        cx = permit_pts["Longitude"].mean()
        cy = permit_pts["Latitude"].mean()
        deg_to_mi_lat = 69.0
        deg_to_mi_lon = 69.0 * abs(math.cos(math.radians(cy)))
        dists = []
        for api10 in api10_to_name:
            if api10 in wb_midpoints:
                lon, lat = wb_midpoints[api10]
            elif api10 in api10_to_fallback_pos:
                lon, lat = api10_to_fallback_pos[api10]
            else:
                continue
            d_mi = (((lon - cx) * deg_to_mi_lon) ** 2 + ((lat - cy) * deg_to_mi_lat) ** 2) ** 0.5
            dists.append((d_mi, api10))
        dists.sort()
        cap = labelNearestN if (labelNearestN and labelNearestN > 0) else len(dists)
        for i, (d_mi, api10) in enumerate(dists[:cap], start=1):
            numbered_wells.append({
                "num": i, "api10": api10,
                "name": api10_to_name.get(api10, ""),
                "dist_mi": round(d_mi, 2),
                "first_prod": api10_to_first_prod.get(api10),
            })
            api10_to_number[api10] = i

    # AFE permit info
    afe_permits = []
    if afeData is not None and not afeData.empty:
        for i, (_idx, row) in enumerate(afeData.iterrows()):
            letter = chr(ord("A") + i) if i < 26 else chr(ord("A") + (i // 26 - 1)) + chr(ord("A") + (i % 26))
            afe_permits.append({
                "letter": letter,
                "name": str(row.get("Well Name", f"Well {i+1}")).strip(),
                "lz": _canonicalFormation(str(row.get("Landing Zone", "")).strip().upper()),
            })

    # Permit locations for star markers
    permit_markers = []
    if permit_pts is not None and not permit_pts.empty:
        for _, r in permit_pts.iterrows():
            permit_markers.append({
                "lon": float(r["Longitude"]), "lat": float(r["Latitude"]),
                "name": str(r.get("WellName", "")) if pd.notna(r.get("WellName")) else "",
            })

    # Match AFE permits to lat/lon by well name so we can interpolate the
    # subsurface parameter at each AFE location (and fall back to the permit centroid).
    def _normName(s):
        return re.sub(r"[^A-Z0-9]", "", str(s).upper())
    permit_by_name = {_normName(pm["name"]): pm for pm in permit_markers if pm["name"]}
    permit_centroid = None
    if permit_pts is not None and not permit_pts.empty:
        permit_centroid = (float(permit_pts["Longitude"].mean()), float(permit_pts["Latitude"].mean()))
    afe_with_loc = []
    for ap in afe_permits:
        norm = _normName(ap["name"])
        pm = permit_by_name.get(norm) if norm else None
        if pm is None and norm:
            for k, v in permit_by_name.items():
                if k and (norm in k or k in norm):
                    pm = v
                    break
        if pm is not None:
            lon, lat = pm["lon"], pm["lat"]
        elif permit_centroid is not None:
            lon, lat = permit_centroid
        else:
            lon, lat = None, None
        afe_with_loc.append({
            "letter": ap["letter"],
            "name": ap["name"],
            "lz": ap["lz"],
            "lon": lon,
            "lat": lat,
        })

    # Determine formations — each gets its own slicer entry. When multiple zones are present,
    # also emit a None ("All Formations") view so sparse api_list runs (e.g. 5 wells across
    # 3 Wolfcamp zones, ~2 per zone) still render — per-zone slices fall under the < 4 cutoff
    # but the aggregate has enough points to interpolate. Geo drivers already does this.
    if "Formation" in df.columns:
        formation_values = sorted(df["Formation"].dropna().astype(str).str.upper().unique().tolist())
    else:
        formation_values = []
    if len(formation_values) > 1:
        formations = [None] + formation_values
    elif formation_values:
        formations = formation_values
    else:
        formations = [None]

    api10_to_formation = {}
    if offsetData is not None and "Formation" in offsetData.columns:
        for _, r in offsetData[["API10", "Formation"]].dropna().iterrows():
            api10_to_formation[str(r["API10"])] = str(r["Formation"]).upper()

    # Build Plotly figure JSON for each (parameter, formation) combo
    print(f"Generating interactive HTML heat maps: {htmlPath}")
    figures = []  # list of {title, plotly_json}

    for param in parameters:
        if param not in df.columns:
            print(f"  Skipping {param}: column not in subsurface data")
            continue

        for fm_label in formations:
            if fm_label is None:
                df_fm = df
                api10_set_fm = None
            else:
                df_fm = df[df["Formation"].astype(str).str.upper() == fm_label]
                api10_set_fm = set(df_fm["API10"].astype(str).tolist())

            values = pd.to_numeric(df_fm[param], errors="coerce")
            mask = values.notna()
            if mask.sum() < 4:
                print(f"  Skipping {param} [{fm_label or 'ALL'}]: only {int(mask.sum())} valid points (need >= 4)")
                continue

            points_fm = df_fm[["Longitude", "Latitude"]].values
            interp = griddata(
                points_fm[mask.values], values[mask].values,
                (grid_lon, grid_lat), method="linear",
            )

            # Replace NaN with None for JSON
            z_data = []
            for row in interp.tolist():
                z_data.append([None if (v is None or v != v) else round(v, 4) for v in row])

            # Estimate parameter value at each AFE permit location (interpolation).
            # Linear first; fall back to nearest if the point sits outside the convex hull.
            valid_pts = points_fm[mask.values]
            valid_vals = values[mask].values
            afe_estimates = []
            for ap in afe_with_loc:
                if ap["lon"] is None or ap["lat"] is None:
                    afe_estimates.append({
                        "letter": ap["letter"], "name": ap["name"], "lz": ap["lz"], "value": None,
                    })
                    continue
                lin = griddata(valid_pts, valid_vals, ([ap["lon"]], [ap["lat"]]), method="linear")
                v = float(lin[0]) if lin is not None and not np.isnan(lin[0]) else None
                if v is None:
                    nr = griddata(valid_pts, valid_vals, ([ap["lon"]], [ap["lat"]]), method="nearest")
                    v = float(nr[0]) if nr is not None and not np.isnan(nr[0]) else None
                afe_estimates.append({
                    "letter": ap["letter"], "name": ap["name"], "lz": ap["lz"],
                    "value": None if v is None else round(v, 4),
                })

            # Wellbore traces
            wb_traces = []
            if wb_groups is not None:
                for _api10, group in wb_groups:
                    if api10_set_fm is not None and str(_api10) not in api10_set_fm:
                        continue
                    num = api10_to_number.get(str(_api10))
                    name = api10_to_name.get(str(_api10), str(_api10))
                    label = f"#{num} {name}" if num else name
                    wb_traces.append({
                        "lon": group["Longitude"].values.tolist(),
                        "lat": group["Latitude"].values.tolist(),
                        "label": label,
                    })

            # Well number markers at heels
            num_markers = []
            nw_fm = numbered_wells if api10_set_fm is None else [w for w in numbered_wells if w["api10"] in api10_set_fm]
            for w in nw_fm:
                api10 = w["api10"]
                if api10 in wb_heels:
                    lon, lat = wb_heels[api10]
                elif api10 in api10_to_fallback_pos:
                    lon, lat = api10_to_fallback_pos[api10]
                else:
                    continue
                num_markers.append({
                    "lon": lon, "lat": lat,
                    "num": w["num"], "name": w["name"],
                    "dist": w["dist_mi"],
                    "first_prod": w["first_prod"],
                })

            param_label = _subsurfaceParamLabel(param)
            title_suffix = f" [{fm_label}]" if fm_label else ""
            figures.append({
                "title": f"{param_label}{title_suffix} — {mask.sum()} wells",
                "param": param_label,
                "param_key": param,
                "fm_label": fm_label,
                "z": z_data,
                "lon": lon_list,
                "lat": lat_list,
                "wb_traces": wb_traces,
                "num_markers": num_markers,
                "permit_markers": permit_markers,
                "afe_estimates": afe_estimates,
            })
            print(f"  Prepared {param} [{fm_label or 'ALL'}] ({mask.sum()} wells)")

    # Aggregate AFE subsurface estimates for the Geo Drivers page table.
    # For each AFE, prefer the heatmap matching its landing-zone formation; fall back
    # to the all-formations heatmap, then to any available formation for that parameter.
    afe_subsurface_table_params = [
        p for p in [
            "TVD", "TOC_Avg", "SW_Avg", "Porosity_Avg",
            "Permeability_Avg", "Thickness_Avg", "VClay_Avg", "Brittleness_Avg",
        ]
        if p in df.columns
    ]
    estimate_index = {}  # (param_key, fm_label) -> {letter: value}
    for fig in figures:
        key = (fig["param_key"], fig["fm_label"])
        estimate_index[key] = {est["letter"]: est["value"] for est in fig.get("afe_estimates", [])}

    afe_subsurface_estimates = []
    for param in afe_subsurface_table_params:
        row = {"param": param, "label": _subsurfaceParamLabel(param), "by_letter": {}}
        for ap in afe_with_loc:
            val = None
            if (param, ap["lz"]) in estimate_index:
                val = estimate_index[(param, ap["lz"])].get(ap["letter"])
            if val is None and (param, None) in estimate_index:
                val = estimate_index[(param, None)].get(ap["letter"])
            if val is None:
                for (p, _fm), letters in estimate_index.items():
                    if p == param:
                        v = letters.get(ap["letter"])
                        if v is not None:
                            val = v
                            break
            row["by_letter"][ap["letter"]] = val
        afe_subsurface_estimates.append(row)

    # Build geo-driver diagnostic data — sliced by formation when multiple exist.
    # Mixing zones in one regression dilutes intra-formation signal, so each formation
    # gets its own bucket plus an "All Formations" view.
    afe_estimate_lookup_all = {row["param"]: row["by_letter"] for row in afe_subsurface_estimates}

    def _buildGeoDriverAnalysis(df_subset, est_lookup):
        analyses = []
        if df_subset is None or df_subset.empty:
            return analyses
        response_param_local = _geoDriverProductionParam(afeData, df_subset)
        if response_param_local not in df_subset.columns:
            return analyses
        y_all, response_label, estimate_label, afe_lateral_length = _geoDriverResponseSeries(
            df_subset, response_param_local, afeData
        )
        estimate_targets = _geoDriverEstimateTargets(afeData)
        for driver in [p for p in GEO_DRIVER_PARAMS if p in df_subset.columns]:
            x = pd.to_numeric(df_subset[driver], errors="coerce")
            y = y_all
            valid = x.notna() & y.notna()
            if valid.sum() < 4:
                continue
            x_valid = x[valid]
            y_valid = y[valid]
            r = x_valid.corr(y_valid)
            if pd.isna(r):
                continue
            line_x = []
            line_y = []
            x10, x50, x90 = np.percentile(x_valid, [10, 50, 90])
            slope = None
            intercept = None
            est10 = None
            est50 = None
            est90 = None
            if x_valid.nunique() > 1:
                slope, intercept = np.polyfit(x_valid, y_valid, 1)
                line_x = [float(x_valid.min()), float(x_valid.max())]
                line_y = [float(slope * line_x[0] + intercept), float(slope * line_x[1] + intercept)]
                est10 = float(slope * x10 + intercept)
                est50 = float(slope * x50 + intercept)
                est90 = float(slope * x90 + intercept)
                if afe_lateral_length:
                    est10 *= afe_lateral_length
                    est50 *= afe_lateral_length
                    est90 *= afe_lateral_length
            afe_points = []
            estimate_for_driver = est_lookup.get(driver, {})
            for ap in afe_with_loc:
                est_x = estimate_for_driver.get(ap["letter"])
                if est_x is None:
                    continue
                pred_y = float(slope * est_x + intercept) if slope is not None else None
                afe_points.append({
                    "letter": ap["letter"],
                    "name": ap["name"],
                    "lz": ap["lz"],
                    "x": float(est_x),
                    "y": pred_y,
                })
            analyses.append({
                "param": driver,
                "label": _subsurfaceParamLabel(driver),
                "response": response_label,
                "estimate_label": estimate_label,
                "afe_lateral_length": afe_lateral_length,
                "x": [float(v) for v in x_valid.tolist()],
                "y": [float(v) for v in y_valid.tolist()],
                "line_x": line_x,
                "line_y": line_y,
                "r": float(r),
                "n": int(valid.sum()),
                "x10": float(x10),
                "x50": float(x50),
                "x90": float(x90),
                "est_p10": None if est10 is None or est90 is None else max(est10, est90),
                "est_p50": est50,
                "est_p90": None if est10 is None or est90 is None else min(est10, est90),
                "afe_points": afe_points,
                "estimates": {
                    target: _geoDriverEstimateForTarget(df_subset, driver, target, afe_lateral_length)
                    for target in estimate_targets
                },
            })
        analyses.sort(key=lambda row: row["r"], reverse=True)
        for i, row in enumerate(analyses, start=1):
            row["rank"] = i
        return analyses

    def _estLookupForFormation(fm_label):
        # Per-formation: AFE markers sit on this formation's heat-map value at the AFE
        # location, so the predicted response reflects "what if we drilled this AFE to this zone".
        out = {}
        for param in GEO_DRIVER_PARAMS:
            by_letter = {}
            idx_entry = estimate_index.get((param, fm_label), {})
            for ap in afe_with_loc:
                by_letter[ap["letter"]] = idx_entry.get(ap["letter"])
            out[param] = by_letter
        return out

    non_null_formations = [f for f in formations if f is not None]
    geo_driver_buckets = []
    all_drivers = _buildGeoDriverAnalysis(df, afe_estimate_lookup_all)
    if len(non_null_formations) > 1:
        geo_driver_buckets.append({"label": "All Formations", "fm": None, "drivers": all_drivers})
        for fm in non_null_formations:
            df_fm = df[df["Formation"].astype(str).str.upper() == fm]
            fm_drivers = _buildGeoDriverAnalysis(df_fm, _estLookupForFormation(fm))
            if fm_drivers:
                geo_driver_buckets.append({"label": fm, "fm": fm, "drivers": fm_drivers})
    else:
        only_fm = non_null_formations[0] if non_null_formations else None
        only_label = only_fm if only_fm else "All Formations"
        geo_driver_buckets.append({"label": only_label, "fm": only_fm, "drivers": all_drivers})

    # Drop empty buckets but always keep at least one so JS has something to render.
    non_empty_buckets = [b for b in geo_driver_buckets if b["drivers"]]
    if non_empty_buckets:
        geo_driver_buckets = non_empty_buckets

    # Monthly forecast buckets — peer-blended P10/P50/P90 oil/gas/water curves,
    # lateral-normalized to AFE, then scaled by the R²-weighted geo-driver multiplier.
    monthly_forecast_buckets = []
    if monthlyForecastData is not None and not monthlyForecastData.empty:
        monthly_df = monthlyForecastData.copy()
        if "API10" in monthly_df.columns:
            monthly_df["API10"] = monthly_df["API10"].astype("string")
            for bucket in geo_driver_buckets:
                fm = bucket.get("fm")
                df_subset = df if fm is None else df[df["Formation"].astype(str).str.upper() == fm]
                monthly_bucket = _buildMonthlyForecastBucket(
                    df_subset, monthly_df, bucket.get("drivers") or [], afeData
                )
                if monthly_bucket:
                    monthly_forecast_buckets.append({
                        "label": bucket["label"],
                        "fm": fm,
                        "curves": monthly_bucket,
                    })

    # Build well index table data
    well_index = numbered_wells

    # Assemble HTML
    html_parts = []
    html_parts.append(f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Subsurface Heat Maps — {dsuName}</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: Arial, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: #f5f5f5; color: #333; }}
  .header {{ background: #0a2a5e; color: white; padding: 18px 28px; }}
  .header h1 {{ font-size: 22px; font-weight: 600; }}
  .header .sub {{ font-size: 13px; opacity: 0.8; margin-top: 4px; }}
  .tabs {{ display: flex; flex-wrap: wrap; gap: 6px; padding: 12px 28px;
           background: #fff; border-bottom: 1px solid #ddd; position: sticky; top: 0; z-index: 10; }}
  .tabs button {{ padding: 6px 14px; border: 1px solid #ccc; background: #fff; border-radius: 4px;
                  cursor: pointer; font-size: 12px; white-space: nowrap; }}
  .tabs button.active {{ background: #0a2a5e; color: #fff; border-color: #0a2a5e; }}
  .tabs button:hover:not(.active) {{ background: #eee; }}
  .page {{ display: none; padding: 16px 28px; }}
  .page.active {{ display: block; }}
  .page-content {{ display: flex; gap: 20px; }}
  .fm-bar {{ margin-bottom: 12px; font-size: 12px; }}
  .fm-bar label {{ font-weight: bold; margin-right: 8px; }}
  .fm-bar select {{ padding: 4px 10px; font-size: 12px; border: 1px solid #ccc; border-radius: 4px; background: #fff; }}
  .plot-wrap {{ flex: 1; min-width: 0; }}
  .plot-box {{ width: 100%; height: 680px; }}
  .sidebar {{ width: 320px; flex-shrink: 0; }}
  .sidebar .panel {{ background: #fff; border: 1px solid #ddd; border-radius: 4px;
                     padding: 12px; margin-bottom: 12px; font-size: 11px; font-family: monospace; }}
  .sidebar .panel h3 {{ font-size: 12px; margin-bottom: 6px; font-family: sans-serif; }}
  .sidebar .panel .row {{ display: flex; gap: 4px; }}
  .sidebar .panel .row .n {{ min-width: 24px; text-align: right; color: #0a2a5e; font-weight: bold; }}
  .table-page {{ display: none; padding: 16px 28px; }}
  .table-page.active {{ display: block; }}
  .driver-grid {{ display: grid; grid-template-columns: repeat(2, minmax(360px, 1fr)); gap: 14px; }}
  .driver-plot {{ height: 300px; background: #fff; border: 1px solid #ddd; }}
  .driver-rank {{ margin-bottom: 14px; max-width: 720px; }}
  .estimate-note {{ background: #fff; border: 1px solid #ddd; padding: 10px 12px; margin-bottom: 14px; max-width: 720px; font-size: 12px; }}
  table {{ border-collapse: collapse; width: 100%; background: #fff; }}
  th {{ background: #f28c28; color: #fff; text-align: left; padding: 6px 10px; font-size: 12px; }}
  td {{ padding: 5px 10px; font-size: 11px; border-bottom: 1px solid #eee; font-family: monospace; }}
  tr:hover td {{ background: #f0f4ff; }}
</style>
</head>
<body>
<div class="header">
  <h1>Subsurface Heat Maps — {dsuName}</h1>
  <div class="sub">Formation: {formation_label} &middot; Vintage: {vintage_label} &middot; Interactive viewer &middot; Generated by Shalehaven</div>
</div>
<div class="tabs" id="tabs"></div>
<div id="pages"></div>
<script>
const FIGURES = {_json.dumps(figures, separators=(',', ':'))};
const WELL_INDEX = {_json.dumps(well_index, separators=(',', ':'))};
const AFE_PERMITS = {_json.dumps(afe_permits, separators=(',', ':'))};
const AFE_WITH_LOC = {_json.dumps(afe_with_loc, separators=(',', ':'))};
const AFE_SUBSURFACE_ESTIMATES = {_json.dumps(afe_subsurface_estimates, separators=(',', ':'))};
const GEO_DRIVER_BUCKETS = {_json.dumps(geo_driver_buckets, separators=(',', ':'))};
const MONTHLY_FORECAST_BUCKETS = {_json.dumps(monthly_forecast_buckets, separators=(',', ':'))};
const FORMATIONS = {_json.dumps([f for f in formations if f is not None], separators=(',', ':'))};

const tabsEl = document.getElementById('tabs');
const pagesEl = document.getElementById('pages');

function buildPlot(fig, divId) {{
  const traces = [];

  // Heatmap contour
  traces.push({{
    type: 'contour',
    z: fig.z,
    x: fig.lon,
    y: fig.lat,
    colorscale: 'Viridis',
    contours: {{ coloring: 'heatmap', showlines: true, showlabels: true, labelfont: {{ size: 9 }} }},
    colorbar: {{ title: fig.param, thickness: 15, len: 0.8 }},
    opacity: 0.85,
    hovertemplate: 'Lon: %{{x:.4f}}<br>Lat: %{{y:.4f}}<br>' + fig.param + ': %{{z:.3f}}<extra></extra>',
  }});

  // Wellbore lateral paths
  fig.wb_traces.forEach(wb => {{
    traces.push({{
      type: 'scattergl',
      mode: 'lines',
      x: wb.lon,
      y: wb.lat,
      line: {{ color: 'black', width: 1.5 }},
      hoverinfo: 'text',
      text: wb.label,
      showlegend: false,
    }});
  }});

  // Well number markers
  if (fig.num_markers.length > 0) {{
    traces.push({{
      type: 'scatter',
      mode: 'markers+text',
      x: fig.num_markers.map(m => m.lon),
      y: fig.num_markers.map(m => m.lat),
      text: fig.num_markers.map(m => String(m.num)),
      textposition: 'top right',
      textfont: {{ size: 9, color: '#0a2a5e', family: 'monospace' }},
      marker: {{ size: 5, color: '#0a2a5e' }},
      hoverinfo: 'text',
      hovertext: fig.num_markers.map(m =>
        '#' + m.num + ' ' + m.name + '<br>Dist: ' + m.dist + ' mi' +
        (m.first_prod ? '<br>1st Prod: ' + m.first_prod : '')),
      showlegend: false,
    }});
  }}

  // AFE permit stars
  if (fig.permit_markers.length > 0) {{
    traces.push({{
      type: 'scatter',
      mode: 'markers',
      x: fig.permit_markers.map(m => m.lon),
      y: fig.permit_markers.map(m => m.lat),
      marker: {{ symbol: 'star', size: 14, color: 'red', line: {{ color: 'black', width: 1 }} }},
      hoverinfo: 'text',
      hovertext: fig.permit_markers.map(m => 'AFE: ' + m.name),
      name: 'AFE Permits',
      showlegend: true,
    }});
  }}

  const layout = {{
    title: {{ text: fig.title, font: {{ size: 15 }} }},
    xaxis: {{ title: 'Longitude', scaleanchor: 'y', scaleratio: 1 }},
    yaxis: {{ title: 'Latitude' }},
    margin: {{ l: 60, r: 20, t: 50, b: 50 }},
    hovermode: 'closest',
    dragmode: 'zoom',
  }};

  Plotly.newPlot(divId, traces, layout, {{ responsive: true }});
}}

function renderGeoDrivers(bucketIdx) {{
  const geoPage = document.getElementById('page-geo');
  if (!geoPage) return;
  const safeIdx = (bucketIdx >= 0 && bucketIdx < GEO_DRIVER_BUCKETS.length) ? bucketIdx : 0;
  const bucket = GEO_DRIVER_BUCKETS[safeIdx];
  const drivers = (bucket && bucket.drivers) || [];

  const fmtEst = v => (v === null || v === undefined || Number.isNaN(v)) ? '' : Math.round(v).toLocaleString();
  const fmtSub = v => (v === null || v === undefined || Number.isNaN(v)) ? '—' : (Math.abs(v) >= 100 ? Math.round(v).toLocaleString() : Number(v).toFixed(3));

  // Per-driver predicted-response lookup tied to the active bucket.
  const PRED_BY_DRIVER = {{}};
  drivers.forEach(row => {{
    PRED_BY_DRIVER[row.param] = {{}};
    (row.afe_points || []).forEach(p => {{
      PRED_BY_DRIVER[row.param][p.letter] = p.y;
    }});
  }});
  const responseUnit = drivers.length > 0 ? drivers[0].response : 'response';

  // Formation dropdown (only when more than one bucket is available).
  let fmBarHTML = '';
  if (GEO_DRIVER_BUCKETS.length > 1) {{
    fmBarHTML = '<div class="fm-bar"><label>Formation:</label><select id="geo-fm-select">' +
      GEO_DRIVER_BUCKETS.map((b, i) => '<option value="' + i + '"' + (i === safeIdx ? ' selected' : '') + '>' + b.label + ' (' + (b.drivers ? b.drivers.length : 0) + ' drivers)</option>').join('') +
      '</select></div>';
  }}

  // AFE subsurface estimate table — per-AFE-LZ estimate; predicted /ft swaps with the active bucket.
  let subsurfaceHTML = '';
  if (AFE_SUBSURFACE_ESTIMATES.length > 0 && AFE_WITH_LOC.length > 0) {{
    subsurfaceHTML = '<h3 style="margin:0 0 4px 0;font-size:14px;">AFE Subsurface Estimates (heat map at each AFE location)</h3>';
    subsurfaceHTML += '<div style="font-size:11px;color:#555;margin-bottom:8px;">Top: property estimate at AFE landing zone. Bottom (gray): predicted ' + responseUnit + ' from this driver\\'s ' + (bucket ? bucket.label : '') + ' regression.</div>';
    subsurfaceHTML += '<table class="driver-rank"><tr><th>Property</th>' +
      AFE_WITH_LOC.map(ap => '<th>' + ap.letter + '. ' + ap.name + (ap.lz ? '<br><span style="font-weight:normal;opacity:0.85;">[' + ap.lz + ']</span>' : '') + '</th>').join('') +
      '</tr>';
    AFE_SUBSURFACE_ESTIMATES.forEach(row => {{
      const pred = PRED_BY_DRIVER[row.param] || null;
      subsurfaceHTML += '<tr><td>' + row.label + '</td>' +
        AFE_WITH_LOC.map(ap => {{
          const propStr = fmtSub(row.by_letter[ap.letter]);
          const predY = pred ? pred[ap.letter] : undefined;
          const predStr = (pred && predY !== undefined && predY !== null && !Number.isNaN(predY))
            ? '<div style="font-size:10px;color:#888;margin-top:2px;">' + fmtSub(predY) + ' /ft</div>'
            : '';
          return '<td>' + propStr + predStr + '</td>';
        }}).join('') +
        '</tr>';
    }});
    subsurfaceHTML += '</table>';
  }}

  let geoContent = fmBarHTML + subsurfaceHTML;

  if (drivers.length > 0) {{
    let rankHTML = '<h3 style="margin:14px 0 8px 0;font-size:14px;">Geo Driver Correlations — ' + (bucket ? bucket.label : '') + '</h3>';
    rankHTML += '<table class="driver-rank"><tr><th>Rank</th><th>Property</th><th>Correlation</th><th>n</th></tr>';
    drivers.forEach(row => {{
      rankHTML += '<tr><td>' + row.rank + '</td><td>' + row.label + '</td><td>' + row.r.toFixed(2) + '</td><td>' + row.n + '</td></tr>';
    }});
    rankHTML += '</table>';
    const estimateTargets = Object.keys(drivers[0].estimates || {{}}).filter(k => drivers[0].estimates[k]);
    const targetLabel = key => drivers[0].estimates[key].label;
    let estimateHTML = '<table class="driver-rank"><tr><th>Rank</th><th>Property</th>' +
      estimateTargets.map(key => '<th>' + targetLabel(key) + '<br>P10/P50/P90</th>').join('') + '</tr>';
    drivers.forEach(row => {{
      estimateHTML += '<tr><td>' + row.rank + '</td><td>' + row.label + '</td>' +
        estimateTargets.map(key => {{
          const est = row.estimates[key];
          return '<td>' + (est ? (fmtEst(est.p10) + ' / ' + fmtEst(est.p50) + ' / ' + fmtEst(est.p90)) : '') + '</td>';
        }}).join('') + '</tr>';
    }});
    estimateHTML += '</table>';
    const afeLength = drivers[0].afe_lateral_length;
    const estimateNote = '<div class="estimate-note">Estimated production from linear fit' +
      (afeLength ? ' normalized to AFE lateral length less 500 ft: ' + Math.round(afeLength).toLocaleString() + ' ft' : '') +
      '</div>';
    geoContent += rankHTML + estimateNote + estimateHTML + '<div class="driver-grid">' +
      drivers.map((row, i) => '<div class="driver-plot" id="driver-plot-' + i + '"></div>').join('') +
      '</div>';
  }} else {{
    geoContent += '<div style="font-size:13px;color:#666;padding:14px;">No geo-driver data for ' + (bucket ? bucket.label : 'this slice') + ' (need ≥4 wells with both driver and response).</div>';
  }}

  geoPage.innerHTML = geoContent;

  drivers.forEach((row, i) => {{
    const traces = [{{
      type: 'scatter',
      mode: 'markers',
      x: row.x,
      y: row.y,
      marker: {{ size: 6, color: '#1f77b4', opacity: 0.65, line: {{ color: 'black', width: 0.4 }} }},
      hovertemplate: row.label + ': %{{x:.3f}}<br>' + row.response + ': %{{y:.3f}}<extra></extra>',
      showlegend: false,
    }}];
    if (row.line_x.length > 0) {{
      traces.push({{
        type: 'scatter',
        mode: 'lines',
        x: row.line_x,
        y: row.line_y,
        line: {{ color: 'firebrick', width: 2 }},
        hoverinfo: 'skip',
        showlegend: false,
      }});
    }}
    const afePts = (row.afe_points || []).filter(p => p.x !== null && p.x !== undefined && p.y !== null && p.y !== undefined);
    if (afePts.length > 0) {{
      traces.push({{
        type: 'scatter',
        mode: 'markers+text',
        x: afePts.map(p => p.x),
        y: afePts.map(p => p.y),
        text: afePts.map(p => p.letter),
        textposition: 'top center',
        textfont: {{ size: 11, color: '#0a2a5e', family: 'sans-serif', weight: 'bold' }},
        marker: {{ symbol: 'star', size: 16, color: 'red', line: {{ color: 'black', width: 1 }} }},
        hovertemplate: '%{{text}} — ' + row.label + ': %{{x:.3f}}<br>Pred ' + row.response + ': %{{y:.3f}}<extra>AFE</extra>',
        showlegend: false,
      }});
    }}
    Plotly.newPlot('driver-plot-' + i, traces, {{
      title: {{ text: '#' + row.rank + ' ' + row.label + '  r=' + row.r.toFixed(2), font: {{ size: 13 }} }},
      xaxis: {{ title: row.label }},
      yaxis: {{ title: row.response }},
      margin: {{ l: 65, r: 16, t: 42, b: 52 }},
      hovermode: 'closest',
    }}, {{ responsive: true }});
  }});

  if (GEO_DRIVER_BUCKETS.length > 1) {{
    const sel = document.getElementById('geo-fm-select');
    if (sel) {{
      sel.addEventListener('change', e => {{
        renderGeoDrivers(parseInt(e.target.value));
      }});
    }}
  }}
}}

function renderMonthlyForecast(bucketIdx) {{
  const page = document.getElementById('page-mforecast');
  if (!page) return;
  const safeIdx = (bucketIdx >= 0 && bucketIdx < MONTHLY_FORECAST_BUCKETS.length) ? bucketIdx : 0;
  const bucket = MONTHLY_FORECAST_BUCKETS[safeIdx];
  if (!bucket || !bucket.curves) {{
    page.innerHTML = '<div style="padding:14px;color:#666;">No monthly forecast data available.</div>';
    return;
  }}
  const c = bucket.curves;
  const fmtBig = v => Math.round(v).toLocaleString();

  let fmBarHTML = '';
  if (MONTHLY_FORECAST_BUCKETS.length > 1) {{
    fmBarHTML = '<div class="fm-bar"><label>Formation:</label><select id="mforecast-fm-select">' +
      MONTHLY_FORECAST_BUCKETS.map((b, i) =>
        '<option value="' + i + '"' + (i === safeIdx ? ' selected' : '') + '>' + b.label + '</option>'
      ).join('') +
      '</select></div>';
  }}

  const noteHTML = '<div class="estimate-note">' +
    'Peer monthly forecasts lateral-normalized to AFE lateral length (' +
      Math.round(c.afe_lateral_length).toLocaleString() + ' ft, less 500 ft) and scaled by R²-weighted geo-driver multiplier (' +
      c.multiplier.toFixed(3) + '×). ' +
      c.peer_count_with_forecast + ' of ' + c.peer_count + ' peer wells in this slice had monthly forecasts.' +
    '</div>';

  const sum = c.summary;
  const summaryHTML = '<table class="driver-rank">' +
    '<tr><th>Phase</th><th>P10 Cum</th><th>P50 Cum</th><th>P90 Cum</th></tr>' +
    '<tr><td>Oil (bbl)</td><td>' + fmtBig(sum.oil_p10_cum) + '</td><td>' + fmtBig(sum.oil_p50_cum) + '</td><td>' + fmtBig(sum.oil_p90_cum) + '</td></tr>' +
    '<tr><td>Gas (Mcf)</td><td>' + fmtBig(sum.gas_p10_cum) + '</td><td>' + fmtBig(sum.gas_p50_cum) + '</td><td>' + fmtBig(sum.gas_p90_cum) + '</td></tr>' +
    '<tr><td>Water (bbl)</td><td>' + fmtBig(sum.water_p10_cum) + '</td><td>' + fmtBig(sum.water_p50_cum) + '</td><td>' + fmtBig(sum.water_p90_cum) + '</td></tr>' +
    '</table>';

  page.innerHTML = fmBarHTML + noteHTML + summaryHTML +
    '<div class="driver-grid" style="grid-template-columns:1fr;">' +
      '<div class="driver-plot" id="mf-plot-oil" style="height:360px;"></div>' +
      '<div class="driver-plot" id="mf-plot-gas" style="height:360px;"></div>' +
      '<div class="driver-plot" id="mf-plot-water" style="height:360px;"></div>' +
    '</div>';

  const phases = [
    {{id: 'mf-plot-oil',   label: 'Oil',   unit: 'bbl/month', color: '#1f7a1f', keys: ['oil_p10','oil_p50','oil_p90']}},
    {{id: 'mf-plot-gas',   label: 'Gas',   unit: 'Mcf/month', color: '#d62728', keys: ['gas_p10','gas_p50','gas_p90']}},
    {{id: 'mf-plot-water', label: 'Water', unit: 'bbl/month', color: '#1f77b4', keys: ['water_p10','water_p50','water_p90']}},
  ];

  phases.forEach(p => {{
    const traces = [
      {{type: 'scatter', mode: 'lines', name: 'P90', x: c.months, y: c[p.keys[2]],
        line: {{color: p.color, width: 1, dash: 'dot'}}, hovertemplate: 'Mo %{{x}}: %{{y:,.0f}}<extra>P90</extra>'}},
      {{type: 'scatter', mode: 'lines', name: 'P50', x: c.months, y: c[p.keys[1]],
        line: {{color: p.color, width: 2.5}}, hovertemplate: 'Mo %{{x}}: %{{y:,.0f}}<extra>P50</extra>'}},
      {{type: 'scatter', mode: 'lines', name: 'P10', x: c.months, y: c[p.keys[0]],
        line: {{color: p.color, width: 1, dash: 'dot'}}, hovertemplate: 'Mo %{{x}}: %{{y:,.0f}}<extra>P10</extra>'}},
    ];
    Plotly.newPlot(p.id, traces, {{
      title: {{text: p.label + ' — P10/P50/P90 (' + p.unit + ')', font: {{size: 13}}}},
      xaxis: {{title: 'Months on Production'}},
      yaxis: {{title: p.unit, rangemode: 'tozero'}},
      margin: {{l: 70, r: 16, t: 42, b: 52}},
      hovermode: 'x unified',
      legend: {{orientation: 'h', y: -0.18}},
    }}, {{responsive: true}});
  }});

  if (MONTHLY_FORECAST_BUCKETS.length > 1) {{
    const sel = document.getElementById('mforecast-fm-select');
    if (sel) {{
      sel.addEventListener('change', e => {{
        renderMonthlyForecast(parseInt(e.target.value));
      }});
    }}
  }}
}}

// Group figures by parameter so each parameter is one tab with a Formation dropdown.
const FIGURES_BY_PARAM = {{}};
const PARAM_ORDER = [];
const PARAM_LABELS = {{}};
FIGURES.forEach(fig => {{
  if (!(fig.param_key in FIGURES_BY_PARAM)) {{
    FIGURES_BY_PARAM[fig.param_key] = [];
    PARAM_ORDER.push(fig.param_key);
    PARAM_LABELS[fig.param_key] = fig.param;
  }}
  FIGURES_BY_PARAM[fig.param_key].push(fig);
}});

// Sort each param's variants by FORMATIONS order so the dropdown is consistent across tabs.
const FORMATION_ORDER_INDEX = {{}};
FORMATIONS.forEach((f, i) => {{ FORMATION_ORDER_INDEX[f] = i; }});
PARAM_ORDER.forEach(pk => {{
  FIGURES_BY_PARAM[pk].sort((a, b) => {{
    const ai = a.fm_label === null ? -1 : (FORMATION_ORDER_INDEX[a.fm_label] ?? 999);
    const bi = b.fm_label === null ? -1 : (FORMATION_ORDER_INDEX[b.fm_label] ?? 999);
    return ai - bi;
  }});
}});

const fmtEstVal = v => (v === null || v === undefined || Number.isNaN(v))
  ? '—'
  : (Math.abs(v) >= 100 ? Math.round(v).toLocaleString() : Number(v).toFixed(3));

function renderHeatmapPage(paramKey, fmLabel) {{
  const figs = FIGURES_BY_PARAM[paramKey] || [];
  let fig = figs.find(f => f.fm_label === fmLabel);
  if (!fig) fig = figs[0];
  if (!fig) return;
  const page = document.getElementById('page-' + paramKey);
  if (!page) return;

  let fmBarHTML = '';
  if (figs.length > 1) {{
    fmBarHTML = '<div class="fm-bar"><label>Formation:</label><select data-param-key="' + paramKey + '">' +
      figs.map(f => {{
        const wellMatch = f.title.match(/(\\d+) wells/);
        const wellCount = wellMatch ? wellMatch[1] : '?';
        const optLabel = (f.fm_label || 'All') + ' (' + wellCount + ' wells)';
        const sel = f.fm_label === fig.fm_label ? ' selected' : '';
        const val = f.fm_label === null ? '__NONE__' : f.fm_label;
        return '<option value="' + val + '"' + sel + '>' + optLabel + '</option>';
      }}).join('') +
      '</select></div>';
  }}

  const fmSuffix = fig.fm_label ? ' [' + fig.fm_label + ']' : '';
  page.innerHTML = fmBarHTML + `
    <div class="page-content">
      <div class="plot-wrap"><div class="plot-box" id="plot-${{paramKey}}"></div></div>
      <div class="sidebar">
        <div class="panel">
          <h3>AFE Permits</h3>
          ${{AFE_PERMITS.map(p => '<div class="row"><span class="n">' + p.letter + '.</span> ' + p.name + ' [' + p.lz + ']</div>').join('')}}
        </div>
        <div class="panel">
          <h3>AFE Estimate &mdash; ${{fig.param}}${{fmSuffix}}</h3>
          ${{(fig.afe_estimates || []).map(e => '<div class="row"><span class="n">' + (e.letter || '') + '.</span> ' + e.name + ': ' + fmtEstVal(e.value) + '</div>').join('')}}
        </div>
        <div class="panel">
          <h3>Offset Wells (by distance)</h3>
          ${{fig.num_markers.map(m => '<div class="row"><span class="n">' + m.num + '.</span> ' + m.name + '</div>').join('')}}
        </div>
      </div>
    </div>`;

  buildPlot(fig, 'plot-' + paramKey);

  if (figs.length > 1) {{
    const sel = page.querySelector('.fm-bar select');
    if (sel) {{
      sel.addEventListener('change', e => {{
        const v = e.target.value;
        renderHeatmapPage(paramKey, v === '__NONE__' ? null : v);
      }});
    }}
  }}
}}

// Build one tab + empty page per unique parameter
PARAM_ORDER.forEach((paramKey, i) => {{
  const btn = document.createElement('button');
  btn.textContent = PARAM_LABELS[paramKey];
  btn.dataset.idx = paramKey;
  if (i === 0) btn.classList.add('active');
  tabsEl.appendChild(btn);

  const page = document.createElement('div');
  page.className = 'page' + (i === 0 ? ' active' : '');
  page.id = 'page-' + paramKey;
  pagesEl.appendChild(page);
}});

// Geo driver diagnostic tab — content is built lazily on activation by renderGeoDrivers.
const hasAnyDrivers = GEO_DRIVER_BUCKETS.some(b => b.drivers && b.drivers.length > 0);
if (hasAnyDrivers || AFE_SUBSURFACE_ESTIMATES.length > 0) {{
  const geoBtn = document.createElement('button');
  geoBtn.textContent = 'Geo Drivers';
  geoBtn.dataset.idx = 'geo';
  tabsEl.appendChild(geoBtn);

  const geoPage = document.createElement('div');
  geoPage.className = 'table-page';
  geoPage.id = 'page-geo';
  pagesEl.appendChild(geoPage);
}}

// Monthly Forecast tab — peer-blended P10/P50/P90 oil/gas/water, scaled by geo drivers.
const hasMonthlyForecast = MONTHLY_FORECAST_BUCKETS.length > 0;
if (hasMonthlyForecast) {{
  const mfBtn = document.createElement('button');
  mfBtn.textContent = 'Monthly Forecast';
  mfBtn.dataset.idx = 'mforecast';
  tabsEl.appendChild(mfBtn);

  const mfPage = document.createElement('div');
  mfPage.className = 'table-page';
  mfPage.id = 'page-mforecast';
  pagesEl.appendChild(mfPage);
}}

// Well index tab
const idxBtn = document.createElement('button');
idxBtn.textContent = 'Well Index';
idxBtn.dataset.idx = 'idx';
tabsEl.appendChild(idxBtn);

const idxPage = document.createElement('div');
idxPage.className = 'table-page';
idxPage.id = 'page-idx';
let tableHTML = '<table><tr><th>#</th><th>Well Name</th><th>API10</th><th>Dist (mi)</th><th>1st Prod</th></tr>';
WELL_INDEX.forEach(w => {{
  tableHTML += '<tr><td>' + w.num + '</td><td>' + w.name + '</td><td>' + w.api10 +
    '</td><td>' + w.dist_mi + '</td><td>' + (w.first_prod || '') + '</td></tr>';
}});
tableHTML += '</table>';
idxPage.innerHTML = tableHTML;
pagesEl.appendChild(idxPage);

// Tab switching
let plotted = new Set();
tabsEl.addEventListener('click', e => {{
  if (e.target.tagName !== 'BUTTON') return;
  tabsEl.querySelectorAll('button').forEach(b => b.classList.remove('active'));
  e.target.classList.add('active');
  document.querySelectorAll('.page, .table-page').forEach(p => p.classList.remove('active'));
  const idx = e.target.dataset.idx;
  const el = document.getElementById('page-' + idx);
  if (el) el.classList.add('active');
  if (idx === 'geo' && !plotted.has(idx)) {{
    renderGeoDrivers(0);
    plotted.add(idx);
  }} else if (idx === 'mforecast' && !plotted.has(idx)) {{
    renderMonthlyForecast(0);
    plotted.add(idx);
  }} else if (idx !== 'idx' && idx !== 'geo' && idx !== 'mforecast' && !plotted.has(idx)) {{
    const figs = FIGURES_BY_PARAM[idx] || [];
    const initialFm = figs.length > 0 ? figs[0].fm_label : null;
    renderHeatmapPage(idx, initialFm);
    plotted.add(idx);
  }}
}});

// Render the first parameter's first formation
if (PARAM_ORDER.length > 0) {{
  const firstParam = PARAM_ORDER[0];
  const firstFigs = FIGURES_BY_PARAM[firstParam];
  renderHeatmapPage(firstParam, firstFigs[0].fm_label);
  plotted.add(firstParam);
}}
</script>
</body>
</html>""")

    html_content = "\n".join(html_parts)
    with open(htmlPath, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"Done. Saved interactive heat maps to {htmlPath}")
    webbrowser.open(htmlPath)
    return monthly_forecast_buckets


## Export header data, monthly forecast data, and monthly production to Excel files
## headerData_{DSU Name}.xlsx       - offset wells with EUR
## forecastData_{DSU Name}.xlsx     - monthly forecast volumes
## monthly_production_{DSU Name}.xlsx - historical monthly production
def printData(forecastData, monthlyForecastData, monthlyProductionData, pathToAfeSummary):
    # Parse DSU name from AFE Summary filename (everything after the "-")
    afeDir = os.path.dirname(pathToAfeSummary)
    afeFilename = os.path.splitext(os.path.basename(pathToAfeSummary))[0]
    dsuName = afeFilename.split("-", 1)[1].strip() if "-" in afeFilename else afeFilename

    # Create "Data" subfolder next to AFE Summary (or use existing one)
    outputDir = os.path.join(afeDir, "Data")
    os.makedirs(outputDir, exist_ok=True)

    # Export header data (offset wells with EUR)
    headerPath = os.path.join(outputDir, f"headerData_{dsuName}.xlsx")
    forecastData.to_excel(headerPath, index=False)
    print(f"Exported {len(forecastData)} offset wells with EUR to {headerPath}")

    # Export monthly forecast data
    forecastPath = os.path.join(outputDir, f"forecastData_{dsuName}.xlsx")
    monthlyForecastData.to_excel(forecastPath, index=False)
    print(f"Exported {len(monthlyForecastData)} monthly forecast rows to {forecastPath}")

    # Export historical monthly production data
    productionPath = os.path.join(outputDir, f"monthly_production_{dsuName}.xlsx")
    monthlyProductionData.to_excel(productionPath, index=False)
    print(f"Exported {len(monthlyProductionData)} monthly production rows to {productionPath}")


## Pull WellDetails + WellSpacing for operator analysis
## Filters by operator name (fuzzy matched) and formation(s) from the AFE Summary.
## Returns a merged DataFrame with completion, production, and spacing columns.
def getOperatorAnalysisData(afeData):

    paths = getNoviBulkPaths()

    # Resolve operator and formations from AFE
    operators = afeData["Operator"].dropna().str.strip().unique().tolist()
    formations = afeData["Landing Zone"].dropna().str.upper().unique().tolist()

    print(f"Loading operator analysis data for: {operators}")
    print(f"  Formations: {formations}")

    # Load WellDetails
    wells = pd.read_csv(
        paths["tsv"]["WellDetails"], sep="\t", low_memory=False, dtype={"API10": "string"}
    )

    # Fuzzy match operator — match if either name contains the other (handles "Diamondback" vs "Diamondback Energy")
    def _op_match(novi_op):
        novi_upper = str(novi_op).upper().strip()
        for afe_op in operators:
            afe_upper = afe_op.upper().strip()
            if afe_upper in novi_upper or novi_upper in afe_upper:
                return True
        return False
    op_mask = wells["CurrentOperator"].fillna("").apply(_op_match)
    # Fuzzy match formation — match if the AFE formation appears anywhere in the Novi formation name
    # (handles "WOODFORD" matching "SUB-WOODFORD")
    fm_pattern = "|".join(re.escape(fm) for fm in formations)
    fm_mask = wells["Formation"].fillna("").str.upper().str.contains(fm_pattern, regex=True)
    hz_mask = wells["IsHorizontalWell"].astype(str).str.lower().isin(["t", "true", "1"])

    filtered = wells[op_mask & fm_mask & hz_mask].copy()
    print(f"  {len(filtered):,} horizontal wells match operator + formation filter")

    if filtered.empty:
        return filtered

    # Compute derived completion metrics
    filtered["ProppantLbsPerFt"] = pd.to_numeric(filtered["FirstCompletionProppantLbsPerFt"], errors="coerce")
    filtered["FluidBblPerFt"] = (
        pd.to_numeric(filtered["FirstCompletionFluidVolume"], errors="coerce") / 42.0
        / pd.to_numeric(filtered["LateralLength"], errors="coerce").replace(0, float("nan"))
    )
    filtered["Cum12MBOEPerFt"] = (
        pd.to_numeric(filtered["Cum12MBOE"], errors="coerce")
        / pd.to_numeric(filtered["LateralLength"], errors="coerce").replace(0, float("nan"))
    )

    # Merge spacing data
    spacing = pd.read_csv(
        paths["tsv"]["WellSpacing"], sep="\t", low_memory=False, dtype={"API10": "string"}
    )
    filtered = filtered.merge(spacing[["API10", "ClosestWellXY", "WellsInRadius", "IsChild",
                                        "BoundednessScore"]], on="API10", how="left",
                               suffixes=("", "_spacing"))
    print(f"  Merged spacing data. {filtered['IsChild'].notna().sum():,} wells have spacing info.")

    # Classify frac type from FracFocus ingredients
    filtered = _mergeFracType(filtered, paths)

    return filtered


## Classify each well's frac type from FracFocusIngredients.
## Scans ffPurpose, ffTradeName, and ffIngredientName for keywords — newer FracFocus filings
## often bundle everything under "Ingredient Container Purpose" with no specific purpose labels,
## so trade names and ingredient names are checked as a fallback.
## Returns: "Gel/Hybrid" if gel/crosslink/guar detected, "Slickwater" if friction reducer only,
## "Unknown" if no FracFocus data. Chunked read since FracFocusIngredients is ~1.6 GB.
def _classifyFracType(api10_set, paths):

    ingredientsPath = paths["tsv"]["FracFocusIngredients"]

    GEL_KEYWORDS = ["gel", "cross", "guar", "viscosif"]
    FR_KEYWORDS = ["friction", "fr-", "fr ", "slickwater", "slick water"]
    # Ingredient/trade name indicators for wells using the "Ingredient Container" format
    GEL_INGREDIENTS = ["guar", "crosslink", "cross link", "xlink", "viscosif", "gelling agent"]
    FR_INGREDIENTS = ["friction reducer", "polyacrylamide", "slickwater",
                      "polyacrylate", "acrylamide polymer", "acrylate copoly"]

    chunk_size = 1_000_000
    well_flags = {}  # api10 -> {"gel": bool, "fr": bool}

    print(f"  Classifying frac type for {len(api10_set):,} wells from FracFocusIngredients...")
    rows_scanned = 0

    for i, chunk in enumerate(
        pd.read_csv(
            ingredientsPath,
            sep="\t",
            chunksize=chunk_size,
            low_memory=False,
            dtype={"API10": "string"},
            usecols=["API10", "ffPurpose", "ffTradeName", "ffIngredientName"],
        ),
        1,
    ):
        rows_scanned += len(chunk)
        hits = chunk[chunk["API10"].isin(api10_set)].copy()
        if hits.empty:
            continue

        for _, row in hits.iterrows():
            api10 = row["API10"]
            if api10 not in well_flags:
                well_flags[api10] = {"gel": False, "fr": False}

            # Check all three text fields for indicators
            texts = [
                str(row.get("ffPurpose", "")).lower(),
                str(row.get("ffTradeName", "")).lower(),
                str(row.get("ffIngredientName", "")).lower(),
            ]
            combined = " ".join(texts)

            if any(kw in combined for kw in GEL_KEYWORDS + GEL_INGREDIENTS):
                well_flags[api10]["gel"] = True
            if any(kw in combined for kw in FR_KEYWORDS + FR_INGREDIENTS):
                well_flags[api10]["fr"] = True

        if i % 5 == 0:
            print(f"    Chunk {i}: scanned {rows_scanned:,} rows, matched {len(well_flags):,} wells")

    # Classify — gel/crosslink chemicals are distinctive and always appear in any disclosure
    # format, so if a well has FracFocus data but no gel indicators, it's slickwater.
    # "Unknown" only when the well has no FracFocus filing at all.
    result = {}
    for api10 in api10_set:
        flags = well_flags.get(api10)
        if flags is None:
            result[api10] = "Unknown"
        elif flags["gel"]:
            result[api10] = "Gel/Hybrid"
        else:
            result[api10] = "Slickwater"

    counts = pd.Series(result.values()).value_counts().to_dict()
    print(f"  Frac type classification: {counts}")

    return result


## Merge frac type classification into a DataFrame with API10 column
def _mergeFracType(df, paths):
    api10_set = set(df["API10"].astype("string").tolist())
    frac_types = _classifyFracType(api10_set, paths)
    df = df.copy()
    df["FracType"] = df["API10"].astype("string").map(frac_types).fillna("Unknown")
    return df


## Pull ALL horizontal wells within 5 miles of the AFE location for peer comparison.
## Resolves AFE location from T/R/S (via BLM PLSS) or local permits, builds a 5-mile
## bounding box, and returns WellDetails + WellSpacing for every operator in the area.
def getPeerAnalysisData(afeData):

    paths = getNoviBulkPaths()

    formations = afeData["Landing Zone"].dropna().str.upper().unique().tolist()

    # Resolve AFE well locations from T/R/S
    lats, lons = [], []
    for _, row in afeData.iterrows():
        twp = str(row.get("Township", "")).strip()
        rng = str(row.get("Range", "")).strip()
        sec = str(row.get("Section", "")).strip()
        state_abbr = STATE_ABBR_MAP.get(str(row.get("State", "")).strip().lower())
        if twp and rng and sec and state_abbr:
            centroid = _fetchSectionCentroid(state_abbr, twp, rng, sec)
            if centroid:
                lats.append(centroid[0])
                lons.append(centroid[1])

    # Fall back to local permits if T/R/S didn't produce any locations
    if not lats:
        localPermits = pd.read_csv(
            paths["tsv"]["WellPermits"], sep="\t", low_memory=False,
            dtype={"API10": "string", "ID": "string"},
        )
        for _, row in afeData.iterrows():
            api_number = str(row["API Number"])
            county = row["County"]
            state = row["State"]
            is_texas = str(state).strip().lower() == "texas"
            if is_texas:
                match = localPermits[(localPermits["API10"] == api_number) & (localPermits["County"] == county)]
            else:
                match = localPermits[(localPermits["ID"] == api_number) & (localPermits["County"] == county)]
            match = match.dropna(subset=["Latitude", "Longitude"])
            if not match.empty:
                lats.append(match["Latitude"].iloc[0])
                lons.append(match["Longitude"].iloc[0])

    if not lats:
        print("  Could not resolve any AFE locations for peer analysis.")
        return pd.DataFrame()

    # Build 5-mile bounding box around centroid of all AFE wells
    center_lat = sum(lats) / len(lats)
    center_lon = sum(lons) / len(lons)
    miles = 5.0
    lat_offset = miles / 69.0
    lon_offset = miles / (69.0 * abs(math.cos(math.radians(center_lat))))

    min_lat = center_lat - lat_offset
    max_lat = center_lat + lat_offset
    min_lon = center_lon - lon_offset
    max_lon = center_lon + lon_offset

    print(f"Loading peer analysis data within {miles} mi of ({center_lat:.4f}, {center_lon:.4f})")
    print(f"  Formations: {formations}")
    print(f"  Bounding box: Lat [{min_lat:.4f}, {max_lat:.4f}], Lon [{min_lon:.4f}, {max_lon:.4f}]")

    wells = pd.read_csv(
        paths["tsv"]["WellDetails"], sep="\t", low_memory=False, dtype={"API10": "string"}
    )

    hz_mask = wells["IsHorizontalWell"].astype(str).str.lower().isin(["t", "true", "1"])
    fm_pattern = "|".join(re.escape(fm) for fm in formations)
    fm_mask = wells["Formation"].fillna("").str.upper().str.contains(fm_pattern, regex=True)
    geo_mask = (
        (wells["MPLatitude"] >= min_lat) & (wells["MPLatitude"] <= max_lat)
        & (wells["MPLongitude"] >= min_lon) & (wells["MPLongitude"] <= max_lon)
    )

    filtered = wells[hz_mask & fm_mask & geo_mask].copy()
    print(f"  {len(filtered):,} horizontal wells in area across {filtered['CurrentOperator'].nunique()} operators")

    if filtered.empty:
        return filtered

    # Derived metrics (same as getOperatorAnalysisData)
    filtered["ProppantLbsPerFt"] = pd.to_numeric(filtered["FirstCompletionProppantLbsPerFt"], errors="coerce")
    filtered["FluidBblPerFt"] = (
        pd.to_numeric(filtered["FirstCompletionFluidVolume"], errors="coerce") / 42.0
        / pd.to_numeric(filtered["LateralLength"], errors="coerce").replace(0, float("nan"))
    )
    filtered["Cum12MBOEPerFt"] = (
        pd.to_numeric(filtered["Cum12MBOE"], errors="coerce")
        / pd.to_numeric(filtered["LateralLength"], errors="coerce").replace(0, float("nan"))
    )
    filtered["EUR50YRBOEPerFt"] = (
        pd.to_numeric(filtered["EUR50YRBOE"], errors="coerce")
        / pd.to_numeric(filtered["LateralLength"], errors="coerce").replace(0, float("nan"))
    )

    # Merge spacing
    spacing = pd.read_csv(
        paths["tsv"]["WellSpacing"], sep="\t", low_memory=False, dtype={"API10": "string"}
    )
    filtered = filtered.merge(spacing[["API10", "ClosestWellXY", "WellsInRadius", "IsChild",
                                        "BoundednessScore"]], on="API10", how="left",
                               suffixes=("", "_spacing"))

    # Classify frac type
    filtered = _mergeFracType(filtered, paths)

    # Report operator breakdown
    op_counts = filtered["CurrentOperator"].value_counts()
    print(f"  Operator breakdown:")
    for op, count in op_counts.items():
        print(f"    {op}: {count} wells")

    return filtered


## Generate an interactive HTML operator analysis: completion trends, production performance,
## spacing impact, frac type, and peer comparison pages. Uses Plotly.js for zoom/hover charts.
## Output: operator_analysis_{DSU Name}.html in the AFE Data/ folder.
def plotOperatorAnalysisHTML(analysisData, pathToAfeSummary, peerData=None):
    import json as _json
    import webbrowser
    import numpy as np

    if analysisData is None or analysisData.empty:
        print("No operator analysis data — skipping HTML export.")
        return

    # Resolve output path
    afeDir = os.path.dirname(pathToAfeSummary)
    afeFilename = os.path.splitext(os.path.basename(pathToAfeSummary))[0]
    dsuName = afeFilename.split("-", 1)[1].strip() if "-" in afeFilename else afeFilename
    outputDir = os.path.join(afeDir, "Data")
    os.makedirs(outputDir, exist_ok=True)
    htmlPath = os.path.join(outputDir, f"operator_analysis_{dsuName}.html")

    operatorMode = analysisData["CurrentOperator"].dropna().mode()
    operator = operatorMode.iloc[0] if not operatorMode.empty else "Unknown"
    formations = sorted(analysisData["Formation"].dropna().str.upper().unique().tolist())
    fm_label = " / ".join(formations) if formations else "ALL"

    df = analysisData.copy()
    df["Vintage"] = pd.to_numeric(df["FirstProductionYear"], errors="coerce")
    df = df[df["Vintage"].notna() & (df["Vintage"] >= 2014)].copy()
    df["Vintage"] = df["Vintage"].astype(int)

    print(f"Generating interactive operator analysis HTML: {htmlPath}")
    print(f"  Operator: {operator}")
    print(f"  Formations: {fm_label}")
    print(f"  Vintage range: {df['Vintage'].min()}-{df['Vintage'].max()}, {len(df):,} wells")

    # ---- Helper: build bar chart data grouped by vintage ----
    def _vintage_bar(dataframe, col, color):
        col_numeric = pd.to_numeric(dataframe[col], errors="coerce")
        grouped = dataframe.assign(_val=col_numeric).groupby("Vintage")["_val"]
        means = grouped.mean()
        counts = grouped.count()
        valid = means[counts >= 3].dropna()
        if valid.empty:
            return None
        annotations = [f"n={int(counts[v])}" for v in valid.index]
        return {
            "x": valid.index.tolist(),
            "y": [round(v, 4) for v in valid.values],
            "annotations": annotations,
            "color": color,
        }

    # ---- Helper: build horizontal bar chart data grouped by operator ----
    def _peer_hbar(dataframe, col, afe_operator_upper):
        col_numeric = pd.to_numeric(dataframe[col], errors="coerce")
        grouped = dataframe.assign(_val=col_numeric).groupby("CurrentOperator")["_val"]
        means = grouped.mean().dropna().sort_values(ascending=True)
        if means.empty:
            return None
        counts = grouped.count()
        labels = []
        colors = []
        anns = []
        for op in means.index:
            short = op if len(op) <= 20 else op[:19] + "\u2026"
            labels.append(short)
            colors.append("steelblue" if op.upper() == afe_operator_upper else "lightgray")
            anns.append(f"n={int(counts[op])}")
        return {
            "y": labels,
            "x": [round(v, 4) for v in means.values],
            "colors": colors,
            "annotations": anns,
        }

    # ================================================================
    # TAB 1: Completion Design Trends
    # ================================================================
    tab1_charts = []

    for col, title, color in [
        ("LateralLength", "Avg Lateral Length (ft)", "steelblue"),
        ("ProppantLbsPerFt", "Avg Proppant (lbs/ft)", "firebrick"),
        ("FluidBblPerFt", "Avg Fluid (bbl/ft)", "teal"),
    ]:
        bd = _vintage_bar(df, col, color)
        tab1_charts.append({"type": "bar", "title": title, "data": bd,
                            "xlabel": "First Production Year", "ylabel": title})

    # Scatter: Proppant/Fluid ratio vs EUR 50yr BOE
    prop = pd.to_numeric(df["FirstCompletionProppantMass"], errors="coerce")
    fluid = pd.to_numeric(df["FirstCompletionFluidVolume"], errors="coerce") / 42.0
    eur50 = pd.to_numeric(df["EUR50YRBOE"], errors="coerce")
    prop_fluid = prop / fluid.replace(0, float("nan"))
    scatter_mask = prop_fluid.notna() & eur50.notna()
    scatter_data = None
    if scatter_mask.sum() >= 3:
        scatter_data = {
            "x": [round(v, 4) for v in prop_fluid[scatter_mask].values],
            "y": [round(v, 4) for v in eur50[scatter_mask].values],
            "color": df.loc[scatter_mask, "Vintage"].tolist(),
        }
    tab1_charts.append({"type": "scatter", "title": "Proppant/Fluid Ratio vs EUR 50yr BOE",
                         "data": scatter_data,
                         "xlabel": "Proppant / Fluid (lbs/bbl)", "ylabel": "EUR 50yr BOE"})

    # ================================================================
    # TAB 2: Production Performance
    # ================================================================
    tab2_charts = []

    # Cum12M BOE/ft (only wells with 12+ months)
    months_on = pd.to_numeric(df["LastReportedMonthsOnProduction"], errors="coerce")
    df_12m = df[months_on >= 12]
    bd = _vintage_bar(df_12m, "Cum12MBOEPerFt", "steelblue")
    tab2_charts.append({"type": "bar", "title": "Cum 12M BOE / Lateral Foot", "data": bd,
                         "xlabel": "First Production Year", "ylabel": "BOE/ft"})

    # EUR50yr vs Lateral Length scatter — Permian wells use Oil EUR (gas is uneconomic)
    is_permian = _isPermianOilBasin(formations)
    eur_col = "EUR50YROil" if is_permian else "EUR50YRBOE"
    eur_label = "EUR 50yr Oil (BO)" if is_permian else "EUR 50yr BOE"
    eur = pd.to_numeric(df[eur_col], errors="coerce")
    ll = pd.to_numeric(df["LateralLength"], errors="coerce")
    scatter_mask = eur.notna() & ll.notna()
    scatter_data = None
    if scatter_mask.sum() >= 3:
        x_fit = ll[scatter_mask].values
        y_fit = eur[scatter_mask].values
        slope, intercept, _r2, fit_label = _linearFitWithR2(x_fit, y_fit)
        x_min, x_max = float(x_fit.min()), float(x_fit.max())
        scatter_data = {
            "x": [round(v, 4) for v in x_fit],
            "y": [round(v, 4) for v in y_fit],
            "color": df.loc[scatter_mask, "Vintage"].tolist(),
            "fit": {
                "x": [x_min, x_max],
                "y": [round(slope * x_min + intercept, 4), round(slope * x_max + intercept, 4)],
                "label": fit_label,
            },
        }
    tab2_charts.append({"type": "scatter", "title": f"{eur_label} vs Lateral Length",
                         "data": scatter_data,
                         "xlabel": "Lateral Length (ft)", "ylabel": eur_label})

    # Peak Month BOE Rate by vintage
    bd = _vintage_bar(df, "PeakMonthBOERate", "darkorange")
    tab2_charts.append({"type": "bar", "title": "Avg Peak Month BOE Rate", "data": bd,
                         "xlabel": "First Production Year", "ylabel": "BOE/month"})

    # Cum Life GOR by vintage
    bd = _vintage_bar(df, "CumLifeGOR", "gray")
    tab2_charts.append({"type": "bar", "title": "Avg Cum Life GOR", "data": bd,
                         "xlabel": "First Production Year", "ylabel": "GOR (scf/bbl)"})

    # ================================================================
    # TAB 3: Spacing Impact
    # ================================================================
    tab3_charts = []
    cum12m = pd.to_numeric(df["Cum12MBOE"], errors="coerce")

    # Child vs Parent box plot
    is_child = df["IsChild"]
    child_vals = cum12m[is_child == True].dropna()
    parent_vals = cum12m[is_child == False].dropna()
    box_data = None
    if len(child_vals) >= 3 and len(parent_vals) >= 3:
        box_data = {
            "labels": [f"Parent (n={len(parent_vals)})", f"Child (n={len(child_vals)})"],
            "values": [parent_vals.tolist(), child_vals.tolist()],
            "colors": ["steelblue", "firebrick"],
        }
    tab3_charts.append({"type": "box", "title": "Child vs Parent — Cum 12M BOE",
                         "data": box_data, "ylabel": "Cum 12M BOE"})

    # Boundedness Score scatter
    bound = pd.to_numeric(df["BoundednessScore"], errors="coerce")
    sm = bound.notna() & cum12m.notna()
    sd = None
    if sm.sum() >= 3:
        sd = {"x": bound[sm].tolist(), "y": cum12m[sm].tolist(), "color_single": "teal"}
    tab3_charts.append({"type": "scatter_single", "title": "Boundedness Score vs Cum 12M BOE",
                         "data": sd, "xlabel": "Boundedness Score", "ylabel": "Cum 12M BOE"})

    # Closest Well Distance scatter
    closest = pd.to_numeric(df["ClosestWellXY"], errors="coerce")
    sm = closest.notna() & cum12m.notna()
    sd = None
    if sm.sum() >= 3:
        sd = {"x": closest[sm].tolist(), "y": cum12m[sm].tolist(), "color_single": "darkorange"}
    tab3_charts.append({"type": "scatter_single", "title": "Closest Well Distance vs Cum 12M BOE",
                         "data": sd, "xlabel": "Closest Well (ft)", "ylabel": "Cum 12M BOE"})

    # Wells in Radius scatter
    wir = pd.to_numeric(df["WellsInRadius"], errors="coerce")
    sm = wir.notna() & cum12m.notna()
    sd = None
    if sm.sum() >= 3:
        sd = {"x": wir[sm].tolist(), "y": cum12m[sm].tolist(), "color_single": "purple"}
    tab3_charts.append({"type": "scatter_single", "title": "Wells in Radius vs Cum 12M BOE",
                         "data": sd, "xlabel": "Wells in Radius", "ylabel": "Cum 12M BOE"})

    # ================================================================
    # TAB 4: Frac Type Analysis
    # ================================================================
    tab4_charts = []
    has_frac = "FracType" in df.columns
    if has_frac:
        frac_colors_map = {"Slickwater": "steelblue", "Gel/Hybrid": "firebrick", "Unknown": "lightgray"}
        # Stacked bar: frac type by vintage
        frac_by_year = df.groupby(["Vintage", "FracType"]).size().unstack(fill_value=0)
        frac_cols = [c for c in ["Slickwater", "Gel/Hybrid", "Unknown"] if c in frac_by_year.columns]
        stacked_data = None
        if frac_cols:
            stacked_data = {
                "vintages": frac_by_year.index.tolist(),
                "series": [],
            }
            for fc in frac_cols:
                stacked_data["series"].append({
                    "name": fc,
                    "values": frac_by_year[fc].tolist(),
                    "color": frac_colors_map.get(fc, "gray"),
                })
        tab4_charts.append({"type": "stacked_bar", "title": "Frac Type by Vintage Year",
                             "data": stacked_data,
                             "xlabel": "First Production Year", "ylabel": "Well Count"})

        # Box plot: Cum12M BOE by frac type
        months_on_ft = pd.to_numeric(df["LastReportedMonthsOnProduction"], errors="coerce")
        df_ft = df[months_on_ft >= 12]
        frac_types_present = [ft for ft in ["Slickwater", "Gel/Hybrid"] if ft in df_ft["FracType"].values]
        frac_box = None
        if len(frac_types_present) >= 1:
            frac_box = {"labels": [], "values": [], "colors": []}
            for ft in frac_types_present:
                vals = pd.to_numeric(df_ft[df_ft["FracType"] == ft]["Cum12MBOE"], errors="coerce").dropna()
                frac_box["labels"].append(f"{ft} (n={len(vals)})")
                frac_box["values"].append(vals.tolist())
                frac_box["colors"].append(frac_colors_map.get(ft, "gray"))
        tab4_charts.append({"type": "box", "title": "Cum 12M BOE by Frac Type",
                             "data": frac_box, "ylabel": "Cum 12M BOE"})

    # ================================================================
    # TAB 5 & 6: Peer Comparison
    # ================================================================
    tab5_charts = []
    tab6_charts = []
    has_peer = False
    if peerData is not None and not peerData.empty:
        peer = peerData.copy()
        peer["Vintage"] = pd.to_numeric(peer["FirstProductionYear"], errors="coerce")
        peer = peer[peer["Vintage"].notna() & (peer["Vintage"] >= 2020)].copy()
        peer["Vintage"] = peer["Vintage"].astype(int)
        op_counts = peer["CurrentOperator"].value_counts()
        valid_ops = op_counts[op_counts >= 20].index.tolist()
        peer = peer[peer["CurrentOperator"].isin(valid_ops)].copy()

        if len(valid_ops) >= 2:
            has_peer = True
            afe_operator_upper = operator.upper()

            # Tab 5: EUR & Production
            hb = _peer_hbar(peer, "EUR50YRBOEPerFt", afe_operator_upper)
            tab5_charts.append({"type": "hbar", "title": "EUR 50yr BOE / Lateral Foot",
                                 "data": hb, "xlabel": "BOE/ft"})

            months_on_peer = pd.to_numeric(peer["LastReportedMonthsOnProduction"], errors="coerce")
            peer_12m = peer[months_on_peer >= 12]
            hb = _peer_hbar(peer_12m, "Cum12MBOEPerFt", afe_operator_upper)
            tab5_charts.append({"type": "hbar", "title": "Cum 12M BOE / Lateral Foot",
                                 "data": hb, "xlabel": "BOE/ft"})

            hb = _peer_hbar(peer, "PeakMonthBOERate", afe_operator_upper)
            tab5_charts.append({"type": "hbar", "title": "Avg Peak Month BOE Rate",
                                 "data": hb, "xlabel": "BOE/month"})

            hb = _peer_hbar(peer, "CumLifeGOR", afe_operator_upper)
            tab5_charts.append({"type": "hbar", "title": "Avg Cum Life GOR",
                                 "data": hb, "xlabel": "GOR (scf/bbl)"})

            # Tab 6: Peer Completion Design
            hb = _peer_hbar(peer, "LateralLength", afe_operator_upper)
            tab6_charts.append({"type": "hbar", "title": "Avg Lateral Length (ft)",
                                 "data": hb, "xlabel": "Lateral Length (ft)"})

            hb = _peer_hbar(peer, "ProppantLbsPerFt", afe_operator_upper)
            tab6_charts.append({"type": "hbar", "title": "Avg Proppant (lbs/ft)",
                                 "data": hb, "xlabel": "lbs/ft"})

            hb = _peer_hbar(peer, "FluidBblPerFt", afe_operator_upper)
            tab6_charts.append({"type": "hbar", "title": "Avg Fluid (bbl/ft)",
                                 "data": hb, "xlabel": "bbl/ft"})

            # Proppant/Fluid ratio by operator
            prop_raw = pd.to_numeric(peer["FirstCompletionProppantMass"], errors="coerce")
            fluid_raw = pd.to_numeric(peer["FirstCompletionFluidVolume"], errors="coerce") / 42.0
            peer_pf = prop_raw / fluid_raw.replace(0, float("nan"))
            peer_tmp = peer.assign(PropFluidRatio=peer_pf)
            hb = _peer_hbar(peer_tmp.rename(columns={"PropFluidRatio": "_PFR"}), "_PFR", afe_operator_upper) if peer_pf.notna().sum() > 0 else None
            # Manual build since column name is derived
            if peer_pf.notna().any():
                grouped = peer.assign(_val=peer_pf).groupby("CurrentOperator")["_val"]
                means = grouped.mean().dropna().sort_values(ascending=True)
                counts = grouped.count()
                if not means.empty:
                    labels = []
                    colors = []
                    anns = []
                    for op in means.index:
                        short = op if len(op) <= 20 else op[:19] + "\u2026"
                        labels.append(short)
                        colors.append("steelblue" if op.upper() == afe_operator_upper else "lightgray")
                        anns.append(f"n={int(counts[op])}")
                    hb = {"y": labels, "x": [round(v, 4) for v in means.values],
                          "colors": colors, "annotations": anns}
                else:
                    hb = None
            else:
                hb = None
            tab6_charts.append({"type": "hbar", "title": "Avg Proppant/Fluid Ratio (lbs/bbl)",
                                 "data": hb, "xlabel": "lbs/bbl"})

    # ================================================================
    # Build the tabs list
    # ================================================================
    tabs = [
        {"name": "Completion Trends", "charts": tab1_charts},
        {"name": "Production", "charts": tab2_charts},
        {"name": "Spacing Impact", "charts": tab3_charts},
    ]
    if has_frac and tab4_charts:
        tabs.append({"name": "Frac Type", "charts": tab4_charts})
    if has_peer and tab5_charts:
        tabs.append({"name": "Peer EUR & Production", "charts": tab5_charts})
    if has_peer and tab6_charts:
        tabs.append({"name": "Peer Completion Design", "charts": tab6_charts})

    tabs_json = _json.dumps(tabs, separators=(",", ":"))

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Operator Analysis — {dsuName}</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: #f5f5f5; color: #333; }}
  .header {{ background: #0a2a5e; color: white; padding: 18px 28px; }}
  .header h1 {{ font-size: 22px; font-weight: 600; }}
  .header .sub {{ font-size: 13px; opacity: 0.8; margin-top: 4px; }}
  .tabs {{ display: flex; flex-wrap: wrap; gap: 6px; padding: 12px 28px;
           background: #fff; border-bottom: 1px solid #ddd; position: sticky; top: 0; z-index: 10; }}
  .tabs button {{ padding: 6px 14px; border: 1px solid #ccc; background: #fff; border-radius: 4px;
                  cursor: pointer; font-size: 12px; white-space: nowrap; }}
  .tabs button.active {{ background: #0a2a5e; color: #fff; border-color: #0a2a5e; }}
  .tabs button:hover:not(.active) {{ background: #eee; }}
  .page {{ display: none; padding: 16px 28px; }}
  .page.active {{ display: block; }}
  .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  .plot-cell {{ height: 400px; background: #fff; border: 1px solid #ddd; border-radius: 4px; }}
</style>
</head>
<body>
<div class="header">
  <h1>Operator Analysis &mdash; {dsuName}</h1>
  <div class="sub">{operator} &middot; {fm_label} &middot; Interactive viewer &middot; Generated by Shalehaven</div>
</div>
<div class="tabs" id="tabs"></div>
<div id="pages"></div>
<script>
const TABS = {tabs_json};
const tabsEl = document.getElementById('tabs');
const pagesEl = document.getElementById('pages');
const plotted = new Set();

// Build tab buttons and page containers
TABS.forEach((tab, i) => {{
  const btn = document.createElement('button');
  btn.textContent = tab.name;
  btn.dataset.idx = i;
  if (i === 0) btn.classList.add('active');
  tabsEl.appendChild(btn);

  const page = document.createElement('div');
  page.className = 'page' + (i === 0 ? ' active' : '');
  page.id = 'page-' + i;
  let gridHTML = '<div class="grid">';
  tab.charts.forEach((c, ci) => {{
    gridHTML += '<div class="plot-cell" id="plot-' + i + '-' + ci + '"></div>';
  }});
  gridHTML += '</div>';
  page.innerHTML = gridHTML;
  pagesEl.appendChild(page);
}});

// Tab switching with lazy render
tabsEl.addEventListener('click', e => {{
  if (e.target.tagName !== 'BUTTON') return;
  tabsEl.querySelectorAll('button').forEach(b => b.classList.remove('active'));
  e.target.classList.add('active');
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  const idx = e.target.dataset.idx;
  document.getElementById('page-' + idx).classList.add('active');
  if (!plotted.has(idx)) {{
    renderTab(parseInt(idx));
    plotted.add(idx);
  }}
}});

function renderTab(tabIdx) {{
  const tab = TABS[tabIdx];
  tab.charts.forEach((chart, ci) => {{
    const divId = 'plot-' + tabIdx + '-' + ci;
    if (!chart.data) {{
      document.getElementById(divId).innerHTML =
        '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:#999;">Insufficient data</div>';
      return;
    }}
    const traces = [];
    const layout = {{
      title: {{ text: chart.title, font: {{ size: 14 }} }},
      margin: {{ l: 60, r: 20, t: 45, b: 50 }},
      hovermode: 'closest',
    }};

    if (chart.type === 'bar') {{
      traces.push({{
        type: 'bar',
        x: chart.data.x,
        y: chart.data.y,
        marker: {{ color: chart.data.color, opacity: 0.8,
                   line: {{ color: 'black', width: 0.3 }} }},
        text: chart.data.annotations,
        textposition: 'outside',
        textfont: {{ size: 9, color: '#444' }},
        hoverinfo: 'x+y',
      }});
      layout.xaxis = {{ title: chart.xlabel }};
      layout.yaxis = {{ title: chart.ylabel }};
      layout.showlegend = false;
    }}

    else if (chart.type === 'scatter') {{
      traces.push({{
        type: 'scatter',
        mode: 'markers',
        x: chart.data.x,
        y: chart.data.y,
        marker: {{
          size: 6,
          color: chart.data.color,
          colorscale: 'Plasma',
          showscale: true,
          colorbar: {{ title: 'Vintage', thickness: 12 }},
          opacity: 0.7,
          line: {{ color: 'black', width: 0.2 }},
        }},
        hoverinfo: 'x+y',
        showlegend: false,
      }});
      if (chart.data.fit) {{
        traces.push({{
          type: 'scatter',
          mode: 'lines',
          x: chart.data.fit.x,
          y: chart.data.fit.y,
          line: {{ color: 'black', width: 1.6, dash: 'dash' }},
          name: chart.data.fit.label,
          hoverinfo: 'name',
        }});
        layout.showlegend = true;
        layout.legend = {{ x: 0.02, y: 0.98, bgcolor: 'rgba(255,255,255,0.85)' }};
      }} else {{
        layout.showlegend = false;
      }}
      layout.xaxis = {{ title: chart.xlabel }};
      layout.yaxis = {{ title: chart.ylabel }};
    }}

    else if (chart.type === 'scatter_single') {{
      traces.push({{
        type: 'scatter',
        mode: 'markers',
        x: chart.data.x,
        y: chart.data.y,
        marker: {{
          size: 6,
          color: chart.data.color_single,
          opacity: 0.6,
          line: {{ color: 'black', width: 0.2 }},
        }},
        hoverinfo: 'x+y',
      }});
      layout.xaxis = {{ title: chart.xlabel }};
      layout.yaxis = {{ title: chart.ylabel }};
      layout.showlegend = false;
    }}

    else if (chart.type === 'box') {{
      chart.data.values.forEach((vals, bi) => {{
        traces.push({{
          type: 'box',
          y: vals,
          name: chart.data.labels[bi],
          marker: {{ color: chart.data.colors[bi] }},
          boxpoints: 'outliers',
        }});
      }});
      layout.yaxis = {{ title: chart.ylabel || '' }};
      layout.showlegend = false;
    }}

    else if (chart.type === 'stacked_bar') {{
      chart.data.series.forEach(s => {{
        traces.push({{
          type: 'bar',
          x: chart.data.vintages,
          y: s.values,
          name: s.name,
          marker: {{ color: s.color, line: {{ color: 'black', width: 0.3 }} }},
        }});
      }});
      layout.barmode = 'stack';
      layout.xaxis = {{ title: chart.xlabel }};
      layout.yaxis = {{ title: chart.ylabel }};
    }}

    else if (chart.type === 'hbar') {{
      traces.push({{
        type: 'bar',
        orientation: 'h',
        y: chart.data.y,
        x: chart.data.x,
        marker: {{ color: chart.data.colors,
                   line: {{ color: 'black', width: 0.3 }} }},
        text: chart.data.annotations,
        textposition: 'outside',
        textfont: {{ size: 9, color: '#444' }},
        hoverinfo: 'x+y',
      }});
      layout.xaxis = {{ title: chart.xlabel || '' }};
      layout.yaxis = {{ automargin: true }};
      layout.margin.l = 140;
      layout.showlegend = false;
    }}

    Plotly.newPlot(divId, traces, layout, {{ responsive: true }});
  }});
}}

// Render first tab
renderTab(0);
plotted.add('0');
</script>
</body>
</html>"""

    with open(htmlPath, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Done. Saved interactive operator analysis to {htmlPath}")
    webbrowser.open(htmlPath)
