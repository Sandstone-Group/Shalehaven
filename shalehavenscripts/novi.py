## Novi API Functions - Authentication, Permit Lookup, Offset Well Search, EUR Forecasting, and Data Export
## Developed by Michael Tanner

import requests
import os
import math
import difflib
import re
import pandas as pd


BASE_URL = "https://insight.novilabs.com/api/"

STATE_ABBR_MAP = {
    "new mexico": "NM", "colorado": "CO", "wyoming": "WY", "north dakota": "ND",
    "montana": "MT", "ohio": "OH", "pennsylvania": "PA", "texas": "TX",
    "south dakota": "SD", "oklahoma": "OK", "kansas": "KS", "louisiana": "LA",
    "california": "CA", "utah": "UT", "alaska": "AK",
}
# Also accept abbreviations directly (AFE files sometimes use "OH" instead of "Ohio")
STATE_ABBR_MAP.update({v.lower(): v for v in STATE_ABBR_MAP.values()})


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


## Retrieve offset wells within N miles of permit locations, filtered by formation
## Reads from local Novi bulk export (D:\novi) — no API call. Run runNoviBulk() first.
## token and scope are kept for backward compat with main_model.py but unused.
## Formation name is pulled from AFE Summary "Landing Zone" column and uppercased to match Novi format
def getWells(token, permitData, afeData, scope="us-horizontals"):

    # Calculate bounding box around the permit locations
    # 1 degree latitude ~ 69 miles, 1 degree longitude ~ 69 * cos(lat) miles
    miles = float(input("Enter search radius in miles: ").strip())
    lat_offset = miles / 69.0
    avg_lat = permitData["Latitude"].mean()
    lon_offset = miles / (69.0 * abs(math.cos(math.radians(avg_lat))))

    # Expand permit min/max by offset to create the search boundary
    min_lat = permitData["Latitude"].min() - lat_offset
    max_lat = permitData["Latitude"].max() + lat_offset
    min_lon = permitData["Longitude"].min() - lon_offset
    max_lon = permitData["Longitude"].max() + lon_offset

    # Pull all unique formations from AFE Summary and uppercase to match Novi format
    formations = afeData["Landing Zone"].dropna().str.upper().unique().tolist()

    # Point Pleasant and Utica are stacked in the Appalachian basin —
    # when evaluating either, pull both so they share one offset set
    if "POINT PLEASANT" in formations and "UTICA" not in formations:
        formations.append("UTICA")
    elif "UTICA" in formations and "POINT PLEASANT" not in formations:
        formations.append("POINT PLEASANT")

    print(f"Searching for wells within {miles} miles of permit locations, Formations: {formations}")
    print(f"Bounding box: Lat [{min_lat:.4f}, {max_lat:.4f}], Lon [{min_lon:.4f}, {max_lon:.4f}]")

    # Load WellDetails from local bulk export
    paths = getNoviBulkPaths()
    wellDetailsPath = paths["tsv"]["WellDetails"]

    # API10 is varchar(32) per Novi schema — pin as string so downstream filters/joins
    # against ForecastWellMonths/Years (also pinned to string) match correctly
    wells = pd.read_csv(wellDetailsPath, sep="\t", low_memory=False, dtype={"API10": "string"})
    print(f"  Loaded {len(wells):,} total wells from bulk export")

    # Apply bounding box + formation + vintage filter
    # FirstProductionYear >= 2018 keeps modern completions; null years (never produced) are
    # excluded automatically since NaN >= 2018 evaluates to False
    mask = (
        (wells["MPLatitude"] >= min_lat)
        & (wells["MPLatitude"] <= max_lat)
        & (wells["MPLongitude"] >= min_lon)
        & (wells["MPLongitude"] <= max_lon)
        & (wells["Formation"].isin(formations))
        & (wells["FirstProductionYear"] >= 2018)
    )
    filtered = wells[mask].copy()

    print(f"Done. Retrieved {len(filtered):,} wells matching criteria.")
    return filtered


## Retrieve well permits from Novi for each AFE row
## Looks up permits from the local bulk export (D:\novi). Texas wells matched by API10,
## non-Texas matched by ID. If no local match exists (e.g. unpermitted well), falls back
## to resolving the well's Township/Range/Section via BLM PLSS to get a section centroid.
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

    all_permits = []
    trs_fallback_count = 0

    # Loop through each well in the AFE Summary
    for index, row in afeData.iterrows():
        api_number = str(row["API Number"])
        county = row["County"]
        state = row["State"]
        is_texas = str(state).strip().lower() == "texas"

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

        # No local match — resolve location from AFE Township/Range/Section via BLM PLSS
        print(f"  No local match — resolving location from T/R/S")
        twp_str = str(row.get("Township", "")).strip()
        rng_str = str(row.get("Range", "")).strip()
        sec_str = str(row.get("Section", "")).strip()
        state_abbr = STATE_ABBR_MAP.get(str(state).strip().lower())

        if twp_str and rng_str and sec_str and state_abbr:
            centroid = _fetchSectionCentroid(state_abbr, twp_str, rng_str, sec_str)
            if centroid:
                lat, lon = centroid
                well_name = str(row.get("Well Name", f"Well {index + 1}")).strip()
                synthetic = pd.DataFrame([{
                    "Latitude": lat,
                    "Longitude": lon,
                    "WellName": well_name,
                    "County": county,
                    "State": state,
                    "API10": api_number if api_number else None,
                }])
                all_permits.append(synthetic)
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

    print(f"\nDone. Retrieved {len(permitDf)} well permits total ({trs_fallback_count} wells resolved via T/R/S).")

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


## Generate a multi-page PDF of interpolated heat maps for subsurface parameters
## Uses scipy.interpolate.griddata (linear) to interpolate point values onto a regular grid,
## overlays All.shp operator acreage + Census state + county outlines as a basemap.
## Optionally overlays offset well lateral paths (from getNoviWellboreLocations) and AFE
## permit locations with labels (from getWellPermits).
## One parameter per page. Output: subsurface_heatmaps_{DSU Name}.pdf in the AFE Data/ folder.
def plotSubsurfaceHeatMaps(subsurfaceData, pathToAfeSummary, parameters=None, permitData=None, wellboreLocationsData=None, offsetData=None, labelNearestN=20, afeData=None):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    from scipy.interpolate import griddata
    import geopandas as gpd
    import numpy as np

    if parameters is None:
        parameters = [
            "TVD",
            "TOC_Avg",
            "SW_Avg",
            "Porosity_Avg",
            "Permeability_Avg",
            "Thickness_Avg",
            "VClay_Avg",
            "Brittleness_Avg",
        ]

    # Resolve output path (same Data/ folder as printData)
    afeDir = os.path.dirname(pathToAfeSummary)
    afeFilename = os.path.splitext(os.path.basename(pathToAfeSummary))[0]
    dsuName = afeFilename.split("-", 1)[1].strip() if "-" in afeFilename else afeFilename
    outputDir = os.path.join(afeDir, "Data")
    os.makedirs(outputDir, exist_ok=True)
    pdfPath = os.path.join(outputDir, f"subsurface_heatmaps_{dsuName}.pdf")

    if subsurfaceData is None or subsurfaceData.empty:
        print("No subsurface data — skipping heat map export.")
        return

    # Drop rows without lat/lon
    df = subsurfaceData.dropna(subset=["Latitude", "Longitude"]).copy()
    if df.empty:
        print("No subsurface rows with valid Lat/Lon — skipping heat map export.")
        return

    # Build interpolation grid covering the data extent (with small margin)
    margin = 0.05
    lon_min, lon_max = df["Longitude"].min() - margin, df["Longitude"].max() + margin
    lat_min, lat_max = df["Latitude"].min() - margin, df["Latitude"].max() + margin
    grid_lon, grid_lat = np.meshgrid(
        np.linspace(lon_min, lon_max, 200),
        np.linspace(lat_min, lat_max, 200),
    )
    points = df[["Longitude", "Latitude"]].values

    # Load basemap layers once (state + county from Census TIGER, All.shp from Novi extract)
    print("Loading basemap layers...")
    basemaps = _ensureBasemaps()
    states_gdf = gpd.read_file(basemaps["state"])
    counties_gdf = gpd.read_file(basemaps["county"])

    # Clip Census layers to the data extent for faster rendering
    from shapely.geometry import box
    bbox = box(lon_min, lat_min, lon_max, lat_max)
    states_clip = states_gdf[states_gdf.intersects(bbox)]
    counties_clip = counties_gdf[counties_gdf.intersects(bbox)]

    # Novi All.shp operator acreage underlay (from the bulk extract)
    try:
        paths = getNoviBulkPaths()
        allShpPath = os.path.join(paths["extract_dir"], "Shapefiles", "All", "All.shp")
        if os.path.exists(allShpPath):
            all_gdf = gpd.read_file(allShpPath)
            all_clip = all_gdf[all_gdf.intersects(bbox)]
            print(f"  Loaded operator acreage underlay ({len(all_clip)} polygons in extent)")
        else:
            all_clip = None
            print("  No All.shp found, skipping operator acreage underlay")
    except Exception as e:
        all_clip = None
        print(f"  Could not load operator acreage underlay: {e}")

    # BLM PLSS layers (townships + sections) for the bbox — cached on disk after first fetch
    # Texas has no BLM PLSS coverage (uses Block/Survey/Abstract instead) — log a warning if
    # offset wells touch Texas so the user knows why those areas have no grid overlay.
    if "State" in df.columns and df["State"].astype(str).str.lower().str.contains("texas").any():
        print("  WARNING: offset wells include Texas — BLM PLSS does not cover Texas.")
        print("           Texas areas will have no township/section grid overlay (planned for v3 via TX GLO).")
    # Pad the PLSS fetch bbox generously (~35 mi in each direction). The offset wells'
    # extent doesn't always reach the AFE's actual section — e.g. if the AFE targets
    # 21S/26E but the offsets cluster in 21S/25E, the section we need to outline sits
    # just outside the tight bbox and PLSS fetches a layer that doesn't contain it.
    # PLSS data is small so the extra coverage is essentially free.
    PLSS_PAD = 0.5
    plss_townships, plss_sections = _fetchPlssLayers(
        lon_min - PLSS_PAD, lat_min - PLSS_PAD, lon_max + PLSS_PAD, lat_max + PLSS_PAD
    )

    # Pre-group wellbore trajectories by API10 (sorted by Path) for fast per-page plotting
    wb_groups = None
    wb_midpoints = {}  # api10 -> (lon, lat) midpoint of trajectory (used for distance ranking)
    wb_heels = {}      # api10 -> (lon, lat) heel of lateral (first path point) for number labels
    if wellboreLocationsData is not None and not wellboreLocationsData.empty:
        wb_clean = wellboreLocationsData.dropna(subset=["Latitude", "Longitude"])
        wb_groups = list(wb_clean.sort_values(["API10", "Path"]).groupby("API10"))
        for api10, group in wb_groups:
            mid = len(group) // 2
            wb_midpoints[api10] = (group["Longitude"].iloc[mid], group["Latitude"].iloc[mid])
            wb_heels[api10] = (group["Longitude"].iloc[0], group["Latitude"].iloc[0])
        print(f"  Loaded {len(wb_groups)} offset well trajectories ({len(wb_clean):,} total points)")

    # Build api10 -> WellName / operator / first-prod-year lookup from offsetData
    # plus a fallback (lon, lat) using MPLatitude/MPLongitude for wells without wellbore trajectory data.
    api10_to_name = {}
    api10_to_operator = {}
    api10_to_first_prod = {}
    api10_to_fallback_pos = {}
    if offsetData is not None and not offsetData.empty:
        optional_cols = ["WellName", "MPLatitude", "MPLongitude", "CurrentOperator", "FirstProductionYear"]
        cols_needed = ["API10"] + [c for c in optional_cols if c in offsetData.columns]
        sub = offsetData[cols_needed].copy()
        for _, r in sub.iterrows():
            api10 = str(r["API10"]) if pd.notna(r["API10"]) else None
            if not api10:
                continue
            if "WellName" in sub.columns and pd.notna(r.get("WellName")):
                api10_to_name[api10] = str(r["WellName"])
            if "CurrentOperator" in sub.columns and pd.notna(r.get("CurrentOperator")):
                api10_to_operator[api10] = str(r["CurrentOperator"])
            if "FirstProductionYear" in sub.columns and pd.notna(r.get("FirstProductionYear")):
                try:
                    api10_to_first_prod[api10] = int(r["FirstProductionYear"])
                except (ValueError, TypeError):
                    pass
            if "MPLatitude" in sub.columns and "MPLongitude" in sub.columns \
                    and pd.notna(r.get("MPLatitude")) and pd.notna(r.get("MPLongitude")):
                api10_to_fallback_pos[api10] = (float(r["MPLongitude"]), float(r["MPLatitude"]))

    # AFE-driven DSU section matching:
    # For each AFE row, parse Township/Range/Section and find matching BLM PLSS section polygons.
    # Each AFE row gets a letter (A, B, C, ...) and may span multiple sections (e.g. "9,10").
    # Wells with no PLSS match (TX, PA, missing T/R/S) fall back to a red star at the permit point.
    def _afe_letter(i):
        if i < 26:
            return chr(ord("A") + i)
        return chr(ord("A") + (i // 26 - 1)) + chr(ord("A") + (i % 26))

    # Tuples include the AFE row's Landing Zone (uppercased) so we can filter the
    # legend per-formation at render time without re-iterating afeData. The DSU section
    # groups stay GLOBAL (no filtering) so the box always renders on every page.
    numbered_permits = []   # list of (letter, well_name, lz_upper)
    dsu_section_groups = [] # list of (letter, well_name, sections_gdf) — global, never filtered
    permit_fallbacks = []   # list of (letter, well_name, lon, lat, lz_upper)

    if afeData is not None and not afeData.empty:
        print(f"Matching {len(afeData)} AFE rows to PLSS sections...")
        # Diagnostic: dump the first few township values from the PLSS layer so we
        # can compare format against what we parse from the AFE Summary.
        if plss_townships is not None and not plss_townships.empty:
            sample_cols = [c for c in ["STATEABBR", "TWNSHPNO", "TWNSHPDIR", "RANGENO", "RANGEDIR"] if c in plss_townships.columns]
            print(f"  PLSS township sample (first 3 rows, cols={sample_cols}):")
            for _, trow in plss_townships[sample_cols].head(3).iterrows():
                print(f"    {dict(trow)}")
        for i, (_idx, row) in enumerate(afeData.iterrows()):
            letter = _afe_letter(i)
            well_name = str(row.get("Well Name", f"Well {i + 1}")).strip()
            lz_upper = str(row.get("Landing Zone", "")).strip().upper()
            numbered_permits.append((letter, well_name, lz_upper))

            state_full = str(row.get("State", "")).strip().lower()
            twp_str = str(row.get("Township", "")).strip()
            rng_str = str(row.get("Range", "")).strip()
            sec_str = str(row.get("Section", "")).strip()
            state_abbr = STATE_ABBR_MAP.get(state_full)

            twp_no, twp_dir = _parse_tr(twp_str, "twp")
            rng_no, rng_dir = _parse_tr(rng_str, "rng")
            print(
                f"  {letter}. {well_name}: state={state_abbr or '?'} "
                f"twp={twp_str!r}->({twp_no},{twp_dir}) "
                f"rng={rng_str!r}->({rng_no},{rng_dir}) "
                f"sec={sec_str!r}"
            )

            matched_sections = None
            if (
                state_abbr
                and twp_str and rng_str and sec_str
                and plss_townships is not None and not plss_townships.empty
                and plss_sections is not None and not plss_sections.empty
            ):
                if twp_no and twp_dir and rng_no and rng_dir:
                    twp_match = plss_townships[
                        (plss_townships["STATEABBR"] == state_abbr)
                        & (plss_townships["TWNSHPNO"].astype(str) == twp_no)
                        & (plss_townships["TWNSHPDIR"].astype(str) == twp_dir)
                        & (plss_townships["RANGENO"].astype(str) == rng_no)
                        & (plss_townships["RANGEDIR"].astype(str) == rng_dir)
                    ]
                    if not twp_match.empty:
                        plssids = twp_match["PLSSID"].tolist()
                        try:
                            sec_nums = [s.strip().zfill(2) for s in sec_str.split(",") if s.strip()]
                        except Exception:
                            sec_nums = []
                        if sec_nums:
                            sec_match = plss_sections[
                                plss_sections["PLSSID"].isin(plssids)
                                & plss_sections["FRSTDIVNO"].astype(str).isin(sec_nums)
                            ]
                            if not sec_match.empty:
                                matched_sections = sec_match
                                print(f"  {letter}. {well_name}: {len(sec_match)} sections matched ({twp_str} {rng_str} sec {sec_str})")

            if matched_sections is not None and not matched_sections.empty:
                dsu_section_groups.append((letter, well_name, matched_sections))
            else:
                # Fallback: use the permit point (looked up from permitData by WellName)
                fallback_lon = fallback_lat = None
                if permitData is not None and not permitData.empty and "WellName" in permitData.columns:
                    p_match = permitData[permitData["WellName"].astype(str) == well_name]
                    p_match = p_match.dropna(subset=["Latitude", "Longitude"])
                    if not p_match.empty:
                        fallback_lon = p_match["Longitude"].iloc[0]
                        fallback_lat = p_match["Latitude"].iloc[0]
                if fallback_lon is not None:
                    permit_fallbacks.append((letter, well_name, fallback_lon, fallback_lat, lz_upper))
                    print(f"  {letter}. {well_name}: no PLSS section match — using star fallback at permit point")
                else:
                    print(f"  {letter}. {well_name}: no PLSS section match and no permit point — skipping marker")

    # Number EVERY offset well sequentially (1, 2, 3, ...) ordered by distance from the
    # AFE permit centroid. The on-map number is the lookup key; the full name → operator →
    # API → distance table is rendered on a dedicated appendix page at the end of the PDF.
    # `labelNearestN` is preserved for back-compat: 0 (default) means "label all".
    numbered_wells = []          # list of (number, api10, name, operator, dist_mi, first_prod) sorted by distance
    api10_to_number = {}
    api10_to_distance_mi = {}

    permit_pts = None
    if permitData is not None and not permitData.empty:
        permit_pts = permitData.dropna(subset=["Latitude", "Longitude"])

    if (
        permit_pts is not None
        and not permit_pts.empty
        and api10_to_name
    ):
        permit_centroid_lon = permit_pts["Longitude"].mean()
        permit_centroid_lat = permit_pts["Latitude"].mean()
        # Distance in degrees for ranking; converted to miles below using local cos(lat) factor
        deg_to_mi_lat = 69.0
        deg_to_mi_lon = 69.0 * abs(math.cos(math.radians(permit_centroid_lat)))

        dists = []
        for api10 in api10_to_name.keys():
            # Prefer wellbore midpoint, fall back to MP (surface point) if no trajectory data
            if api10 in wb_midpoints:
                lon, lat = wb_midpoints[api10]
            elif api10 in api10_to_fallback_pos:
                lon, lat = api10_to_fallback_pos[api10]
            else:
                continue
            d_deg = ((lon - permit_centroid_lon) ** 2 + (lat - permit_centroid_lat) ** 2) ** 0.5
            d_mi = (((lon - permit_centroid_lon) * deg_to_mi_lon) ** 2
                    + ((lat - permit_centroid_lat) * deg_to_mi_lat) ** 2) ** 0.5
            dists.append((d_deg, d_mi, api10))
        dists.sort()

        # Optional cap for back-compat — 0 / None / negative means "all"
        cap = labelNearestN if (labelNearestN and labelNearestN > 0) else len(dists)
        for i, (_d_deg, d_mi, api10) in enumerate(dists[:cap], start=1):
            name = api10_to_name.get(api10, "")
            operator = api10_to_operator.get(api10, "")
            first_prod = api10_to_first_prod.get(api10)
            numbered_wells.append((i, api10, name, operator, d_mi, first_prod))
            api10_to_number[api10] = i
            api10_to_distance_mi[api10] = d_mi
        print(f"  Numbered {len(numbered_wells)} offset wells by distance from permit centroid")

    # Build API10 -> Formation lookup for per-formation lateral coloring
    # (Point Pleasant = black, Utica = orange; all others = black)
    api10_to_formation = {}
    if offsetData is not None and "Formation" in offsetData.columns:
        for _, r in offsetData[["API10", "Formation"]].dropna().iterrows():
            api10_to_formation[str(r["API10"])] = str(r["Formation"]).upper()

    PP_UTICA = {"POINT PLEASANT", "UTICA"}

    # Determine landing zones to render. If subsurface has multiple Formation values
    # (one per AFE landing zone), render one page per (parameter, formation) combo.
    # Point Pleasant + Utica are merged into a single page.
    if "Formation" in df.columns:
        formations = sorted(df["Formation"].dropna().astype(str).str.upper().unique().tolist())
    else:
        formations = []

    # Merge PP + Utica into one combined label if both are present
    if PP_UTICA.issubset(set(formations)):
        formations = [f for f in formations if f not in PP_UTICA]
        formations.insert(0, "POINT PLEASANT / UTICA")
    elif "POINT PLEASANT" in formations or "UTICA" in formations:
        # Only one present but might still want the combined label for consistency
        present = [f for f in formations if f in PP_UTICA]
        formations = [f for f in formations if f not in PP_UTICA]
        formations.insert(0, "POINT PLEASANT / UTICA")

    if not formations:
        formations = [None]

    print(f"Generating heat map PDF: {pdfPath}")
    with PdfPages(pdfPath) as pdf:
        for param in parameters:
            if param not in df.columns:
                print(f"  Skipping {param}: column not in subsurface data")
                continue

            for fm_label in formations:
                # Slice subsurface, wellbores, offset legend, and AFE legend to this
                # formation. Map extent + DSU box + basemap stay GLOBAL — only the
                # heatmap data and the side legend change per formation page.
                if fm_label is None:
                    df_fm = df
                    api10_set_fm = None
                elif fm_label == "POINT PLEASANT / UTICA":
                    df_fm = df[df["Formation"].astype(str).str.upper().isin(PP_UTICA)]
                    api10_set_fm = set(df_fm["API10"].astype(str).tolist())
                else:
                    df_fm = df[df["Formation"].astype(str).str.upper() == fm_label]
                    api10_set_fm = set(df_fm["API10"].astype(str).tolist())

                values = pd.to_numeric(df_fm[param], errors="coerce")
                mask = values.notna()
                if mask.sum() < 4:
                    print(f"  Skipping {param} [{fm_label or 'ALL'}]: only {mask.sum()} valid values (need >= 4 for interpolation)")
                    continue

                points_fm = df_fm[["Longitude", "Latitude"]].values

                # Filter wellbores and offset numbering to this formation's API10s
                if api10_set_fm is None:
                    wb_groups_fm = wb_groups
                    numbered_wells_fm = numbered_wells
                else:
                    wb_groups_fm = (
                        [(a, g) for a, g in wb_groups if str(a) in api10_set_fm]
                        if wb_groups is not None else None
                    )
                    numbered_wells_fm = [w for w in numbered_wells if str(w[1]) in api10_set_fm]

                # Filter AFE permit legend by Landing Zone (DSU box stays global)
                if fm_label is None:
                    numbered_permits_fm = numbered_permits
                    permit_fallbacks_fm = permit_fallbacks
                elif fm_label == "POINT PLEASANT / UTICA":
                    numbered_permits_fm = [p for p in numbered_permits if p[2] in PP_UTICA]
                    permit_fallbacks_fm = [p for p in permit_fallbacks if p[4] in PP_UTICA]
                else:
                    numbered_permits_fm = [p for p in numbered_permits if p[2] == fm_label]
                    permit_fallbacks_fm = [p for p in permit_fallbacks if p[4] == fm_label]

                # Linear interpolation — NaN outside the convex hull (no dishonest extrapolation)
                interp = griddata(
                    points_fm[mask.values],
                    values[mask].values,
                    (grid_lon, grid_lat),
                    method="linear",
                )

                # Wider figure when we have a numbered-well legend to render on the right
                if numbered_wells_fm:
                    fig, ax = plt.subplots(figsize=(14, 8.5))
                    fig.subplots_adjust(left=0.06, right=0.69, top=0.93, bottom=0.07)
                else:
                    fig, ax = plt.subplots(figsize=(11, 8.5))
                import matplotlib.patheffects as path_effects

                # Layer order (bottom → top):
                #  1. Operator acreage underlay
                #  2. County boundaries
                #  3. PLSS section boundaries (subtle grid)
                #  4. Heat map contourf
                #  5. Wellbore lateral paths (uniform black)
                #  6. PLSS township boundaries (brown)
                #  7. State boundaries (black)
                #  8. Offset well markers (small black dots)
                #  9. AFE DSU section box (black, GLOBAL — same on every page)
                #  9. AFE permit stars (red, filtered to this formation)
                # 10. PLSS section number labels
                # 11. PLSS township T/R labels
                # 12. AFE permit name labels

                if all_clip is not None and not all_clip.empty:
                    all_clip.plot(ax=ax, facecolor="#f0f0f0", edgecolor="#cccccc", linewidth=0.2, zorder=1)
                if not counties_clip.empty:
                    counties_clip.boundary.plot(ax=ax, color="#999999", linewidth=0.4, zorder=2)

                if plss_sections is not None and not plss_sections.empty:
                    plss_sections.boundary.plot(ax=ax, color="#bbbbbb", linewidth=0.3, alpha=0.4, zorder=3)

                cf = ax.contourf(grid_lon, grid_lat, interp, levels=20, cmap="viridis", zorder=4, alpha=0.85)

                # Offset well lateral paths — colored by formation for PP/Utica combo
                if wb_groups_fm is not None:
                    has_pp = False
                    has_utica = False
                    for _api10, group in wb_groups_fm:
                        fm = api10_to_formation.get(str(_api10), "")
                        if fm == "UTICA":
                            color = "orange"
                            has_utica = True
                        else:
                            color = "black"
                            if fm == "POINT PLEASANT":
                                has_pp = True
                        ax.plot(
                            group["Longitude"].values,
                            group["Latitude"].values,
                            color=color,
                            linewidth=1.0,
                            alpha=0.85,
                            zorder=5,
                        )
                    # Add formation color legend entries for PP/Utica pages
                    if has_pp:
                        ax.plot([], [], color="black", linewidth=1.0, label="Point Pleasant")
                    if has_utica:
                        ax.plot([], [], color="orange", linewidth=1.0, label="Utica")

                if plss_townships is not None and not plss_townships.empty:
                    plss_townships.boundary.plot(ax=ax, color="#8b6914", linewidth=0.7, alpha=0.7, zorder=6)

                if not states_clip.empty:
                    states_clip.boundary.plot(ax=ax, color="black", linewidth=1.0, zorder=7)

                ax.scatter(
                    df_fm["Longitude"][mask], df_fm["Latitude"][mask],
                    c="black", s=6, alpha=0.6, zorder=8, label="Offset Wells",
                )

                # AFE DSU highlight — bold black perimeter around the union of all DSU sections.
                # GLOBAL: drawn from the unfiltered dsu_section_groups so the box always
                # appears on every formation page regardless of Landing Zone matching.
                from shapely.ops import unary_union
                for _letter, _name, sec_gdf in dsu_section_groups:
                    union_geom = unary_union(list(sec_gdf.geometry))
                    gpd.GeoSeries([union_geom], crs=sec_gdf.crs).boundary.plot(
                        ax=ax, color="black", linewidth=2.2, zorder=9,
                    )
                # Star fallback for AFE rows with no PLSS section match (TX, PA, etc.)
                if permit_fallbacks_fm:
                    fb_lon = [f[2] for f in permit_fallbacks_fm]
                    fb_lat = [f[3] for f in permit_fallbacks_fm]
                    ax.scatter(
                        fb_lon, fb_lat,
                        marker="*", c="red", s=180, edgecolor="black", linewidth=0.8,
                        zorder=9, label="AFE Permit (no PLSS)",
                    )

                # === Labels (all on top of data layers) ===

                if plss_sections is not None and not plss_sections.empty and "FRSTDIVLAB" in plss_sections.columns:
                    sec_halo = [path_effects.withStroke(linewidth=1.5, foreground="white")]
                    for _, sec in plss_sections.iterrows():
                        c = sec.geometry.centroid
                        if lon_min <= c.x <= lon_max and lat_min <= c.y <= lat_max:
                            ax.annotate(
                                str(sec["FRSTDIVLAB"]),
                                xy=(c.x, c.y),
                                ha="center", va="center",
                                fontsize=5,
                                color="#555555",
                                zorder=10,
                                path_effects=sec_halo,
                            )

                if plss_townships is not None and not plss_townships.empty and "TWNSHPLAB" in plss_townships.columns:
                    tr_halo = [path_effects.withStroke(linewidth=2.0, foreground="white")]
                    for _, twp in plss_townships.iterrows():
                        c = twp.geometry.centroid
                        if lon_min <= c.x <= lon_max and lat_min <= c.y <= lat_max:
                            ax.annotate(
                                str(twp["TWNSHPLAB"]),
                                xy=(c.x, c.y),
                                ha="center", va="center",
                                fontsize=7,
                                fontweight="bold",
                                color="#5a4509",
                                zorder=11,
                                path_effects=tr_halo,
                            )

                # Offset well numbers — small navy boxed badges at each lateral's heel
                if numbered_wells_fm:
                    num_box = dict(
                        boxstyle="round,pad=0.15",
                        facecolor="white",
                        edgecolor="#0a2a5e",
                        linewidth=0.5,
                    )
                    for number, api10, _name, _op, _dmi, _fp in numbered_wells_fm:
                        if api10 in wb_heels:
                            lon, lat = wb_heels[api10]
                        elif api10 in api10_to_fallback_pos:
                            lon, lat = api10_to_fallback_pos[api10]
                        else:
                            continue
                        if not (lon_min <= lon <= lon_max and lat_min <= lat <= lat_max):
                            continue
                        ax.annotate(
                            str(number),
                            xy=(lon, lat),
                            xytext=(4, 4),
                            textcoords="offset points",
                            fontsize=5,
                            fontweight="bold",
                            color="#0a2a5e",
                            zorder=11,
                            bbox=num_box,
                        )

                # AFE letter labels — only rendered for non-PLSS fallback stars (TX, PA)
                permit_halo = [path_effects.withStroke(linewidth=2.5, foreground="white")]
                for letter, _name, lon, lat, _lz in permit_fallbacks_fm:
                    ax.annotate(
                        letter,
                        xy=(lon, lat),
                        xytext=(8, 8),
                        textcoords="offset points",
                        fontsize=12,
                        fontweight="bold",
                        color="darkred",
                        zorder=12,
                        path_effects=permit_halo,
                    )

                ax.set_xlim(lon_min, lon_max)
                ax.set_ylim(lat_min, lat_max)
                ax.set_xlabel("Longitude")
                ax.set_ylabel("Latitude")
                title_suffix = f" [{fm_label}]" if fm_label else ""
                ax.set_title(f"{param}{title_suffix} — {mask.sum():,} wells")
                ax.legend(loc="upper right", fontsize=8)
                ax.set_aspect("equal", adjustable="box")
                cbar = plt.colorbar(cf, ax=ax, shrink=0.8)
                cbar.set_label(param)

                # Side legend: AFE permits + offset wells, both filtered to this formation
                MAX_NAME_LEN = 38
                PERMIT_LINE_H = 0.018
                OFFSET_LINE_H = 0.016
                STACK_GAP = 0.04

                permit_box_h = ((len(numbered_permits_fm) + 2) * PERMIT_LINE_H) if numbered_permits_fm else 0
                offset_box_h = ((len(numbered_wells_fm) + 2) * OFFSET_LINE_H) if numbered_wells_fm else 0
                total_h = permit_box_h + (STACK_GAP if (permit_box_h and offset_box_h) else 0) + offset_box_h
                stack_top = 0.5 + total_h / 2

                permit_y_top = stack_top
                if numbered_permits_fm:
                    header = f"AFE Permits [{fm_label}]" if fm_label else "AFE Permits"
                    permit_lines = [header, "-" * 32]
                    for letter, name, _lz in numbered_permits_fm:
                        display = name if len(name) <= MAX_NAME_LEN else name[: MAX_NAME_LEN - 3] + "..."
                        permit_lines.append(f"{letter}. {display}")
                    permit_text = "\n".join(permit_lines)
                    fig.text(
                        0.71, permit_y_top, permit_text,
                        fontsize=7,
                        family="monospace",
                        fontweight="bold",
                        verticalalignment="top",
                        horizontalalignment="left",
                        bbox=dict(facecolor="white", edgecolor="black", linewidth=0.6, pad=6),
                    )

                if numbered_wells_fm:
                    offset_y_top = stack_top - permit_box_h - (STACK_GAP if permit_box_h else 0)
                    offset_lines = [f"Offset Wells (by distance, n={len(numbered_wells_fm)})", "-" * 32]
                    num_width = len(str(len(numbered_wells_fm)))
                    for number, _api10, name, _op, _dmi, _fp in numbered_wells_fm:
                        display = name if len(name) <= MAX_NAME_LEN else name[: MAX_NAME_LEN - 3] + "..."
                        offset_lines.append(f"{str(number).rjust(num_width)}. {display}")
                    offset_text = "\n".join(offset_lines)
                    fig.text(
                        0.71, offset_y_top, offset_text,
                        fontsize=6.5,
                        family="monospace",
                        verticalalignment="top",
                        horizontalalignment="left",
                        bbox=dict(facecolor="white", edgecolor="black", linewidth=0.6, pad=6),
                    )

                pdf.savefig(fig, bbox_inches="tight")
                plt.close(fig)
                print(f"  Plotted {param} [{fm_label or 'ALL'}] ({mask.sum():,} wells)")

        # === Appendix pages ===
        # Full offset well index (number → name → API10 → distance → first prod year),
        # rendered as multi-page tables, ~50 rows per page. Operator column removed.
        if numbered_wells:
            ROWS_PER_PAGE = 50
            COL_HEADERS = ["#", "Well Name", "API10", "Dist (mi)", "1st Prod"]
            COL_WIDTHS = [0.05, 0.55, 0.18, 0.10, 0.09]

            def _truncate(s, n):
                s = str(s) if s is not None else ""
                return s if len(s) <= n else s[: n - 1] + "…"

            total_pages = (len(numbered_wells) + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE
            for page_i in range(total_pages):
                start = page_i * ROWS_PER_PAGE
                end = min(start + ROWS_PER_PAGE, len(numbered_wells))
                page_rows = numbered_wells[start:end]

                fig = plt.figure(figsize=(11, 8.5))
                fig.subplots_adjust(left=0.04, right=0.96, top=0.94, bottom=0.05)
                ax = fig.add_subplot(111)
                ax.axis("off")

                title = f"Offset Well Index — {dsuName}  (page {page_i + 1}/{total_pages}, wells {start + 1}-{end} of {len(numbered_wells)})"
                ax.set_title(title, fontsize=11, fontweight="bold", loc="left", pad=14)

                # Build table data
                table_data = []
                for number, api10, name, _op, dmi, fp in page_rows:
                    table_data.append([
                        str(number),
                        _truncate(name, 64),
                        str(api10),
                        f"{dmi:.2f}" if dmi is not None else "",
                        str(fp) if fp is not None else "",
                    ])

                table = ax.table(
                    cellText=table_data,
                    colLabels=COL_HEADERS,
                    colWidths=COL_WIDTHS,
                    loc="upper left",
                    cellLoc="left",
                )
                table.auto_set_font_size(False)
                table.set_fontsize(7)
                table.scale(1.0, 1.15)

                # Style header row
                for col_i in range(len(COL_HEADERS)):
                    cell = table[(0, col_i)]
                    cell.set_facecolor("#0a2a5e")
                    cell.set_text_props(color="white", fontweight="bold")

                pdf.savefig(fig, bbox_inches="tight")
                plt.close(fig)

            print(f"  Appended {total_pages} offset well index page(s)")

    print(f"Done. Saved heat maps to {pdfPath}")


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
