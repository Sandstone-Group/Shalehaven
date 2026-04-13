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


## Check if the Novi bulk export needs refreshing
## Hits the bulk endpoint for metadata, compares ExportDate against local manifest
## Only downloads if a newer export is available on the server
def checkNoviDbStatus(envPath=r"C:\Users\Michael Tanner\code\.env", outputDir=None):
    import json

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


## Generate an interactive HTML page of interpolated heat maps for subsurface parameters
## Uses Plotly.js (loaded from CDN) for zoomable, hoverable contour maps with well overlays.
## Mirrors the data pipeline of plotSubsurfaceHeatMaps but outputs a single self-contained HTML
## file instead of a static PDF.  Open the resulting file in any browser.
## Output: subsurface_heatmaps_{DSU Name}.html in the AFE Data/ folder.
def plotSubsurfaceHeatMapsHTML(subsurfaceData, pathToAfeSummary, parameters=None, permitData=None,
                                wellboreLocationsData=None, offsetData=None, labelNearestN=20, afeData=None):
    from scipy.interpolate import griddata
    import numpy as np
    import json as _json
    import webbrowser

    if parameters is None:
        parameters = [
            "TVD", "TOC_Avg", "SW_Avg", "Porosity_Avg",
            "Permeability_Avg", "Thickness_Avg", "VClay_Avg", "Brittleness_Avg",
        ]

    afeDir = os.path.dirname(pathToAfeSummary)
    afeFilename = os.path.splitext(os.path.basename(pathToAfeSummary))[0]
    dsuName = afeFilename.split("-", 1)[1].strip() if "-" in afeFilename else afeFilename
    outputDir = os.path.join(afeDir, "Data")
    os.makedirs(outputDir, exist_ok=True)
    htmlPath = os.path.join(outputDir, f"subsurface_heatmaps_{dsuName}.html")

    if subsurfaceData is None or subsurfaceData.empty:
        print("No subsurface data — skipping HTML heat map export.")
        return

    df = subsurfaceData.dropna(subset=["Latitude", "Longitude"]).copy()
    if df.empty:
        print("No subsurface rows with valid Lat/Lon — skipping HTML heat map export.")
        return

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
                "lz": str(row.get("Landing Zone", "")).strip().upper(),
            })

    # Permit locations for star markers
    permit_markers = []
    if permit_pts is not None and not permit_pts.empty:
        for _, r in permit_pts.iterrows():
            permit_markers.append({
                "lon": float(r["Longitude"]), "lat": float(r["Latitude"]),
                "name": str(r.get("WellName", "")) if pd.notna(r.get("WellName")) else "",
            })

    # Determine formations
    PP_UTICA = {"POINT PLEASANT", "UTICA"}
    if "Formation" in df.columns:
        formations = sorted(df["Formation"].dropna().astype(str).str.upper().unique().tolist())
    else:
        formations = []
    if PP_UTICA.issubset(set(formations)):
        formations = [f for f in formations if f not in PP_UTICA]
        formations.insert(0, "POINT PLEASANT / UTICA")
    elif any(f in PP_UTICA for f in formations):
        formations = [f for f in formations if f not in PP_UTICA]
        formations.insert(0, "POINT PLEASANT / UTICA")
    if not formations:
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
            elif fm_label == "POINT PLEASANT / UTICA":
                df_fm = df[df["Formation"].astype(str).str.upper().isin(PP_UTICA)]
                api10_set_fm = set(df_fm["API10"].astype(str).tolist())
            else:
                df_fm = df[df["Formation"].astype(str).str.upper() == fm_label]
                api10_set_fm = set(df_fm["API10"].astype(str).tolist())

            values = pd.to_numeric(df_fm[param], errors="coerce")
            mask = values.notna()
            if mask.sum() < 4:
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

            title_suffix = f" [{fm_label}]" if fm_label else ""
            figures.append({
                "title": f"{param}{title_suffix} — {mask.sum()} wells",
                "param": param,
                "z": z_data,
                "lon": lon_list,
                "lat": lat_list,
                "wb_traces": wb_traces,
                "num_markers": num_markers,
                "permit_markers": permit_markers,
            })
            print(f"  Prepared {param} [{fm_label or 'ALL'}] ({mask.sum()} wells)")

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
  .page.active {{ display: flex; gap: 20px; }}
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
  table {{ border-collapse: collapse; width: 100%; background: #fff; }}
  th {{ background: #0a2a5e; color: #fff; text-align: left; padding: 6px 10px; font-size: 12px; }}
  td {{ padding: 5px 10px; font-size: 11px; border-bottom: 1px solid #eee; font-family: monospace; }}
  tr:hover td {{ background: #f0f4ff; }}
</style>
</head>
<body>
<div class="header">
  <h1>Subsurface Heat Maps — {dsuName}</h1>
  <div class="sub">Interactive viewer &middot; Generated by Shalehaven</div>
</div>
<div class="tabs" id="tabs"></div>
<div id="pages"></div>
<script>
const FIGURES = {_json.dumps(figures, separators=(',', ':'))};
const WELL_INDEX = {_json.dumps(well_index, separators=(',', ':'))};
const AFE_PERMITS = {_json.dumps(afe_permits, separators=(',', ':'))};

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

// Build tabs + pages
FIGURES.forEach((fig, i) => {{
  const btn = document.createElement('button');
  btn.textContent = fig.title.split(' —')[0];
  btn.dataset.idx = i;
  if (i === 0) btn.classList.add('active');
  tabsEl.appendChild(btn);

  const page = document.createElement('div');
  page.className = 'page' + (i === 0 ? ' active' : '');
  page.id = 'page-' + i;
  page.innerHTML = `
    <div class="plot-wrap"><div class="plot-box" id="plot-${{i}}"></div></div>
    <div class="sidebar">
      <div class="panel">
        <h3>AFE Permits</h3>
        ${{AFE_PERMITS.map(p => '<div class="row"><span class="n">' + p.letter + '.</span> ' + p.name + ' [' + p.lz + ']</div>').join('')}}
      </div>
      <div class="panel">
        <h3>Offset Wells (by distance)</h3>
        ${{fig.num_markers.map(m => '<div class="row"><span class="n">' + m.num + '.</span> ' + m.name + '</div>').join('')}}
      </div>
    </div>`;
  pagesEl.appendChild(page);
}});

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
  if (idx !== 'idx' && !plotted.has(idx)) {{
    buildPlot(FIGURES[parseInt(idx)], 'plot-' + idx);
    plotted.add(idx);
  }}
}});

// Render first plot
buildPlot(FIGURES[0], 'plot-0');
plotted.add('0');
</script>
</body>
</html>""")

    html_content = "\n".join(html_parts)
    with open(htmlPath, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"Done. Saved interactive heat maps to {htmlPath}")
    webbrowser.open(htmlPath)


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
    if "POINT PLEASANT" in formations and "UTICA" not in formations:
        formations.append("UTICA")
    elif "UTICA" in formations and "POINT PLEASANT" not in formations:
        formations.append("POINT PLEASANT")

    print(f"Loading operator analysis data for: {operators}")
    print(f"  Formations: {formations}")

    # Load WellDetails
    wells = pd.read_csv(
        paths["tsv"]["WellDetails"], sep="\t", low_memory=False, dtype={"API10": "string"}
    )

    # Fuzzy match operator — match if the AFE operator string appears anywhere in CurrentOperator
    op_pattern = "|".join(re.escape(op.upper()) for op in operators)
    op_mask = wells["CurrentOperator"].fillna("").str.upper().str.contains(op_pattern, regex=True)
    fm_mask = wells["Formation"].fillna("").str.upper().isin(formations)
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
    if "POINT PLEASANT" in formations and "UTICA" not in formations:
        formations.append("UTICA")
    elif "UTICA" in formations and "POINT PLEASANT" not in formations:
        formations.append("POINT PLEASANT")

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
    fm_mask = wells["Formation"].fillna("").str.upper().isin(formations)
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


## Generate operator analysis PDF with completion trends, production performance, spacing impact,
## and peer comparison pages. Exports to Data/ folder as operator_analysis_{DSU Name}.pdf.
def plotOperatorAnalysis(analysisData, pathToAfeSummary, peerData=None):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    import numpy as np

    if analysisData is None or analysisData.empty:
        print("No operator analysis data — skipping PDF export.")
        return

    # Resolve output path
    afeDir = os.path.dirname(pathToAfeSummary)
    afeFilename = os.path.splitext(os.path.basename(pathToAfeSummary))[0]
    dsuName = afeFilename.split("-", 1)[1].strip() if "-" in afeFilename else afeFilename
    outputDir = os.path.join(afeDir, "Data")
    os.makedirs(outputDir, exist_ok=True)
    pdfPath = os.path.join(outputDir, f"operator_analysis_{dsuName}.pdf")

    operator = analysisData["CurrentOperator"].mode().iloc[0] if not analysisData["CurrentOperator"].empty else "Unknown"
    formations = sorted(analysisData["Formation"].dropna().str.upper().unique().tolist())
    fm_label = " / ".join(formations) if formations else "ALL"

    df = analysisData.copy()
    df["Vintage"] = pd.to_numeric(df["FirstProductionYear"], errors="coerce")
    df = df[df["Vintage"].notna() & (df["Vintage"] >= 2014)].copy()
    df["Vintage"] = df["Vintage"].astype(int)

    print(f"Generating operator analysis PDF: {pdfPath}")
    print(f"  Operator: {operator}")
    print(f"  Formations: {fm_label}")
    print(f"  Vintage range: {df['Vintage'].min()}-{df['Vintage'].max()}, {len(df):,} wells")

    with PdfPages(pdfPath) as pdf:

        # === PAGE 1: Completion Design Trends ===
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle(f"Completion Design Trends — {operator}\n{fm_label}", fontsize=13, fontweight="bold")

        # Top row + bottom-left: bar charts by vintage
        bar_metrics = [
            ("LateralLength", "Avg Lateral Length (ft)", "steelblue"),
            ("ProppantLbsPerFt", "Avg Proppant (lbs/ft)", "firebrick"),
            ("FluidBblPerFt", "Avg Fluid (bbl/ft)", "teal"),
        ]

        for ax, (col, label, color) in zip([axes[0, 0], axes[0, 1], axes[1, 0]], bar_metrics):
            col_numeric = pd.to_numeric(df[col], errors="coerce")
            grouped = df.assign(_val=col_numeric).groupby("Vintage")["_val"]
            means = grouped.mean()
            counts = grouped.count()
            valid = means[counts >= 3].dropna()
            if valid.empty:
                ax.text(0.5, 0.5, f"Insufficient data\nfor {label}", ha="center", va="center", transform=ax.transAxes)
                ax.set_title(label)
                continue
            ax.bar(valid.index, valid.values, color=color, alpha=0.8, edgecolor="black", linewidth=0.3)
            for x, y, n in zip(valid.index, valid.values, counts[valid.index]):
                ax.annotate(f"n={n}", xy=(x, y), ha="center", va="bottom", fontsize=7, color="#444444")
            ax.set_title(label)
            ax.set_xlabel("First Production Year")
            ax.set_ylabel(label)
            ax.grid(axis="y", alpha=0.3)

        # Bottom-right: Proppant/Fluid ratio vs EUR 50yr BOE scatter
        ax = axes[1, 1]
        prop = pd.to_numeric(df["FirstCompletionProppantMass"], errors="coerce")
        fluid = pd.to_numeric(df["FirstCompletionFluidVolume"], errors="coerce") / 42.0
        eur50 = pd.to_numeric(df["EUR50YRBOE"], errors="coerce")
        prop_fluid = (prop / fluid.replace(0, float("nan")))
        scatter_mask = prop_fluid.notna() & eur50.notna()
        if scatter_mask.sum() >= 3:
            sc = ax.scatter(prop_fluid[scatter_mask], eur50[scatter_mask],
                           c=df.loc[scatter_mask, "Vintage"], cmap="plasma",
                           s=20, alpha=0.7, edgecolor="black", linewidth=0.2)
            plt.colorbar(sc, ax=ax, label="Vintage", shrink=0.8)
        else:
            ax.text(0.5, 0.5, "Insufficient data", ha="center", va="center", transform=ax.transAxes)
        ax.set_title("Proppant/Fluid Ratio vs EUR 50yr BOE")
        ax.set_xlabel("Proppant / Fluid (lbs/bbl)")
        ax.set_ylabel("EUR 50yr BOE")
        ax.grid(alpha=0.3)

        fig.tight_layout(rect=[0, 0, 1, 0.93])
        pdf.savefig(fig)
        plt.close(fig)
        print("  Page 1: Completion Design Trends")

        # === PAGE 2: Production Performance ===
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle(f"Production Performance — {operator}\n{fm_label}", fontsize=13, fontweight="bold")

        # 2a: Cum12M BOE/ft by vintage — only wells with >= 12 months on production
        ax = axes[0, 0]
        months_on = pd.to_numeric(df["LastReportedMonthsOnProduction"], errors="coerce")
        df_12m = df[months_on >= 12]
        col_numeric = pd.to_numeric(df_12m["Cum12MBOEPerFt"], errors="coerce")
        grouped = df_12m.assign(_val=col_numeric).groupby("Vintage")["_val"]
        means = grouped.mean()
        counts = grouped.count()
        valid = means[counts >= 3].dropna()
        if not valid.empty:
            ax.bar(valid.index, valid.values, color="steelblue", alpha=0.8, edgecolor="black", linewidth=0.3)
            for x, y, n in zip(valid.index, valid.values, counts[valid.index]):
                ax.annotate(f"n={n}", xy=(x, y), ha="center", va="bottom", fontsize=7, color="#444444")
        ax.set_title("Cum 12M BOE / Lateral Foot")
        ax.set_xlabel("First Production Year")
        ax.set_ylabel("BOE/ft")
        ax.grid(axis="y", alpha=0.3)

        # 2b: EUR50yr BOE vs lateral length scatter
        ax = axes[0, 1]
        eur = pd.to_numeric(df["EUR50YRBOE"], errors="coerce")
        ll = pd.to_numeric(df["LateralLength"], errors="coerce")
        scatter_mask = eur.notna() & ll.notna()
        if scatter_mask.sum() >= 3:
            sc = ax.scatter(ll[scatter_mask], eur[scatter_mask], c=df.loc[scatter_mask, "Vintage"],
                           cmap="plasma", s=20, alpha=0.7, edgecolor="black", linewidth=0.2)
            plt.colorbar(sc, ax=ax, label="Vintage", shrink=0.8)
        ax.set_title("EUR 50yr BOE vs Lateral Length")
        ax.set_xlabel("Lateral Length (ft)")
        ax.set_ylabel("EUR 50yr BOE")
        ax.grid(alpha=0.3)

        # 2c: Peak month BOE rate by vintage
        ax = axes[1, 0]
        col_numeric = pd.to_numeric(df["PeakMonthBOERate"], errors="coerce")
        grouped = df.assign(_val=col_numeric).groupby("Vintage")["_val"]
        means = grouped.mean()
        counts = grouped.count()
        valid = means[counts >= 3].dropna()
        if not valid.empty:
            ax.bar(valid.index, valid.values, color="darkorange", alpha=0.8, edgecolor="black", linewidth=0.3)
            for x, y, n in zip(valid.index, valid.values, counts[valid.index]):
                ax.annotate(f"n={n}", xy=(x, y), ha="center", va="bottom", fontsize=7, color="#444444")
        ax.set_title("Avg Peak Month BOE Rate")
        ax.set_xlabel("First Production Year")
        ax.set_ylabel("BOE/month")
        ax.grid(axis="y", alpha=0.3)

        # 2d: Cum Life GOR by vintage
        ax = axes[1, 1]
        col_numeric = pd.to_numeric(df["CumLifeGOR"], errors="coerce")
        grouped = df.assign(_val=col_numeric).groupby("Vintage")["_val"]
        means = grouped.mean()
        counts = grouped.count()
        valid = means[counts >= 3].dropna()
        if not valid.empty:
            ax.bar(valid.index, valid.values, color="gray", alpha=0.8, edgecolor="black", linewidth=0.3)
            for x, y, n in zip(valid.index, valid.values, counts[valid.index]):
                ax.annotate(f"n={n}", xy=(x, y), ha="center", va="bottom", fontsize=7, color="#444444")
        ax.set_title("Avg Cum Life GOR")
        ax.set_xlabel("First Production Year")
        ax.set_ylabel("GOR (scf/bbl)")
        ax.grid(axis="y", alpha=0.3)

        fig.tight_layout(rect=[0, 0, 1, 0.93])
        pdf.savefig(fig)
        plt.close(fig)
        print("  Page 2: Production Performance")

        # === PAGE 3: Spacing Impact ===
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle(f"Spacing Impact — {operator}\n{fm_label}", fontsize=13, fontweight="bold")

        cum12m = pd.to_numeric(df["Cum12MBOE"], errors="coerce")

        # 3a: Child vs Parent Cum12M BOE
        ax = axes[0, 0]
        is_child = df["IsChild"]
        child_vals = cum12m[is_child == True].dropna()
        parent_vals = cum12m[is_child == False].dropna()
        if len(child_vals) >= 3 and len(parent_vals) >= 3:
            bp = ax.boxplot(
                [parent_vals.values, child_vals.values],
                labels=[f"Parent (n={len(parent_vals)})", f"Child (n={len(child_vals)})"],
                patch_artist=True,
            )
            bp["boxes"][0].set_facecolor("steelblue")
            bp["boxes"][1].set_facecolor("firebrick")
        else:
            ax.text(0.5, 0.5, "Insufficient child/parent\ndata for comparison",
                    ha="center", va="center", transform=ax.transAxes)
        ax.set_title("Child vs Parent — Cum 12M BOE")
        ax.set_ylabel("Cum 12M BOE")
        ax.grid(axis="y", alpha=0.3)

        # 3b: Boundedness Score vs Cum12M BOE
        ax = axes[0, 1]
        bound = pd.to_numeric(df["BoundednessScore"], errors="coerce")
        scatter_mask = bound.notna() & cum12m.notna()
        if scatter_mask.sum() >= 3:
            ax.scatter(bound[scatter_mask], cum12m[scatter_mask], s=20, alpha=0.6,
                      color="teal", edgecolor="black", linewidth=0.2)
        ax.set_title("Boundedness Score vs Cum 12M BOE")
        ax.set_xlabel("Boundedness Score")
        ax.set_ylabel("Cum 12M BOE")
        ax.grid(alpha=0.3)

        # 3c: Closest Well Distance vs Cum12M BOE
        ax = axes[1, 0]
        closest = pd.to_numeric(df["ClosestWellXY"], errors="coerce")
        scatter_mask = closest.notna() & cum12m.notna()
        if scatter_mask.sum() >= 3:
            ax.scatter(closest[scatter_mask], cum12m[scatter_mask], s=20, alpha=0.6,
                      color="darkorange", edgecolor="black", linewidth=0.2)
        ax.set_title("Closest Well Distance vs Cum 12M BOE")
        ax.set_xlabel("Closest Well (ft)")
        ax.set_ylabel("Cum 12M BOE")
        ax.grid(alpha=0.3)

        # 3d: Wells in Radius vs Cum12M BOE
        ax = axes[1, 1]
        wir = pd.to_numeric(df["WellsInRadius"], errors="coerce")
        scatter_mask = wir.notna() & cum12m.notna()
        if scatter_mask.sum() >= 3:
            ax.scatter(wir[scatter_mask], cum12m[scatter_mask], s=20, alpha=0.6,
                      color="purple", edgecolor="black", linewidth=0.2)
        ax.set_title("Wells in Radius vs Cum 12M BOE")
        ax.set_xlabel("Wells in Radius")
        ax.set_ylabel("Cum 12M BOE")
        ax.grid(alpha=0.3)

        fig.tight_layout(rect=[0, 0, 1, 0.93])
        pdf.savefig(fig)
        plt.close(fig)
        print("  Page 3: Spacing Impact")

        # === PAGE 4: Frac Type Analysis ===
        if "FracType" in df.columns:
            fig, axes = plt.subplots(1, 2, figsize=(14, 7))
            fig.suptitle(f"Frac Type Analysis — {operator}\n{fm_label}", fontsize=13, fontweight="bold")

            frac_colors = {"Slickwater": "steelblue", "Gel/Hybrid": "firebrick", "Unknown": "lightgray"}

            # 4a: Frac type breakdown by vintage (stacked bar)
            ax = axes[0]
            frac_by_year = df.groupby(["Vintage", "FracType"]).size().unstack(fill_value=0)
            # Reorder columns for consistent stacking
            frac_cols = [c for c in ["Slickwater", "Gel/Hybrid", "Unknown"] if c in frac_by_year.columns]
            if frac_cols:
                frac_by_year = frac_by_year[frac_cols]
                frac_by_year.plot(kind="bar", stacked=True, ax=ax,
                                  color=[frac_colors.get(c, "gray") for c in frac_cols],
                                  edgecolor="black", linewidth=0.3)
                ax.legend(fontsize=8)
            ax.set_title("Frac Type by Vintage Year")
            ax.set_xlabel("First Production Year")
            ax.set_ylabel("Well Count")
            ax.grid(axis="y", alpha=0.3)

            # 4b: Cum12M BOE by frac type (box plot)
            ax = axes[1]
            months_on_ft = pd.to_numeric(df["LastReportedMonthsOnProduction"], errors="coerce")
            df_ft = df[months_on_ft >= 12]
            frac_types_present = [ft for ft in ["Slickwater", "Gel/Hybrid"] if ft in df_ft["FracType"].values]
            if len(frac_types_present) >= 1:
                box_data = [pd.to_numeric(df_ft[df_ft["FracType"] == ft]["Cum12MBOE"], errors="coerce").dropna().values
                            for ft in frac_types_present]
                box_labels = [f"{ft}\n(n={len(d)})" for ft, d in zip(frac_types_present, box_data)]
                bp = ax.boxplot(box_data, labels=box_labels, patch_artist=True)
                for patch, ft in zip(bp["boxes"], frac_types_present):
                    patch.set_facecolor(frac_colors.get(ft, "gray"))
            else:
                ax.text(0.5, 0.5, "Insufficient data", ha="center", va="center", transform=ax.transAxes)
            ax.set_title("Cum 12M BOE by Frac Type")
            ax.set_ylabel("Cum 12M BOE")
            ax.grid(axis="y", alpha=0.3)

            fig.tight_layout(rect=[0, 0, 1, 0.91])
            pdf.savefig(fig)
            plt.close(fig)
            print("  Page 4: Frac Type Analysis")

        # === PEER COMPARISON PAGES ===
        if peerData is not None and not peerData.empty:
            peer = peerData.copy()
            peer["Vintage"] = pd.to_numeric(peer["FirstProductionYear"], errors="coerce")
            peer = peer[peer["Vintage"].notna() & (peer["Vintage"] >= 2020)].copy()
            peer["Vintage"] = peer["Vintage"].astype(int)

            # Only include active operators with 20+ wells since 2020
            op_counts = peer["CurrentOperator"].value_counts()
            valid_ops = op_counts[op_counts >= 20].index.tolist()
            peer = peer[peer["CurrentOperator"].isin(valid_ops)].copy()

            if len(valid_ops) >= 2:
                # Highlight the AFE operator
                afe_operator = operator.upper()

                def _op_colors(ops):
                    colors = []
                    for op in ops:
                        if op.upper() == afe_operator.upper():
                            colors.append("steelblue")
                        else:
                            colors.append("lightgray")
                    return colors

                def _op_short(name, maxlen=20):
                    return name if len(name) <= maxlen else name[:maxlen - 1] + "…"

                # === PAGE 4: Peer EUR & Production Comparison ===
                fig, axes = plt.subplots(2, 2, figsize=(14, 10))
                fig.suptitle(f"Peer Comparison — {fm_label}\n(5-mile radius, 20+ wells since 2020)",
                             fontsize=13, fontweight="bold")

                # 4a: EUR 50yr BOE/ft by operator
                ax = axes[0, 0]
                eur_ft = pd.to_numeric(peer["EUR50YRBOEPerFt"], errors="coerce")
                grouped = peer.assign(_val=eur_ft).groupby("CurrentOperator")["_val"]
                means = grouped.mean().dropna().sort_values(ascending=False)
                if not means.empty:
                    labels = [_op_short(op) for op in means.index]
                    colors = _op_colors(means.index)
                    counts = grouped.count()
                    ax.barh(range(len(means)), means.values, color=colors, edgecolor="black", linewidth=0.3)
                    ax.set_yticks(range(len(means)))
                    ax.set_yticklabels(labels, fontsize=8)
                    for i, (val, op) in enumerate(zip(means.values, means.index)):
                        ax.annotate(f"n={counts[op]}", xy=(val, i), va="center", fontsize=7, color="#444444")
                    ax.invert_yaxis()
                ax.set_title("EUR 50yr BOE / Lateral Foot")
                ax.set_xlabel("BOE/ft")
                ax.grid(axis="x", alpha=0.3)

                # 4b: Cum12M BOE/ft by operator (only wells with 12+ months)
                ax = axes[0, 1]
                months_on_peer = pd.to_numeric(peer["LastReportedMonthsOnProduction"], errors="coerce")
                peer_12m = peer[months_on_peer >= 12]
                cum_ft = pd.to_numeric(peer_12m["Cum12MBOEPerFt"], errors="coerce")
                grouped = peer_12m.assign(_val=cum_ft).groupby("CurrentOperator")["_val"]
                means = grouped.mean().dropna().sort_values(ascending=False)
                if not means.empty:
                    labels = [_op_short(op) for op in means.index]
                    colors = _op_colors(means.index)
                    counts = grouped.count()
                    ax.barh(range(len(means)), means.values, color=colors, edgecolor="black", linewidth=0.3)
                    ax.set_yticks(range(len(means)))
                    ax.set_yticklabels(labels, fontsize=8)
                    for i, (val, op) in enumerate(zip(means.values, means.index)):
                        ax.annotate(f"n={counts[op]}", xy=(val, i), va="center", fontsize=7, color="#444444")
                    ax.invert_yaxis()
                ax.set_title("Cum 12M BOE / Lateral Foot")
                ax.set_xlabel("BOE/ft")
                ax.grid(axis="x", alpha=0.3)

                # 4c: Peak month BOE rate by operator
                ax = axes[1, 0]
                peak = pd.to_numeric(peer["PeakMonthBOERate"], errors="coerce")
                grouped = peer.assign(_val=peak).groupby("CurrentOperator")["_val"]
                means = grouped.mean().dropna().sort_values(ascending=False)
                if not means.empty:
                    labels = [_op_short(op) for op in means.index]
                    colors = _op_colors(means.index)
                    counts = grouped.count()
                    ax.barh(range(len(means)), means.values, color=colors, edgecolor="black", linewidth=0.3)
                    ax.set_yticks(range(len(means)))
                    ax.set_yticklabels(labels, fontsize=8)
                    for i, (val, op) in enumerate(zip(means.values, means.index)):
                        ax.annotate(f"n={counts[op]}", xy=(val, i), va="center", fontsize=7, color="#444444")
                    ax.invert_yaxis()
                ax.set_title("Avg Peak Month BOE Rate")
                ax.set_xlabel("BOE/month")
                ax.grid(axis="x", alpha=0.3)

                # 4d: GOR by operator
                ax = axes[1, 1]
                gor = pd.to_numeric(peer["CumLifeGOR"], errors="coerce")
                grouped = peer.assign(_val=gor).groupby("CurrentOperator")["_val"]
                means = grouped.mean().dropna().sort_values(ascending=False)
                if not means.empty:
                    labels = [_op_short(op) for op in means.index]
                    colors = _op_colors(means.index)
                    counts = grouped.count()
                    ax.barh(range(len(means)), means.values, color=colors, edgecolor="black", linewidth=0.3)
                    ax.set_yticks(range(len(means)))
                    ax.set_yticklabels(labels, fontsize=8)
                    for i, (val, op) in enumerate(zip(means.values, means.index)):
                        ax.annotate(f"n={counts[op]}", xy=(val, i), va="center", fontsize=7, color="#444444")
                    ax.invert_yaxis()
                ax.set_title("Avg Cum Life GOR")
                ax.set_xlabel("GOR (scf/bbl)")
                ax.grid(axis="x", alpha=0.3)

                fig.tight_layout(rect=[0, 0, 1, 0.91])
                pdf.savefig(fig)
                plt.close(fig)
                print("  Page 4: Peer EUR & Production Comparison")

                # === PAGE 5: Peer Completion Design Comparison ===
                fig, axes = plt.subplots(2, 2, figsize=(14, 10))
                fig.suptitle(f"Peer Completion Design — {fm_label}\n(5-mile radius, 20+ wells since 2020)",
                             fontsize=13, fontweight="bold")

                # 5a: Avg lateral length by operator
                ax = axes[0, 0]
                ll = pd.to_numeric(peer["LateralLength"], errors="coerce")
                grouped = peer.assign(_val=ll).groupby("CurrentOperator")["_val"]
                means = grouped.mean().dropna().sort_values(ascending=False)
                if not means.empty:
                    labels = [_op_short(op) for op in means.index]
                    colors = _op_colors(means.index)
                    counts = grouped.count()
                    ax.barh(range(len(means)), means.values, color=colors, edgecolor="black", linewidth=0.3)
                    ax.set_yticks(range(len(means)))
                    ax.set_yticklabels(labels, fontsize=8)
                    for i, (val, op) in enumerate(zip(means.values, means.index)):
                        ax.annotate(f"n={counts[op]}", xy=(val, i), va="center", fontsize=7, color="#444444")
                    ax.invert_yaxis()
                ax.set_title("Avg Lateral Length (ft)")
                ax.set_xlabel("Lateral Length (ft)")
                ax.grid(axis="x", alpha=0.3)

                # 5b: Avg proppant lbs/ft by operator
                ax = axes[0, 1]
                prop = pd.to_numeric(peer["ProppantLbsPerFt"], errors="coerce")
                grouped = peer.assign(_val=prop).groupby("CurrentOperator")["_val"]
                means = grouped.mean().dropna().sort_values(ascending=False)
                if not means.empty:
                    labels = [_op_short(op) for op in means.index]
                    colors = _op_colors(means.index)
                    counts = grouped.count()
                    ax.barh(range(len(means)), means.values, color=colors, edgecolor="black", linewidth=0.3)
                    ax.set_yticks(range(len(means)))
                    ax.set_yticklabels(labels, fontsize=8)
                    for i, (val, op) in enumerate(zip(means.values, means.index)):
                        ax.annotate(f"n={counts[op]}", xy=(val, i), va="center", fontsize=7, color="#444444")
                    ax.invert_yaxis()
                ax.set_title("Avg Proppant (lbs/ft)")
                ax.set_xlabel("lbs/ft")
                ax.grid(axis="x", alpha=0.3)

                # 5c: Avg fluid bbl/ft by operator
                ax = axes[1, 0]
                fluid = pd.to_numeric(peer["FluidBblPerFt"], errors="coerce")
                grouped = peer.assign(_val=fluid).groupby("CurrentOperator")["_val"]
                means = grouped.mean().dropna().sort_values(ascending=False)
                if not means.empty:
                    labels = [_op_short(op) for op in means.index]
                    colors = _op_colors(means.index)
                    counts = grouped.count()
                    ax.barh(range(len(means)), means.values, color=colors, edgecolor="black", linewidth=0.3)
                    ax.set_yticks(range(len(means)))
                    ax.set_yticklabels(labels, fontsize=8)
                    for i, (val, op) in enumerate(zip(means.values, means.index)):
                        ax.annotate(f"n={counts[op]}", xy=(val, i), va="center", fontsize=7, color="#444444")
                    ax.invert_yaxis()
                ax.set_title("Avg Fluid (bbl/ft)")
                ax.set_xlabel("bbl/ft")
                ax.grid(axis="x", alpha=0.3)

                # 5d: Proppant/Fluid ratio by operator
                ax = axes[1, 1]
                prop_raw = pd.to_numeric(peer["FirstCompletionProppantMass"], errors="coerce")
                fluid_raw = pd.to_numeric(peer["FirstCompletionFluidVolume"], errors="coerce") / 42.0
                prop_fluid = prop_raw / fluid_raw.replace(0, float("nan"))
                grouped = peer.assign(_val=prop_fluid).groupby("CurrentOperator")["_val"]
                means = grouped.mean().dropna().sort_values(ascending=False)
                if not means.empty:
                    labels = [_op_short(op) for op in means.index]
                    colors = _op_colors(means.index)
                    counts = grouped.count()
                    ax.barh(range(len(means)), means.values, color=colors, edgecolor="black", linewidth=0.3)
                    ax.set_yticks(range(len(means)))
                    ax.set_yticklabels(labels, fontsize=8)
                    for i, (val, op) in enumerate(zip(means.values, means.index)):
                        ax.annotate(f"n={counts[op]}", xy=(val, i), va="center", fontsize=7, color="#444444")
                    ax.invert_yaxis()
                ax.set_title("Avg Proppant/Fluid Ratio (lbs/bbl)")
                ax.set_xlabel("lbs/bbl")
                ax.grid(axis="x", alpha=0.3)

                fig.tight_layout(rect=[0, 0, 1, 0.91])
                pdf.savefig(fig)
                plt.close(fig)
                print("  Page 5: Peer Completion Design Comparison")

                # === PAGE 6: Peer Frac Type Comparison ===
                if "FracType" in peer.columns:
                    fig, axes = plt.subplots(1, 2, figsize=(14, 7))
                    fig.suptitle(f"Peer Frac Type Comparison — {fm_label}\n(5-mile radius, 20+ wells since 2020)",
                                 fontsize=13, fontweight="bold")

                    frac_colors = {"Slickwater": "steelblue", "Gel/Hybrid": "firebrick", "Unknown": "lightgray"}

                    # 6a: Frac type % by operator (stacked horizontal bar)
                    ax = axes[0]
                    frac_pct = peer.groupby(["CurrentOperator", "FracType"]).size().unstack(fill_value=0)
                    frac_cols = [c for c in ["Slickwater", "Gel/Hybrid", "Unknown"] if c in frac_pct.columns]
                    if frac_cols:
                        frac_pct = frac_pct[frac_cols]
                        frac_pct_norm = frac_pct.div(frac_pct.sum(axis=1), axis=0) * 100
                        # Sort by slickwater % descending
                        sort_col = "Slickwater" if "Slickwater" in frac_pct_norm.columns else frac_cols[0]
                        frac_pct_norm = frac_pct_norm.sort_values(sort_col, ascending=True)
                        labels = [_op_short(op) for op in frac_pct_norm.index]
                        frac_pct_norm.index = labels
                        frac_pct_norm.plot(kind="barh", stacked=True, ax=ax,
                                           color=[frac_colors.get(c, "gray") for c in frac_cols],
                                           edgecolor="black", linewidth=0.3)
                        # Add well count annotations
                        totals = frac_pct.sum(axis=1)
                        for i_bar, (op, total) in enumerate(zip(frac_pct.index, totals)):
                            short = _op_short(op)
                            idx = list(frac_pct_norm.index).index(short) if short in frac_pct_norm.index else None
                            if idx is not None:
                                ax.annotate(f"n={total}", xy=(101, idx), va="center", fontsize=7, color="#444444")
                        ax.legend(fontsize=8, loc="lower right")
                    ax.set_title("Frac Type Mix by Operator (%)")
                    ax.set_xlabel("% of Wells")
                    ax.set_xlim(0, 115)

                    # 6b: Cum12M BOE by frac type across all peers (box plot)
                    ax = axes[1]
                    months_on_peer2 = pd.to_numeric(peer["LastReportedMonthsOnProduction"], errors="coerce")
                    peer_12m2 = peer[months_on_peer2 >= 12]
                    frac_types_present = [ft for ft in ["Slickwater", "Gel/Hybrid"]
                                          if ft in peer_12m2["FracType"].values]
                    if len(frac_types_present) >= 1:
                        box_data = [pd.to_numeric(peer_12m2[peer_12m2["FracType"] == ft]["Cum12MBOE"],
                                    errors="coerce").dropna().values for ft in frac_types_present]
                        box_labels = [f"{ft}\n(n={len(d)})" for ft, d in zip(frac_types_present, box_data)]
                        bp = ax.boxplot(box_data, labels=box_labels, patch_artist=True)
                        for patch, ft in zip(bp["boxes"], frac_types_present):
                            patch.set_facecolor(frac_colors.get(ft, "gray"))
                    else:
                        ax.text(0.5, 0.5, "Insufficient data", ha="center", va="center", transform=ax.transAxes)
                    ax.set_title("Cum 12M BOE by Frac Type (All Peers)")
                    ax.set_ylabel("Cum 12M BOE")
                    ax.grid(axis="y", alpha=0.3)

                    fig.tight_layout(rect=[0, 0, 1, 0.91])
                    pdf.savefig(fig)
                    plt.close(fig)
                    print("  Page 6: Peer Frac Type Comparison")

            else:
                print("  Skipping peer pages: fewer than 2 operators with 20+ wells since 2020 in area")

    print(f"Done. Saved operator analysis to {pdfPath}")
