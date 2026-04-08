## Novi API Functions - Authentication, Permit Lookup, Offset Well Search, EUR Forecasting, and Data Export
## Developed by Michael Tanner

import requests
import os
import math
import difflib
import pandas as pd


BASE_URL = "https://insight.novilabs.com/api/"


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
## Tries the local bulk export first, falls back to the API only when local returns zero matches.
## Why hybrid: bulk export refreshes weekly, so a permit dropping mid-week (e.g. Tuesday)
## won't be in the local TSV until the next refresh. API fallback catches brand-new wells.
## Texas wells: matched by API10. Non-Texas: matched by ID (Novi's standard permit ID field).
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
    api_fallback_count = 0

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

        # Zero local matches — fall back to API for this well in case the permit dropped
        # after the last bulk export refresh
        print(f"  No local match — falling back to API")
        api_fallback_count += 1

        if is_texas:
            params = {
                "authentication_token": token,
                "scope": scope,
                "q[API10_eq]": api_number,
                "q[County_eq]": county,
                "q[State_eq]": state,
            }
        else:
            params = {
                "authentication_token": token,
                "scope": scope,
                "q[ID_eq]": api_number,
                "q[County_eq]": county,
                "q[State_eq]": state,
            }

        # Paginate through API permit results for this well
        page = 1
        well_permits_api = []
        while True:
            params["page"] = page
            print(f"  Fetching API page {page}...")

            for attempt in range(1, 6):
                try:
                    response = requests.get(BASE_URL + "v3/well_permits.json", params=params, timeout=300)
                    response.raise_for_status()
                    break
                except requests.exceptions.ReadTimeout:
                    print(f"  Request timed out (attempt {attempt}/5), retrying...")
                    if attempt == 5:
                        raise

            data = response.json()
            if not data:
                break

            well_permits_api.extend(data)
            print(f"  API page {page} returned {len(data)} permits ({len(well_permits_api)} total so far)")

            if len(data) < 100:
                break

            page += 1

        if well_permits_api:
            all_permits.append(pd.DataFrame(well_permits_api))

    # Concatenate all matches (mix of DataFrames from local + API)
    if all_permits:
        permitDf = pd.concat(all_permits, ignore_index=True)
    else:
        permitDf = pd.DataFrame()

    print(f"\nDone. Retrieved {len(permitDf)} well permits total ({api_fallback_count} wells fell back to API).")

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
    import urllib.request
    import urllib.error
    import geopandas as gpd

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
    MAX_PAGES = 20  # safety cap — 20k features is far more than any reasonable AFE bbox

    def _fetch_layer(layer_id, label):
        cachePath = os.path.join(plssDir, f"plss_{label}_{bbox_str}.geojson")
        if os.path.exists(cachePath):
            try:
                gdf = gpd.read_file(cachePath)
                print(f"  PLSS {label}: {len(gdf)} features (cache hit)")
                return gdf
            except Exception as e:
                print(f"  PLSS {label}: cache file corrupt ({e}), refetching")

        all_features = []
        offset = 0
        for page in range(MAX_PAGES):
            url = (
                f"{BASE}/{layer_id}/query"
                f"?where=1=1"
                f"&geometry={rl_min},{rt_min},{rl_max},{rt_max}"
                f"&geometryType=esriGeometryEnvelope"
                f"&inSR=4326&outSR=4326"
                f"&outFields=*"
                f"&f=geojson"
                f"&resultRecordCount={PAGE_SIZE}"
                f"&resultOffset={offset}"
            )
            try:
                with urllib.request.urlopen(url, timeout=60) as r:
                    data = json.loads(r.read())
            except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
                print(f"  PLSS {label}: BLM service unreachable ({e}), skipping layer")
                return gpd.GeoDataFrame()

            page_features = data.get("features", [])
            if not page_features:
                break
            all_features.extend(page_features)
            if len(page_features) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        if not all_features:
            print(f"  PLSS {label}: 0 features in bbox (likely non-PLSS area)")
            return gpd.GeoDataFrame()

        # Reassemble into a FeatureCollection and save to cache
        fc = {"type": "FeatureCollection", "features": all_features}
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
    plss_townships, plss_sections = _fetchPlssLayers(lon_min, lat_min, lon_max, lat_max)

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
    STATE_ABBR_MAP = {
        "new mexico": "NM", "colorado": "CO", "wyoming": "WY", "north dakota": "ND",
        "montana": "MT", "ohio": "OH", "pennsylvania": "PA", "texas": "TX",
        "south dakota": "SD", "oklahoma": "OK", "kansas": "KS", "louisiana": "LA",
        "california": "CA", "utah": "UT", "alaska": "AK",
    }

    def _afe_letter(i):
        if i < 26:
            return chr(ord("A") + i)
        return chr(ord("A") + (i // 26 - 1)) + chr(ord("A") + (i % 26))

    numbered_permits = []  # list of (letter, well_name) for the legend, in AFE row order
    dsu_section_groups = []  # list of (letter, well_name, sections_gdf) for rendering boxes
    permit_fallbacks = []  # list of (letter, well_name, lon, lat) — TX/PA/missing-T-R-S wells

    if afeData is not None and not afeData.empty:
        print(f"Matching {len(afeData)} AFE rows to PLSS sections...")
        for i, (_idx, row) in enumerate(afeData.iterrows()):
            letter = _afe_letter(i)
            well_name = str(row.get("Well Name", f"Well {i + 1}")).strip()
            numbered_permits.append((letter, well_name))

            state_full = str(row.get("State", "")).strip().lower()
            twp_str = str(row.get("Township", "")).strip()
            rng_str = str(row.get("Range", "")).strip()
            sec_str = str(row.get("Section", "")).strip()
            state_abbr = STATE_ABBR_MAP.get(state_full)

            matched_sections = None
            if (
                state_abbr
                and twp_str and rng_str and sec_str
                and plss_townships is not None and not plss_townships.empty
                and plss_sections is not None and not plss_sections.empty
            ):
                # Parse "18S" → ("018", "S"), "30E" → ("030", "E")
                twp_no = "".join(c for c in twp_str if c.isdigit()).zfill(3)
                twp_dir = "".join(c for c in twp_str if c.isalpha()).upper()
                rng_no = "".join(c for c in rng_str if c.isdigit()).zfill(3)
                rng_dir = "".join(c for c in rng_str if c.isalpha()).upper()

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
                    permit_fallbacks.append((letter, well_name, fallback_lon, fallback_lat))
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

    # Build operator → color map from offsetData. Most-common operators get the most distinct
    # tab20 colors; rare operators (rank > 19) collapse to a single gray "Other" bucket so the
    # legend stays compact and the map stays readable.
    import matplotlib.cm as cm
    operator_counts = {}
    for op in api10_to_operator.values():
        if op:
            operator_counts[op] = operator_counts.get(op, 0) + 1
    operators_ranked = sorted(operator_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    OTHER_COLOR = "#888888"
    UNKNOWN_COLOR = "#444444"
    MAX_OPERATOR_COLORS = 19
    tab20 = cm.get_cmap("tab20", 20)
    operator_to_color = {}
    for i, (op, _cnt) in enumerate(operators_ranked):
        if i < MAX_OPERATOR_COLORS:
            operator_to_color[op] = tab20(i)
        else:
            operator_to_color[op] = OTHER_COLOR
    api10_to_color = {}
    for api10 in api10_to_name.keys():
        op = api10_to_operator.get(api10)
        if op and op in operator_to_color:
            api10_to_color[api10] = operator_to_color[op]
        else:
            api10_to_color[api10] = UNKNOWN_COLOR
    operator_legend_entries = [(op, operator_to_color[op], cnt) for op, cnt in operators_ranked[:MAX_OPERATOR_COLORS]]
    other_count = sum(cnt for _op, cnt in operators_ranked[MAX_OPERATOR_COLORS:])
    if other_count:
        operator_legend_entries.append((f"Other ({len(operators_ranked) - MAX_OPERATOR_COLORS} ops)", OTHER_COLOR, other_count))

    print(f"Generating heat map PDF: {pdfPath}")
    with PdfPages(pdfPath) as pdf:
        for param in parameters:
            if param not in df.columns:
                print(f"  Skipping {param}: column not in subsurface data")
                continue

            values = pd.to_numeric(df[param], errors="coerce")
            mask = values.notna()
            if mask.sum() < 4:
                print(f"  Skipping {param}: only {mask.sum()} valid values (need >= 4 for interpolation)")
                continue

            # Linear interpolation — NaN outside the convex hull (no dishonest extrapolation)
            interp = griddata(
                points[mask],
                values[mask].values,
                (grid_lon, grid_lat),
                method="linear",
            )

            # Wider figure when we have a numbered-well legend to render on the right
            if numbered_wells:
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
            #  5. Wellbore lateral paths
            #  6. PLSS township boundaries (brown)
            #  7. State boundaries (black)
            #  8. Offset well markers (small black dots)
            #  9. AFE permit stars (red)
            # 10. PLSS section number labels
            # 11. PLSS township T/R labels
            # 12. AFE permit name labels
            # All labels are at the top of the stack so they're never covered by data layers.

            if all_clip is not None and not all_clip.empty:
                all_clip.plot(ax=ax, facecolor="#f0f0f0", edgecolor="#cccccc", linewidth=0.2, zorder=1)
            if not counties_clip.empty:
                counties_clip.boundary.plot(ax=ax, color="#999999", linewidth=0.4, zorder=2)

            if plss_sections is not None and not plss_sections.empty:
                plss_sections.boundary.plot(ax=ax, color="#bbbbbb", linewidth=0.3, alpha=0.4, zorder=3)

            cf = ax.contourf(grid_lon, grid_lat, interp, levels=20, cmap="viridis", zorder=4, alpha=0.85)

            # Offset well lateral paths — colored by operator (most common operators get
            # tab20 colors, rare ones collapse to a single "Other" gray)
            if wb_groups is not None:
                for api10, group in wb_groups:
                    ax.plot(
                        group["Longitude"].values,
                        group["Latitude"].values,
                        color=api10_to_color.get(api10, UNKNOWN_COLOR),
                        linewidth=1.2,
                        alpha=0.85,
                        zorder=5,
                    )

            if plss_townships is not None and not plss_townships.empty:
                plss_townships.boundary.plot(ax=ax, color="#8b6914", linewidth=0.7, alpha=0.7, zorder=6)

            if not states_clip.empty:
                states_clip.boundary.plot(ax=ax, color="black", linewidth=1.0, zorder=7)

            ax.scatter(
                df["Longitude"][mask], df["Latitude"][mask],
                c="black", s=6, alpha=0.6, zorder=8, label="Offset Wells",
            )

            # AFE DSU highlight — bold black perimeter around the union of all DSU sections.
            # For multi-section DSUs (e.g. "9,10") the unary_union drops internal shared edges
            # so the box outlines only the outer perimeter of the combined sections.
            from shapely.ops import unary_union
            for _letter, _name, sec_gdf in dsu_section_groups:
                union_geom = unary_union(list(sec_gdf.geometry))
                gpd.GeoSeries([union_geom], crs=sec_gdf.crs).boundary.plot(
                    ax=ax, color="black", linewidth=2.2, zorder=9,
                )
            # Star fallback for AFE rows with no PLSS section match (TX, PA, etc.)
            if permit_fallbacks:
                fb_lon = [f[2] for f in permit_fallbacks]
                fb_lat = [f[3] for f in permit_fallbacks]
                ax.scatter(
                    fb_lon, fb_lat,
                    marker="*", c="red", s=180, edgecolor="black", linewidth=0.8,
                    zorder=9, label="AFE Permit (no PLSS)",
                )

            # === Labels (all on top of data layers) ===

            # Section number labels — small, light gray, thin halo
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

            # Township T/R labels — bold brown, thicker halo
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

            # Offset well numbers — drawn at the heel of each lateral, styled as a small
            # navy-on-white boxed badge so they read as clearly distinct from the gray PLSS
            # section numbers (which are unboxed, centered, and lighter).
            # Falls back to MPLatitude/MPLongitude if a well has no wellbore trajectory data.
            if numbered_wells:
                num_box = dict(
                    boxstyle="round,pad=0.15",
                    facecolor="white",
                    edgecolor="#0a2a5e",
                    linewidth=0.5,
                )
                for number, api10, _name, _op, _dmi, _fp in numbered_wells:
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

            # AFE letter labels — only rendered for non-PLSS fallback stars (TX, PA).
            # The DSU section boxes themselves are left unlabeled per user request — the
            # box alone identifies the DSU, and the side legend maps letters to well names.
            permit_halo = [path_effects.withStroke(linewidth=2.5, foreground="white")]
            for letter, _name, lon, lat in permit_fallbacks:
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
            ax.set_title(f"{param} — {mask.sum():,} wells")
            ax.legend(loc="upper right", fontsize=8)
            ax.set_aspect("equal", adjustable="box")
            cbar = plt.colorbar(cf, ax=ax, shrink=0.8)
            cbar.set_label(param)

            # Side legend: AFE permits at top (bold), offset wells below.
            # Stack is dynamically centered vertically around y=0.5 (right-middle of the figure).
            # Permits use uppercase letters (A, B, C...) bold; offsets use lowercase letters (a, b, c...).
            MAX_NAME_LEN = 38
            PERMIT_LINE_H = 0.018  # approximate fig-y units per 7pt monospace line
            OFFSET_LINE_H = 0.016  # approximate fig-y units per 6.5pt monospace line
            STACK_GAP = 0.04

            permit_box_h = ((len(numbered_permits) + 2) * PERMIT_LINE_H) if numbered_permits else 0
            offset_box_h = ((len(numbered_wells) + 2) * OFFSET_LINE_H) if numbered_wells else 0
            total_h = permit_box_h + (STACK_GAP if (permit_box_h and offset_box_h) else 0) + offset_box_h
            stack_top = 0.5 + total_h / 2

            permit_y_top = stack_top  # top of the permits box
            if numbered_permits:
                permit_lines = ["AFE Permits", "-" * 32]
                for letter, name in numbered_permits:
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

            if numbered_wells:
                # Position the offset table directly below the permits box (with gap).
                # Wells are numbered 1..N by distance from the AFE permit centroid; numbers
                # match the small navy badges drawn at each lateral's heel on the map.
                offset_y_top = stack_top - permit_box_h - (STACK_GAP if permit_box_h else 0)
                offset_lines = [f"Offset Wells (by distance, n={len(numbered_wells)})", "-" * 32]
                num_width = len(str(len(numbered_wells)))
                for number, _api10, name, _op, _dmi, _fp in numbered_wells:
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
            print(f"  Plotted {param} ({mask.sum():,} wells)")

        # === Appendix pages ===
        # Full offset well index (number → name → operator → API10 → distance → first prod year)
        # plus the operator color key. Rendered as multi-page tables, ~50 rows per page.
        if numbered_wells:
            ROWS_PER_PAGE = 50
            COL_HEADERS = ["#", "Well Name", "Operator", "API10", "Dist (mi)", "1st Prod"]
            COL_WIDTHS = [0.05, 0.40, 0.27, 0.13, 0.08, 0.07]  # fractions of table width

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
                for number, api10, name, op, dmi, fp in page_rows:
                    table_data.append([
                        str(number),
                        _truncate(name, 48),
                        _truncate(op, 32),
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

                # Color the operator cell background by the operator color (subtle tint)
                for row_i, (_n, _a, _name, op, _d, _fp) in enumerate(page_rows, start=1):
                    color = operator_to_color.get(op)
                    if color is not None:
                        cell = table[(row_i, 2)]  # operator column
                        # Lighten by blending with white at 70% white
                        try:
                            r, g, b = color[0], color[1], color[2]
                            tint = (0.7 + 0.3 * r, 0.7 + 0.3 * g, 0.7 + 0.3 * b)
                            cell.set_facecolor(tint)
                        except (TypeError, IndexError):
                            pass

                pdf.savefig(fig, bbox_inches="tight")
                plt.close(fig)

            # Operator color key page (compact, fits on one page even with 19 entries)
            if operator_legend_entries:
                from matplotlib.lines import Line2D
                fig = plt.figure(figsize=(11, 8.5))
                ax = fig.add_subplot(111)
                ax.axis("off")
                ax.set_title(
                    f"Operator Color Key — {dsuName}",
                    fontsize=12, fontweight="bold", loc="left", pad=14,
                )
                handles = [
                    Line2D([0], [0], color=color, linewidth=3.0,
                           label=f"{op[:50]}{'...' if len(op) > 50 else ''}  —  {cnt} well(s)")
                    for op, color, cnt in operator_legend_entries
                ]
                ax.legend(
                    handles=handles,
                    loc="upper left",
                    fontsize=10,
                    frameon=False,
                    handlelength=3.0,
                    labelspacing=0.7,
                )
                pdf.savefig(fig, bbox_inches="tight")
                plt.close(fig)
                print(f"  Appended operator color key page ({len(operator_legend_entries)} entries)")

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
