### Deal Evaluation Review Sheet ingestion
### Flattens the SHLP25 and SHLP26 tabs of the Deal Evaluation Review Sheet xlsm
### into a single long-format Excel file consumable by Power BI.
### Developed by Michael Tanner

import os
import pandas as pd
import openpyxl


## Parse a single deal sheet tab into a long-format DataFrame
## Each tab is a stack of status sections (Won / In Review / Approved - Lost / Killed),
## each with its own repeated "Operator..." header row. Section headers live in col A
## with col B empty; data rows live under the most recent "Operator" header.
## - ws: openpyxl worksheet
## - fundName: string label written into the "Fund" column (e.g. "SHLP25")
def _parseDealTab(ws, fundName):
    # Materialize all rows so we can look ahead when classifying ambiguous lines
    allRows = list(ws.iter_rows(values_only=True))

    def _isOperatorHeader(row):
        return row and isinstance(row[0], str) and row[0].strip() == "Operator"

    records = []
    currentSection = None
    currentHeaders = None  # trimmed at first None — anything past is summary noise

    for idx, row in enumerate(allRows):
        if not row or all(c is None for c in row):
            continue

        a = row[0]
        b = row[1] if len(row) > 1 else None

        # Header row — capture column names up to the first None / non-string cell.
        # Header rows often have stray summary cells to the right (e.g. "Total Won",
        # raw sum values) that we don't want as column names.
        if isinstance(a, str) and a.strip() == "Operator":
            headers = []
            for c in row:
                if c is None or not isinstance(c, str):
                    break
                headers.append(c.strip())
            currentHeaders = headers
            continue

        # Skip section subtotal rows
        if isinstance(a, str) and a.strip() == "Total":
            continue

        # Possible section header — col A string, col B empty, not Operator/Total.
        # Confirm by looking ahead: a true section header is followed by an "Operator"
        # header row within the next 3 non-empty rows. Without that, it's just a
        # sparse data row whose only populated cell is the operator name.
        if isinstance(a, str) and b is None:
            looksLikeSection = False
            seen = 0
            for j in range(idx + 1, min(idx + 6, len(allRows))):
                nxt = allRows[j]
                if not nxt or all(c is None for c in nxt):
                    continue
                seen += 1
                if _isOperatorHeader(nxt):
                    looksLikeSection = True
                    break
                if seen >= 3:
                    break
            if looksLikeSection:
                currentSection = a.strip()
                continue
            # else: fall through and treat as a data row

        # Data row — needs an active header and a real operator value in col A
        if currentHeaders is None or a is None:
            continue

        rec = {"Fund": fundName, "Section": currentSection}
        for i, val in enumerate(row):
            if i < len(currentHeaders):
                rec[currentHeaders[i]] = val
        records.append(rec)

    return pd.DataFrame(records)


## Ingest the Deal Evaluation Review Sheet xlsm and write a flat Power-BI-ready Excel file
## - pathToWorkbook: full path to "Deal Evaluation Review Sheet_v2.xlsm"
## - outputPath (optional): full path for the flattened xlsx output. If omitted,
##   writes "deal_pipeline_flat.xlsx" next to the source workbook.
## - tabs (optional): which tabs to read (default ["SHLP25", "SHLP26"])
## Returns the combined DataFrame.
def buildDealPipeline(pathToWorkbook, outputPath=None, tabs=None):
    if tabs is None:
        tabs = ["SHLP25", "SHLP26"]

    if not os.path.exists(pathToWorkbook):
        raise FileNotFoundError(f"Deal sheet not found at {pathToWorkbook}")

    print(f"Loading {pathToWorkbook}...")
    wb = openpyxl.load_workbook(pathToWorkbook, data_only=True, keep_vba=False)

    frames = []
    for tabName in tabs:
        if tabName not in wb.sheetnames:
            print(f"  WARNING: tab '{tabName}' not found, skipping")
            continue
        df = _parseDealTab(wb[tabName], tabName)
        print(f"  {tabName}: parsed {len(df)} deal rows across {df['Status'].nunique()} statuses")
        frames.append(df)

    if not frames:
        raise RuntimeError("No tabs parsed — nothing to write")

    combined = pd.concat(frames, ignore_index=True, sort=False)

    # Drop columns that are entirely empty (stray cells from off-grid summary boxes)
    combined = combined.dropna(axis=1, how="all")

    # Stable column order: Fund + Section first, then everything else in first-seen order
    leading = [c for c in ("Fund", "Section") if c in combined.columns]
    other = [c for c in combined.columns if c not in leading]
    combined = combined[leading + other]

    # Coerce date-ish columns to datetime so Power BI auto-detects them as dates
    for col in combined.columns:
        if any(k in col.lower() for k in ("date", "fpd")):
            combined[col] = pd.to_datetime(combined[col], errors="coerce")

    # Force SHP % Exposure to a numeric decimal (some rows arrive as strings/percent text).
    # Values >1 are assumed to be whole-number percents (e.g. 2.93 → 0.0293) and divided by 100.
    if "SHP % Exposure" in combined.columns:
        s = combined["SHP % Exposure"]
        if s.dtype == object:
            s = s.astype(str).str.replace("%", "", regex=False).str.strip()
        s = pd.to_numeric(s, errors="coerce")
        s = s.where(s.isna() | (s.abs() <= 1), s / 100.0)
        combined["SHP % Exposure"] = s

    if outputPath is None:
        outputDir = r"C:\Users\Michael Tanner\OneDrive - Shalehaven Energy & Asset Management LLC\Operations - Documents\# database"
        os.makedirs(outputDir, exist_ok=True)
        outputPath = os.path.join(outputDir, "deal_pipeline_flat.xlsx")

    # Parse Contact List tab if present — flat table + bridge table for Basins slicer
    contactDf = None
    basinsBridge = None
    if "Contact List" in wb.sheetnames:
        ws = wb["Contact List"]
        rows = list(ws.iter_rows(values_only=True))
        if rows:
            headers = [str(c).strip() if c else f"Col{i}" for i, c in enumerate(rows[0])]
            contactDf = pd.DataFrame(rows[1:], columns=headers)
            contactDf = contactDf.dropna(how="all")
            contactDf.insert(0, "ContactID", range(1, len(contactDf) + 1))
            # Coerce date columns
            for col in contactDf.columns:
                if any(k in col.lower() for k in ("date", "touch")):
                    contactDf[col] = pd.to_datetime(contactDf[col], errors="coerce")
            # Build bridge table: one row per (ContactID, Basin) for Power BI slicer
            if "Basins" in contactDf.columns:
                bridge_rows = []
                for _, row in contactDf.iterrows():
                    basins_raw = str(row["Basins"]) if pd.notna(row["Basins"]) else ""
                    for basin in basins_raw.split(","):
                        basin = basin.strip()
                        if basin:
                            bridge_rows.append({"ContactID": row["ContactID"], "Basin": basin})
                basinsBridge = pd.DataFrame(bridge_rows)
            # Drop Basins from the main contact table since the bridge replaces it
            if "Basins" in contactDf.columns:
                contactDf = contactDf.drop(columns=["Basins"])
            print(f"  Contact List: parsed {len(contactDf)} contacts, {len(basinsBridge) if basinsBridge is not None else 0} basin mappings")

    with pd.ExcelWriter(outputPath, engine="openpyxl") as writer:
        combined.to_excel(writer, index=False, sheet_name="DealPipeline")
        if contactDf is not None:
            contactDf.to_excel(writer, index=False, sheet_name="ContactList")
        if basinsBridge is not None:
            basinsBridge.to_excel(writer, index=False, sheet_name="ContactBasins")

    print(f"Wrote {len(combined)} deal rows × {len(combined.columns)} cols to {outputPath}")
    if contactDf is not None:
        print(f"Wrote {len(contactDf)} contacts to ContactList sheet")
    if basinsBridge is not None:
        print(f"Wrote {len(basinsBridge)} basin mappings to ContactBasins sheet")
    return combined
