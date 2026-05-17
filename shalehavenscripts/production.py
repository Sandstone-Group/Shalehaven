import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
import re

load_dotenv()

COMBOCURVE_COLUMNS = ["date", "chosenID", "oil", "gas", "water", "dataSource"]

_WELL_NUM_RE = re.compile(r'#?0*(\d+)[Hh]\s*$')


def _latest_file_in_dir(path):
    return max(
        (os.path.join(path, f) for f in os.listdir(path)),
        key=os.path.getmtime,
    )


"""

Script to import production data and format for ComboCurve upload. For Admiral Permian wells in 2024 LP portfolio.

"""

def admiralPermianProductionData(pathToData):

    print("Getting Admiral Permian Production Data")

    pathToData = _latest_file_in_dir(pathToData)

    data = pd.read_excel(pathToData)

    # API comes in with dashes; strip them and pad to 14 chars
    data['API'] = data['API'].str.replace('-', '') + '0000'
    data['API'] = data['API'].astype(str)
    data = data[data['API'] != "nan"]

    data = data[['Date', 'API', 'Oil Prod', 'Gas Prod', 'Water Prod']]
    data['dataSource'] = "other"
    data.columns = COMBOCURVE_COLUMNS

    return data

def huntOilProductionData(pathToData, huntWells):

    print("Getting Hunt Oil Production Data")

    pathToData = _latest_file_in_dir(pathToData)

    data = pd.read_excel(pathToData)

    nameToChosenId = dict(zip(huntWells['wellName'], huntWells['chosenID']))
    data['API'] = data['API'].astype(str)
    data['API'] = data['LEASE'].map(nameToChosenId).fillna(data['API'])

    data = data[['D_DATE', 'API', 'OIL_BBLS', 'GAS_MCF', 'WATER_BBLS']]
    data['dataSource'] = "other"
    data.columns = COMBOCURVE_COLUMNS

    return data

"""

Get Aethon Production Data

"""

def aethonProductionData(pathToData):

    print("Getting Aethon Production Data")

    pathToData = _latest_file_in_dir(pathToData)

    data = pd.read_csv(pathToData)
    data['API'] = data['API'].astype(str)
    data = data[data['OperatorID'] == 9724]

    data = data[['Production Date', 'API', 'Oil Production', 'Gas Production', 'Water Production']]
    data['dataSource'] = "other"
    data.columns = COMBOCURVE_COLUMNS

    return data

"""

Get Devon Production Data - PDS

"""

def devonProductionData(pathToData):

    print("Getting Devon Production Data")

    pathToData = _latest_file_in_dir(pathToData)
    name = os.path.basename(pathToData)

    if name.startswith("PDSWDX"):
        data = pd.read_csv(pathToData)
        data = data[:-1]
        data['API'] = data['API'].astype(str).str[:-2] + '00'
        data = data[['Prod Date', 'API', 'Oil Prod', 'Gas Prod', 'Water Prod']]
    else:
        data = pd.read_csv(pathToData)
        data['API'] = data['API'].astype(str)
        data = data[data['OperatorID'] == 1014]
        data = data[['Production Date', 'API', 'Oil Production', 'Gas Production', 'Water Production']]

    data['dataSource'] = "other"
    data.columns = COMBOCURVE_COLUMNS

    return data

"""

Get COP Production Data - PDS

"""

def copProductionData(pathToData):
    print("Getting ConocoPhillips Production Data")

    pathToData = _latest_file_in_dir(pathToData)
    name = os.path.basename(pathToData)

    if name.startswith("PDSWDX"):
        data = pd.read_csv(pathToData)
        data = data[:-1]
        data['API'] = data['API'].astype(str).str[:-2] + '00'
        data = data[['PRODDATE', 'API', 'OIL PROD', 'GAS PROD', 'WATER PROD']]
    else:
        data = pd.read_csv(pathToData)
        data['API'] = data['API'].astype(str)
        # NOTE: both devonProductionData and copProductionData filter OperatorID==1014;
        # one of these is likely wrong — verify against the PDS operator ID for COP.
        data = data[data['OperatorID'] == 1014]
        data = data[['Production Date', 'API', 'Oil Production', 'Gas Production', 'Water Production']]

    data['dataSource'] = "other"
    data.columns = COMBOCURVE_COLUMNS

    return data

"""

Load Spur Energy Data from ProdView excel to ComboCurve Monthly Format

"""

def spurProductionData(pathToData, wellMapping):

    print("Getting Spur Energy Production Data")

    pathToData = _latest_file_in_dir(pathToData)

    data = pd.read_excel(pathToData, header=1)

    # Pre-index well mapping by trailing well-number (e.g. "01H" → 1) so header
    # rows with different zero-padding still match.
    keyByWellNum = {}
    for key in wellMapping:
        m = _WELL_NUM_RE.search(key)
        if m:
            keyByWellNum[int(m.group(1))] = key

    rows = []
    current_chosen_id = None

    for _, row in data.iterrows():
        cell = row['Unit Name/Date']
        if isinstance(cell, str) and cell.strip().upper().startswith("FRIESIAN"):
            well_name = cell.strip()
            if well_name in wellMapping:
                current_chosen_id = wellMapping[well_name]
            else:
                excel_num = _WELL_NUM_RE.search(well_name)
                matched_key = keyByWellNum.get(int(excel_num.group(1))) if excel_num else None
                if matched_key:
                    print(f"  Matched '{well_name}' -> '{matched_key}'")
                    current_chosen_id = wellMapping[matched_key]
                else:
                    print(f"  Warning: No match found for '{well_name}' in wellMapping")
                    current_chosen_id = None
        elif current_chosen_id is not None:
            rows.append({
                'date': pd.to_datetime(cell.strip()),
                'chosenID': current_chosen_id,
                'oil': row['Oil Production (bbl)'],
                'gas': row['Gas Production (MCF)'],
                'water': row['Water Production (bbl)'],
                'dataSource': 'other',
            })

    return pd.DataFrame(rows, columns=COMBOCURVE_COLUMNS)

"""

Convert Ballard Petroleum Production Data from ProdView excel to ComboCurve Monthly Format

"""

def ballardProductionData(pathToData):

    print("Getting Ballard Petroleum Production Data")

    pathToData = _latest_file_in_dir(pathToData)

    data = pd.read_excel(pathToData)

    data = data[['RecordDate', 'API10', 'EstimatedOilProductionBBLS', 'EstimatedGasProductionMCF', 'EstimatedWaterProductionBBLS']]

    # API10 arrives as float (trailing .0); strip leading zeros and '.0', then pad to 14 chars
    data['API'] = data['API10'].astype(str).str.lstrip('0').str.replace('.0', '', regex=False) + '0000'
    data['dataSource'] = "other"

    data = data[['RecordDate', 'API', 'EstimatedOilProductionBBLS', 'EstimatedGasProductionMCF', 'EstimatedWaterProductionBBLS', 'dataSource']]
    data.columns = COMBOCURVE_COLUMNS

    return data

"""

Gets Kraken Resources Production Data from  excel and converts to ComboCurve Monthly Format

"""

def _normalizeWellName(s):
    return s.strip().upper().replace("#", "")


def krakenProductionData(pathToData, wellMapping):

    print("Getting Kraken Resources Production Data")

    pathToData = _latest_file_in_dir(pathToData)

    raw = pd.read_excel(pathToData, sheet_name="actual-size", header=None)

    # Row 2 has well names at columns 1, 4, 7, 10, ... (each well spans 3 cols: oil, gas, water)
    # Row 3 onward is data; column 0 is RecordDate
    well_row = raw.iloc[2]
    data_rows = raw.iloc[4:].reset_index(drop=True)

    wellMappingNorm = {_normalizeWellName(k): v for k, v in wellMapping.items()}

    well_cols = [(i, str(well_row.iloc[i])) for i in range(1, len(well_row), 3)
                 if pd.notna(well_row.iloc[i]) and "Total" not in str(well_row.iloc[i])]

    rows = []
    for col_idx, well_name in well_cols:
        if "DELORES" in well_name.upper():
            print(f"  Skipping {well_name} (not our well)")
            continue

        chosen_id = wellMapping.get(well_name.strip()) or wellMappingNorm.get(_normalizeWellName(well_name))
        if chosen_id is None:
            print(f"  Warning: No match found for '{well_name}' in wellMapping, skipping")
            continue

        for _, row in data_rows.iterrows():
            date_val = row.iloc[0]
            if pd.isna(date_val):
                continue
            try:
                parsed_date = pd.to_datetime(date_val)
            except (ValueError, TypeError):
                continue
            rows.append({
                'date': parsed_date,
                'chosenID': chosen_id,
                'oil': row.iloc[col_idx],
                'gas': row.iloc[col_idx + 1],
                'water': row.iloc[col_idx + 2],
                'dataSource': 'other',
            })

    data = pd.DataFrame(rows, columns=COMBOCURVE_COLUMNS)
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=15)
    data = data[data['date'] >= cutoff]
    print(f"  Parsed {len(data)} production rows (last 15 days) for {len(well_cols) - 1} wells")

    return data

"""

Get EOG Resources Production Data - PDS

"""

def eogProductionData(pathToData):

    print("Getting EOG Resources Production Data")

    pathToData = _latest_file_in_dir(pathToData)

    data = pd.read_csv(pathToData)
    data = data[:-1]

    # API arrives in scientific notation (e.g. 4.90058E+13); cast through int to drop the exponent.
    data['API'] = data['API'].astype(float).astype('int64').astype(str).str.zfill(14)

    data = data[['PRODDATE', 'API', 'OIL PROD', 'GAS PROD', 'WATER PROD']]
    data['dataSource'] = "other"
    data.columns = COMBOCURVE_COLUMNS

    return data

"""
    
    Merge daily production data with updated and original type curves from ComboCurve.

"""

def mergeProductionWithTypeCurves(dailyprod, updated, original, wellList, pathToDatabase):

    print("Begin Merging dailyprod with Orginal and Updated Type Curves")

    for df in (dailyprod, updated, original):
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True).dt.tz_localize(None)

    mergedData = pd.merge(updated, dailyprod, how='left', on=['date', 'well'], suffixes=('', '_dailyprod'))
    mergedData = pd.merge(mergedData, original, how='left', on=['date', 'well'], suffixes=('', '_original'))

    mergedData = mergedData.rename(columns={
        'oil': 'oil_updated',
        'gas': 'gas_updated',
        'water': 'water_updated',
    })

    for col in ['oil_original', 'gas_original', 'water_original',
                'oil_dailyprod', 'gas_dailyprod', 'water_dailyprod']:
        mergedData[col] = mergedData[col].fillna("")

    mergedData = (
        mergedData
        .sort_values(by=['wellName', 'date'])
        .drop(columns=['wellName_original', 'API_original', 'wellName_dailyprod', 'API_dailyprod'])
        .reset_index(drop=True)
    )

    mergedData.to_excel(os.path.join(pathToDatabase, r"daily_merge.xlsx"))

    print("Finished Merging Original and Updated Type Curves")

    return mergedData

"""
    
    Create cumulative production from daily production data, updated type curves, and original type curves.  The data should have a index column (1 through n) then date, well, oil, gas, water columns for daily production, updated type curves, and original type curve cumulative.    
    
"""

def cumulativeProduction(data, pathToDatabase):

    print("Begin Creating Cumulative Production Data")

    sourceCols = [
        'oil_dailyprod', 'gas_dailyprod', 'water_dailyprod',
        'oil_updated', 'gas_updated', 'water_updated',
        'oil_original', 'gas_original', 'water_original',
    ]
    cumCols = [f"{c}_cum" for c in sourceCols]
    outputCols = (
        ["day", "well", "wellName", "API"]
        + [f"{c}_dailyprod_cum" for c in ("oil", "gas", "water")]
        + [f"{c}_updated_cum" for c in ("oil", "gas", "water")]
        + [f"{c}_original_cum" for c in ("oil", "gas", "water")]
    )

    frames = []
    for well in data['well'].unique():
        wellData = data[data['well'] == well].copy()

        numProductionDays = len(wellData[wellData['oil_dailyprod'] != ""])

        for col in sourceCols:
            wellData[col] = wellData[col].replace("", 0).astype(float)

        for src, cum in zip(sourceCols, cumCols):
            series = wellData[src].cumsum()
            # Trailing zero-runs inherit the last non-zero cum; leading zeros stay 0.
            series = series.where(series != 0).ffill().fillna(0)
            wellData[cum] = series.shift(fill_value=0)

        wellData['day'] = np.arange(len(wellData))

        # Beyond the last day with actual production, blank out dailyprod cum columns.
        pastProduction = wellData['day'] >= numProductionDays
        for cum in ('oil_dailyprod_cum', 'gas_dailyprod_cum', 'water_dailyprod_cum'):
            wellData.loc[pastProduction, cum] = ""

        frames.append(wellData[outputCols])

    cumulativeData = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=outputCols)

    cumulativeData.to_excel(os.path.join(pathToDatabase, r"cumulative_production.xlsx"))

    print("Finished Creating Cumulative Production Data")

    return cumulativeData





"""

    7-day net production PDF.
    Top-level sections by fund (2024 LP, 2025 LP); each fund pulls wells whose
    fund-specific WI column is > 0 (so a 2024/2025 LP well shows up in BOTH).
    Within a fund: operator-level summary table + per-operator daily breakdown.

    fundWells column mapping (customString1 is just a label — fund membership is
    actually carried by these numeric columns):
        2024 LP: WI = customNumber0, NRI = customNumber1
        2025 LP: WI = customNumber2, NRI = customNumber3

"""

FUND_COLUMN_MAP = {
    "2024 LP": {"wi": "customNumber0", "nri": "customNumber1"},
    "2025 LP": {"wi": "customNumber2", "nri": "customNumber3"},
}


ADMIRAL_OPERATOR_NAME = "ADMIRAL PERMIAN OPERATING LLC"


def buildSevenDayNetProductionPdf(dailyProductions, fundWells, outputDir,
                                  originalTypeCurves=None, updatedTypeCurves=None,
                                  outputName=None):
    from io import BytesIO
    from pathlib import Path
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import LETTER, landscape
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.lib.utils import ImageReader
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak,
        KeepTogether, Image,
    )

    logoPath = Path(__file__).resolve().parent.parent / "logos" / "shp.png"
    pageW, pageH = landscape(LETTER)
    logoH = 0.7 * inch
    logoW = logoH * (1600 / 1200)  # preserve 4:3 aspect

    def _drawLogo(canvas, doc):
        if logoPath.is_file():
            canvas.drawImage(
                ImageReader(str(logoPath)),
                pageW - 0.4*inch - logoW,
                pageH - 0.3*inch - logoH,
                width=logoW, height=logoH,
                mask='auto',
            )

    print("Building 7-Day Net Production PDF")

    df = dailyProductions.copy()
    df['date'] = pd.to_datetime(df['date'], utc=True, errors='coerce').dt.tz_localize(None).dt.normalize()
    df = df.dropna(subset=['date'])

    asOf = df['date'].max()
    if pd.isna(asOf):
        raise ValueError("dailyProductions has no parseable dates")

    # Merge in type-curve forecast (oil + gas) on (well, date).
    # Default source = originalTypeCurves; Admiral Permian wells fall back to
    # updatedTypeCurves because they don't have an original-TC forecast keyed.
    admiralWellIds = set(
        fundWells.loc[fundWells['currentOperator'] == ADMIRAL_OPERATOR_NAME, 'id']
                 .dropna().tolist()
    )

    def _normalizeForecast(fc):
        if fc is None or len(fc) == 0:
            return None
        out = fc[['date', 'well', 'oil', 'gas']].copy()
        out['date'] = pd.to_datetime(out['date'], utc=True, errors='coerce').dt.tz_localize(None).dt.normalize()
        return out

    origFc = _normalizeForecast(originalTypeCurves)
    updFc = _normalizeForecast(updatedTypeCurves)
    parts = []
    if origFc is not None:
        # exclude Admiral wells from original; we'll replace with updated below
        parts.append(origFc[~origFc['well'].isin(admiralWellIds)])
    if updFc is not None and admiralWellIds:
        parts.append(updFc[updFc['well'].isin(admiralWellIds)])

    if parts:
        fc = pd.concat(parts, ignore_index=True)
        fc = fc.rename(columns={'oil': 'forecast_oil', 'gas': 'forecast_gas'})
        df = df.merge(fc, on=['date', 'well'], how='left')
    else:
        df['forecast_oil'] = float('nan')
        df['forecast_gas'] = float('nan')

    # Per-well last N days of non-zero production. Operators report on different
    # cadences (some daily, some lagged), so a calendar-window filter would show
    # zeros for wells that simply hadn't reported yet. Drop fully-zero days first
    # (oil+gas+water == 0 ≈ reporting gap), then rank each well's days by recency.
    # ranks 1–7 = current 7d window (tables/widgets); 8–14 = prior 7d (WoW variance);
    # 1–30 = chart window.
    for stream in ('oil', 'gas', 'water'):
        df[stream] = pd.to_numeric(df[stream], errors='coerce').fillna(0)
    df = df[(df['oil'] + df['gas'] + df['water']) > 0]
    dfRanked = df.sort_values('date').copy()
    dfRanked['rank'] = dfRanked.groupby('well')['date'].rank(method='first', ascending=False)

    wellMeta = fundWells.set_index("id")[
        ["wellName", "currentOperator",
         "customNumber0", "customNumber1", "customNumber2", "customNumber3"]
    ]
    for col in ["customNumber0", "customNumber1", "customNumber2", "customNumber3"]:
        dfRanked[col] = dfRanked['well'].map(wellMeta[col]).fillna(0)
    dfRanked['operator'] = dfRanked['well'].map(wellMeta['currentOperator'])
    mappedNames = dfRanked['well'].map(wellMeta['wellName'])
    dfRanked['wellName'] = (
        mappedNames.fillna(dfRanked['wellName']) if 'wellName' in dfRanked.columns else mappedNames
    )

    df14 = dfRanked[dfRanked['rank'] <= 14].copy()
    df14['window'] = df14['rank'].le(7).map({True: 'current', False: 'prior'})
    df7 = df14[df14['window'] == 'current'].copy()
    df30 = dfRanked[dfRanked['rank'] <= 30].copy()

    runDate = pd.Timestamp.now().normalize()
    pdfName = outputName or f"shalehaven_production_{runDate:%Y-%m-%d}.pdf"
    os.makedirs(outputDir, exist_ok=True)
    pdfPath = os.path.join(outputDir, pdfName)

    doc = SimpleDocTemplate(
        pdfPath, pagesize=landscape(LETTER),
        leftMargin=0.4*inch, rightMargin=0.4*inch,
        topMargin=1.1*inch, bottomMargin=0.5*inch,
        title="Shalehaven 7-Day Production Overview",
    )
    styles = getSampleStyleSheet()
    h1, h2, h3, body = styles['Heading1'], styles['Heading2'], styles['Heading3'], styles['BodyText']

    story = [
        Paragraph("Shalehaven 7-Day Production Overview", h1),
        Paragraph(
            f"Report run {asOf:%Y-%m-%d} &nbsp;|&nbsp; per-well: last 7 days of "
            f"<b>reported</b> production "
            f"&nbsp;|&nbsp; net = gross &times; NRI",
            body,
        ),
        Spacer(1, 0.2*inch),
    ]

    headerStyle = TableStyle([
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#1F4E79")),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
        ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ])

    detailStyle = TableStyle([
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#2E75B6")),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (3, 1), (-1, -1), 'RIGHT'),
        ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
        ('FONTSIZE', (0, 0), (-1, -1), 7.5),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ])

    def _fmt(x, digits=1):
        return f"{x:,.{digits}f}"

    def _pct(x):
        return f"{x:.4%}" if pd.notna(x) and x else "—"

    def _variance(current, prior):
        if prior is None or pd.isna(prior) or prior == 0:
            return None
        return (current - prior) / prior

    def _varStr(v):
        if v is None or pd.isna(v):
            return "—"
        return f"{v:+.1%}"

    def _varColor(v):
        if v is None or pd.isna(v):
            return colors.HexColor("#7F7F7F")
        return colors.HexColor("#2E7D32") if v >= 0 else colors.HexColor("#C62828")

    hdrStyle = ParagraphStyle(
        'sumHdr', parent=body, fontName='Helvetica-Bold',
        textColor=colors.white, fontSize=8, leading=10, alignment=1,
    )
    wTitleStyle = ParagraphStyle(
        'wTitle', parent=body, fontName='Helvetica-Bold', fontSize=9,
        textColor=colors.HexColor('#1F4E79'), alignment=1, leading=11,
    )
    wMetricStyle = ParagraphStyle(
        'wMetric', parent=body, fontName='Helvetica', fontSize=8,
        textColor=colors.HexColor('#595959'), alignment=1, leading=10,
    )
    wValueStyle = ParagraphStyle(
        'wValue', parent=body, fontName='Helvetica-Bold', fontSize=18,
        textColor=colors.HexColor('#1F4E79'), alignment=1, leading=22,
    )
    wUnitStyle = ParagraphStyle(
        'wUnit', parent=body, fontName='Helvetica', fontSize=8,
        textColor=colors.HexColor('#7F7F7F'), alignment=1, leading=10,
    )

    # ─── Top-of-page KPI widgets: 7d Net Oil/Gas Avg per fund (net = gross × NRI) ───
    fundTotals = {}
    for fundLabel, cols in FUND_COLUMN_MAP.items():
        wiCol, nriCol = cols['wi'], cols['nri']
        cur = df14[(df14[wiCol] > 0) & (df14['window'] == 'current')]
        if cur.empty:
            fundTotals[fundLabel] = {'oil': 0.0, 'gas': 0.0}
            continue
        fundTotals[fundLabel] = {
            'oil': float((cur['oil'] * cur[nriCol]).sum()),
            'gas': float((cur['gas'] * cur[nriCol]).sum()),
        }

    def _widgetCell(fundLabel, metric, value, unit):
        return [
            Paragraph(fundLabel, wTitleStyle),
            Paragraph(metric, wMetricStyle),
            Spacer(1, 4),
            Paragraph(f"{value/7:,.1f}", wValueStyle),
            Paragraph(unit, wUnitStyle),
        ]

    widgetRow = []
    for fundLabel in FUND_COLUMN_MAP.keys():
        t = fundTotals.get(fundLabel, {'oil': 0.0, 'gas': 0.0})
        widgetRow.append(_widgetCell(fundLabel, "7d Net Oil Avg", t['oil'], "bbl/d"))
        widgetRow.append(_widgetCell(fundLabel, "7d Net Gas Avg", t['gas'], "Mcf/d"))

    widgetTbl = Table([widgetRow], colWidths=[2.4*inch]*4)
    widgetTbl.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#F8FAFC')),
        ('BOX', (0, 0), (-1, -1), 0.75, colors.HexColor('#1F4E79')),
        ('LINEAFTER', (0, 0), (-2, -1), 0.5, colors.HexColor('#BDBDBD')),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING', (0, 0), (-1, -1), 10),
        ('RIGHTPADDING', (0, 0), (-1, -1), 10),
        ('TOPPADDING', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
    ]))
    story.append(widgetTbl)
    story.append(Spacer(1, 0.2*inch))

    def _opAgg(d):
        return d.groupby('operator').agg(
            wells=('well', 'nunique'),
            gross_oil=('oil', 'sum'), net_oil=('net_oil', 'sum'),
            gross_gas=('gas', 'sum'), net_gas=('net_gas', 'sum'),
        )

    # ─── Pass 1: page-1 fund summary tables (7d totals + daily avg) ───
    fundContexts = {}  # fundLabel -> dict(fundDf, opSummary, wiCol, nriCol)
    for fundLabel, cols in FUND_COLUMN_MAP.items():
        wiCol, nriCol = cols['wi'], cols['nri']
        fundDf = df14[df14[wiCol] > 0].copy()
        if fundDf.empty:
            continue
        fundDf['net_oil'] = fundDf['oil'] * fundDf[nriCol]
        fundDf['net_gas'] = fundDf['gas'] * fundDf[nriCol]
        fundDf['net_water'] = fundDf['water'] * fundDf[nriCol]

        fundBlock = [Paragraph(f"Fund: {fundLabel}", h3)]

        curDf = fundDf[fundDf['window'] == 'current']
        priorDf = fundDf[fundDf['window'] == 'prior']
        curAgg = _opAgg(curDf)
        priorAgg = _opAgg(priorDf).add_suffix('_prior')
        opSummary = (
            curAgg.join(priorAgg, how='left')
                  .fillna({c: 0 for c in priorAgg.columns})
                  .reset_index()
                  .sort_values('net_oil', ascending=False)
        )
        fundContexts[fundLabel] = {
            'fundDf': fundDf, 'opSummary': opSummary,
            'wiCol': wiCol, 'nriCol': nriCol,
        }
        summaryHeader = [
            Paragraph("Operator", hdrStyle),
            Paragraph("Wells", hdrStyle),
            Paragraph("Gross Oil<br/>7d (bbl)", hdrStyle),
            Paragraph("Net Oil<br/>7d (bbl)", hdrStyle),
            Paragraph("Oil Δ<br/>vs prior 7d", hdrStyle),
            Paragraph("Gross Gas<br/>7d (Mcf)", hdrStyle),
            Paragraph("Net Gas<br/>7d (Mcf)", hdrStyle),
            Paragraph("Gas Δ<br/>vs prior 7d", hdrStyle),
        ]
        summaryRows = [summaryHeader]
        varCellColors = []  # (row, col, color) for per-cell text color on Δ columns
        for i, (_, r) in enumerate(opSummary.iterrows(), start=1):
            oilVar = _variance(r['net_oil'], r['net_oil_prior'])
            gasVar = _variance(r['net_gas'], r['net_gas_prior'])
            summaryRows.append([
                r['operator'], f"{int(r['wells']):,}",
                _fmt(r['gross_oil']), _fmt(r['net_oil']), _varStr(oilVar),
                _fmt(r['gross_gas']), _fmt(r['net_gas']), _varStr(gasVar),
            ])
            varCellColors.append((i, 4, _varColor(oilVar)))
            varCellColors.append((i, 7, _varColor(gasVar)))

        totalOilCur = opSummary['net_oil'].sum()
        totalOilPrior = opSummary['net_oil_prior'].sum()
        totalGasCur = opSummary['net_gas'].sum()
        totalGasPrior = opSummary['net_gas_prior'].sum()
        totalOilVar = _variance(totalOilCur, totalOilPrior)
        totalGasVar = _variance(totalGasCur, totalGasPrior)
        summaryRows.append([
            "TOTAL", f"{int(opSummary['wells'].sum()):,}",
            _fmt(opSummary['gross_oil'].sum()), _fmt(totalOilCur), _varStr(totalOilVar),
            _fmt(opSummary['gross_gas'].sum()), _fmt(totalGasCur), _varStr(totalGasVar),
        ])
        totalRowIdx = len(summaryRows) - 1
        varCellColors.append((totalRowIdx, 4, _varColor(totalOilVar)))
        varCellColors.append((totalRowIdx, 7, _varColor(totalGasVar)))

        summaryColWidths = [
            2.6*inch,  # Operator
            0.55*inch, # Wells
            1.05*inch, # 7d Gross Oil
            1.05*inch, # 7d Net Oil
            0.95*inch, # Oil Δ
            1.05*inch, # 7d Gross Gas
            1.05*inch, # 7d Net Gas
            0.95*inch, # Gas Δ
        ]
        sumTbl = Table(summaryRows, hAlign='LEFT', repeatRows=1, colWidths=summaryColWidths)
        sumCommands = headerStyle.getCommands() + [
            ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor("#D9E1F2")),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8.5),
        ]
        for row, col, color in varCellColors:
            sumCommands.append(('TEXTCOLOR', (col, row), (col, row), color))
        sumTbl.setStyle(TableStyle(sumCommands))
        fundBlock.append(sumTbl)
        fundBlock.append(Spacer(1, 0.1*inch))

        # Daily average table (7d totals / 7) — same operator order as summary
        avgHeader = [
            Paragraph("Operator", hdrStyle),
            Paragraph("Wells", hdrStyle),
            Paragraph("Avg Daily<br/>Gross Oil (bbl/d)", hdrStyle),
            Paragraph("Avg Daily<br/>Net Oil (bbl/d)", hdrStyle),
            Paragraph("Avg Daily<br/>Gross Gas (Mcf/d)", hdrStyle),
            Paragraph("Avg Daily<br/>Net Gas (Mcf/d)", hdrStyle),
        ]
        avgRows = [avgHeader]
        for _, r in opSummary.iterrows():
            avgRows.append([
                r['operator'], f"{int(r['wells']):,}",
                _fmt(r['gross_oil']/7), _fmt(r['net_oil']/7),
                _fmt(r['gross_gas']/7), _fmt(r['net_gas']/7),
            ])
        avgRows.append([
            "TOTAL", f"{int(opSummary['wells'].sum()):,}",
            _fmt(opSummary['gross_oil'].sum()/7), _fmt(opSummary['net_oil'].sum()/7),
            _fmt(opSummary['gross_gas'].sum()/7), _fmt(opSummary['net_gas'].sum()/7),
        ])

        avgColWidths = [2.6*inch, 0.55*inch, 1.5*inch, 1.5*inch, 1.5*inch, 1.5*inch]
        avgTbl = Table(avgRows, hAlign='LEFT', repeatRows=1, colWidths=avgColWidths)
        avgTbl.setStyle(TableStyle(headerStyle.getCommands() + [
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#3B6E9C")),
            ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor("#D9E1F2")),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8.5),
        ]))
        fundBlock.append(avgTbl)
        story.append(KeepTogether(fundBlock))
        story.append(Spacer(1, 0.15*inch))

    if fundContexts:
        story.append(PageBreak())

    def _lineChartPng(daily, stream, unit, title, figW=4.4, figH=1.85):
        """Render actual-vs-forecast line chart from a frame with 'date', stream, forecast_{stream}."""
        fig, ax = plt.subplots(figsize=(figW, figH), dpi=130)
        dates_ = daily['date'].tolist()
        ax.plot(dates_, daily[stream], marker='o', linewidth=1.6,
                color='#1F4E79', label='Actual')
        fcCol = f'forecast_{stream}'
        if fcCol in daily.columns and daily[fcCol].notna().any():
            ax.plot(dates_, daily[fcCol], marker='s', linewidth=1.4,
                    linestyle='--', color='#C62828', label='Forecast')
        ax.set_title(f"{title} ({unit})", fontsize=9, loc='left')
        ax.tick_params(labelsize=7)
        ax.legend(fontsize=7, loc='best', frameon=False)
        ax.grid(True, axis='y', alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.set_ylim(bottom=0)
        fig.autofmt_xdate(rotation=30, ha='right')
        fig.tight_layout()
        buf = BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=130)
        plt.close(fig)
        buf.seek(0)
        return buf

    def _fundDailyNet(fundDf30, nriCol):
        """Aggregate fund-scoped df30 into per-date NET actual + forecast (NRI-weighted)."""
        d = fundDf30.copy()
        d['net_oil'] = d['oil'] * d[nriCol]
        d['net_gas'] = d['gas'] * d[nriCol]
        has_oil_fc = 'forecast_oil' in d.columns
        has_gas_fc = 'forecast_gas' in d.columns
        if has_oil_fc:
            d['net_forecast_oil'] = d['forecast_oil'] * d[nriCol]
        if has_gas_fc:
            d['net_forecast_gas'] = d['forecast_gas'] * d[nriCol]

        agg_specs = {'oil': ('net_oil', 'sum'), 'gas': ('net_gas', 'sum')}
        if has_oil_fc:
            agg_specs['forecast_oil'] = ('net_forecast_oil', 'sum')
        if has_gas_fc:
            agg_specs['forecast_gas'] = ('net_forecast_gas', 'sum')
        daily = d.groupby('date').agg(**agg_specs).reset_index().sort_values('date')

        # If on a given date no well in the fund had a forecast, the merge filled
        # NaN → sum = 0. That paints a misleading "zero-forecast" point; replace
        # those with NaN so the chart skips the marker.
        if has_oil_fc:
            counts = d.assign(_has=d['forecast_oil'].notna()).groupby('date')['_has'].sum()
            daily.loc[daily['date'].map(counts) == 0, 'forecast_oil'] = float('nan')
        if has_gas_fc:
            counts = d.assign(_has=d['forecast_gas'].notna()).groupby('date')['_has'].sum()
            daily.loc[daily['date'].map(counts) == 0, 'forecast_gas'] = float('nan')

        return daily

    def _overUnder(fundDaily, stream):
        fcCol = f'forecast_{stream}'
        if fcCol not in fundDaily.columns:
            return None
        mask = fundDaily[fcCol].notna()
        if not mask.any():
            return None
        actual = float(fundDaily.loc[mask, stream].sum())
        forecast = float(fundDaily.loc[mask, fcCol].sum())
        if forecast == 0:
            return None
        return (actual - forecast) / forecast, mask.sum()

    ouStyle = ParagraphStyle(
        'overUnder', parent=body, fontName='Helvetica-Bold', fontSize=11,
        alignment=1, leading=14,
    )

    def _overUnderParagraph(result):
        if result is None:
            return Paragraph('<i>no forecast data</i>',
                             ParagraphStyle('ouNa', parent=body, fontSize=9,
                                            textColor=colors.HexColor('#7F7F7F'),
                                            alignment=1))
        pct, ndays = result
        color = '#2E7D32' if pct >= 0 else '#C62828'
        arrow = '▲' if pct >= 0 else '▼'
        return Paragraph(
            f'<font color="{color}">{arrow} {abs(pct):.1%} '
            f'<font size="8" color="#595959">vs forecast over {ndays}d</font></font>',
            ouStyle,
        )

    # ─── Pass 2: per-fund 30-day actual vs forecast roll-up (page 2+) ───
    for fundLabel, ctx in fundContexts.items():
        wiCol, nriCol = ctx['wiCol'], ctx['nriCol']
        fundDf30 = df30[df30[wiCol] > 0]
        if fundDf30.empty:
            continue
        fundDaily = _fundDailyNet(fundDf30, nriCol)
        fundOilImg = Image(
            _lineChartPng(fundDaily, 'oil', 'bbl/d',
                          f'{fundLabel} — Net Oil (last 30d)'),
            width=4.5*inch, height=1.95*inch,
        )
        fundGasImg = Image(
            _lineChartPng(fundDaily, 'gas', 'Mcf/d',
                          f'{fundLabel} — Net Gas (last 30d)'),
            width=4.5*inch, height=1.95*inch,
        )
        oilOU = _overUnderParagraph(_overUnder(fundDaily, 'oil'))
        gasOU = _overUnderParagraph(_overUnder(fundDaily, 'gas'))

        fundChartRow = Table(
            [[fundOilImg, fundGasImg],
             [oilOU,     gasOU]],
            colWidths=[4.65*inch, 4.65*inch],
        )
        fundChartRow.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, 0), 'TOP'),
            ('VALIGN', (0, 1), (-1, 1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 0),
            ('TOPPADDING', (0, 0), (-1, 0), 0),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 2),
            ('TOPPADDING', (0, 1), (-1, 1), 2),
            ('BOTTOMPADDING', (0, 1), (-1, 1), 6),
        ]))
        story.append(KeepTogether([
            Paragraph(f"Fund: {fundLabel} — 30-Day Actual vs Forecast (Net)", h3),
            fundChartRow,
            Spacer(1, 0.2*inch),
        ]))

    doc.build(story, onFirstPage=_drawLogo, onLaterPages=_drawLogo)
    return {
        'pdfPath': pdfPath,
        'kpis': {
            fundLabel: {
                'oil_avg_bbl_d': v['oil'] / 7,
                'gas_avg_mcf_d': v['gas'] / 7,
            }
            for fundLabel, v in fundTotals.items()
        },
    }


"""
Convert Monthly PDS to ComboCurve Monthly Format

"""

def pdsMonthlyData(pathToData):

    print("Getting PDS Monthly Production Data")

    pathToData = _latest_file_in_dir(pathToData)

    data = pd.read_csv(pathToData)

    data['API'] = data['API'].astype(str).str[:-2] + '00'
    data['Production Date'] = pd.to_datetime(data['Production Date'], format="%m/%d/%Y").dt.strftime("%Y-%m-%d")

    data = data[['Production Date', 'API', 'Oil Production', 'Gas Production', 'Water Production']]
    data['dataSource'] = "other"
    data.columns = COMBOCURVE_COLUMNS

    return data