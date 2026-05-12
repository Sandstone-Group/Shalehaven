## Economics orchestration — ties Novi P50 monthly forecasts to Combocurve econ-model assumptions.
## Steps are added incrementally; computeEconomics() is the single entrypoint called from main_model.

import json as _json
import os
import webbrowser
import pandas as pd

from . import combocurve


## Oil price deck: list of (calendar_date, $/bbl) sorted ascending, from CC pricing record.
def _resolveOilPriceDeck(pricingRec):
    rows = pricingRec.get("priceModel", {}).get("oil", {}).get("rows", []) or []
    deck = []
    for row in rows:
        if row.get("dates") is None or row.get("price") is None:
            continue
        deck.append((pd.Timestamp(row["dates"]), float(row["price"])))
    if not deck:
        raise ValueError("pricing record has no date-based oil rows")
    deck.sort()
    return deck


## Flat NGL pct-of-oil (decimal) from CC pricing record. Time-based NGL decks not supported yet.
def _resolveNglPctOfOil(pricingRec):
    rows = pricingRec.get("priceModel", {}).get("ngl", {}).get("rows", []) or []
    for row in rows:
        if row.get("entireWellLife") == "Flat" and row.get("pctOfOilPrice") is not None:
            return float(row["pctOfOilPrice"]) / 100.0
    raise ValueError("NGL pricing must be flat (entireWellLife='Flat') pctOfOilPrice; time-based not supported yet")


## Flat gas $/MMBtu from CC pricing record. Time-based gas decks aren't supported yet.
def _resolveGasPrice(pricingRec):
    rows = pricingRec.get("priceModel", {}).get("gas", {}).get("rows", []) or []
    for row in rows:
        if row.get("entireWellLife") == "Flat" and row.get("dollarPerMmbtu") is not None:
            return float(row["dollarPerMmbtu"])
    raise ValueError("gas pricing must be flat (entireWellLife='Flat'); time-based not supported yet")


## Sum the three differential tiers for a phase (CC stacks first/second/third).
## phaseKey is "oil" (returns $/bbl) or "gas" (returns $/MMBtu).
def _sumDifferentials(diffRec, phaseKey, valueKey):
    total = 0.0
    diffs = diffRec.get("differentials", {}) or {}
    for tier in ("firstDifferential", "secondDifferential", "thirdDifferential"):
        rows = diffs.get(tier, {}).get(phaseKey, {}).get("rows", []) or []
        for row in rows:
            v = row.get(valueKey)
            if v is not None:
                total += float(v)
                break
    return total


## Unshrunk gas BTU/cf from CC stream-properties record.
def _resolveBtu(streamPropertiesRec):
    btu = streamPropertiesRec.get("btuContent", {}).get("unshrunkGas")
    if btu is None or float(btu) <= 0:
        raise ValueError(f"stream-properties btuContent.unshrunkGas missing or non-positive (got {btu!r})")
    return float(btu)


## Step-function lookup: find the latest deck entry with date <= calendar_date.
## Dates before the first row default to the first row's price.
def _priceAtDate(deck, calendar_date):
    price = deck[0][1]
    for d, p in deck:
        if d <= calendar_date:
            price = p
        else:
            break
    return price


## Resolve Net Revenue Interest from the AFE Summary. Accepts "NRI", "Net Revenue Interest", or any
## column whose name contains both "net" and "revenue". Values > 1 are treated as percent (e.g. 75 → 0.75).
def _resolveAfeNri(afeData):
    if afeData is None or afeData.empty:
        return None
    candidates = []
    for col in afeData.columns:
        norm = str(col).strip().lower().replace("_", " ")
        if norm == "nri" or norm == "net revenue interest" or ("net" in norm and "revenue" in norm):
            vals = pd.to_numeric(afeData[col], errors="coerce").dropna()
            if not vals.empty:
                candidates.extend(vals.tolist())
    if not candidates:
        return None
    nri = float(pd.Series(candidates).mean())
    if nri > 1.0:
        nri /= 100.0
    return nri if 0.0 < nri <= 1.0 else None


## Mid-period NPV of a monthly cash flow series at a given annual discount rate.
## Cashflows are 1-indexed in time (index 0 = month 1). Discount factor uses t = (i + 0.5) / 12.
def _computeNPV(cashflows, annualRate, periodsPerYear=12):
    total = 0.0
    for i, cf in enumerate(cashflows):
        t = (i + 0.5) / periodsPerYear
        total += float(cf) / ((1.0 + annualRate) ** t)
    return total


## Annualized IRR via scipy.optimize.brentq on the mid-period NPV function.
## Returns (irr, reason). Uses NPV(0) to pick scan direction — avoids the floating-point
## noise near r=-1 that previously produced spurious negative roots when the real IRR was positive.
def _computeIRR(cashflows, periodsPerYear=12):
    from scipy.optimize import brentq
    if not cashflows:
        return None, "empty cashflows"
    has_pos = any(cf > 0 for cf in cashflows)
    has_neg = any(cf < 0 for cf in cashflows)
    if not (has_pos and has_neg):
        return None, ("all cashflows positive — no negative outlay" if has_pos else "all cashflows negative — project never recovers")

    npv0 = _computeNPV(cashflows, 0.0, periodsPerYear)
    if npv0 == 0:
        return 0.0, "ok (NPV is zero at r=0)"

    # If NPV(0) > 0 the true IRR is on the positive side (NPV decreases as r grows for a typical project).
    # If NPV(0) < 0 the true IRR is on the negative side. Scanning the wrong half produces spurious
    # roots from numerical noise at r → -1.
    if npv0 > 0:
        bracket_points = [0.0, 0.05, 0.10, 0.20, 0.30, 0.50, 0.75, 1.0, 1.5, 2.0, 5.0, 10.0]
    else:
        bracket_points = [-0.9, -0.7, -0.5, -0.3, -0.1, 0.0]

    npvs = []
    for r in bracket_points:
        try:
            npvs.append((r, _computeNPV(cashflows, r, periodsPerYear)))
        except (OverflowError, ZeroDivisionError):
            continue
    for i in range(len(npvs) - 1):
        r_a, v_a = npvs[i]
        r_b, v_b = npvs[i + 1]
        if v_a == 0:
            return float(r_a), "ok"
        if v_a * v_b < 0:
            try:
                return float(brentq(lambda r: _computeNPV(cashflows, r, periodsPerYear), r_a, r_b, xtol=1e-6)), "ok"
            except (ValueError, RuntimeError) as e:
                return None, f"brentq failed in [{r_a:.3f}, {r_b:.3f}]: {e}"
    return None, f"NPV doesn't cross zero across [{bracket_points[0]:.2f}, {bracket_points[-1]:.1f}]; sum(CF)=${sum(cashflows):,.0f}; NPV(0)=${npv0:,.0f}"


## Resolve total well CapEx from the AFE Summary. Accepts column names containing "capex",
## or "afe" + ("cost" / "net"), or "d&c" + "cost". Returns (value, is_already_net):
##   is_already_net = True  when the column name contains "net" (e.g. "Net AFE", "Net CapEx")
##                  = False when it's gross (caller should multiply by WI)
def _resolveAfeCapex(afeData):
    if afeData is None or afeData.empty:
        return None, None
    candidates = []
    is_net = False
    for col in afeData.columns:
        norm = str(col).strip().lower().replace("_", " ").replace("&", " ")
        match = (
            "capex" in norm
            or ("d c" in norm and "cost" in norm)
            or ("afe" in norm and ("cost" in norm or "net" in norm))
        )
        if not match:
            continue
        vals = pd.to_numeric(afeData[col], errors="coerce").dropna()
        if vals.empty:
            continue
        candidates.extend(vals.tolist())
        if "net" in norm:
            is_net = True
    if not candidates:
        return None, None
    return float(abs(pd.Series(candidates).mean())), is_net


## Resolve Working Interest from the AFE Summary. Accepts "WI", "Working Interest", or any column
## containing both "work" and "interest". Values > 1 are treated as percent (85 → 0.85).
def _resolveAfeWi(afeData):
    if afeData is None or afeData.empty:
        return None
    candidates = []
    for col in afeData.columns:
        norm = str(col).strip().lower().replace("_", " ")
        if norm == "wi" or norm == "working interest" or ("work" in norm and "interest" in norm):
            vals = pd.to_numeric(afeData[col], errors="coerce").dropna()
            if not vals.empty:
                candidates.extend(vals.tolist())
    if not candidates:
        return None
    wi = float(pd.Series(candidates).mean())
    if wi > 1.0:
        wi /= 100.0
    return wi if 0.0 < wi <= 1.0 else None


## Resolve a per-phase severance tax rate (decimal) from CC production-taxes record.
## Only flat / entire_well_life / pct_of_revenue is supported; raises on anything else.
def _resolveSeveranceRate(taxRec, phaseKey):
    rows = taxRec.get("data", {}).get("rows", []) or []
    for row in rows:
        if row.get("category") != "severance_tax" or row.get("key") != phaseKey:
            continue
        if row.get("criteria") != "entire_well_life" or row.get("period") != ["Flat"]:
            raise ValueError(f"severance row for {phaseKey} is not flat/entire_well_life: {row}")
        if row.get("unit") != "pct_of_revenue":
            raise ValueError(f"severance row for {phaseKey} has unsupported unit '{row.get('unit')}'")
        vals = row.get("value") or []
        if not vals:
            return 0.0
        return float(vals[0]) / 100.0
    return 0.0


## Resolve ad valorem tax rate (decimal) and the deductSeveranceTax flag from CC production-taxes record.
def _resolveAdValTax(taxRec):
    rows = taxRec.get("data", {}).get("rows", []) or []
    for row in rows:
        if row.get("category") != "ad_val_tax":
            continue
        if row.get("criteria") != "entire_well_life" or row.get("period") != ["Flat"]:
            raise ValueError(f"ad valorem row is not flat/entire_well_life: {row}")
        if row.get("unit") != "pct_of_revenue":
            raise ValueError(f"ad valorem row has unsupported unit '{row.get('unit')}'")
        vals = row.get("value") or []
        rate = (float(vals[0]) / 100.0) if vals else 0.0
        return rate, bool(row.get("deductSeveranceTax", False))
    return 0.0, False


## Pull the single flat rate out of a CC variable-expense rows array (entireWellLife='Flat').
## Returns 0.0 when rows are empty. Raises if rows are time-based — time-based variable rates aren't supported yet.
def _flatVariableRate(rows, valueKey):
    if not rows:
        return 0.0
    for row in rows:
        if row.get("entireWellLife") == "Flat":
            return float(row.get(valueKey) or 0)
    raise ValueError(f"only flat variable rates supported; got non-flat row in {rows!r}")


## Return the per-month fixed expense $ for a single CC fixed-expense category (e.g. monthlyWellCost).
## Row schema: each row has 'fixedExpense' plus either 'entireWellLife': 'Flat' (covers all months) or
## 'offsetToFirstSegment': N (this row covers N months starting after the previous row's end). After the
## last duration row, the last fixedExpense extends forever (CC default).
def _fixedExpenseForMonth(rows, month):
    if not rows:
        return 0.0
    cum_end = 0
    for row in rows:
        fe = float(row.get("fixedExpense") or 0)
        if row.get("entireWellLife") == "Flat":
            return fe
        offset = row.get("offsetToFirstSegment")
        if offset is None:
            continue
        cum_end += int(offset)
        if month <= cum_end:
            return fe
    return float(rows[-1].get("fixedExpense") or 0)


## Pick the monthly forecast bucket whose formation matches the AFE's primary Landing Zone.
## Falls back to the first non-empty bucket when no formation match is found.
def _selectBucket(afeData, monthlyForecastBuckets):
    buckets = monthlyForecastBuckets or []
    if not buckets:
        return None
    afeFormation = None
    if afeData is not None and "Landing Zone" in afeData.columns:
        lz = afeData["Landing Zone"].dropna().astype(str).str.strip().str.upper()
        if not lz.empty:
            afeFormation = lz.iloc[0]
    if afeFormation:
        match = next((b for b in buckets if b.get("fm") and str(b["fm"]).upper() == afeFormation), None)
        if match:
            return match
    return buckets[0]


## Compute monthly NGL from a P50 gas curve and a CC stream-properties record.
## Standard formula: NGL_bbl = Gas_Mcf × NGL_yield_bbl_per_MMcf / 1000.
## NGL yield is read from record["yields"]["ngl"]["rows"][0]["yield"] (bbl/MMcf).
def computeNglFromGasP50(gasP50, streamPropertiesRec):
    try:
        ngl_yield = streamPropertiesRec["yields"]["ngl"]["rows"][0]["yield"]
    except (KeyError, IndexError, TypeError) as e:
        raise ValueError(f"stream-properties record missing yields.ngl.rows[0].yield: {e}")
    if not ngl_yield:
        raise ValueError(f"stream-properties NGL yield is zero or missing (got {ngl_yield!r})")
    factor = float(ngl_yield) / 1000.0
    return [float(g) * factor for g in gasP50]


## Run the full AFE economics flow against the resolved CC econ models.
## Returns dict carrying month index + P50 phase curves + step-by-step derived quantities,
## so downstream steps (revenue, expenses, NPV, etc.) can append without re-fetching inputs.
## cashPromote: decimal (e.g. 0.15 for 15%) applied to net CapEx as "cost of getting into the deal".
## carry: decimal (e.g. 0.15 for "Carry Through The Tanks") — reduces effective NRI/WI on
## production and OPEX, but CapEx stays at the pre-carry WI (you still pay full D&C share).
## Sensitivity scalers (default 1.0 — no change):
##   capexScaler: multiplies the AFE-derived capex_net BEFORE promote; promote $$ stays frozen at
##                cashPromote × ORIGINAL capex_net so it tracks the proposed AFE, not the scaled one.
##   oilPriceScaler / gasPriceScaler: multiply the raw oil deck / flat gas price BEFORE differential.
def computeEconomics(afeData, monthlyForecastBuckets, afeEconModels, cashPromote=0.0, carry=0.0,
                     capexScaler=1.0, oilPriceScaler=1.0, gasPriceScaler=1.0):
    bucket = _selectBucket(afeData, monthlyForecastBuckets)
    if bucket is None:
        raise ValueError("computeEconomics: no monthly forecast bucket available")
    curves = bucket["curves"]

    econ = {
        "bucket_label": bucket["label"],
        "months": list(curves["months"]),
        "oil_p50": list(curves["oil_p50"]),
        "gas_p50": list(curves["gas_p50"]),
        "water_p50": list(curves["water_p50"]),
    }

    # Step 1: NGL from gas P50 + stream-properties yield
    econ["ngl_p50"] = computeNglFromGasP50(curves["gas_p50"], afeEconModels["stream-properties"])

    # Step 2: Net oil/gas/NGL = gross × NRI from AFE Summary.
    # Carry Through The Tanks (if specified) reduces effective NRI: you get fewer barrels.
    nri = _resolveAfeNri(afeData)
    if nri is None:
        raise ValueError("computeEconomics: NRI column missing or unparseable in AFE Summary")
    carry_pct = float(carry or 0.0)
    effective_nri = nri * (1.0 - carry_pct)
    econ["nri"] = nri
    econ["carry_pct"] = carry_pct
    econ["effective_nri"] = effective_nri
    econ["net_oil_p50"] = [v * effective_nri for v in econ["oil_p50"]]
    econ["net_gas_p50"] = [v * effective_nri for v in econ["gas_p50"]]
    econ["net_ngl_p50"] = [v * effective_nri for v in econ["ngl_p50"]]

    # Step 3: Realized prices per month
    #   realized_oil = oil_price_at_date + sum(oil differentials)    [CC sign convention]
    #   realized_gas = gas_price * (BTU/1038) + sum(gas differentials)
    #   realized_ngl = oil_price_at_date * pctOfOil + sum(ngl differentials)   [CC uses base oil, not realized]
    today = pd.Timestamp.today().normalize()
    oil_price_scaler = float(oilPriceScaler or 1.0)
    gas_price_scaler = float(gasPriceScaler or 1.0)
    oil_deck = [(d, p * oil_price_scaler) for d, p in _resolveOilPriceDeck(afeEconModels["pricing"])]
    gas_price = _resolveGasPrice(afeEconModels["pricing"]) * gas_price_scaler
    ngl_pct = _resolveNglPctOfOil(afeEconModels["pricing"])
    oil_diff_total = _sumDifferentials(afeEconModels["differentials"], "oil", "dollarPerBbl")
    gas_diff_total = _sumDifferentials(afeEconModels["differentials"], "gas", "dollarPerMmbtu")
    ngl_diff_total = _sumDifferentials(afeEconModels["differentials"], "ngl", "dollarPerBbl")
    btu = _resolveBtu(afeEconModels["stream-properties"])
    btu_factor = btu / 1038.0
    realized_oil = []
    realized_gas = []
    realized_ngl = []
    realized_dates = []
    for m in econ["months"]:
        cal_date = today + pd.DateOffset(months=int(m) - 1)
        base_oil = _priceAtDate(oil_deck, cal_date)
        realized_oil.append(base_oil + oil_diff_total)
        realized_gas.append(gas_price * btu_factor + gas_diff_total)
        realized_ngl.append(base_oil * ngl_pct + ngl_diff_total)
        realized_dates.append(cal_date.strftime("%Y-%m-%d"))
    econ["calendar_dates"] = realized_dates
    econ["realized_oil_price"] = realized_oil
    econ["realized_gas_price"] = realized_gas
    econ["realized_ngl_price"] = realized_ngl
    econ["oil_diff_total"] = oil_diff_total
    econ["gas_diff_total"] = gas_diff_total
    econ["ngl_diff_total"] = ngl_diff_total
    econ["ngl_pct_of_oil"] = ngl_pct
    econ["btu_content"] = btu

    # Step 4: Net revenue per phase + total
    #   oil_rev = net_oil_bbl * realized_oil_$/bbl
    #   gas_rev = net_gas_Mcf * (BTU/1000 MMBtu/Mcf) * realized_gas_$/MMBtu
    #   ngl_rev = net_ngl_bbl * realized_ngl_$/bbl
    mmbtu_per_mcf = btu / 1000.0
    econ["net_oil_revenue"] = [v * p for v, p in zip(econ["net_oil_p50"], realized_oil)]
    econ["net_gas_revenue"] = [v * mmbtu_per_mcf * p for v, p in zip(econ["net_gas_p50"], realized_gas)]
    econ["net_ngl_revenue"] = [v * p for v, p in zip(econ["net_ngl_p50"], realized_ngl)]
    econ["total_net_revenue"] = [o + g + n for o, g, n in zip(
        econ["net_oil_revenue"], econ["net_gas_revenue"], econ["net_ngl_revenue"]
    )]

    # Step 5a: Fixed OPEX per month × effective WI
    #   fixed = (monthlyWellCost + otherMonthlyCost1..8) for the month, scaled by post-carry WI.
    wi = _resolveAfeWi(afeData)
    if wi is None:
        raise ValueError("computeEconomics: WI column missing or unparseable in AFE Summary")
    effective_wi = wi * (1.0 - carry_pct)
    econ["wi"] = wi
    econ["effective_wi"] = effective_wi
    fixedExpenses = afeEconModels["expenses"].get("fixedExpenses", {}) or {}
    fixed_categories = ["monthlyWellCost"] + [f"otherMonthlyCost{i}" for i in range(1, 9)]
    fixed_opex = []
    for m in econ["months"]:
        per_month_gross = 0.0
        for cat in fixed_categories:
            rows = fixedExpenses.get(cat, {}).get("rows", []) or []
            per_month_gross += _fixedExpenseForMonth(rows, int(m))
        fixed_opex.append(per_month_gross * effective_wi)
    econ["fixed_opex"] = fixed_opex

    # Step 5b: Variable OPEX per month × WI
    #   oil: sum of all 5 $/bbl categories × gross oil volume × WI
    #   gas G&P (gathering + processing) × gross gas volume × WI
    #   gas OPC (marketing + transportation + other) × gross gas volume × WI
    #   water disposal $/bbl × gross water volume × WI
    expensesRec = afeEconModels["expenses"]
    variableExpenses = expensesRec.get("variableExpenses", {}) or {}
    oilVar = variableExpenses.get("oil", {}) or {}
    gasVar = variableExpenses.get("gas", {}) or {}
    oil_cats = ("gathering", "marketing", "transportation", "processing", "other")
    oil_total_per_bbl = sum(_flatVariableRate(oilVar.get(k, {}).get("rows", []), "dollarPerBbl") for k in oil_cats)
    gp_per_mcf = sum(_flatVariableRate(gasVar.get(k, {}).get("rows", []), "dollarPerMcf") for k in ("gathering", "processing"))
    opc_per_mcf = sum(_flatVariableRate(gasVar.get(k, {}).get("rows", []), "dollarPerMcf") for k in ("marketing", "transportation", "other"))
    water_disposal_rows = expensesRec.get("waterDisposal", {}).get("rows", []) or []
    water_disposal_per_bbl = _flatVariableRate(water_disposal_rows, "dollarPerBbl")
    variable_oil_opex = [v * oil_total_per_bbl * effective_wi for v in econ["oil_p50"]]
    variable_gas_gp = [v * gp_per_mcf * effective_wi for v in econ["gas_p50"]]
    variable_gas_opc = [v * opc_per_mcf * effective_wi for v in econ["gas_p50"]]
    variable_gas_opex = [g + o for g, o in zip(variable_gas_gp, variable_gas_opc)]
    variable_water_disposal = [w * water_disposal_per_bbl * effective_wi for w in econ["water_p50"]]
    total_variable_opex = [a + b + c for a, b, c in zip(variable_oil_opex, variable_gas_opex, variable_water_disposal)]
    econ["variable_oil_opex"] = variable_oil_opex
    econ["variable_gas_gp"] = variable_gas_gp
    econ["variable_gas_opc"] = variable_gas_opc
    econ["variable_gas_opex"] = variable_gas_opex
    econ["variable_water_disposal"] = variable_water_disposal
    econ["total_variable_opex"] = total_variable_opex
    econ["oil_var_rate_per_bbl"] = oil_total_per_bbl
    econ["gas_gp_rate_per_mcf"] = gp_per_mcf
    econ["gas_opc_rate_per_mcf"] = opc_per_mcf
    econ["water_disposal_rate_per_bbl"] = water_disposal_per_bbl

    # Step 5c: Total operating cost per month = fixed + variable
    econ["total_opex"] = [f + v for f, v in zip(fixed_opex, total_variable_opex)]

    # Step 6: Production taxes (severance per phase + ad valorem)
    #   severance_<phase> = net_<phase>_revenue * pct  (CC calculation=nri already baked into net_*_revenue)
    #   ad_val = (total_net_revenue - total_severance if deductSeveranceTax else total_net_revenue) * pct
    taxRec = afeEconModels["production-taxes"]
    sev_oil_rate = _resolveSeveranceRate(taxRec, "oil")
    sev_gas_rate = _resolveSeveranceRate(taxRec, "gas")
    sev_ngl_rate = _resolveSeveranceRate(taxRec, "ngl")
    ad_val_rate, ad_val_deducts_sev = _resolveAdValTax(taxRec)
    severance_oil = [r * sev_oil_rate for r in econ["net_oil_revenue"]]
    severance_gas = [r * sev_gas_rate for r in econ["net_gas_revenue"]]
    severance_ngl = [r * sev_ngl_rate for r in econ["net_ngl_revenue"]]
    total_severance = [o + g + n for o, g, n in zip(severance_oil, severance_gas, severance_ngl)]
    ad_val_tax = []
    for tot_rev, sev in zip(econ["total_net_revenue"], total_severance):
        base = tot_rev - sev if ad_val_deducts_sev else tot_rev
        ad_val_tax.append(base * ad_val_rate)
    total_taxes = [s + a for s, a in zip(total_severance, ad_val_tax)]
    econ["severance_oil"] = severance_oil
    econ["severance_gas"] = severance_gas
    econ["severance_ngl"] = severance_ngl
    econ["total_severance"] = total_severance
    econ["ad_val_tax"] = ad_val_tax
    econ["total_taxes"] = total_taxes
    econ["severance_oil_rate"] = sev_oil_rate
    econ["severance_gas_rate"] = sev_gas_rate
    econ["severance_ngl_rate"] = sev_ngl_rate
    econ["ad_val_rate"] = ad_val_rate
    econ["ad_val_deducts_severance"] = ad_val_deducts_sev

    # Step 7: Net revenue per month = total revenue - OPEX - taxes
    econ["net_revenue"] = [
        r - o - t
        for r, o, t in zip(econ["total_net_revenue"], econ["total_opex"], econ["total_taxes"])
    ]

    # Step 8: CapEx in month 1 (negative — outflow) + net cash flow.
    # "Net AFE" / "Net CapEx" columns are already × WI; only gross values get the WI multiplier.
    # Cash promote = % of net CapEx added on top (cost of getting into the deal).
    capex_value, capex_is_net = _resolveAfeCapex(afeData)
    if capex_value is None:
        raise ValueError("computeEconomics: CapEx column missing in AFE Summary "
                         "(expected one of: Net AFE, CapEx, D&C Cost, AFE Cost)")
    if capex_is_net:
        capex_net = capex_value
        capex_gross = capex_value / wi if wi > 0 else capex_value
    else:
        capex_gross = capex_value
        capex_net = capex_value * wi
    cash_promote_pct = float(cashPromote or 0.0)
    # Promote is paid off the PROPOSED AFE, not the actual scaled CapEx, so freeze the $ amount
    # against the original (unscaled) net capex regardless of capexScaler.
    cash_promote_dollars = capex_net * cash_promote_pct
    capex_scaler = float(capexScaler or 1.0)
    capex_net_raw = capex_net * capex_scaler
    capex_net = capex_net_raw + cash_promote_dollars  # promote folds into invested capital
    capex_signed = [-capex_net if i == 0 else 0.0 for i in range(len(econ["months"]))]
    econ["capex_gross"] = capex_gross * capex_scaler
    econ["capex_net_raw"] = capex_net_raw
    econ["cash_promote_pct"] = cash_promote_pct
    econ["cash_promote_dollars"] = cash_promote_dollars
    econ["capex_scaler"] = capex_scaler
    econ["oil_price_scaler"] = oil_price_scaler
    econ["gas_price_scaler"] = gas_price_scaler
    econ["capex_net"] = capex_net
    econ["capex_source_is_net"] = capex_is_net
    econ["capex"] = capex_signed
    econ["net_cash_flow"] = [nr + cx for nr, cx in zip(econ["net_revenue"], capex_signed)]

    # Step 9: Headline metrics — NPV10, IRR, Project Multiple, Payout
    econ["npv10"] = _computeNPV(econ["net_cash_flow"], 0.10)
    irr_value, irr_reason = _computeIRR(econ["net_cash_flow"])
    econ["irr"] = irr_value
    econ["irr_reason"] = irr_reason
    if irr_value is None:
        print(f"  IRR not computed: {irr_reason}")
    econ["project_multiple"] = (sum(econ["net_revenue"]) / capex_net) if capex_net > 0 else None
    payout_month = None
    running = 0.0
    for i, v in enumerate(econ["net_cash_flow"]):
        running += v
        if running >= 0:
            payout_month = econ["months"][i]
            break
    econ["payout_month"] = payout_month

    return econ


## Run a CAPEX × pricing sensitivity grid. Default: CAPEX -20%..+20% by 5% (9 levels) and
## pricing -10%/0/+10% applied to BOTH oil and gas simultaneously (3 levels). 27 scenarios total.
## Promote $$ stays frozen at the original AFE × promote% per computeEconomics' design.
def computeSensitivity(afeData, monthlyForecastBuckets, afeEconModels,
                       cashPromote=0.0, carry=0.0,
                       capexDeltas=None, priceDeltas=None):
    if capexDeltas is None:
        capexDeltas = [-0.20, -0.15, -0.10, -0.05, 0.0, 0.05, 0.10, 0.15, 0.20]
    if priceDeltas is None:
        priceDeltas = [-0.10, 0.0, 0.10]
    results = []
    for cd in capexDeltas:
        for pd_ in priceDeltas:
            econ_run = computeEconomics(
                afeData, monthlyForecastBuckets, afeEconModels,
                cashPromote=cashPromote, carry=carry,
                capexScaler=1.0 + cd,
                oilPriceScaler=1.0 + pd_,
                gasPriceScaler=1.0 + pd_,
            )
            results.append({
                "capex_delta": float(cd),
                "price_delta": float(pd_),
                "capex_net": float(econ_run["capex_net"]),
                "npv10": float(econ_run["npv10"]) if econ_run["npv10"] is not None else None,
                "irr": float(econ_run["irr"]) if econ_run.get("irr") is not None else None,
                "project_multiple": float(econ_run["project_multiple"]) if econ_run.get("project_multiple") is not None else None,
                "payout_month": econ_run.get("payout_month"),
                "cum_net_cash_flow": float(sum(econ_run["net_cash_flow"])),
            })
    return {
        "capex_deltas": [float(x) for x in capexDeltas],
        "price_deltas": [float(x) for x in priceDeltas],
        "results": results,
    }


## Print a compact summary of the economics result. Useful for CLI runs while we iterate.
def printEconomicsSummary(econ):
    print(f"\n=== Economics — bucket: {econ['bucket_label']} ===")
    print(f"  Months: {len(econ['months'])}")
    print(f"  Cum Oil P50:   {sum(econ['oil_p50']):>14,.0f} bbl")
    print(f"  Cum Gas P50:   {sum(econ['gas_p50']):>14,.0f} Mcf")
    print(f"  Cum Water P50: {sum(econ['water_p50']):>14,.0f} bbl")
    if "ngl_p50" in econ:
        print(f"  Cum NGL P50:   {sum(econ['ngl_p50']):>14,.4f} bbl")
        print(f"  First 12 NGL:  {[round(v, 3) for v in econ['ngl_p50'][:12]]}")
    if "nri" in econ:
        print(f"\n  NRI:           {econ['nri']:.4f}  ({econ['nri'] * 100:.2f}%)")
        if econ.get("carry_pct", 0) > 0:
            print(f"  Carry:         {econ['carry_pct']:.4f}  ({econ['carry_pct'] * 100:.2f}%)")
            print(f"  Effective NRI: {econ['effective_nri']:.4f}  (NRI * (1-carry))")
        print(f"  Cum Net Oil:   {sum(econ['net_oil_p50']):>14,.0f} bbl")
        print(f"  Cum Net Gas:   {sum(econ['net_gas_p50']):>14,.0f} Mcf")
        print(f"  Cum Net NGL:   {sum(econ['net_ngl_p50']):>14,.4f} bbl")
    if "realized_oil_price" in econ:
        ro = econ["realized_oil_price"]
        rg = econ["realized_gas_price"]
        rn = econ["realized_ngl_price"]
        print(f"\n  Oil diff total: ${econ['oil_diff_total']:.2f}/bbl")
        print(f"  Gas diff total: ${econ['gas_diff_total']:.2f}/MMBtu")
        print(f"  NGL diff total: ${econ['ngl_diff_total']:.2f}/bbl  (NGL = {econ['ngl_pct_of_oil']*100:.1f}% of base oil)")
        print(f"  BTU content:    {econ['btu_content']:.0f} BTU/cf  (factor = BTU/1038 = {econ['btu_content']/1038:.4f})")
        print(f"  Realized oil avg: ${sum(ro)/len(ro):.2f}/bbl  (min ${min(ro):.2f}, max ${max(ro):.2f})")
        print(f"  Realized gas:     ${rg[0]:.4f}/MMBtu  (flat)")
        print(f"  Realized NGL avg: ${sum(rn)/len(rn):.2f}/bbl  (min ${min(rn):.2f}, max ${max(rn):.2f})")
        print(f"  First 6 oil $/bbl: {[round(p, 2) for p in ro[:6]]}")
        print(f"  First 6 NGL $/bbl: {[round(p, 2) for p in rn[:6]]}")
        print(f"  First 6 dates:    {econ['calendar_dates'][:6]}")
    if "total_net_revenue" in econ:
        cum_oil_rev = sum(econ["net_oil_revenue"])
        cum_gas_rev = sum(econ["net_gas_revenue"])
        cum_ngl_rev = sum(econ["net_ngl_revenue"])
        cum_total = sum(econ["total_net_revenue"])
        print(f"\n  Cum Net Oil Rev: ${cum_oil_rev:>16,.0f}")
        print(f"  Cum Net Gas Rev: ${cum_gas_rev:>16,.0f}")
        print(f"  Cum Net NGL Rev: ${cum_ngl_rev:>16,.0f}")
        print(f"  Cum TOTAL Rev:   ${cum_total:>16,.0f}")
    if "fixed_opex" in econ:
        fx = econ["fixed_opex"]
        print(f"\n  WI:            {econ['wi']:.4f}  ({econ['wi'] * 100:.2f}%)")
        if econ.get("carry_pct", 0) > 0:
            print(f"  Effective WI:  {econ['effective_wi']:.4f}  (WI * (1-carry); used for OPEX)")
        print(f"  Cum Fixed OPEX:  ${sum(fx):>16,.0f}")
        print(f"  First 12 fixed:  {[round(v, 0) for v in fx[:12]]}")
    if "total_variable_opex" in econ:
        oil_v = sum(econ["variable_oil_opex"])
        gp = sum(econ["variable_gas_gp"])
        opc = sum(econ["variable_gas_opc"])
        wd = sum(econ.get("variable_water_disposal", [0]))
        tot_v = sum(econ["total_variable_opex"])
        print(f"\n  Oil var rate:     ${econ['oil_var_rate_per_bbl']:.2f}/bbl")
        print(f"  Gas G&P rate:     ${econ['gas_gp_rate_per_mcf']:.4f}/Mcf (gathering + processing)")
        print(f"  Gas OPC rate:     ${econ['gas_opc_rate_per_mcf']:.4f}/Mcf (mkt + transport + other)")
        print(f"  Water disp rate:  ${econ.get('water_disposal_rate_per_bbl', 0):.4f}/bbl")
        print(f"  Cum Oil Var OPEX: ${oil_v:>16,.0f}")
        print(f"  Cum Gas G&P:      ${gp:>16,.0f}")
        print(f"  Cum Gas OPC:      ${opc:>16,.0f}")
        print(f"  Cum Water Disp:   ${wd:>16,.0f}")
        print(f"  Cum TOTAL Var:    ${tot_v:>16,.0f}")
    if "total_opex" in econ:
        print(f"\n  Cum TOTAL OPEX:   ${sum(econ['total_opex']):>16,.0f}  (fixed + variable)")
    if "total_taxes" in econ:
        print(f"\n  Sev rates: oil {econ['severance_oil_rate']*100:.2f}%  gas {econ['severance_gas_rate']*100:.2f}%  ngl {econ['severance_ngl_rate']*100:.2f}%")
        print(f"  Ad val rate: {econ['ad_val_rate']*100:.2f}%  (deducts severance: {econ['ad_val_deducts_severance']})")
        print(f"  Cum Severance Oil: ${sum(econ['severance_oil']):>14,.0f}")
        print(f"  Cum Severance Gas: ${sum(econ['severance_gas']):>14,.0f}")
        print(f"  Cum Severance NGL: ${sum(econ['severance_ngl']):>14,.0f}")
        print(f"  Cum TOTAL Severance: ${sum(econ['total_severance']):>12,.0f}")
        print(f"  Cum Ad Val:        ${sum(econ['ad_val_tax']):>14,.0f}")
        print(f"  Cum TOTAL Taxes:   ${sum(econ['total_taxes']):>14,.0f}")
    if "net_revenue" in econ:
        print(f"\n  Cum NET REVENUE:   ${sum(econ['net_revenue']):>14,.0f}  (Total Rev - OPEX - Taxes)")
    if "net_cash_flow" in econ:
        print(f"\n  CapEx gross:       ${econ['capex_gross']:>14,.0f}")
        print(f"  CapEx net raw:     ${econ.get('capex_net_raw', econ['capex_net']):>14,.0f}  (before promote)")
        if econ.get("cash_promote_pct", 0) > 0:
            print(f"  Cash Promote:      {econ['cash_promote_pct']*100:>13.2f}%  (+${econ['cash_promote_dollars']:,.0f})")
        print(f"  CapEx net (final): ${econ['capex_net']:>14,.0f}  (applied as -${econ['capex_net']:,.0f} in month 1)")
        print(f"  Cum NET CASH FLOW: ${sum(econ['net_cash_flow']):>14,.0f}  (Net Rev + CapEx)")
    if "npv10" in econ:
        irr_str = f"{econ['irr']*100:.2f}%" if econ.get("irr") is not None else f"n/a ({econ.get('irr_reason', 'unknown')})"
        mult_str = f"{econ['project_multiple']:.2f}x" if econ.get("project_multiple") is not None else "n/a"
        payout_str = f"month {econ['payout_month']}" if econ.get("payout_month") is not None else "no payout"
        print(f"\n  NPV10:             ${econ['npv10']:>14,.0f}")
        print(f"  IRR:               {irr_str:>15}")
        print(f"  Project Multiple:  {mult_str:>15}  (cum net rev / capex net)")
        print(f"  Months to Payout:  {payout_str:>15}")


## Entrypoint for the AFE economics flow. CLI prompts for basin/cashPromote/carry only fire
## when those args are None (backward compat); main_model.py collects them upfront and passes
## explicit values in. Runs computeEconomics + writes the gross-volume Excel + economics HTML.
def runAfeEconomics(afeData, monthlyForecastBuckets, pathToAfeSummary,
                    basinCode=None, cashPromote=None, carry=None):
    if basinCode is None:
        basinCode = input("Which Basin: ").strip()
    if cashPromote is None:
        cashPromoteRaw = input("Cash Promote (% or decimal, blank=0): ").strip()
        cashPromote = float(cashPromoteRaw) if cashPromoteRaw else 0.0
        if cashPromote > 1.0:
            cashPromote /= 100.0
    if carry is None:
        carryRaw = input("Carry Through Tanks (% or decimal, blank=0): ").strip()
        carry = float(carryRaw) if carryRaw else 0.0
        if carry > 1.0:
            carry /= 100.0

    ccClient = combocurve.ComboCurveClient.from_env()
    companyModels = combocurve.getCompanyModels(ccClient)
    afeEconModels = combocurve.resolveAfeEconModels(afeData, basinCode, companyModels)
    print("\n=== Resolved AFE Econ Models ===")
    for t, r in afeEconModels.items():
        print(f"  {t:18}  {r.get('name')}  (id={r.get('id')})")

    econ = computeEconomics(
        afeData, monthlyForecastBuckets, afeEconModels,
        cashPromote=cashPromote, carry=carry,
    )
    sensitivity = computeSensitivity(
        afeData, monthlyForecastBuckets, afeEconModels,
        cashPromote=cashPromote, carry=carry,
    )
    exportGrossVolumesExcel(econ, pathToAfeSummary)
    plotEconomicsHTML(econ, pathToAfeSummary, sensitivity=sensitivity)
    return econ


## Export gross monthly oil/gas/water volumes to Excel in the same Data folder as the HTML.
## Useful for ad-hoc testing — pull the curves out without re-running the pipeline.
def exportGrossVolumesExcel(econ, pathToAfeSummary):
    afeDir = os.path.dirname(pathToAfeSummary)
    afeFilename = os.path.splitext(os.path.basename(pathToAfeSummary))[0]
    dsuName = afeFilename.split("-", 1)[1].strip() if "-" in afeFilename else afeFilename
    outputDir = os.path.join(afeDir, "Data")
    os.makedirs(outputDir, exist_ok=True)
    outPath = os.path.join(outputDir, f"gross_volumes_{dsuName}.xlsx")
    df = pd.DataFrame({
        "Month": econ["months"],
        "Date": econ["calendar_dates"],
        "Oil (bbl)": econ["oil_p50"],
        "Gas (Mcf)": econ["gas_p50"],
        "Water (bbl)": econ["water_p50"],
    })
    df.to_excel(outPath, index=False)
    print(f"Done. Saved gross volumes to {outPath}")
    return outPath


## Render the economics result as a wide time-series HTML table next to the AFE Summary.
## Output: Data/economics_{dsuName}.html (same folder as plotSubsurfaceHeatMapsHTML).
## sensitivity: optional dict from computeSensitivity() — renders an NPV-vs-CAPEX line chart
## (one line per pricing scenario) plus the 9×3 grid table above the monthly time-series.
def plotEconomicsHTML(econ, pathToAfeSummary, sensitivity=None):
    afeDir = os.path.dirname(pathToAfeSummary)
    afeFilename = os.path.splitext(os.path.basename(pathToAfeSummary))[0]
    dsuName = afeFilename.split("-", 1)[1].strip() if "-" in afeFilename else afeFilename
    outputDir = os.path.join(afeDir, "Data")
    os.makedirs(outputDir, exist_ok=True)
    htmlPath = os.path.join(outputDir, f"economics_{dsuName}.html")

    # Running cumulative cash flow for the trailing table column
    cum_cf = []
    running = 0.0
    for v in econ["net_cash_flow"]:
        running += v
        cum_cf.append(running)
    payout_month = econ.get("payout_month")

    def fmt_int(v): return f"{v:,.0f}"
    def fmt_dec(v, d=2): return f"{v:,.{d}f}"
    def fmt_dollar(v): return f"${v:,.0f}"
    def fmt_pct(v): return f"{v * 100:.2f}%"
    def cls(v): return "num neg" if v < 0 else "num"

    # Per-month broadcast of scalar WI/NRI so they render as columns in the time-series table.
    # Use the effective (post-carry) values since that's what every other column derives from;
    # base WI/NRI/carry are already surfaced in the header subtitle and summary cards.
    n_months_local = len(econ["months"])
    eff_wi = econ.get("effective_wi", econ.get("wi", 0.0))
    eff_nri = econ.get("effective_nri", econ.get("nri", 0.0))
    econ["wi_series"] = [eff_wi] * n_months_local
    econ["nri_series"] = [eff_nri] * n_months_local

    # Column groups: each (group_label, [(col_label, key, formatter)])
    groups = [
        ("Period", [
            ("Mo",   "months",          lambda v: str(int(v))),
            ("Date", "calendar_dates",  lambda v: v),
        ]),
        ("Gross Volumes", [
            ("Oil (bbl)",   "oil_p50",   fmt_int),
            ("Gas (Mcf)",   "gas_p50",   fmt_int),
            ("NGL (bbl)",   "ngl_p50",   lambda v: fmt_dec(v, 2)),
            ("Water (bbl)", "water_p50", fmt_int),
        ]),
        ("Net Volumes (×NRI)", [
            ("Net Oil (bbl)", "net_oil_p50", fmt_int),
            ("Net Gas (Mcf)", "net_gas_p50", fmt_int),
            ("Net NGL (bbl)", "net_ngl_p50", lambda v: fmt_dec(v, 2)),
        ]),
        ("Interests", [
            ("WI",  "wi_series",  fmt_pct),
            ("NRI", "nri_series", fmt_pct),
        ]),
        ("Realized Prices", [
            ("Oil $/bbl",    "realized_oil_price", lambda v: fmt_dec(v, 2)),
            ("Gas $/MMBtu",  "realized_gas_price", lambda v: fmt_dec(v, 4)),
            ("NGL $/bbl",    "realized_ngl_price", lambda v: fmt_dec(v, 2)),
        ]),
        ("Net Revenue", [
            ("Oil Rev",   "net_oil_revenue",   fmt_dollar),
            ("Gas Rev",   "net_gas_revenue",   fmt_dollar),
            ("NGL Rev",   "net_ngl_revenue",   fmt_dollar),
            ("Total Rev", "total_net_revenue", fmt_dollar),
        ]),
        ("OPEX (×WI)", [
            ("Fixed",       "fixed_opex",              fmt_dollar),
            ("Oil Var",     "variable_oil_opex",       fmt_dollar),
            ("Gas G&P",     "variable_gas_gp",         fmt_dollar),
            ("Gas OPC",     "variable_gas_opc",        fmt_dollar),
            ("Water Disp",  "variable_water_disposal", fmt_dollar),
            ("Total OPEX",  "total_opex",              fmt_dollar),
        ]),
        ("Taxes", [
            ("Sev Oil", "severance_oil", fmt_dollar),
            ("Sev Gas", "severance_gas", fmt_dollar),
            ("Sev NGL", "severance_ngl", fmt_dollar),
            ("Ad Val",  "ad_val_tax",    fmt_dollar),
            ("Total Tax", "total_taxes", fmt_dollar),
        ]),
        ("Cash Flow", [
            ("Net Rev (after OPEX+Tax)", "net_revenue",    fmt_dollar),
            ("CapEx",                    "capex",          fmt_dollar),
            ("Net Cash Flow",            "net_cash_flow",  fmt_dollar),
        ]),
    ]

    # Cumulative cash flow as a synthetic column
    n_months = len(econ["months"])

    # Build header rows
    group_header = "<tr class='group-header'>" + "".join(
        f"<th colspan='{len(cols)}'>{label}</th>" for label, cols in groups
    ) + "<th rowspan='2'>Cum Cash Flow</th></tr>"
    col_header = "<tr>" + "".join(
        f"<th>{label}</th>" for _, cols in groups for label, *_ in cols
    ) + "</tr>"

    # Flat (label, key) list for the CSV export — append the synthetic Cum CF column at the end.
    csv_columns = [(label, key) for _, cols in groups for label, key, _ in cols]
    csv_columns.append(("Cum Cash Flow", "_cum_cf"))
    # Build the raw export rows alongside the HTML rows so values land at full precision.
    csv_rows = []

    # Build body rows
    body_rows = []
    for i in range(n_months):
        cells = []
        row_dict = {}
        for _label, cols in groups:
            for col_label, key, formatter in cols:
                v = econ[key][i]
                row_dict[col_label] = v
                if key in ("months", "calendar_dates"):
                    cells.append(f"<td>{formatter(v)}</td>")
                else:
                    cells.append(f"<td class='{cls(v)}'>{formatter(v)}</td>")
        row_dict["Cum Cash Flow"] = cum_cf[i]
        cells.append(f"<td class='{cls(cum_cf[i])}'>{fmt_dollar(cum_cf[i])}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
        csv_rows.append(row_dict)

    # Summary block
    cum_oil   = sum(econ["oil_p50"])
    cum_gas   = sum(econ["gas_p50"])
    cum_ngl   = sum(econ["ngl_p50"])
    cum_water = sum(econ["water_p50"])
    cum_rev   = sum(econ["total_net_revenue"])
    cum_opex  = sum(econ["total_opex"])
    cum_tax   = sum(econ["total_taxes"])
    cum_ncf   = sum(econ["net_cash_flow"])
    payout_str = f"month {payout_month}" if payout_month else f"not in {n_months}-month horizon"

    # Sensitivity table HTML — built only when sensitivity payload is provided.
    sensitivity_section = ""
    sensitivity_js = ""
    if sensitivity and sensitivity.get("results"):
        capex_deltas = sensitivity["capex_deltas"]
        price_deltas = sensitivity["price_deltas"]
        by_key = {(r["capex_delta"], r["price_delta"]): r for r in sensitivity["results"]}
        # IRR table: rows = CAPEX delta, cols = price delta. Red when IRR < 10% (below typical hurdle).
        sens_header = "<tr><th rowspan='2'>CAPEX Δ</th><th colspan='" + str(len(price_deltas)) + "'>IRR (annual)  (price Δ)</th></tr>"
        sens_header += "<tr>" + "".join(f"<th>{p*100:+.0f}%</th>" for p in price_deltas) + "</tr>"
        sens_rows = []
        for cd in capex_deltas:
            row_cells = [f"<td class='label'>{cd*100:+.0f}%</td>"]
            for pd_ in price_deltas:
                r = by_key.get((cd, pd_))
                if r is None or r.get("irr") is None:
                    cell = "—"
                    cls_str = ""
                else:
                    cell = f"{r['irr']*100:.2f}%"
                    cls_str = "neg" if r["irr"] < 0.10 else ""
                if cd == 0.0 and pd_ == 0.0:
                    cls_str = (cls_str + " base").strip()
                row_cells.append(f"<td class='{cls_str}'>{cell}</td>")
            sens_rows.append("<tr>" + "".join(row_cells) + "</tr>")
        irr_table_html = "<table>" + sens_header + "".join(sens_rows) + "</table>"

        # Companion table: IRR, Multiple, Payout per (capex, price) pair — one row per scenario.
        scenario_rows = []
        scenario_rows.append(
            "<tr><th>CAPEX Δ</th><th>Price Δ</th><th>NPV10</th><th>IRR</th><th>Multiple</th><th>Payout</th></tr>"
        )
        for cd in capex_deltas:
            for pd_ in price_deltas:
                r = by_key.get((cd, pd_))
                if r is None:
                    continue
                base_cls = " base" if (cd == 0.0 and pd_ == 0.0) else ""
                npv_cls = "neg" if (r["npv10"] is not None and r["npv10"] < 0) else ""
                irr_str = f"{r['irr']*100:.2f}%" if r.get("irr") is not None else "n/a"
                mult_str = f"{r['project_multiple']:.2f}x" if r.get("project_multiple") is not None else "n/a"
                payout_cell = f"{r['payout_month']} mo" if r.get("payout_month") is not None else "—"
                scenario_rows.append(
                    f"<tr><td class='center{base_cls}'>{cd*100:+.0f}%</td>"
                    f"<td class='center{base_cls}'>{pd_*100:+.0f}%</td>"
                    f"<td class='{npv_cls}{base_cls}'>${r['npv10']:,.0f}</td>"
                    f"<td class='{base_cls}'>{irr_str}</td>"
                    f"<td class='{base_cls}'>{mult_str}</td>"
                    f"<td class='{base_cls}'>{payout_cell}</td></tr>"
                )
        scenario_table_html = "<table>" + "".join(scenario_rows) + "</table>"

        sensitivity_section = (
            "<div class='sensitivity'>"
            "<details>"
            "<summary>Sensitivity — CAPEX × Price (IRR grid + NPV10 chart) — click to expand</summary>"
            "<div class='grid'>"
            "<div><div id='sens-chart' class='chart'></div></div>"
            f"<div>{irr_table_html}</div>"
            "</div>"
            f"<div style='margin-top:14px;'>{scenario_table_html}</div>"
            "</details>"
            "</div>"
        )

        # Plotly traces — one line per price delta, X = CAPEX delta %, Y = NPV10.
        sens_traces = []
        colors = ["#c0392b", "#0a2a5e", "#2e8b57", "#f28c28", "#8e44ad"]
        for i, pd_ in enumerate(price_deltas):
            xs = [cd * 100 for cd in capex_deltas]
            ys = [by_key[(cd, pd_)]["npv10"] for cd in capex_deltas]
            sens_traces.append({
                "type": "scatter", "mode": "lines+markers",
                "name": f"Price {pd_*100:+.0f}%",
                "x": xs, "y": ys,
                "line": {"color": colors[i % len(colors)], "width": 2},
                "marker": {"size": 8},
            })
        # Render lazily on first <details> open — Plotly needs the container to have layout dimensions.
        sensitivity_js = (
            "const __SENS_TRACES = " + _json.dumps(sens_traces) + ";"
            "const __SENS_LAYOUT = {"
            "title: {text: 'NPV10 vs CAPEX Δ', font: {size: 14}},"
            "xaxis: {title: 'CAPEX Δ (%)', zeroline: true, zerolinecolor: '#888'},"
            "yaxis: {title: 'NPV @ 10% ($)', zeroline: true, zerolinecolor: '#888', tickformat: '$,.0f'},"
            "margin: {l: 80, r: 16, t: 40, b: 50},"
            "hovermode: 'x unified',"
            "legend: {orientation: 'h', y: -0.2}"
            "};"
            "let __sensRendered = false;"
            "function __renderSens() {"
            "  if (__sensRendered) return;"
            "  Plotly.newPlot('sens-chart', __SENS_TRACES, __SENS_LAYOUT, {responsive: true});"
            "  __sensRendered = true;"
            "}"
            "document.addEventListener('DOMContentLoaded', () => {"
            "  const det = document.querySelector('.sensitivity details');"
            "  if (!det) return;"
            "  det.addEventListener('toggle', () => { if (det.open) __renderSens(); });"
            "  if (det.open) __renderSens();"
            "});"
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Economics — {dsuName}</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  html, body {{ height: 100%; }}
  body {{ font-family: Arial, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: #f5f5f5; color: #333; display: flex; flex-direction: column; overflow: hidden; }}
  .header {{ background: #0a2a5e; color: white; padding: 14px 28px; flex: 0 0 auto;
             display: flex; align-items: center; justify-content: space-between; gap: 16px; }}
  .header .title-block {{ flex: 1; min-width: 0; }}
  .header h1 {{ font-size: 20px; font-weight: 600; }}
  .header .sub {{ font-size: 12px; opacity: 0.8; margin-top: 4px; }}
  .header .export-btn {{ background: #f28c28; color: #fff; border: none; padding: 8px 14px;
                          border-radius: 4px; cursor: pointer; font-size: 12px; font-weight: bold;
                          white-space: nowrap; flex: 0 0 auto; }}
  .header .export-btn:hover {{ background: #d77a1f; }}
  .header .export-btn:active {{ background: #b86916; }}
  .summary {{ background: #fff; padding: 10px 28px; border-bottom: 1px solid #ddd;
              display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
              gap: 10px; font-size: 12px; flex: 0 0 auto; }}
  .summary .card {{ background: #fafbfc; border: 1px solid #e5e7eb; border-radius: 4px; padding: 6px 10px; }}
  .summary .card .lbl {{ color: #666; font-size: 10px; }}
  .summary .card .val {{ font-family: monospace; font-size: 13px; font-weight: bold; margin-top: 2px; }}
  .summary .card .val.neg {{ color: #c00; }}
  /* Hero metric cards — bigger, color-coded for NPV10 / IRR / Project Multiple */
  .summary .card.hero {{ padding: 10px 14px; border: 2px solid; }}
  .summary .card.hero .lbl {{ font-size: 11px; font-weight: bold; text-transform: uppercase; letter-spacing: 0.5px; }}
  .summary .card.hero .val {{ font-size: 22px; margin-top: 6px; }}
  .summary .card.hero.pos {{ border-color: #2e8b57; background: #ecf7ef; }}
  .summary .card.hero.pos .val {{ color: #1f6e3f; }}
  .summary .card.hero.neg {{ border-color: #c0392b; background: #fdecea; }}
  .summary .card.hero.neg .val {{ color: #962d22; }}
  .summary .card.hero.neutral {{ border-color: #888; background: #f4f4f4; }}
  /* Sensitivity section — collapsible; capped at 60vh when open so the time-series table
     below it always has room (otherwise it gets squeezed to 0 by the flex layout). */
  .sensitivity {{ background: #fff; padding: 10px 28px; border-bottom: 1px solid #ddd; flex: 0 0 auto; }}
  .sensitivity details[open] {{ max-height: 60vh; overflow: auto; }}
  .sensitivity summary {{ cursor: pointer; font-size: 13px; font-weight: bold; padding: 4px 0;
                           list-style: none; user-select: none; }}
  .sensitivity summary::before {{ content: '▶ '; font-size: 10px; }}
  .sensitivity details[open] summary::before {{ content: '▼ '; }}
  .sensitivity .grid {{ display: grid; grid-template-columns: minmax(360px, 1fr) minmax(360px, 1fr);
                         gap: 18px; margin-top: 10px; align-items: start; }}
  .sensitivity .chart {{ height: 320px; }}
  .sensitivity table {{ border-collapse: collapse; width: 100%; font-size: 11px; }}
  .sensitivity table th {{ background: #4a6491; color: #fff; padding: 5px 8px; }}
  .sensitivity table td {{ padding: 4px 8px; border-bottom: 1px solid #eee; font-family: monospace; text-align: right; }}
  .sensitivity table td.label {{ background: #f4f6fa; font-weight: bold; text-align: left; }}
  .sensitivity table td.center {{ text-align: center; }}
  .sensitivity table td.neg {{ color: #c00; }}
  .sensitivity table td.base {{ background: #fff7e6; }}
  /* Fill remaining viewport height; scrollbars (both axes) live inside this box so the
     horizontal scroll is always reachable without scrolling the page to the bottom. */
  .table-wrap {{ flex: 1 1 auto; overflow: auto; padding: 0 28px 16px 28px; min-height: 0; }}
  table {{ border-collapse: collapse; width: max-content; min-width: 100%; background: #fff;
           box-shadow: 0 1px 2px rgba(0, 0, 0, 0.06); }}
  thead th {{ background: #0a2a5e; color: #fff; padding: 6px 10px; font-size: 11px;
              position: sticky; top: 0; z-index: 5; }}
  thead tr.group-header th {{ background: #4a6491; font-size: 11px; border-right: 1px solid #6a85b0; top: 0; }}
  thead tr:nth-child(2) th {{ top: 26px; }}
  td {{ padding: 4px 10px; font-size: 11px; border-bottom: 1px solid #eee; white-space: nowrap; }}
  td.num {{ font-family: monospace; text-align: right; }}
  td.neg {{ color: #c00; }}
  tbody tr:nth-child(even) td {{ background: #fafbfc; }}
  tbody tr:hover td {{ background: #f0f4ff; }}
</style>
</head>
<body>
<div class="header">
  <div class="title-block">
    <h1>Economics — {dsuName}</h1>
    <div class="sub">Bucket: {econ.get("bucket_label", "—")} &middot; NRI: {econ.get("nri", 0)*100:.2f}% &middot; WI: {econ.get("wi", 0)*100:.2f}%{(f" &middot; Carry: {econ.get('carry_pct', 0)*100:.2f}% (eff NRI {econ.get('effective_nri', 0)*100:.2f}%, eff WI {econ.get('effective_wi', 0)*100:.2f}%)" if econ.get("carry_pct", 0) > 0 else "")} &middot; Months: {n_months} &middot; Payout: {payout_str}</div>
  </div>
  <button class="export-btn" onclick="exportCSV()">⬇ Export CSV</button>
</div>
<div class="summary">
  <div class="card hero {('pos' if econ.get('npv10', 0) > 0 else 'neg')}"><div class="lbl">NPV @ 10%</div><div class="val">{fmt_dollar(econ.get('npv10', 0))}</div></div>
  <div class="card hero {('pos' if (econ.get('irr') is not None and econ['irr'] > 0.10) else 'neutral' if econ.get('irr') is None else 'neg')}"><div class="lbl">IRR (annual)</div><div class="val">{(f"{econ['irr']*100:.2f}%" if econ.get('irr') is not None else 'n/a')}</div>{(f'<div style="font-size:10px;color:#666;margin-top:4px;">{econ.get("irr_reason", "")}</div>' if econ.get('irr') is None else '')}</div>
  <div class="card hero {('pos' if (econ.get('project_multiple') or 0) >= 1.0 else 'neg')}"><div class="lbl">Project Multiple</div><div class="val">{(f"{econ['project_multiple']:.2f}x" if econ.get('project_multiple') is not None else 'n/a')}</div></div>
  <div class="card hero {('pos' if (payout_month is not None and payout_month <= 36) else 'neutral' if payout_month is not None else 'neg')}"><div class="lbl">Months to Payout</div><div class="val">{(f"{payout_month} mo" if payout_month is not None else 'no payout')}</div>{(f'<div style="font-size:10px;color:#666;margin-top:4px;">cum CF crosses $0 at month {payout_month}</div>' if payout_month is not None else f'<div style="font-size:10px;color:#666;margin-top:4px;">cum CF stays negative through {n_months} mo</div>')}</div>
  <div class="card"><div class="lbl">Cum Oil (gross)</div><div class="val">{fmt_int(cum_oil)} bbl</div></div>
  <div class="card"><div class="lbl">Cum Gas (gross)</div><div class="val">{fmt_int(cum_gas)} Mcf</div></div>
  <div class="card"><div class="lbl">Cum NGL (gross)</div><div class="val">{fmt_dec(cum_ngl, 2)} bbl</div></div>
  <div class="card"><div class="lbl">Cum Water (gross)</div><div class="val">{fmt_int(cum_water)} bbl</div></div>
  <div class="card"><div class="lbl">Cum Total Revenue</div><div class="val">{fmt_dollar(cum_rev)}</div></div>
  <div class="card"><div class="lbl">Cum OPEX</div><div class="val">{fmt_dollar(cum_opex)}</div></div>
  <div class="card"><div class="lbl">Cum Taxes</div><div class="val">{fmt_dollar(cum_tax)}</div></div>
  <div class="card"><div class="lbl">CapEx Net Raw (×WI)</div><div class="val neg">-{fmt_dollar(econ.get("capex_net_raw", econ.get("capex_net", 0)))}</div></div>
  <div class="card"><div class="lbl">Cash Promote ({(econ.get('cash_promote_pct', 0) * 100):.2f}%)</div><div class="val {'neg' if econ.get('cash_promote_dollars', 0) > 0 else ''}">{('-' + fmt_dollar(econ['cash_promote_dollars'])) if econ.get('cash_promote_dollars', 0) > 0 else '$0'}</div></div>
  <div class="card"><div class="lbl">CapEx Net (final)</div><div class="val neg">-{fmt_dollar(econ.get("capex_net", 0))}</div></div>
  <div class="card"><div class="lbl">Cum Net Cash Flow</div><div class="val {'neg' if cum_ncf < 0 else ''}">{fmt_dollar(cum_ncf)}</div></div>
</div>
{sensitivity_section}
<div class="table-wrap">
  <table>
    <thead>
      {group_header}
      {col_header}
    </thead>
    <tbody>
      {''.join(body_rows)}
    </tbody>
  </table>
</div>
<script>
{sensitivity_js}
const CSV_COLUMNS = {_json.dumps([label for label, _ in csv_columns])};
const CSV_ROWS = {_json.dumps(csv_rows, default=str)};
const CSV_FILENAME = {_json.dumps(f"economics_{dsuName}.csv")};

function exportCSV() {{
  const esc = v => {{
    if (v === null || v === undefined) return "";
    const s = String(v);
    // RFC 4180: quote if contains comma, quote, or newline; escape internal quotes
    if (/[,"\\n\\r]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
    return s;
  }};
  const header = CSV_COLUMNS.map(esc).join(",");
  const lines = CSV_ROWS.map(row => CSV_COLUMNS.map(c => esc(row[c])).join(","));
  const csv = [header].concat(lines).join("\\n");
  const blob = new Blob([csv], {{ type: "text/csv;charset=utf-8;" }});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = CSV_FILENAME;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}}
</script>
</body>
</html>"""

    with open(htmlPath, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Done. Saved economics time-series to {htmlPath}")
    webbrowser.open(htmlPath)
    return htmlPath
