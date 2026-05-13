## Economics orchestration — ties Novi P50 monthly forecasts to Combocurve econ-model assumptions.
## Steps are added incrementally; computeEconomics() is the single entrypoint called from main_model.

import html as _html
import json as _json
import os
import re
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


## Sum every flat rate in a CC variable-expense rows array (entireWellLife='Flat').
## A single CC category slot (e.g. oil.transportation) can hold multiple stacked flat rows —
## each one a separate line item like TRN + OPC — and we want them ALL captured, not just the first.
## Returns 0.0 when rows are empty. Raises if any row is time-based (time-based rates aren't supported yet).
def _flatVariableRate(rows, valueKey):
    if not rows:
        return 0.0
    total = 0.0
    for row in rows:
        if row.get("entireWellLife") != "Flat":
            raise ValueError(f"only flat variable rates supported; got non-flat row in {rows!r}")
        total += float(row.get(valueKey) or 0)
    return total


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
## Routes both sides through novi.FORMATION_ALIASES so geologically-equivalent zones
## (e.g. CODELL SANDSTONE -> CODELL) collapse before matching.
def _selectBucket(afeData, monthlyForecastBuckets):
    from .novi import FORMATION_ALIASES
    def _canon(s):
        s = str(s).strip().upper()
        return FORMATION_ALIASES.get(s, s)
    buckets = monthlyForecastBuckets or []
    if not buckets:
        return None
    afeFormation = None
    if afeData is not None and "Landing Zone" in afeData.columns:
        lz = afeData["Landing Zone"].dropna().astype(str).str.strip().str.upper()
        if not lz.empty:
            afeFormation = _canon(lz.iloc[0])
    if afeFormation:
        match = next((b for b in buckets if b.get("fm") and _canon(b["fm"]) == afeFormation), None)
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


## Run the full economics flow for a SINGLE well (1-row AFE slice).
## Returns dict carrying month index + P50 phase curves + step-by-step derived quantities,
## so downstream steps (revenue, expenses, NPV, etc.) can append without re-fetching inputs.
## cashPromote: decimal (e.g. 0.15 for 15%) applied to net CapEx as "cost of getting into the deal".
## carry: decimal (e.g. 0.15 for "Carry Through The Tanks") — reduces effective NRI/WI on
## production and OPEX, but CapEx stays at the pre-carry WI (you still pay full D&C share).
## Sensitivity scalers (default 1.0 — no change):
##   capexScaler: multiplies the AFE-derived capex_net BEFORE promote; promote $$ stays frozen at
##                cashPromote × ORIGINAL capex_net so it tracks the proposed AFE, not the scaled one.
##   oilPriceScaler / gasPriceScaler: multiply the raw oil deck / flat gas price BEFORE differential.
##
## Public entrypoint is computeEconomics() below; it dispatches single-well vs multi-well.
def _computeWellEconomics(afeData, monthlyForecastBuckets, afeEconModels, cashPromote=0.0, carry=0.0,
                          capexScaler=1.0, oilPriceScaler=1.0, gasPriceScaler=1.0,
                          effectiveDate=None):
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
        "models": {t: ((rec or {}).get("name") or (rec or {}).get("id"))
                    for t, rec in (afeEconModels or {}).items()},
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
    # Anchor month 1 at the user-supplied effective date (or today if blank). The first-of-month
    # snap keeps the price-deck lookup aligned to calendar months, which is how CC's deck is keyed.
    if effectiveDate is None or (isinstance(effectiveDate, str) and not effectiveDate.strip()):
        anchor = pd.Timestamp.today().normalize().replace(day=1)
    else:
        anchor = pd.Timestamp(effectiveDate).normalize().replace(day=1)
    today = anchor
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
    econ["effective_date"] = anchor.strftime("%Y-%m-%d")

    return econ


## Pick a human-readable label for one well from its 1-row DataFrame.
## Prefers Well Name / WellName / Well columns, then API Number / API10 / API, then "Well N".
def _resolveWellLabel(rowDf, idx):
    if rowDf is None or rowDf.empty:
        return f"Well {idx + 1}"
    row = rowDf.iloc[0]
    for col in ("Well Name", "WellName", "Well", "well_name"):
        if col in rowDf.columns and pd.notna(row.get(col)) and str(row.get(col)).strip():
            return str(row.get(col)).strip()
    for col in ("API Number", "API10", "API", "api_number"):
        if col in rowDf.columns and pd.notna(row.get(col)) and str(row.get(col)).strip():
            return f"API {str(row.get(col)).strip()}"
    return f"Well {idx + 1}"


## Aggregate per-well economics into a project total. per_well_results = [(label, well_econ), ...].
## Per-month arrays are summed; per-month prices are taken from the first well (assumed identical
## across wells in the same DSU); per-month NRI/WI series are blended via gross-BOE weighting.
## Headline metrics (NPV10/IRR/Multiple/Payout) are recomputed from the total cashflow.
def _aggregateWellEconomics(per_well_results):
    if not per_well_results:
        raise ValueError("_aggregateWellEconomics: empty input")
    first = per_well_results[0][1]
    n_months = len(first["months"])

    # Per-month arrays summed across wells.
    sum_keys = [
        "oil_p50", "gas_p50", "ngl_p50", "water_p50",
        "net_oil_p50", "net_gas_p50", "net_ngl_p50",
        "net_oil_revenue", "net_gas_revenue", "net_ngl_revenue", "total_net_revenue",
        "fixed_opex", "variable_oil_opex", "variable_gas_gp", "variable_gas_opc",
        "variable_gas_opex", "variable_water_disposal", "total_variable_opex", "total_opex",
        "severance_oil", "severance_gas", "severance_ngl", "total_severance",
        "ad_val_tax", "total_taxes",
        "net_revenue", "capex", "net_cash_flow",
    ]
    total = {}
    for k in sum_keys:
        arrays = [w[1].get(k, [0] * n_months) for w in per_well_results]
        total[k] = [sum(a[i] for a in arrays) for i in range(n_months)]

    # Pass-through (assumed identical across wells in a single DSU run).
    passthrough_keys = (
        "months", "calendar_dates", "effective_date",
        "realized_oil_price", "realized_gas_price", "realized_ngl_price",
        "oil_diff_total", "gas_diff_total", "ngl_diff_total", "ngl_pct_of_oil", "btu_content",
        "oil_var_rate_per_bbl", "gas_gp_rate_per_mcf", "gas_opc_rate_per_mcf", "water_disposal_rate_per_bbl",
        "severance_oil_rate", "severance_gas_rate", "severance_ngl_rate",
        "ad_val_rate", "ad_val_deducts_severance",
        "carry_pct", "cash_promote_pct",
        "capex_scaler", "oil_price_scaler", "gas_price_scaler",
        "capex_source_is_net",
        "models",
    )
    for k in passthrough_keys:
        total[k] = first.get(k)

    # Summed scalars.
    total["capex_gross"] = sum((w[1].get("capex_gross") or 0) for w in per_well_results)
    total["capex_net_raw"] = sum((w[1].get("capex_net_raw") or 0) for w in per_well_results)
    total["cash_promote_dollars"] = sum((w[1].get("cash_promote_dollars") or 0) for w in per_well_results)
    total["capex_net"] = sum((w[1].get("capex_net") or 0) for w in per_well_results)

    # CapEx-weighted blended interests for header / summary cards.
    capex_weight = total["capex_net_raw"] or 1.0
    total["nri"] = sum((w[1].get("nri") or 0) * (w[1].get("capex_net_raw") or 0) for w in per_well_results) / capex_weight
    total["wi"] = sum((w[1].get("wi") or 0) * (w[1].get("capex_net_raw") or 0) for w in per_well_results) / capex_weight
    carry_pct = total.get("carry_pct") or 0.0
    total["effective_nri"] = total["nri"] * (1.0 - carry_pct)
    total["effective_wi"] = total["wi"] * (1.0 - carry_pct)

    # Bucket label — show all distinct buckets joined.
    labels = sorted({w[1].get("bucket_label", "") for w in per_well_results if w[1].get("bucket_label")})
    total["bucket_label"] = "+".join(labels) if labels else "Multi-well"

    # Per-month BOE-weighted interest series for the time-series table columns.
    nri_series = []
    wi_series = []
    for i in range(n_months):
        boe_sum = sum(((w[1]["oil_p50"][i] or 0) + (w[1]["gas_p50"][i] or 0) / 6.0) for w in per_well_results)
        if boe_sum > 0:
            nri_i = sum(((w[1].get("effective_nri", w[1].get("nri", 0)) or 0)
                          * ((w[1]["oil_p50"][i] or 0) + (w[1]["gas_p50"][i] or 0) / 6.0))
                         for w in per_well_results) / boe_sum
            wi_i = sum(((w[1].get("effective_wi", w[1].get("wi", 0)) or 0)
                         * ((w[1]["oil_p50"][i] or 0) + (w[1]["gas_p50"][i] or 0) / 6.0))
                        for w in per_well_results) / boe_sum
        else:
            nri_i = total["effective_nri"]
            wi_i = total["effective_wi"]
        nri_series.append(nri_i)
        wi_series.append(wi_i)
    total["nri_series"] = nri_series
    total["wi_series"] = wi_series

    # Headline metrics on the project-total cashflow.
    total["npv10"] = _computeNPV(total["net_cash_flow"], 0.10)
    irr_value, irr_reason = _computeIRR(total["net_cash_flow"])
    total["irr"] = irr_value
    total["irr_reason"] = irr_reason
    total["project_multiple"] = (sum(total["net_revenue"]) / total["capex_net"]) if total["capex_net"] > 0 else None
    running = 0.0
    payout_month = None
    for i, v in enumerate(total["net_cash_flow"]):
        running += v
        if running >= 0:
            payout_month = total["months"][i]
            break
    total["payout_month"] = payout_month
    return total


## Project-level economics. For multi-row AFEs (multiple wells in one DSU), runs per-row
## and aggregates the per-month arrays into a project total — headline NPV10/IRR/Multiple/
## Payout are computed on the summed cashflow. Returned dict is shape-compatible with the
## single-well case; for multi-well runs it carries `wells` = list of (label, well_econ)
## tuples so downstream HTML can render per-well sections.
def computeEconomics(afeData, monthlyForecastBuckets, afeEconModels, cashPromote=0.0, carry=0.0,
                     capexScaler=1.0, oilPriceScaler=1.0, gasPriceScaler=1.0,
                     effectiveDate=None):
    if afeData is None or afeData.empty:
        raise ValueError("computeEconomics: afeData is empty")
    if len(afeData) == 1:
        econ = _computeWellEconomics(
            afeData, monthlyForecastBuckets, afeEconModels,
            cashPromote=cashPromote, carry=carry,
            capexScaler=capexScaler, oilPriceScaler=oilPriceScaler, gasPriceScaler=gasPriceScaler,
            effectiveDate=effectiveDate,
        )
        econ["wells"] = []
        return econ

    per_well = []
    for idx in range(len(afeData)):
        row_df = afeData.iloc[idx:idx + 1].copy()
        label = _resolveWellLabel(row_df, idx)
        well_econ = _computeWellEconomics(
            row_df, monthlyForecastBuckets, afeEconModels,
            cashPromote=cashPromote, carry=carry,
            capexScaler=capexScaler, oilPriceScaler=oilPriceScaler, gasPriceScaler=gasPriceScaler,
            effectiveDate=effectiveDate,
        )
        formation = ""
        if "Landing Zone" in row_df.columns:
            lz = row_df["Landing Zone"].iloc[0]
            if pd.notna(lz):
                formation = str(lz).strip().upper()
        well_econ["formation"] = formation or "Unknown"
        per_well.append((label, well_econ))

    total = _aggregateWellEconomics(per_well)
    total["wells"] = per_well
    return total


## Run a CAPEX × pricing sensitivity grid. Default: CAPEX -20%..+20% by 5% (9 levels) and
## pricing -10%/0/+10% applied to BOTH oil and gas simultaneously (3 levels). 27 scenarios total.
## Promote $$ stays frozen at the original AFE × promote% per computeEconomics' design.
def computeSensitivity(afeData, monthlyForecastBuckets, afeEconModels,
                       cashPromote=0.0, carry=0.0,
                       capexDeltas=None, priceDeltas=None,
                       effectiveDate=None):
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
                effectiveDate=effectiveDate,
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


## Goal seek: solve cashPromote required to hit IRR=15% and Project Multiple=1.5x.
## Mode is decided by the carry argument that was passed to computeEconomics for the base run:
##   - carry == 0   → 1D mode. Single solve per target at carry=0, plus a scan curve so the
##                    HTML can plot IRR/Multiple vs cashPromote across [0%, 100%].
##   - carry  > 0   → 2D mode. Sweep carry over carryGrid (default 0/10/20/30/40/50%) and at
##                    each carry level solve cashPromote for each target. Yields the iso-curve
##                    of (carry, cash) combos that all hit the buyer hurdle.
##
## Both IRR and Project Multiple are monotone decreasing in cashPromote (more promote =
## more invested capital = worse for buyer), so monotone bisection over [0, 2.0] is safe.
## Carry is also monotone-worsening for the buyer (less revenue, capex unchanged), so at
## higher carry the required cash to hit the same target trends downward — and eventually
## goes infeasible (deal can't hit target even at 0 cash).
def computeGoalSeek(afeData, monthlyForecastBuckets, afeEconModels,
                    cashPromote=0.0, carry=0.0,
                    targetIrr=0.15, targetMultiple=1.5,
                    carryGrid=None, scanCashGrid=None,
                    effectiveDate=None):
    mode_2d = float(carry or 0.0) > 0.0
    if carryGrid is None:
        carryGrid = [0.0, 0.10, 0.20, 0.30, 0.40, 0.50]
    if scanCashGrid is None:
        scanCashGrid = [0.0, 0.025, 0.05, 0.075, 0.10, 0.125, 0.15, 0.20, 0.25,
                        0.30, 0.35, 0.40, 0.50, 0.65, 0.80, 1.00]

    def _econAt(cash, carry_val):
        return computeEconomics(
            afeData, monthlyForecastBuckets, afeEconModels,
            cashPromote=float(cash), carry=float(carry_val),
            effectiveDate=effectiveDate,
        )

    def _metric(econ, target_key):
        v = econ.get(target_key)
        return float(v) if v is not None else None

    def _solveCash(carry_val, target_key, target_val, search_hi=2.0, max_iter=50, tol=1e-5):
        m_lo = _metric(_econAt(0.0, carry_val), target_key)
        if m_lo is None:
            return None, "infeasible_no_metric_at_zero"
        if m_lo < target_val:
            return None, f"infeasible: at 0 promote, {target_key}={m_lo:.4f} below target {target_val:.4f}"
        m_hi = _metric(_econAt(search_hi, carry_val), target_key)
        if m_hi is not None and m_hi >= target_val:
            return None, f"target met even at {search_hi*100:.0f}% promote ({target_key}={m_hi:.4f})"
        lo, hi = 0.0, search_hi
        for _ in range(max_iter):
            mid = 0.5 * (lo + hi)
            m_mid = _metric(_econAt(mid, carry_val), target_key)
            if m_mid is None or m_mid < target_val:
                hi = mid
            else:
                lo = mid
            if hi - lo < tol:
                break
        return 0.5 * (lo + hi), "ok"

    targets = [
        ("irr",               float(targetIrr),       "IRR",              "pct"),
        ("project_multiple",  float(targetMultiple),  "Project Multiple", "mult"),
    ]

    result = {
        "mode": "2d" if mode_2d else "1d",
        "target_irr": float(targetIrr),
        "target_multiple": float(targetMultiple),
        "base_carry": float(carry or 0.0),
        "base_cash_promote": float(cashPromote or 0.0),
        "points": [],
    }

    if not mode_2d:
        # 1D: solve each target at carry=0, plus scan curve for the chart.
        for tkey, tval, tlabel, tunit in targets:
            cash, status = _solveCash(0.0, tkey, tval)
            result["points"].append({
                "target_key": tkey,
                "target_label": tlabel,
                "target_value": tval,
                "target_unit": tunit,
                "carry": 0.0,
                "cash_required": cash,
                "status": status,
            })
        scan = []
        for c in scanCashGrid:
            econ = _econAt(c, 0.0)
            scan.append({
                "cash": float(c),
                "irr": _metric(econ, "irr"),
                "project_multiple": _metric(econ, "project_multiple"),
                "npv10": _metric(econ, "npv10"),
            })
        result["scan_cash_grid"] = [float(c) for c in scanCashGrid]
        result["scan"] = scan
        result["carry_grid"] = [0.0]
        return result

    # 2D: sweep carry × targets.
    result["carry_grid"] = [float(c) for c in carryGrid]
    for c in carryGrid:
        # Capture base metrics at cash=0 so the table can show "what the deal looks like
        # at this carry before any cash promote" — useful when a point is infeasible.
        base_econ = _econAt(0.0, c)
        for tkey, tval, tlabel, tunit in targets:
            cash, status = _solveCash(c, tkey, tval)
            result["points"].append({
                "target_key": tkey,
                "target_label": tlabel,
                "target_value": tval,
                "target_unit": tunit,
                "carry": float(c),
                "cash_required": cash,
                "status": status,
                "metric_at_zero_cash": _metric(base_econ, tkey),
            })
    return result


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
## Suffix appended to economics output filenames so the offset-cohort source is visible.
##   "api_list" (CC type curve) → "_combocurve"
##   "radius"   (radius search) → "_offset"
##   anything else / None       → ""
def _modeFilenameSuffix(searchMode):
    if searchMode == "api_list":
        return "_combocurve"
    if searchMode == "radius":
        return "_offset"
    return ""


def runAfeEconomics(afeData, monthlyForecastBuckets, pathToAfeSummary,
                    basinCode=None, cashPromote=None, carry=None, vintageCutoff=None,
                    effectiveDate=None, searchMode=None,
                    permitData=None, offsetData=None, wellboreLocationsData=None):
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
        effectiveDate=effectiveDate,
    )
    sensitivity = computeSensitivity(
        afeData, monthlyForecastBuckets, afeEconModels,
        cashPromote=cashPromote, carry=carry,
        effectiveDate=effectiveDate,
    )
    goalSeek = computeGoalSeek(
        afeData, monthlyForecastBuckets, afeEconModels,
        cashPromote=cashPromote, carry=carry,
        effectiveDate=effectiveDate,
    )
    exportGrossVolumesExcel(econ, pathToAfeSummary)
    plotEconomicsHTML(econ, pathToAfeSummary, sensitivity=sensitivity, goalSeek=goalSeek,
                      afeData=afeData, vintageCutoff=vintageCutoff, searchMode=searchMode,
                      permitData=permitData, offsetData=offsetData, wellboreLocationsData=wellboreLocationsData)
    exportEconomicsPDF(econ, pathToAfeSummary, afeData=afeData, vintageCutoff=vintageCutoff,
                       goalSeek=goalSeek, searchMode=searchMode,
                       permitData=permitData, offsetData=offsetData,
                       wellboreLocationsData=wellboreLocationsData)
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


## Pull display strings for Operator / State / County out of the AFE Summary DataFrame.
## Multi-row AFEs (multi-well DSU) get unique values joined with ", ". Falls back to "—" when missing.
def _resolveAfeBannerIdentity(afeData):
    def _uniq(col):
        if afeData is None or col not in afeData.columns:
            return "—"
        s = afeData[col].dropna().astype(str).str.strip()
        s = s[s != ""]
        if s.empty:
            return "—"
        vals = sorted(s.unique().tolist())
        return ", ".join(vals)
    return {
        "operator": _uniq("Operator"),
        "state":    _uniq("State"),
        "county":   _uniq("County"),
    }


## Build the "Assumptions" group/value list that powers both the HTML section and the PDF page.
## Returns [(group_label, [(key, value_str), ...]), ...] — values are pre-formatted strings.
def _buildAssumptionGroups(econ, dsuName, formationLabel, vintageLabel):
    def _dollar(v): return f"${(v or 0):,.0f}"
    models = econ.get("models") or {}
    fixed_first = (econ.get("fixed_opex") or [0])[0]
    eff_wi = econ.get("effective_wi") or econ.get("wi") or 0
    fixed_first_gross = (fixed_first / eff_wi) if eff_wi else fixed_first
    ro = (econ.get("realized_oil_price") or [0])[0]
    rg = (econ.get("realized_gas_price") or [0])[0]
    rn = (econ.get("realized_ngl_price") or [0])[0]
    capex_src = "Net AFE column" if econ.get("capex_source_is_net") else "Gross AFE × WI"
    return [
        ("Run Inputs", [
            ("DSU",                 dsuName),
            ("Formation(s)",        formationLabel),
            ("Vintage cutoff",      vintageLabel),
            ("Forecast bucket",     econ.get("bucket_label", "—")),
            ("Effective date",      econ.get("effective_date", "—")),
            ("Months in horizon",   f"{len(econ.get('months') or [])}"),
        ]),
        ("Interests / Carry / Promote", [
            ("NRI (gross)",         f"{(econ.get('nri') or 0)*100:.4f}%"),
            ("WI  (gross)",         f"{(econ.get('wi')  or 0)*100:.4f}%"),
            ("Carry %",             f"{(econ.get('carry_pct') or 0)*100:.2f}%"),
            ("Effective NRI",       f"{(econ.get('effective_nri') or 0)*100:.4f}%"),
            ("Effective WI",        f"{(econ.get('effective_wi')  or 0)*100:.4f}%"),
            ("Cash Promote %",      f"{(econ.get('cash_promote_pct') or 0)*100:.2f}%"),
        ]),
        ("CapEx", [
            ("CapEx source",        capex_src),
            ("CapEx Gross",         _dollar(econ.get('capex_gross'))),
            ("CapEx Net Raw (×WI)", _dollar(econ.get('capex_net_raw'))),
            ("Cash Promote $",      _dollar(econ.get('cash_promote_dollars'))),
            ("CapEx Net (final)",   _dollar(econ.get('capex_net'))),
            ("CapEx scaler",        f"{(econ.get('capex_scaler') or 1.0):.3f}×"),
        ]),
        ("Pricing & Streams (CC models)", [
            ("Pricing model",            models.get("pricing", "—")),
            ("Differentials model",      models.get("differentials", "—")),
            ("Stream Properties model",  models.get("stream-properties", "—")),
            ("Oil price scaler",         f"{(econ.get('oil_price_scaler') or 1.0):.3f}×"),
            ("Gas price scaler",         f"{(econ.get('gas_price_scaler') or 1.0):.3f}×"),
            ("BTU content",              f"{(econ.get('btu_content') or 0):,.0f} Btu/scf"),
            ("NGL % of oil",             f"{(econ.get('ngl_pct_of_oil') or 0)*100:.2f}%"),
            ("Oil diff (total)",         f"${(econ.get('oil_diff_total') or 0):.4f}/bbl"),
            ("Gas diff (total)",         f"${(econ.get('gas_diff_total') or 0):.4f}/MMBtu"),
            ("NGL diff (total)",         f"${(econ.get('ngl_diff_total') or 0):.4f}/bbl"),
            ("Realized Oil  (mo 1)",     f"${ro:,.2f}/bbl"),
            ("Realized Gas  (mo 1)",     f"${rg:,.4f}/MMBtu"),
            ("Realized NGL  (mo 1)",     f"${rn:,.2f}/bbl"),
        ]),
        ("OPEX (CC expenses model)", [
            ("Expenses model",       models.get("expenses", "—")),
            ("Fixed OPEX  (mo 1, 100%)", _dollar(fixed_first_gross) + "/mo"),
            ("Oil Variable",         f"${(econ.get('oil_var_rate_per_bbl')      or 0):.4f}/bbl"),
            ("Gas G&P",              f"${(econ.get('gas_gp_rate_per_mcf')       or 0):.4f}/Mcf"),
            ("Gas OPC",              f"${(econ.get('gas_opc_rate_per_mcf')      or 0):.4f}/Mcf"),
            ("Water Disposal",       f"${(econ.get('water_disposal_rate_per_bbl') or 0):.4f}/bbl"),
        ]),
        ("Taxes (CC production-taxes model)", [
            ("Production-Taxes model",      models.get("production-taxes", "—")),
            ("Severance Oil",               f"{(econ.get('severance_oil_rate') or 0)*100:.3f}%"),
            ("Severance Gas",               f"{(econ.get('severance_gas_rate') or 0)*100:.3f}%"),
            ("Severance NGL",               f"{(econ.get('severance_ngl_rate') or 0)*100:.3f}%"),
            ("Ad Valorem",                  f"{(econ.get('ad_val_rate')        or 0)*100:.3f}%"),
            ("Ad Val deducts severance?",   "Yes" if econ.get("ad_val_deducts_severance") else "No"),
        ]),
    ]


## One-page executive-summary PDF for the AFE economics run. Designed to be email-attachable
## (tech.py:sendEmail consumes a list of file paths).
## Layout: header band (DSU + formation + vintage + NRI/WI/carry/bucket/payout/date),
## 2x2 hero metric cards (NPV10 / IRR / Multiple / Payout, color-coded green/red/neutral),
## cumulative cashflow line chart with payout marker, 2-column cumulative summary table
## (volumes left, $ right). reportlab + matplotlib are imported lazily so the rest of
## economics.py keeps loading even if either package is missing.
def exportEconomicsPDF(econ, pathToAfeSummary, afeData=None, vintageCutoff=None, goalSeek=None, searchMode=None,
                       permitData=None, offsetData=None, wellboreLocationsData=None):
    import io
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from datetime import datetime
    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.lib import colors

    afeDir = os.path.dirname(pathToAfeSummary)
    afeFilename = os.path.splitext(os.path.basename(pathToAfeSummary))[0]
    dsuName = afeFilename.split("-", 1)[1].strip() if "-" in afeFilename else afeFilename
    outputDir = os.path.join(afeDir, "Data")
    os.makedirs(outputDir, exist_ok=True)
    pdfPath = os.path.join(outputDir, f"economics_{dsuName}{_modeFilenameSuffix(searchMode)}.pdf")

    # Formation + vintage labels (mirrors the HTML header logic so both stay consistent).
    afe_forms = []
    if afeData is not None and "Landing Zone" in afeData.columns:
        afe_forms = sorted(afeData["Landing Zone"].dropna().astype(str).str.strip().str.upper().unique().tolist())
    formation_label = ", ".join(afe_forms) if afe_forms else "—"
    vintage_label = f"≥ {int(vintageCutoff)}" if vintageCutoff is not None else "API10 list / unfiltered"

    # Cumulative cashflow series + summary numbers.
    cum_cf = []
    running = 0.0
    for v in econ["net_cash_flow"]:
        running += v
        cum_cf.append(running)
    payout = econ.get("payout_month")
    cum_oil = sum(econ["oil_p50"])
    cum_gas = sum(econ["gas_p50"])
    cum_ngl = sum(econ["ngl_p50"])
    cum_water = sum(econ["water_p50"])
    cum_boe = cum_oil + cum_gas / 6.0
    eff_wi_for_boe = econ.get("effective_wi") or econ.get("wi") or 0.0
    cum_opex = sum(econ["total_opex"])
    opex_per_boe = (cum_opex / eff_wi_for_boe / cum_boe) if (cum_boe > 0 and eff_wi_for_boe > 0) else None
    cum_rev = sum(econ["total_net_revenue"])
    cum_tax = sum(econ["total_taxes"])
    cum_ncf = sum(econ["net_cash_flow"])

    # Cashflow chart -> in-memory PNG embedded in the PDF.
    fig, ax = plt.subplots(figsize=(10.0, 3.0), dpi=150)
    months = list(econ["months"])
    ax.plot(months, cum_cf, color="#0a2a5e", linewidth=2)
    ax.fill_between(months, 0, cum_cf, where=[c >= 0 for c in cum_cf],
                    color="#2e8b57", alpha=0.15, interpolate=True)
    ax.fill_between(months, 0, cum_cf, where=[c < 0 for c in cum_cf],
                    color="#c0392b", alpha=0.15, interpolate=True)
    ax.axhline(0, color="#888", linewidth=0.5)
    if payout is not None:
        ax.axvline(payout, color="#c0392b", linestyle="--", linewidth=1)
        peak_pos = max(cum_cf) if cum_cf else 0
        ax.annotate(f"Payout: month {payout}",
                    xy=(payout, 0), xytext=(payout, peak_pos * 0.1 if peak_pos > 0 else 0),
                    fontsize=8, color="#c0392b", ha="left")
    ax.set_xlabel("Month")
    ax.set_ylabel("Cumulative Net Cash Flow ($)")
    ax.set_title("Cumulative Net Cash Flow", fontsize=12, fontweight="bold")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(
        lambda x, _: f"${x/1e6:.1f}M" if abs(x) >= 1e6 else f"${x/1e3:.0f}K"
    ))
    ax.grid(True, linestyle=":", linewidth=0.5, alpha=0.5)
    fig.tight_layout()
    chart_buf = io.BytesIO()
    fig.savefig(chart_buf, format="png", bbox_inches="tight", facecolor="white")
    plt.close(fig)
    chart_buf.seek(0)

    # --- Build PDF (landscape letter = 11" x 8.5"; ~10" wide usable area) ---
    doc = SimpleDocTemplate(pdfPath, pagesize=landscape(letter),
                            leftMargin=0.5 * inch, rightMargin=0.5 * inch,
                            topMargin=0.4 * inch, bottomMargin=0.4 * inch)
    story = []

    title_style = ParagraphStyle("title", fontSize=18, leading=22,
                                  textColor=colors.white, fontName="Helvetica-Bold")
    sub_style = ParagraphStyle("sub", fontSize=9, leading=12,
                                textColor=colors.HexColor("#cfd6e3"))

    payout_str = f"month {payout}" if payout is not None else "no payout"
    carry_pct = econ.get("carry_pct", 0) or 0
    promote_pct_hdr = econ.get("cash_promote_pct", 0) or 0
    carry_str = (f" · Carry: {carry_pct*100:.2f}% (eff NRI {econ.get('effective_nri', 0)*100:.2f}%, "
                 f"eff WI {econ.get('effective_wi', 0)*100:.2f}%)") if carry_pct > 0 else " · Carry: 0%"

    ident = _resolveAfeBannerIdentity(afeData)
    _esc = _html.escape  # reportlab Paragraph treats text as XML — escape & < > to be safe
    sub_identity = (
        f"Operator: {_esc(ident['operator'])} · State: {_esc(ident['state'])} · County: {_esc(ident['county'])} · "
        f"Formation: {_esc(formation_label)} · Vintage: {_esc(vintage_label)} · "
        f"Bucket: {_esc(str(econ.get('bucket_label', '—')))} · "
        f"Effective: {_esc(str(econ.get('effective_date', '—')))}"
    )
    sub_interests = (
        f"NRI: {econ.get('nri', 0)*100:.2f}% · WI: {econ.get('wi', 0)*100:.2f}% · "
        f"Cash Promote: {promote_pct_hdr*100:.2f}%{carry_str} · "
        f"Payout: {payout_str} · Generated {datetime.now().strftime('%Y-%m-%d')}"
    )
    afe_total      = econ.get("capex_gross") or 0
    capex_net_raw  = econ.get("capex_net_raw") or econ.get("capex_net") or 0
    promote_dollar = econ.get("cash_promote_dollars") or 0
    capex_net_fin  = econ.get("capex_net") or 0
    sub_capex = (
        f"AFE: ${afe_total:,.0f} · CapEx Net Raw: ${capex_net_raw:,.0f} · "
        f"Cash Promote: -${promote_dollar:,.0f} · CapEx Net (final): ${capex_net_fin:,.0f}"
    )

    header_block = Table([[
        [Paragraph(f"AFE Economics — {dsuName}", title_style),
         Spacer(0, 4),
         Paragraph(sub_identity, sub_style),
         Spacer(0, 2),
         Paragraph(sub_interests, sub_style),
         Spacer(0, 2),
         Paragraph(sub_capex, sub_style)]
    ]], colWidths=[10.0 * inch])
    header_block.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#0a2a5e")),
        ("LEFTPADDING", (0, 0), (-1, -1), 14),
        ("RIGHTPADDING", (0, 0), (-1, -1), 14),
        ("TOPPADDING", (0, 0), (-1, -1), 12),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
    ]))
    story.append(header_block)
    story.append(Spacer(0, 12))

    # Hero card builder.
    def _heroCard(label, value_str, status, sub=None):
        bg = {"pos": colors.HexColor("#ecf7ef"), "neg": colors.HexColor("#fdecea"),
              "neutral": colors.HexColor("#f4f4f4")}[status]
        border = {"pos": colors.HexColor("#2e8b57"), "neg": colors.HexColor("#c0392b"),
                  "neutral": colors.HexColor("#888888")}[status]
        val_color = {"pos": colors.HexColor("#1f6e3f"), "neg": colors.HexColor("#962d22"),
                     "neutral": colors.HexColor("#333333")}[status]
        lbl_s = ParagraphStyle("hl", fontSize=8, leading=10,
                                textColor=colors.HexColor("#666666"), fontName="Helvetica-Bold")
        val_s = ParagraphStyle("hv", fontSize=22, leading=26,
                                textColor=val_color, fontName="Helvetica-Bold")
        sub_s = ParagraphStyle("hs", fontSize=7, leading=9,
                                textColor=colors.HexColor("#666666"))
        rows = [[Paragraph(label.upper(), lbl_s)],
                [Spacer(0, 4)],
                [Paragraph(value_str, val_s)]]
        if sub:
            rows += [[Spacer(0, 3)], [Paragraph(sub, sub_s)]]
        inner = Table(rows, colWidths=[4.8 * inch])
        inner.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), bg),
            ("BOX", (0, 0), (-1, -1), 1.5, border),
            ("LEFTPADDING", (0, 0), (-1, -1), 12),
            ("RIGHTPADDING", (0, 0), (-1, -1), 12),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        return inner

    # NPV
    npv = econ.get("npv10", 0) or 0
    npv_status = "pos" if npv > 0 else "neg"
    # IRR
    irr = econ.get("irr")
    if irr is not None:
        irr_str = f"{irr * 100:.2f}%"
        irr_status = "pos" if irr > 0.10 else "neg"
        irr_sub = None
    else:
        irr_str = "n/a"
        irr_status = "neutral"
        irr_sub = econ.get("irr_reason") or "no solution"
    # Multiple
    mult = econ.get("project_multiple")
    if mult is not None:
        mult_str = f"{mult:.2f}x"
        mult_status = "pos" if mult >= 1.0 else "neg"
    else:
        mult_str = "n/a"
        mult_status = "neutral"
    # Payout
    if payout is not None:
        payout_card_str = f"{payout} mo"
        payout_status = "pos" if payout <= 36 else "neutral"
        payout_sub = f"cum CF crosses $0 at month {payout}"
    else:
        payout_card_str = "no payout"
        payout_status = "neg"
        payout_sub = f"cum CF stays negative through {len(econ['months'])} mo"

    hero_grid = Table([
        [_heroCard("NPV @ 10%", f"${npv:,.0f}", npv_status),
         _heroCard("IRR (annual)", irr_str, irr_status, sub=irr_sub)],
        [_heroCard("Project Multiple", mult_str, mult_status,
                   sub="cum net rev / capex net" if mult is not None else None),
         _heroCard("Months to Payout", payout_card_str, payout_status, sub=payout_sub)],
    ], colWidths=[5.0 * inch, 5.0 * inch])
    hero_grid.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    story.append(hero_grid)
    story.append(Spacer(0, 12))

    # Cashflow chart
    story.append(Image(chart_buf, width=10.0 * inch, height=3.0 * inch))
    story.append(Spacer(0, 10))

    # Cumulative summary table — two label/value pairs per row (volumes left, $ right).
    promote_pct = econ.get("cash_promote_pct", 0) or 0
    promote_dollars = econ.get("cash_promote_dollars", 0) or 0
    left_rows = [
        ("Cum Oil (gross)",   f"{cum_oil:,.0f} bbl"),
        ("Cum Gas (gross)",   f"{cum_gas:,.0f} Mcf"),
        ("Cum NGL (gross)",   f"{cum_ngl:,.2f} bbl"),
        ("Cum Water (gross)", f"{cum_water:,.0f} bbl"),
        ("Cum BOE (gross)",   f"{cum_boe:,.0f} BOE  (Oil + Gas/6)"),
    ]
    right_rows = [
        ("Cum Total Revenue",     f"${cum_rev:,.0f}"),
        ("Cum OPEX",              f"${cum_opex:,.0f}"),
        ("OPEX $/BOE",            f"${opex_per_boe:.2f}" if opex_per_boe is not None else "n/a"),
        ("Cum Taxes",             f"${cum_tax:,.0f}"),
        ("Cum Net Cash Flow",     f"${cum_ncf:,.0f}"),
    ]
    # Pad to equal length, then append a capex/promote block beneath.
    while len(left_rows) < len(right_rows):
        left_rows.append(("", ""))
    while len(right_rows) < len(left_rows):
        right_rows.append(("", ""))
    capex_rows = [
        ("CapEx Net Raw (×WI)",                       f"-${(econ.get('capex_net_raw') or econ.get('capex_net', 0)):,.0f}"),
        (f"Cash Promote ({promote_pct*100:.2f}%)",    f"-${promote_dollars:,.0f}" if promote_dollars > 0 else "$0"),
        ("CapEx Net (final, in month 1)",             f"-${econ.get('capex_net', 0):,.0f}"),
    ]

    lbl_s = ParagraphStyle("tlbl", fontSize=9, leading=11, textColor=colors.HexColor("#555555"))
    val_s = ParagraphStyle("tval", fontSize=10, leading=12, fontName="Helvetica-Bold")
    neg_s = ParagraphStyle("tneg", fontSize=10, leading=12, fontName="Helvetica-Bold",
                            textColor=colors.HexColor("#962d22"))

    def _row(l_lbl, l_val, r_lbl, r_val):
        # color negative values red for the right column ($ values starting with '-')
        return [
            Paragraph(l_lbl, lbl_s), Paragraph(l_val, val_s),
            Paragraph(r_lbl, lbl_s),
            Paragraph(r_val, neg_s if str(r_val).strip().startswith("-") else val_s),
        ]

    table_data = []
    for (ll, lv), (rl, rv) in zip(left_rows, right_rows):
        table_data.append(_row(ll, lv, rl, rv))
    # Capex block — spans 4 columns visually with label left + value right; pad blank left cells.
    for cl, cv in capex_rows:
        table_data.append([
            Paragraph("", lbl_s), Paragraph("", val_s),
            Paragraph(cl, lbl_s),
            Paragraph(cv, neg_s if str(cv).strip().startswith("-") else val_s),
        ])

    summary_table = Table(table_data, colWidths=[2.3 * inch, 2.5 * inch, 2.6 * inch, 2.6 * inch])
    summary_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#fafbfc")),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#dddddd")),
        ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#eeeeee")),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(summary_table)

    # ============================================================
    # Page 2 — Goal Seek (only when goalSeek payload is provided)
    # ============================================================
    # Mode is set by computeGoalSeek based on whether base-run carry was non-zero:
    #   1D (carry == 0) → 2 answer cards + IRR-vs-cash + Multiple-vs-cash scan charts
    #   2D (carry  > 0) → iso-curve chart (required cash vs carry) + per-carry table
    if goalSeek and goalSeek.get("points"):
        story.append(PageBreak())

        gs_mode = goalSeek.get("mode", "1d")
        gs_tgt_irr = goalSeek.get("target_irr", 0.15)
        gs_tgt_mult = goalSeek.get("target_multiple", 1.5)
        gs_points = goalSeek["points"]

        # Section header band (smaller than page-1 header).
        gs_header_title = ParagraphStyle("gsh", fontSize=14, leading=18,
                                          textColor=colors.white, fontName="Helvetica-Bold")
        gs_header_sub = ParagraphStyle("gss", fontSize=9, leading=12,
                                        textColor=colors.HexColor("#cfd6e3"))
        if gs_mode == "1d":
            gs_subtitle = (f"Carry held at 0% · solving for Cash Promote that hits "
                           f"{gs_tgt_irr*100:.0f}% IRR and {gs_tgt_mult:.1f}x Multiple")
        else:
            gs_subtitle = (f"Carry sweep {goalSeek['carry_grid'][0]*100:.0f}–"
                           f"{goalSeek['carry_grid'][-1]*100:.0f}% · iso-target frontier "
                           f"for {gs_tgt_irr*100:.0f}% IRR and {gs_tgt_mult:.1f}x Multiple")
        gs_header = Table([[
            [Paragraph(f"Goal Seek — {dsuName}", gs_header_title),
             Spacer(0, 3),
             Paragraph(gs_subtitle, gs_header_sub)]
        ]], colWidths=[10.0 * inch])
        gs_header.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#0a2a5e")),
            ("LEFTPADDING", (0, 0), (-1, -1), 14),
            ("RIGHTPADDING", (0, 0), (-1, -1), 14),
            ("TOPPADDING", (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ]))
        story.append(gs_header)
        story.append(Spacer(0, 12))

        if gs_mode == "1d":
            # Answer cards — reuse _heroCard. Status = pos when a solve was found, neg otherwise.
            irr_point = next((p for p in gs_points if p["target_key"] == "irr"), None)
            mult_point = next((p for p in gs_points if p["target_key"] == "project_multiple"), None)

            def _gsCard(point, target_label, target_str):
                if point is None or point.get("cash_required") is None:
                    return _heroCard(
                        f"Cash Promote for {target_label}",
                        "n/a",
                        "neg",
                        sub=(point.get("status") if point else "no solution"),
                    )
                return _heroCard(
                    f"Cash Promote for {target_label}",
                    f"{point['cash_required'] * 100:.2f}%",
                    "pos",
                    sub="solved by monotone bisection",
                )

            answer_grid = Table([[
                _gsCard(irr_point, f"{gs_tgt_irr*100:.0f}% IRR", None),
                _gsCard(mult_point, f"{gs_tgt_mult:.1f}x Multiple", None),
            ]], colWidths=[5.0 * inch, 5.0 * inch])
            answer_grid.setStyle(TableStyle([
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]))
            story.append(answer_grid)
            story.append(Spacer(0, 14))

            # Scan curves side-by-side: IRR-vs-cash + Multiple-vs-cash, 1x2 subplot grid.
            scan = goalSeek.get("scan", [])
            cash_xs = [s["cash"] * 100 for s in scan]
            # IRR curve cuts off once the project stops being economic — extend through the first
            # point where IRR <= 0 (so the crossing is visible) and drop everything beyond.
            irr_xs = []
            irr_ys = []
            for s in scan:
                irr_xs.append(s["cash"] * 100)
                irr_ys.append(s["irr"] * 100 if s["irr"] is not None else None)
                if s["irr"] is None or s["irr"] <= 0:
                    break
            mult_ys = [s["project_multiple"] if s["project_multiple"] is not None else None for s in scan]
            irr_solve = irr_point["cash_required"] if irr_point and irr_point.get("cash_required") is not None else None
            mult_solve = mult_point["cash_required"] if mult_point and mult_point.get("cash_required") is not None else None

            fig_gs, (ax_irr, ax_mult) = plt.subplots(1, 2, figsize=(10.0, 3.5), dpi=150)
            ax_irr.plot(irr_xs, irr_ys, color="#0a2a5e", linewidth=2, marker="o", markersize=4)
            ax_irr.axhline(gs_tgt_irr * 100, color="#c0392b", linestyle=":", linewidth=1)
            if irr_solve is not None:
                ax_irr.axvline(irr_solve * 100, color="#c0392b", linestyle="--", linewidth=1)
            ax_irr.set_xlabel("Cash Promote (%)")
            ax_irr.set_ylabel("IRR (%)")
            ax_irr.set_title(f"IRR vs Cash Promote (target {gs_tgt_irr*100:.0f}%)", fontsize=10, fontweight="bold")
            ax_irr.grid(True, linestyle=":", linewidth=0.5, alpha=0.5)

            ax_mult.plot(cash_xs, mult_ys, color="#2e8b57", linewidth=2, marker="o", markersize=4)
            ax_mult.axhline(gs_tgt_mult, color="#c0392b", linestyle=":", linewidth=1)
            if mult_solve is not None:
                ax_mult.axvline(mult_solve * 100, color="#c0392b", linestyle="--", linewidth=1)
            ax_mult.set_xlabel("Cash Promote (%)")
            ax_mult.set_ylabel("Project Multiple (x)")
            ax_mult.set_title(f"Multiple vs Cash Promote (target {gs_tgt_mult:.1f}x)", fontsize=10, fontweight="bold")
            ax_mult.grid(True, linestyle=":", linewidth=0.5, alpha=0.5)

            fig_gs.tight_layout()
            gs_buf = io.BytesIO()
            fig_gs.savefig(gs_buf, format="png", bbox_inches="tight", facecolor="white")
            plt.close(fig_gs)
            gs_buf.seek(0)
            story.append(Image(gs_buf, width=10.0 * inch, height=3.5 * inch))

        else:
            # 2D — iso-curve chart (full width) + per-carry table.
            carry_grid = goalSeek["carry_grid"]
            by_key = {(p["carry"], p["target_key"]): p for p in gs_points}

            xs_irr = [p["carry"] * 100 for p in gs_points
                       if p["target_key"] == "irr" and p["cash_required"] is not None]
            ys_irr = [p["cash_required"] * 100 for p in gs_points
                       if p["target_key"] == "irr" and p["cash_required"] is not None]
            xs_mult = [p["carry"] * 100 for p in gs_points
                        if p["target_key"] == "project_multiple" and p["cash_required"] is not None]
            ys_mult = [p["cash_required"] * 100 for p in gs_points
                        if p["target_key"] == "project_multiple" and p["cash_required"] is not None]

            fig_gs, ax = plt.subplots(figsize=(10.0, 3.5), dpi=150)
            if xs_irr:
                ax.plot(xs_irr, ys_irr, color="#0a2a5e", linewidth=2, marker="o", markersize=6,
                        label=f"{gs_tgt_irr*100:.0f}% IRR")
            if xs_mult:
                ax.plot(xs_mult, ys_mult, color="#2e8b57", linewidth=2, marker="s", markersize=6,
                        label=f"{gs_tgt_mult:.1f}x Multiple")
            ax.set_xlabel("Carry (%)")
            ax.set_ylabel("Required Cash Promote (%)")
            ax.set_title("Required Cash Promote vs Carry (iso-target frontier)",
                         fontsize=12, fontweight="bold")
            ax.grid(True, linestyle=":", linewidth=0.5, alpha=0.5)
            ax.legend(loc="best", fontsize=9)
            fig_gs.tight_layout()
            gs_buf = io.BytesIO()
            fig_gs.savefig(gs_buf, format="png", bbox_inches="tight", facecolor="white")
            plt.close(fig_gs)
            gs_buf.seek(0)
            story.append(Image(gs_buf, width=10.0 * inch, height=3.5 * inch))
            story.append(Spacer(0, 10))

            # Per-carry table — rows = carry levels, cols = required cash for each target.
            tbl_lbl_s = ParagraphStyle("gtlbl", fontSize=9, leading=11,
                                       textColor=colors.HexColor("#555555"), fontName="Helvetica-Bold")
            tbl_val_s = ParagraphStyle("gtval", fontSize=10, leading=12, fontName="Helvetica-Bold")
            tbl_neg_s = ParagraphStyle("gtneg", fontSize=9, leading=11,
                                       textColor=colors.HexColor("#962d22"))
            tbl_hdr_s = ParagraphStyle("gthdr", fontSize=9, leading=11,
                                       textColor=colors.white, fontName="Helvetica-Bold")

            table_rows = [[
                Paragraph("Carry", tbl_hdr_s),
                Paragraph(f"Cash for {gs_tgt_irr*100:.0f}% IRR", tbl_hdr_s),
                Paragraph(f"Cash for {gs_tgt_mult:.1f}x Multiple", tbl_hdr_s),
            ]]
            for c in carry_grid:
                row_cells = [Paragraph(f"{c*100:.0f}%", tbl_lbl_s)]
                for tkey in ("irr", "project_multiple"):
                    p = by_key.get((c, tkey))
                    if p is None or p["cash_required"] is None:
                        mz = p.get("metric_at_zero_cash") if p else None
                        if mz is not None:
                            if tkey == "irr":
                                txt = f"n/a  (IRR {mz*100:.1f}% at 0)"
                            else:
                                txt = f"n/a  (Mult {mz:.2f}x at 0)"
                        else:
                            txt = "n/a"
                        row_cells.append(Paragraph(txt, tbl_neg_s))
                    else:
                        row_cells.append(Paragraph(f"{p['cash_required']*100:.2f}%", tbl_val_s))
                table_rows.append(row_cells)

            gs_tbl = Table(table_rows, colWidths=[2.0 * inch, 4.0 * inch, 4.0 * inch])
            gs_tbl.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#4a6491")),
                ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#fafbfc")),
                ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#dddddd")),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#eeeeee")),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]))
            story.append(gs_tbl)

    # ============================================================
    # Assumptions page — every input that drove this run, grouped.
    # ============================================================
    story.append(PageBreak())

    asmp_header_title = ParagraphStyle("ah", fontSize=14, leading=18,
                                        textColor=colors.white, fontName="Helvetica-Bold")
    asmp_header_sub   = ParagraphStyle("as", fontSize=9, leading=12,
                                        textColor=colors.HexColor("#cfd6e3"))
    asmp_header = Table([[
        [Paragraph(f"Assumptions — {dsuName}", asmp_header_title),
         Spacer(0, 3),
         Paragraph("Every input driving the calculation: AFE inputs, CC econ-models, and resolved rates.", asmp_header_sub)]
    ]], colWidths=[10.0 * inch])
    asmp_header.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#0a2a5e")),
        ("LEFTPADDING", (0, 0), (-1, -1), 14),
        ("RIGHTPADDING", (0, 0), (-1, -1), 14),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
    ]))
    story.append(asmp_header)
    story.append(Spacer(0, 10))

    asmp_title_s = ParagraphStyle("agt", fontSize=10, leading=12,
                                   textColor=colors.HexColor("#0a2a5e"), fontName="Helvetica-Bold")
    asmp_lbl_s   = ParagraphStyle("agl", fontSize=8, leading=10,
                                   textColor=colors.HexColor("#555555"))
    asmp_val_s   = ParagraphStyle("agv", fontSize=8, leading=10,
                                   textColor=colors.HexColor("#222222"), fontName="Helvetica-Bold")

    def _asmpGroupTable(grpLabel, items):
        rows = [[Paragraph(grpLabel, asmp_title_s), ""]]
        for k, v in items:
            rows.append([Paragraph(str(k), asmp_lbl_s), Paragraph(str(v), asmp_val_s)])
        t = Table(rows, colWidths=[1.6 * inch, 1.6 * inch])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eaecef")),
            ("SPAN",       (0, 0), (-1, 0)),
            ("BOX",        (0, 0), (-1, -1), 0.5, colors.HexColor("#dddddd")),
            ("INNERGRID",  (0, 1), (-1, -1), 0.25, colors.HexColor("#eeeeee")),
            ("LEFTPADDING",  (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING",   (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 3),
            ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
        ]))
        return t

    asmp_groups = _buildAssumptionGroups(econ, dsuName, formation_label, vintage_label)
    asmp_cards = [_asmpGroupTable(lbl, items) for lbl, items in asmp_groups]
    # Lay out three columns per row to fit the 10-inch page width.
    grid_rows = []
    for i in range(0, len(asmp_cards), 3):
        chunk = asmp_cards[i:i + 3]
        while len(chunk) < 3:
            chunk.append("")
        grid_rows.append(chunk)
    asmp_grid = Table(grid_rows, colWidths=[3.3 * inch] * 3)
    asmp_grid.setStyle(TableStyle([
        ("LEFTPADDING",  (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING",   (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 8),
        ("VALIGN",       (0, 0), (-1, -1), "TOP"),
    ]))
    story.append(asmp_grid)

    # ============================================================
    # Offset-wells map page — AFE permits (red ★) + offset wellbores/midpoints.
    # Rendered with matplotlib to PNG, embedded as an Image.
    # ============================================================
    map_permit_pts = []
    if permitData is not None and not permitData.empty and "Latitude" in permitData.columns and "Longitude" in permitData.columns:
        for _, r in permitData.dropna(subset=["Latitude", "Longitude"]).iterrows():
            map_permit_pts.append((float(r["Longitude"]), float(r["Latitude"]),
                                    str(r.get("WellName") or r.get("API10") or "AFE")))

    map_offset_pts = []
    if offsetData is not None and not offsetData.empty and "MPLatitude" in offsetData.columns and "MPLongitude" in offsetData.columns:
        for _, r in offsetData.dropna(subset=["MPLatitude", "MPLongitude"]).iterrows():
            map_offset_pts.append((float(r["MPLongitude"]), float(r["MPLatitude"])))

    map_wellbore_groups = []
    if wellboreLocationsData is not None and not wellboreLocationsData.empty \
            and "API10" in wellboreLocationsData.columns:
        wb_clean = wellboreLocationsData.dropna(subset=["Latitude", "Longitude"])
        sort_cols = [c for c in ("API10", "Path") if c in wb_clean.columns]
        for _api10, g in wb_clean.sort_values(sort_cols).groupby("API10"):
            map_wellbore_groups.append(
                ([float(x) for x in g["Longitude"].tolist()],
                 [float(x) for x in g["Latitude"].tolist()])
            )

    if map_permit_pts or map_offset_pts or map_wellbore_groups:
        story.append(PageBreak())

        map_header_title = ParagraphStyle("mh", fontSize=14, leading=18,
                                           textColor=colors.white, fontName="Helvetica-Bold")
        map_header_sub   = ParagraphStyle("ms", fontSize=9, leading=12,
                                           textColor=colors.HexColor("#cfd6e3"))
        sub_count = (
            f"AFE permits: {len(map_permit_pts)} · "
            f"Offset wells: {len(map_offset_pts)} · "
            f"Wellbore traces: {len(map_wellbore_groups)}"
        )
        map_header = Table([[
            [Paragraph(f"Offset Wells Map — {dsuName}", map_header_title),
             Spacer(0, 3),
             Paragraph(sub_count, map_header_sub)]
        ]], colWidths=[10.0 * inch])
        map_header.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#0a2a5e")),
            ("LEFTPADDING", (0, 0), (-1, -1), 14),
            ("RIGHTPADDING", (0, 0), (-1, -1), 14),
            ("TOPPADDING", (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ]))
        story.append(map_header)
        story.append(Spacer(0, 8))

        # ----- Optional regional context map (left): 100-mi radius, counties + states + AFE ★.
        # Falls back gracefully if basemaps can't be downloaded or geopandas isn't installed.
        regional_ax_data = None
        afe_centroid = None
        if map_permit_pts:
            afe_centroid = (
                sum(p[0] for p in map_permit_pts) / len(map_permit_pts),
                sum(p[1] for p in map_permit_pts) / len(map_permit_pts),
            )
            try:
                import geopandas as gpd
                from math import cos, radians
                from .novi import _ensureBasemaps
                basemap_paths = _ensureBasemaps()
                center_lon, center_lat = afe_centroid
                # 100-mi square bbox around AFE centroid; cosine corrects longitude shrink with latitude.
                radius_mi = 100.0
                d_lat = radius_mi / 69.0
                d_lon = radius_mi / (69.0 * max(cos(radians(center_lat)), 0.1))
                bbox = (center_lon - d_lon, center_lat - d_lat,
                        center_lon + d_lon, center_lat + d_lat)
                states_gdf   = gpd.read_file(basemap_paths["state"],  bbox=bbox)
                counties_gdf = gpd.read_file(basemap_paths["county"], bbox=bbox)
                regional_ax_data = (states_gdf, counties_gdf, afe_centroid, bbox, d_lat, d_lon)
            except Exception as e:
                print(f"  (regional context map skipped — {e})")

        # ----- Side-by-side figure: regional context (left) + offset detail (right). -----
        if regional_ax_data is not None:
            map_fig, (region_ax, map_ax) = plt.subplots(1, 2, figsize=(10.0, 5.0), dpi=150,
                                                         gridspec_kw={"width_ratios": [1, 1]})
        else:
            map_fig, map_ax = plt.subplots(figsize=(10.0, 6.0), dpi=150)
            region_ax = None

        # ----- Right axis: existing offset-wells detail. -----
        for lons, lats in map_wellbore_groups:
            map_ax.plot(lons, lats, color="#0a2a5e", linewidth=0.8, alpha=0.7)
        if map_offset_pts:
            map_ax.scatter(
                [p[0] for p in map_offset_pts],
                [p[1] for p in map_offset_pts],
                s=14, c="#0a2a5e", alpha=0.65, edgecolors="none",
                label=f"Offset Wells ({len(map_offset_pts)})",
            )
        if map_permit_pts:
            map_ax.scatter(
                [p[0] for p in map_permit_pts],
                [p[1] for p in map_permit_pts],
                s=240, c="red", marker="*", edgecolors="black", linewidths=0.8, zorder=5,
                label=f"AFE Permits ({len(map_permit_pts)})",
            )
        map_ax.set_title("Offset Wells (detail)", fontsize=11, fontweight="bold")
        map_ax.set_aspect("equal", adjustable="datalim")
        map_ax.set_xticks([])
        map_ax.set_yticks([])
        map_ax.grid(True, linestyle=":", linewidth=0.5, alpha=0.5)
        if map_offset_pts or map_permit_pts:
            map_ax.legend(loc="upper left", fontsize=8, framealpha=0.85)

        # ----- Left axis: regional context (counties + states + AFE ★). -----
        if region_ax is not None:
            states_gdf, counties_gdf, (cx, cy), bbox, d_lat, d_lon = regional_ax_data
            counties_gdf.plot(
                ax=region_ax, facecolor="#f7f7f9", edgecolor="#bcbcbc", linewidth=0.4,
            )
            states_gdf.boundary.plot(
                ax=region_ax, edgecolor="#333333", linewidth=1.2,
            )
            # AFE star on top
            region_ax.scatter(
                [p[0] for p in map_permit_pts], [p[1] for p in map_permit_pts],
                s=260, c="red", marker="*", edgecolors="black", linewidths=0.9, zorder=10,
                label="AFE",
            )
            region_ax.set_xlim(bbox[0], bbox[2])
            region_ax.set_ylim(bbox[1], bbox[3])
            region_ax.set_aspect("equal", adjustable="box")
            region_ax.set_title(f"Regional Context — 100 mi radius @ ({cy:.2f}°, {cx:.2f}°)",
                                 fontsize=11, fontweight="bold")
            region_ax.set_xticks([])
            region_ax.set_yticks([])
            region_ax.grid(True, linestyle=":", linewidth=0.4, alpha=0.4)

            # Clip helper: take the visible portion of each polygon (inside bbox)
            # so labels land on what's actually drawn, not on the full geometry.
            from shapely.geometry import box as _shp_box
            view_box = _shp_box(bbox[0], bbox[1], bbox[2], bbox[3])

            def _visibleLabelPoint(geom):
                if geom is None or geom.is_empty:
                    return None
                clipped = geom.intersection(view_box)
                if clipped.is_empty:
                    return None
                return clipped.representative_point()

            # ----- County labels: always shown, font scales with cohort size. -----
            if "NAME" in counties_gdf.columns:
                n_counties = len(counties_gdf)
                county_fs = 7 if n_counties <= 15 else 6 if n_counties <= 30 else 5 if n_counties <= 60 else 4
                for _, r in counties_gdf.iterrows():
                    pt = _visibleLabelPoint(r.geometry)
                    if pt is None:
                        continue
                    region_ax.text(
                        pt.x, pt.y, str(r["NAME"]),
                        fontsize=county_fs, color="#555555",
                        ha="center", va="center", zorder=4,
                    )

            # ----- State labels: bold, larger, with a faint white halo for legibility over counties. -----
            state_name_col = next((c for c in ("NAME", "STUSPS", "STATE_NAME") if c in states_gdf.columns), None)
            if state_name_col is not None:
                import matplotlib.patheffects as _pe
                for _, r in states_gdf.iterrows():
                    pt = _visibleLabelPoint(r.geometry)
                    if pt is None:
                        continue
                    label = str(r[state_name_col]).upper()
                    region_ax.text(
                        pt.x, pt.y, label,
                        fontsize=12, fontweight="bold", color="#0a2a5e",
                        ha="center", va="center", zorder=8, alpha=0.85,
                        path_effects=[_pe.withStroke(linewidth=2.5, foreground="white")],
                    )

            region_ax.legend(loc="upper left", fontsize=8, framealpha=0.85)

        map_fig.tight_layout()
        map_buf = io.BytesIO()
        map_fig.savefig(map_buf, format="png", bbox_inches="tight", facecolor="white")
        plt.close(map_fig)
        map_buf.seek(0)
        # Image height tracks the figure aspect (10x5 → 5" tall, single 10x6 → 6" tall).
        img_height = 5.0 if region_ax is not None else 6.0
        story.append(Image(map_buf, width=10.0 * inch, height=img_height * inch))

    # ============================================================
    # Pricing page (last) — realized oil + gas as a segments table (one row per
    # constant-price run). Collapses long flat stretches so a 360-month deck shows
    # as ~30 rows of "Jan 2026 → Dec 2026: $69.50/bbl · $2.43/MMBtu".
    # ============================================================
    oil_series = econ.get("realized_oil_price") or []
    gas_series = econ.get("realized_gas_price") or []
    if oil_series or gas_series:
        story.append(PageBreak())

        price_title_style = ParagraphStyle("ph", fontSize=14, leading=18,
                                            textColor=colors.white, fontName="Helvetica-Bold")
        price_sub_style   = ParagraphStyle("ps", fontSize=9, leading=12,
                                            textColor=colors.HexColor("#cfd6e3"))
        oil_first  = oil_series[0] if oil_series else None
        oil_last   = oil_series[-1] if oil_series else None
        gas_first  = gas_series[0] if gas_series else None
        gas_last   = gas_series[-1] if gas_series else None
        sub_bits = []
        if oil_first is not None:
            sub_bits.append(f"Oil: ${oil_first:.2f}/bbl → ${oil_last:.2f}/bbl")
        if gas_first is not None:
            sub_bits.append(f"Gas: ${gas_first:.4f}/MMBtu → ${gas_last:.4f}/MMBtu")
        sub_bits.append(f"Months: {len(oil_series or gas_series)}")
        price_sub = " · ".join(sub_bits)

        price_header = Table([[
            [Paragraph(f"Pricing — {dsuName}", price_title_style),
             Spacer(0, 3),
             Paragraph(price_sub, price_sub_style)]
        ]], colWidths=[10.0 * inch])
        price_header.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#0a2a5e")),
            ("LEFTPADDING", (0, 0), (-1, -1), 14),
            ("RIGHTPADDING", (0, 0), (-1, -1), 14),
            ("TOPPADDING", (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ]))
        story.append(price_header)
        story.append(Spacer(0, 8))

        # Resolve calendar dates for date-range labels (fall back to month index).
        cal_dates_raw = econ.get("calendar_dates") or []
        cal_dates = None
        if cal_dates_raw and len(cal_dates_raw) == len(oil_series or gas_series):
            try:
                cal_dates = [pd.Timestamp(d) for d in cal_dates_raw]
            except Exception:
                cal_dates = None

        # Build segments — each row is a run of months where (oil, gas) stayed constant.
        n_months_price = len(oil_series or gas_series)
        def _key(i):
            o = round(oil_series[i], 6) if oil_series else None
            g = round(gas_series[i], 8) if gas_series else None
            return (o, g)
        segments = []
        seg_start = 0
        for i in range(1, n_months_price):
            if _key(i) != _key(seg_start):
                segments.append((seg_start, i - 1))
                seg_start = i
        segments.append((seg_start, n_months_price - 1))

        # Format helpers.
        def _fmtDate(i):
            if cal_dates is not None:
                return cal_dates[i].strftime("%b %Y")
            return f"Month {i + 1}"

        # Build the table data.
        header_row = ["#", "Months", "Date Range", "Oil ($/bbl)", "Gas ($/MMBtu)"]
        body_rows = []
        for idx, (s, e) in enumerate(segments, start=1):
            n_mo = e - s + 1
            date_range = f"{_fmtDate(s)} → {_fmtDate(e)}" if e > s else _fmtDate(s)
            oil_str = f"${oil_series[s]:,.2f}" if oil_series else "—"
            gas_str = f"${gas_series[s]:,.4f}" if gas_series else "—"
            body_rows.append([str(idx), str(n_mo), date_range, oil_str, gas_str])

        # Style the table — navy header, alternating row background, monospace numerics.
        cell_lbl = ParagraphStyle("plbl", fontSize=8, leading=10,
                                    textColor=colors.HexColor("#222222"))
        cell_num = ParagraphStyle("pnum", fontSize=8, leading=10,
                                    textColor=colors.HexColor("#222222"),
                                    fontName="Helvetica-Bold", alignment=2)  # right-align
        head_lbl = ParagraphStyle("phead", fontSize=8, leading=10,
                                    textColor=colors.white,
                                    fontName="Helvetica-Bold")
        head_num = ParagraphStyle("phnum", fontSize=8, leading=10,
                                    textColor=colors.white,
                                    fontName="Helvetica-Bold", alignment=2)

        table_data = [[
            Paragraph(header_row[0], head_num),
            Paragraph(header_row[1], head_num),
            Paragraph(header_row[2], head_lbl),
            Paragraph(header_row[3], head_num),
            Paragraph(header_row[4], head_num),
        ]]
        for row in body_rows:
            table_data.append([
                Paragraph(row[0], cell_num),
                Paragraph(row[1], cell_num),
                Paragraph(row[2], cell_lbl),
                Paragraph(row[3], cell_num),
                Paragraph(row[4], cell_num),
            ])

        price_table = Table(
            table_data,
            colWidths=[0.5 * inch, 0.7 * inch, 3.0 * inch, 1.5 * inch, 1.5 * inch],
            repeatRows=1,
        )
        price_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0a2a5e")),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1),
                [colors.HexColor("#fafbfc"), colors.HexColor("#ffffff")]),
            ("BOX",       (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
            ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#eeeeee")),
            ("LEFTPADDING",  (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING",   (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 3),
            ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(price_table)

    doc.build(story)
    print(f"Done. Saved economics PDF to {pdfPath}")
    return pdfPath


## Render the economics result as a wide time-series HTML table next to the AFE Summary.
## Output: Data/economics_{dsuName}.html (same folder as plotSubsurfaceHeatMapsHTML).
## sensitivity: optional dict from computeSensitivity() — renders an NPV-vs-CAPEX line chart
## (one line per pricing scenario) plus the 9×3 grid table above the monthly time-series.
def plotEconomicsHTML(econ, pathToAfeSummary, sensitivity=None, goalSeek=None,
                      afeData=None, vintageCutoff=None, searchMode=None,
                      permitData=None, offsetData=None, wellboreLocationsData=None):
    afeDir = os.path.dirname(pathToAfeSummary)
    afeFilename = os.path.splitext(os.path.basename(pathToAfeSummary))[0]
    dsuName = afeFilename.split("-", 1)[1].strip() if "-" in afeFilename else afeFilename
    outputDir = os.path.join(afeDir, "Data")
    os.makedirs(outputDir, exist_ok=True)
    mode_suffix = _modeFilenameSuffix(searchMode)
    htmlPath = os.path.join(outputDir, f"economics_{dsuName}{mode_suffix}.html")

    # Formation + vintage cutoff labels for the header subtitle so the analyst can see
    # what filter built the offset cohort that produced this forecast.
    afe_formations_for_header = []
    if afeData is not None and "Landing Zone" in afeData.columns:
        afe_formations_for_header = sorted(
            afeData["Landing Zone"].dropna().astype(str).str.strip().str.upper().unique().tolist()
        )
    formation_label = ", ".join(afe_formations_for_header) if afe_formations_for_header else "—"
    vintage_label = f"≥ {int(vintageCutoff)}" if vintageCutoff is not None else "API10 list / unfiltered"

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
            ("Gross Oil (bbl)",   "oil_p50",   fmt_int),
            ("Gross Gas (Mcf)",   "gas_p50",   fmt_int),
            ("Gross NGL (bbl)",   "ngl_p50",   lambda v: fmt_dec(v, 2)),
            ("Gross Water (bbl)", "water_p50", fmt_int),
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

    # Helper: ensure wi_series / nri_series are present (per-well dicts come bare, total has
    # blended series from _aggregateWellEconomics, single-well total has the broadcast above).
    def _broadcastInterestSeries(e):
        n = len(e["months"])
        if "wi_series" not in e:
            e["wi_series"] = [e.get("effective_wi", e.get("wi", 0.0))] * n
        if "nri_series" not in e:
            e["nri_series"] = [e.get("effective_nri", e.get("nri", 0.0))] * n

    # Helper: build (body_rows HTML strings, cum_cf array) for one econ dict.
    def _buildBody(e):
        _broadcastInterestSeries(e)
        cum_cf_local = []
        running_local = 0.0
        for v in e["net_cash_flow"]:
            running_local += v
            cum_cf_local.append(running_local)
        rows = []
        for i in range(len(e["months"])):
            cells = []
            for _label, cols in groups:
                for col_label, key, formatter in cols:
                    v = e[key][i]
                    if key in ("months", "calendar_dates"):
                        cells.append(f"<td>{formatter(v)}</td>")
                    else:
                        cells.append(f"<td class='{cls(v)}'>{formatter(v)}</td>")
            cells.append(f"<td class='{cls(cum_cf_local[i])}'>{fmt_dollar(cum_cf_local[i])}</td>")
            rows.append("<tr>" + "".join(cells) + "</tr>")
        return rows, cum_cf_local

    # Helper: render one collapsible monthly-cashflow <details> section.
    def _renderMonthlySection(e, section_label, is_open):
        body, _cum = _buildBody(e)
        n_local = len(e["months"])
        open_attr = " open" if is_open else ""
        toggle = "click to collapse" if is_open else "click to expand"
        return (
            f"<details class='table-wrap'{open_attr}>"
            f"<summary>{section_label} — {n_local} months — {toggle}</summary>"
            f"<table><thead>{group_header}{col_header}</thead>"
            f"<tbody>{''.join(body)}</tbody></table>"
            f"</details>"
        )

    # CSV export — total + one representative well per formation.
    def _buildCsvRows(e):
        _broadcastInterestSeries(e)
        n_local = len(e["months"])
        cum_cf_local, running_local = [], 0.0
        for v in e["net_cash_flow"]:
            running_local += v
            cum_cf_local.append(running_local)
        rows_local = []
        for i in range(n_local):
            row_dict = {}
            for _label, cols in groups:
                for col_label, key, _formatter in cols:
                    row_dict[col_label] = e[key][i]
            row_dict["Cum Cash Flow"] = cum_cf_local[i]
            rows_local.append(row_dict)
        return rows_local

    def _safeFilenamePart(s):
        return re.sub(r"[^A-Za-z0-9_-]+", "_", str(s)).strip("_") or "Unknown"

    wells_list = econ.get("wells") or []
    csv_datasets = {
        "_total": {
            "label":    "Total",
            "rows":     _buildCsvRows(econ),
            "filename": f"economics_{dsuName}.csv",
        }
    }
    seen_formations = set()
    for w_label, w_econ in wells_list:
        formation = w_econ.get("formation", "Unknown")
        if formation in seen_formations:
            continue
        seen_formations.add(formation)
        csv_datasets[f"formation:{formation}"] = {
            "label":    f"{formation} ({w_label})",
            "rows":     _buildCsvRows(w_econ),
            "filename": f"economics_{dsuName}_{_safeFilenamePart(formation)}.csv",
        }

    export_buttons_html = "".join(
        f'<button class="export-btn" data-csv-key="{_html.escape(k, quote=True)}">'
        + ("⬇ Export CSV — Total" if k == "_total" else f"⬇ CSV — {_html.escape(ds['label'])}")
        + "</button>"
        for k, ds in csv_datasets.items()
    )

    # Render the monthly sections: Total (open) first, then each well (closed) top-to-bottom.
    if wells_list:
        total_section_label = "Monthly Cash Flow — Total"
    else:
        total_section_label = "Monthly Cash Flow"
    monthly_section_blocks = [_renderMonthlySection(econ, total_section_label, is_open=True)]
    for w_label, w_econ in wells_list:
        monthly_section_blocks.append(
            _renderMonthlySection(w_econ, f"Monthly Cash Flow — {w_label}", is_open=False)
        )
    monthly_sections = "".join(monthly_section_blocks)

    # Summary block
    cum_oil   = sum(econ["oil_p50"])
    cum_gas   = sum(econ["gas_p50"])
    cum_ngl   = sum(econ["ngl_p50"])
    cum_water = sum(econ["water_p50"])
    cum_rev   = sum(econ["total_net_revenue"])
    cum_opex  = sum(econ["total_opex"])
    cum_tax   = sum(econ["total_taxes"])
    cum_ncf   = sum(econ["net_cash_flow"])
    # BOE per industry convention: 6 Mcf = 1 BOE; NGLs intentionally excluded here.
    # total_opex is already × effective_wi, so divide out to get a WI-invariant $/BOE rate.
    cum_boe = cum_oil + cum_gas / 6.0
    eff_wi_for_boe = econ.get("effective_wi") or econ.get("wi") or 0.0
    opex_per_boe = (cum_opex / eff_wi_for_boe / cum_boe) if (cum_boe > 0 and eff_wi_for_boe > 0) else None
    payout_str = f"month {payout_month}" if payout_month else f"not in {n_months}-month horizon"

    # ---- Assumptions section: every input that drives the calculation, grouped. ----
    assumption_groups = _buildAssumptionGroups(econ, dsuName, formation_label, vintage_label)
    _ag_html_blocks = []
    for grp_label, items in assumption_groups:
        rows_html = "".join(
            f"<tr><td class='lbl'>{_html.escape(str(k))}</td>"
            f"<td class='val'>{_html.escape(str(v))}</td></tr>"
            for k, v in items
        )
        _ag_html_blocks.append(
            f"<div class='assump-group'><h3>{_html.escape(grp_label)}</h3>"
            f"<table>{rows_html}</table></div>"
        )
    assumptions_section = (
        "<div class='assumptions'>"
        "<details open>"
        "<summary>Assumptions — every input driving this run — click to collapse</summary>"
        f"<div class='assump-grid'>{''.join(_ag_html_blocks)}</div>"
        "</details>"
        "</div>"
    )

    # ---- Offset-Wells Map: AFE permits as red stars + offset wellbores/markers. ----
    map_permit_pts = []
    if permitData is not None and not permitData.empty and "Latitude" in permitData.columns and "Longitude" in permitData.columns:
        for _, r in permitData.dropna(subset=["Latitude", "Longitude"]).iterrows():
            map_permit_pts.append({
                "lat":  float(r["Latitude"]),
                "lon":  float(r["Longitude"]),
                "name": str(r.get("WellName") or r.get("API10") or "AFE Permit"),
            })

    map_offset_pts = []
    if offsetData is not None and not offsetData.empty:
        lat_col = "MPLatitude" if "MPLatitude" in offsetData.columns else None
        lon_col = "MPLongitude" if "MPLongitude" in offsetData.columns else None
        if lat_col and lon_col:
            for _, r in offsetData.dropna(subset=[lat_col, lon_col]).iterrows():
                map_offset_pts.append({
                    "lat":  float(r[lat_col]),
                    "lon":  float(r[lon_col]),
                    "api10": str(r.get("API10") or ""),
                    "name": str(r.get("WellName") or ""),
                    "operator": str(r.get("OperatorName") or r.get("Operator") or ""),
                    "first_prod": (int(r["FirstProductionYear"]) if "FirstProductionYear" in offsetData.columns and pd.notna(r.get("FirstProductionYear")) else None),
                })

    map_wellbore_groups = []
    if wellboreLocationsData is not None and not wellboreLocationsData.empty:
        wb_clean = wellboreLocationsData.dropna(subset=["Latitude", "Longitude"])
        sort_cols = [c for c in ("API10", "Path") if c in wb_clean.columns]
        if "API10" in wb_clean.columns:
            for api10, g in wb_clean.sort_values(sort_cols).groupby("API10"):
                map_wellbore_groups.append({
                    "api10": str(api10),
                    "lons":  [float(x) for x in g["Longitude"].tolist()],
                    "lats":  [float(x) for x in g["Latitude"].tolist()],
                })

    has_map_data = bool(map_permit_pts or map_offset_pts or map_wellbore_groups)
    map_tab_label = ""
    if has_map_data:
        map_payload_json = _json.dumps({
            "permit":    map_permit_pts,
            "offset":    map_offset_pts,
            "wellbores": map_wellbore_groups,
        }, default=str)
        offset_count_label = f"{len(map_offset_pts)} offset wells" if map_offset_pts else (
            f"{len(map_wellbore_groups)} wellbore traces" if map_wellbore_groups else "no offset data"
        )
        map_tab_label = "Offset Wells Map"
        map_section = (
            "<div class='pane-body offset-map'>"
            f"<div class='pane-title'>Offset Wells Map — AFE permits (★) + {offset_count_label}</div>"
            "<div id='offset-map-chart' class='offset-map-chart'></div>"
            "</div>"
        )
        # Render fn only — tab-bar JS calls this when the pane activates.
        map_init_js = (
            f"const OFFSET_MAP = {map_payload_json};\n"
            "let __mapRendered = false;\n"
            "function __renderOffsetMap() {\n"
            "  if (__mapRendered) return;\n"
            "  const m = OFFSET_MAP;\n"
            "  const traces = [];\n"
            "  (m.wellbores || []).forEach(wb => {\n"
            "    traces.push({ type:'scatter', mode:'lines', x:wb.lons, y:wb.lats,\n"
            "      line:{color:'#0a2a5e', width:1.2}, hoverinfo:'text', text:wb.api10, showlegend:false });\n"
            "  });\n"
            "  if (m.offset && m.offset.length) {\n"
            "    traces.push({ type:'scatter', mode:'markers',\n"
            "      x: m.offset.map(p => p.lon), y: m.offset.map(p => p.lat),\n"
            "      marker:{ size:6, color:'#0a2a5e', opacity:0.7 },\n"
            "      hoverinfo:'text',\n"
            "      hovertext: m.offset.map(p => (p.name || p.api10) + (p.operator ? '<br>Operator: ' + p.operator : '') + (p.first_prod ? '<br>1st Prod: ' + p.first_prod : '') + (p.api10 ? '<br>API10: ' + p.api10 : '')),\n"
            "      name: 'Offset Wells', showlegend: true });\n"
            "  }\n"
            "  if (m.permit && m.permit.length) {\n"
            "    traces.push({ type:'scatter', mode:'markers',\n"
            "      x: m.permit.map(p => p.lon), y: m.permit.map(p => p.lat),\n"
            "      marker:{ symbol:'star', size:18, color:'red', line:{ color:'black', width:1 } },\n"
            "      hoverinfo:'text',\n"
            "      hovertext: m.permit.map(p => 'AFE: ' + p.name),\n"
            "      name: 'AFE Permits', showlegend: true });\n"
            "  }\n"
            "  const layout = {\n"
            "    xaxis: { title: 'Longitude', scaleanchor: 'y', scaleratio: 1 },\n"
            "    yaxis: { title: 'Latitude' },\n"
            "    margin: { l: 60, r: 20, t: 20, b: 50 },\n"
            "    legend: { x: 0.01, y: 0.99, bgcolor: 'rgba(255,255,255,0.85)' },\n"
            "    hovermode: 'closest', dragmode: 'zoom',\n"
            "  };\n"
            "  Plotly.newPlot('offset-map-chart', traces, layout, { responsive: true });\n"
            "  __mapRendered = true;\n"
            "}\n"
        )
    else:
        map_section = ""
        map_init_js = ""

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
            "<div class='pane-body sensitivity'>"
            "<div class='pane-title'>Sensitivity — CAPEX × Price (IRR grid + NPV10 chart)</div>"
            "<div class='grid'>"
            "<div><div id='sens-chart' class='chart'></div></div>"
            f"<div>{irr_table_html}</div>"
            "</div>"
            f"<div style='margin-top:14px;'>{scenario_table_html}</div>"
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
        )

    # Goal Seek section — collapsible block beneath the Sensitivity block.
    # Mode is decided by computeGoalSeek() based on whether `carry` was non-zero at the base run.
    #   1D: two answer cards ("Cash Promote @ 15% IRR", "@ 1.5x Mult") + scan curves for context.
    #   2D: iso-curve chart (required cash vs carry) + per-carry table with feasibility notes.
    goal_seek_section = ""
    goal_seek_js = ""
    if goalSeek and goalSeek.get("points"):
        gs_mode = goalSeek.get("mode", "1d")
        gs_tgt_irr_pct = goalSeek.get("target_irr", 0.15) * 100
        gs_tgt_mult = goalSeek.get("target_multiple", 1.5)
        gs_points = goalSeek["points"]

        if gs_mode == "1d":
            # Build the two answer cards.
            cards_html_parts = []
            for p in gs_points:
                if p["target_key"] == "irr":
                    card_label = f"Cash Promote for {p['target_value']*100:.0f}% IRR"
                else:
                    card_label = f"Cash Promote for {p['target_value']:.1f}x Multiple"
                if p["cash_required"] is None:
                    card_cls = "neg"
                    card_val = "n/a"
                    card_note = p.get("status", "no solution")
                else:
                    card_cls = "pos"
                    card_val = f"{p['cash_required']*100:.2f}%"
                    card_note = "solved by monotone bisection"
                cards_html_parts.append(
                    f'<div class="card hero {card_cls}">'
                    f'<div class="lbl">{card_label}</div>'
                    f'<div class="val">{card_val}</div>'
                    f'<div style="font-size:10px;color:#666;margin-top:4px;">{card_note}</div>'
                    f'</div>'
                )
            gs_answers_html = '<div class="goalseek-answers">' + "".join(cards_html_parts) + '</div>'

            # Scan curves: IRR vs cash, Mult vs cash.
            scan = goalSeek.get("scan", [])
            cash_xs = [s["cash"] * 100 for s in scan]
            # IRR curve cuts off once the project stops being economic — extend through the first
            # point where IRR <= 0 (so the crossing is visible) and drop everything beyond.
            irr_xs_cut = []
            irr_ys = []
            for s in scan:
                irr_xs_cut.append(s["cash"] * 100)
                irr_ys.append(s["irr"] * 100 if s["irr"] is not None else None)
                if s["irr"] is None or s["irr"] <= 0:
                    break
            mult_ys = [s["project_multiple"] if s["project_multiple"] is not None else None for s in scan]
            irr_solve = next((p["cash_required"] for p in gs_points if p["target_key"] == "irr"), None)
            mult_solve = next((p["cash_required"] for p in gs_points if p["target_key"] == "project_multiple"), None)

            irr_shapes = [{
                "type": "line", "yref": "y", "y0": gs_tgt_irr_pct, "y1": gs_tgt_irr_pct,
                "xref": "paper", "x0": 0, "x1": 1,
                "line": {"color": "#c0392b", "width": 1, "dash": "dot"},
            }]
            if irr_solve is not None:
                irr_shapes.append({
                    "type": "line", "x0": irr_solve * 100, "x1": irr_solve * 100,
                    "yref": "paper", "y0": 0, "y1": 1,
                    "line": {"color": "#c0392b", "width": 1, "dash": "dash"},
                })
            mult_shapes = [{
                "type": "line", "yref": "y", "y0": gs_tgt_mult, "y1": gs_tgt_mult,
                "xref": "paper", "x0": 0, "x1": 1,
                "line": {"color": "#c0392b", "width": 1, "dash": "dot"},
            }]
            if mult_solve is not None:
                mult_shapes.append({
                    "type": "line", "x0": mult_solve * 100, "x1": mult_solve * 100,
                    "yref": "paper", "y0": 0, "y1": 1,
                    "line": {"color": "#c0392b", "width": 1, "dash": "dash"},
                })

            gs_irr_traces = [{
                "type": "scatter", "mode": "lines+markers", "name": "IRR",
                "x": irr_xs_cut, "y": irr_ys,
                "line": {"color": "#0a2a5e", "width": 2}, "marker": {"size": 7},
            }]
            gs_mult_traces = [{
                "type": "scatter", "mode": "lines+markers", "name": "Multiple",
                "x": cash_xs, "y": mult_ys,
                "line": {"color": "#2e8b57", "width": 2}, "marker": {"size": 7},
            }]

            goal_seek_section = (
                "<div class='pane-body goalseek'>"
                f"<div class='pane-title'>Goal Seek — Cash Promote to hit {gs_tgt_irr_pct:.0f}% IRR or "
                f"{gs_tgt_mult:.1f}x Multiple (carry held at 0%)</div>"
                f"{gs_answers_html}"
                "<div class='grid'>"
                "<div><div id='gs-irr-chart' class='chart'></div></div>"
                "<div><div id='gs-mult-chart' class='chart'></div></div>"
                "</div>"
                "</div>"
            )

            goal_seek_js = (
                "const __GS_IRR_TRACES = " + _json.dumps(gs_irr_traces) + ";"
                "const __GS_MULT_TRACES = " + _json.dumps(gs_mult_traces) + ";"
                "const __GS_IRR_LAYOUT = {"
                f"title: {{text: 'IRR vs Cash Promote (target {gs_tgt_irr_pct:.0f}%)', font: {{size: 14}}}},"
                "xaxis: {title: 'Cash Promote (%)', zeroline: true, zerolinecolor: '#888'},"
                "yaxis: {title: 'IRR (%)', ticksuffix: '%', zeroline: true, zerolinecolor: '#888'},"
                "margin: {l: 70, r: 16, t: 40, b: 50},"
                "shapes: " + _json.dumps(irr_shapes) + "};"
                "const __GS_MULT_LAYOUT = {"
                f"title: {{text: 'Multiple vs Cash Promote (target {gs_tgt_mult:.1f}x)', font: {{size: 14}}}},"
                "xaxis: {title: 'Cash Promote (%)', zeroline: true, zerolinecolor: '#888'},"
                "yaxis: {title: 'Project Multiple (x)', zeroline: true, zerolinecolor: '#888'},"
                "margin: {l: 70, r: 16, t: 40, b: 50},"
                "shapes: " + _json.dumps(mult_shapes) + "};"
                "let __gsRendered = false;"
                "function __renderGS() {"
                "  if (__gsRendered) return;"
                "  Plotly.newPlot('gs-irr-chart', __GS_IRR_TRACES, __GS_IRR_LAYOUT, {responsive: true});"
                "  Plotly.newPlot('gs-mult-chart', __GS_MULT_TRACES, __GS_MULT_LAYOUT, {responsive: true});"
                "  __gsRendered = true;"
                "}"
            )
        else:
            # 2D: build per-carry table + iso-curve chart.
            carry_grid = goalSeek["carry_grid"]
            by_key = {(p["carry"], p["target_key"]): p for p in gs_points}
            gs_table_rows = [
                "<tr><th rowspan='2'>Carry</th><th colspan='2'>Cash Promote Required</th></tr>",
                f"<tr><th>{gs_tgt_irr_pct:.0f}% IRR</th><th>{gs_tgt_mult:.1f}x Multiple</th></tr>",
            ]
            for c in carry_grid:
                row_cells = [f"<td class='label'>{c*100:.0f}%</td>"]
                for tkey in ("irr", "project_multiple"):
                    p = by_key.get((c, tkey))
                    if p is None or p["cash_required"] is None:
                        mz = p.get("metric_at_zero_cash") if p else None
                        if mz is not None:
                            if tkey == "irr":
                                cell_txt = f"n/a (IRR {mz*100:.1f}% at 0)"
                            else:
                                cell_txt = f"n/a (Mult {mz:.2f}x at 0)"
                        else:
                            cell_txt = "n/a"
                        row_cells.append(f"<td class='neg'>{cell_txt}</td>")
                    else:
                        row_cells.append(f"<td>{p['cash_required']*100:.2f}%</td>")
                gs_table_rows.append("<tr>" + "".join(row_cells) + "</tr>")
            gs_table_html = "<table>" + "".join(gs_table_rows) + "</table>"

            xs_irr = [p["carry"]*100 for p in gs_points if p["target_key"] == "irr" and p["cash_required"] is not None]
            ys_irr = [p["cash_required"]*100 for p in gs_points if p["target_key"] == "irr" and p["cash_required"] is not None]
            xs_mult = [p["carry"]*100 for p in gs_points if p["target_key"] == "project_multiple" and p["cash_required"] is not None]
            ys_mult = [p["cash_required"]*100 for p in gs_points if p["target_key"] == "project_multiple" and p["cash_required"] is not None]

            gs_traces = [
                {"type": "scatter", "mode": "lines+markers", "name": f"{gs_tgt_irr_pct:.0f}% IRR",
                 "x": xs_irr, "y": ys_irr,
                 "line": {"color": "#0a2a5e", "width": 2}, "marker": {"size": 8}},
                {"type": "scatter", "mode": "lines+markers", "name": f"{gs_tgt_mult:.1f}x Multiple",
                 "x": xs_mult, "y": ys_mult,
                 "line": {"color": "#2e8b57", "width": 2}, "marker": {"size": 8}},
            ]

            goal_seek_section = (
                "<div class='pane-body goalseek'>"
                f"<div class='pane-title'>Goal Seek — Cash/Carry trade-off frontier for {gs_tgt_irr_pct:.0f}% IRR or "
                f"{gs_tgt_mult:.1f}x Multiple</div>"
                "<div class='grid'>"
                "<div><div id='gs-iso-chart' class='chart'></div></div>"
                f"<div>{gs_table_html}</div>"
                "</div>"
                "</div>"
            )

            goal_seek_js = (
                "const __GS_TRACES = " + _json.dumps(gs_traces) + ";"
                "const __GS_LAYOUT = {"
                "title: {text: 'Required Cash Promote vs Carry (iso-target frontier)', font: {size: 14}},"
                "xaxis: {title: 'Carry (%)', zeroline: true, zerolinecolor: '#888'},"
                "yaxis: {title: 'Required Cash Promote (%)', zeroline: true, zerolinecolor: '#888'},"
                "margin: {l: 70, r: 16, t: 40, b: 60},"
                "hovermode: 'x unified',"
                "legend: {orientation: 'h', y: -0.2}"
                "};"
                "let __gsRendered = false;"
                "function __renderGS() {"
                "  if (__gsRendered) return;"
                "  Plotly.newPlot('gs-iso-chart', __GS_TRACES, __GS_LAYOUT, {responsive: true});"
                "  __gsRendered = true;"
                "}"
            )

    # ---- Banner sub-lines: identity (operator/state/county), interests, capex $$. ----
    _ident = _resolveAfeBannerIdentity(afeData)
    _carry_pct_val = econ.get("carry_pct", 0) or 0
    _carry_html = (
        f" &middot; Carry: {_carry_pct_val*100:.2f}% (eff NRI {econ.get('effective_nri', 0)*100:.2f}%, "
        f"eff WI {econ.get('effective_wi', 0)*100:.2f}%)"
        if _carry_pct_val > 0 else " &middot; Carry: 0%"
    )
    banner_identity_html = (
        f"Operator: {_html.escape(_ident['operator'])} &middot; "
        f"State: {_html.escape(_ident['state'])} &middot; "
        f"County: {_html.escape(_ident['county'])} &middot; "
        f"Formation: {_html.escape(formation_label)} &middot; "
        f"Vintage: {_html.escape(vintage_label)} &middot; "
        f"Bucket: {_html.escape(str(econ.get('bucket_label', '—')))} &middot; "
        f"Effective: {_html.escape(str(econ.get('effective_date', '—')))}"
    )
    banner_interest_html = (
        f"NRI: {econ.get('nri', 0)*100:.2f}% &middot; "
        f"WI: {econ.get('wi', 0)*100:.2f}% &middot; "
        f"Cash Promote: {econ.get('cash_promote_pct', 0)*100:.2f}%"
        f"{_carry_html} &middot; "
        f"Months: {n_months} &middot; Payout: {payout_str}"
    )
    _afe_total      = econ.get("capex_gross") or 0
    _capex_net_raw  = econ.get("capex_net_raw") or econ.get("capex_net") or 0
    _promote_dollar = econ.get("cash_promote_dollars") or 0
    _capex_net_fin  = econ.get("capex_net") or 0
    banner_capex_html = (
        f"AFE: ${_afe_total:,.0f} &middot; "
        f"CapEx Net Raw: ${_capex_net_raw:,.0f} &middot; "
        f"Cash Promote: -${_promote_dollar:,.0f} &middot; "
        f"CapEx Net (final): ${_capex_net_fin:,.0f}"
    )

    # Tab navigation — Summary is always the first tab and shows the in-place
    # cards/assumptions/monthly content (wrapped in #summary-content). Other tabs
    # full-page-swap to their own .tab-pane content (Map / Sensitivity / Goal Seek).
    # Each non-Summary tab is conditional on its content existing (no map data, no
    # sensitivity payload, no goal-seek → tab simply isn't rendered).
    tab_specs = [("pane-summary", "Summary", None)]  # (id, label, render_fn_name)
    if map_section:
        tab_specs.append(("pane-map",         map_tab_label or "Map", "__renderOffsetMap"))
    if sensitivity_section:
        tab_specs.append(("pane-sensitivity", "Sensitivity",          "__renderSens"))
    if goal_seek_section:
        tab_specs.append(("pane-goalseek",    "Goal Seek",            "__renderGS"))

    first_id = tab_specs[0][0]  # "pane-summary"
    if len(tab_specs) > 1:
        tabs_bar_html = "<div class='tabs-bar'>" + "".join(
            f"<button class='tab-btn{(' active' if tid == first_id else '')}' data-tab='{tid}'>{_html.escape(label)}</button>"
            for tid, label, _fn in tab_specs
        ) + "</div>"
    else:
        tabs_bar_html = ""  # no point showing a single-tab navigation strip
    # Only render panes for non-Summary tabs (Summary is the inline #summary-content block).
    section_by_id = {
        "pane-map":         map_section,
        "pane-sensitivity": sensitivity_section,
        "pane-goalseek":    goal_seek_section,
    }
    tabs_panes_html = "".join(
        f"<div class='tab-pane' id='{tid}'>{section_by_id[tid]}</div>"
        for tid, _label, _fn in tab_specs if tid != "pane-summary"
    )
    tab_render_map = {tid: fn for tid, _label, fn in tab_specs if fn}
    tabs_init_js = (
        f"const TAB_RENDERERS = {_json.dumps(tab_render_map)};\n"
        "function activateTab(tabId) {\n"
        "  const summary = document.getElementById('summary-content');\n"
        "  if (tabId === 'pane-summary') {\n"
        "    if (summary) summary.style.display = '';\n"
        "    document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));\n"
        "  } else {\n"
        "    if (summary) summary.style.display = 'none';\n"
        "    document.querySelectorAll('.tab-pane').forEach(p => p.classList.toggle('active', p.id === tabId));\n"
        "  }\n"
        "  document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.getAttribute('data-tab') === tabId));\n"
        "  const fnName = TAB_RENDERERS[tabId];\n"
        "  if (fnName && typeof window[fnName] === 'function') window[fnName]();\n"
        "}\n"
        "document.addEventListener('DOMContentLoaded', () => {\n"
        "  document.querySelectorAll('.tab-btn').forEach(b => {\n"
        "    b.addEventListener('click', () => activateTab(b.getAttribute('data-tab')));\n"
        "  });\n"
        f"  activateTab({_json.dumps(first_id)});\n"
        "});\n"
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
  .header .export-buttons {{ display: flex; flex-wrap: wrap; gap: 8px; flex: 0 0 auto;
                              justify-content: flex-end; max-width: 60%; }}
  .header .export-btn {{ background: #f28c28; color: #fff; border: none; padding: 8px 14px;
                          border-radius: 4px; cursor: pointer; font-size: 12px; font-weight: bold;
                          white-space: nowrap; }}
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
  /* Assumptions block — multi-column grid of label/value tables. */
  .assumptions {{ background: #fff; padding: 10px 28px; border-bottom: 1px solid #ddd; flex: 0 0 auto; }}
  .assumptions details[open] {{ max-height: 60vh; overflow: auto; }}
  .assumptions summary {{ cursor: pointer; font-size: 13px; font-weight: bold; padding: 4px 0;
                           list-style: none; user-select: none; }}
  .assumptions summary::before {{ content: '▶ '; font-size: 10px; }}
  .assumptions details[open] summary::before {{ content: '▼ '; }}
  .assumptions .assump-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
                                gap: 12px; margin-top: 8px; }}
  .assumptions .assump-group {{ background: #fafbfc; border: 1px solid #e5e7eb; border-radius: 4px; padding: 8px 12px; }}
  .assumptions .assump-group h3 {{ font-size: 12px; color: #0a2a5e; margin-bottom: 6px; border-bottom: 1px solid #e5e7eb; padding-bottom: 4px; }}
  .assumptions .assump-group table {{ width: 100%; border-collapse: collapse; }}
  .assumptions .assump-group td {{ padding: 2px 4px; font-size: 11px; border: none; }}
  .assumptions .assump-group td.lbl {{ color: #666; width: 55%; }}
  .assumptions .assump-group td.val {{ font-family: monospace; text-align: right; font-weight: 600; }}
  /* Offset-wells map block */
  .offset-map .offset-map-chart {{ width: 100%; height: 60vh; min-height: 420px; }}
  /* Sensitivity + Goal Seek sections — collapsible; capped at 60vh when open so the
     time-series table below always has room (otherwise it gets squeezed to 0 by the flex layout). */
  .sensitivity .grid, .goalseek .grid {{ display: grid; grid-template-columns: minmax(360px, 1fr) minmax(360px, 1fr);
                         gap: 18px; margin-top: 10px; align-items: start; }}
  .sensitivity .chart, .goalseek .chart {{ height: 320px; }}
  .sensitivity table, .goalseek table {{ border-collapse: collapse; width: 100%; font-size: 11px; }}
  .sensitivity table th, .goalseek table th {{ background: #4a6491; color: #fff; padding: 5px 8px; }}
  .sensitivity table td, .goalseek table td {{ padding: 4px 8px; border-bottom: 1px solid #eee; font-family: monospace; text-align: right; }}
  .sensitivity table td.label, .goalseek table td.label {{ background: #f4f6fa; font-weight: bold; text-align: left; }}
  .sensitivity table td.center, .goalseek table td.center {{ text-align: center; }}
  .sensitivity table td.neg, .goalseek table td.neg {{ color: #c00; }}
  .sensitivity table td.base {{ background: #fff7e6; }}
  /* Goal-seek 1D answer cards: 2-column grid of hero cards, sized to match the existing
     summary .card.hero style (the .card.hero rules above already apply since we use the same classes). */
  .goalseek .goalseek-answers {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
                                  gap: 12px; margin: 10px 0 6px; }}
  .goalseek .goalseek-answers .card {{ background: #fafbfc; border: 1px solid #e5e7eb; border-radius: 4px; padding: 10px 14px; }}
  .goalseek .goalseek-answers .card .lbl {{ color: #666; font-size: 11px; font-weight: bold;
                                              text-transform: uppercase; letter-spacing: 0.5px; }}
  .goalseek .goalseek-answers .card .val {{ font-family: monospace; font-size: 22px; font-weight: bold; margin-top: 6px; }}
  .goalseek .goalseek-answers .card.hero {{ border: 2px solid; }}
  .goalseek .goalseek-answers .card.hero.pos {{ border-color: #2e8b57; background: #ecf7ef; }}
  .goalseek .goalseek-answers .card.hero.pos .val {{ color: #1f6e3f; }}
  .goalseek .goalseek-answers .card.hero.neg {{ border-color: #c0392b; background: #fdecea; }}
  .goalseek .goalseek-answers .card.hero.neg .val {{ color: #962d22; }}
  /* Tab strip (Summary / Map / Sensitivity / Goal Seek) — sits directly below header. */
  .tabs-bar {{ background: #fff; border-bottom: 2px solid #0a2a5e; padding: 12px 28px 0 28px;
                flex: 0 0 auto; display: flex; gap: 6px; }}
  .tab-btn {{ background: #f0f2f6; color: #555; border: 1px solid #d0d5dd; border-bottom: none;
              padding: 10px 20px; cursor: pointer; font-size: 13px; font-weight: 600;
              border-radius: 4px 4px 0 0;
              transition: background 0.12s, color 0.12s; }}
  .tab-btn:hover {{ background: #e2e8f0; color: #222; }}
  .tab-btn.active {{ background: #0a2a5e; color: #fff; border-color: #0a2a5e; }}
  .tab-pane {{ display: none; background: #fff; padding: 14px 28px; }}
  .tab-pane.active {{ display: block; flex: 1 1 auto; min-height: 0; overflow: auto; }}
  .tab-pane .pane-title {{ font-size: 13px; font-weight: bold; color: #0a2a5e; margin-bottom: 10px; }}
  /* Summary "page" wrapper — re-establishes the flex column behavior so the inner
     monthly-cashflow <details> can grow/scroll the same way it did before tabs existed. */
  #summary-content {{ display: flex; flex-direction: column; flex: 1 1 auto; min-height: 0; overflow: hidden; }}
  /* Time-series table is collapsible via <details>. When open, the <details> itself is the
     scroll container (both axes) and fills the remaining viewport. Avoid `display: flex` on
     <details> — it has cross-browser quirks where the inner flex child loses its computed
     height and the scrollbars become unreachable. */
  .table-wrap {{ background: #fff; flex: 0 0 auto; border-bottom: 1px solid #ddd; }}
  .table-wrap[open] {{ flex: 1 1 auto; overflow: auto; min-height: 0; padding-bottom: 16px; }}
  .table-wrap > summary {{ padding: 10px 28px; cursor: pointer; font-size: 13px; font-weight: bold;
                            list-style: none; user-select: none; background: #fff; }}
  .table-wrap > summary::before {{ content: '▶ '; font-size: 10px; }}
  .table-wrap[open] > summary::before {{ content: '▼ '; }}
  .table-wrap > table {{ margin: 0 28px; }}
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
    <div class="sub">{banner_identity_html}</div>
    <div class="sub">{banner_interest_html}</div>
    <div class="sub">{banner_capex_html}</div>
  </div>
  <div class="export-buttons">{export_buttons_html}</div>
</div>
{tabs_bar_html}
<div id="summary-content">
<div class="summary">
  <div class="card hero {('pos' if econ.get('npv10', 0) > 0 else 'neg')}"><div class="lbl">NPV @ 10%</div><div class="val">{fmt_dollar(econ.get('npv10', 0))}</div></div>
  <div class="card hero {('pos' if (econ.get('irr') is not None and econ['irr'] > 0.10) else 'neutral' if econ.get('irr') is None else 'neg')}"><div class="lbl">IRR (annual)</div><div class="val">{(f"{econ['irr']*100:.2f}%" if econ.get('irr') is not None else 'n/a')}</div>{(f'<div style="font-size:10px;color:#666;margin-top:4px;">{econ.get("irr_reason", "")}</div>' if econ.get('irr') is None else '')}</div>
  <div class="card hero {('pos' if (econ.get('project_multiple') or 0) >= 1.0 else 'neg')}"><div class="lbl">Project Multiple</div><div class="val">{(f"{econ['project_multiple']:.2f}x" if econ.get('project_multiple') is not None else 'n/a')}</div></div>
  <div class="card hero {('pos' if (payout_month is not None and payout_month <= 36) else 'neutral' if payout_month is not None else 'neg')}"><div class="lbl">Months to Payout</div><div class="val">{(f"{payout_month} mo" if payout_month is not None else 'no payout')}</div>{(f'<div style="font-size:10px;color:#666;margin-top:4px;">cum CF crosses $0 at month {payout_month}</div>' if payout_month is not None else f'<div style="font-size:10px;color:#666;margin-top:4px;">cum CF stays negative through {n_months} mo</div>')}</div>
  <div class="card"><div class="lbl">Cum Oil (gross)</div><div class="val">{fmt_int(cum_oil)} bbl</div></div>
  <div class="card"><div class="lbl">Cum Gas (gross)</div><div class="val">{fmt_int(cum_gas)} Mcf</div></div>
  <div class="card"><div class="lbl">Cum NGL (gross)</div><div class="val">{fmt_dec(cum_ngl, 2)} bbl</div></div>
  <div class="card"><div class="lbl">Cum Water (gross)</div><div class="val">{fmt_int(cum_water)} bbl</div></div>
  <div class="card"><div class="lbl">Cum BOE (gross)</div><div class="val">{fmt_int(cum_boe)} BOE</div><div style="font-size:10px;color:#666;margin-top:2px;">Oil + Gas/6</div></div>
  <div class="card"><div class="lbl">Cum Total Revenue</div><div class="val">{fmt_dollar(cum_rev)}</div></div>
  <div class="card"><div class="lbl">Cum OPEX</div><div class="val">{fmt_dollar(cum_opex)}</div></div>
  <div class="card"><div class="lbl">OPEX $/BOE</div><div class="val">{(f"${opex_per_boe:.2f}" if opex_per_boe is not None else 'n/a')}</div><div style="font-size:10px;color:#666;margin-top:2px;">life-of-well, WI-invariant</div></div>
  <div class="card"><div class="lbl">Cum Taxes</div><div class="val">{fmt_dollar(cum_tax)}</div></div>
  <div class="card"><div class="lbl">CapEx Net Raw (×WI)</div><div class="val neg">-{fmt_dollar(econ.get("capex_net_raw", econ.get("capex_net", 0)))}</div></div>
  <div class="card"><div class="lbl">Cash Promote ({(econ.get('cash_promote_pct', 0) * 100):.2f}%)</div><div class="val {'neg' if econ.get('cash_promote_dollars', 0) > 0 else ''}">{('-' + fmt_dollar(econ['cash_promote_dollars'])) if econ.get('cash_promote_dollars', 0) > 0 else '$0'}</div></div>
  <div class="card"><div class="lbl">CapEx Net (final)</div><div class="val neg">-{fmt_dollar(econ.get("capex_net", 0))}</div></div>
  <div class="card"><div class="lbl">Cum Net Cash Flow</div><div class="val {'neg' if cum_ncf < 0 else ''}">{fmt_dollar(cum_ncf)}</div></div>
</div>
{assumptions_section}
{monthly_sections}
</div>
{tabs_panes_html}
<script>
{sensitivity_js}
{goal_seek_js}
{map_init_js}
{tabs_init_js}
const CSV_COLUMNS = {_json.dumps([label for label, _ in csv_columns])};
const CSV_DATASETS = {_json.dumps({k: {"rows": v["rows"], "filename": v["filename"]} for k, v in csv_datasets.items()}, default=str)};

function exportCSV(key) {{
  const ds = CSV_DATASETS[key];
  if (!ds) {{ console.warn("exportCSV: no dataset for key", key); return; }}
  const esc = v => {{
    if (v === null || v === undefined) return "";
    const s = String(v);
    // RFC 4180: quote if contains comma, quote, or newline; escape internal quotes
    if (/[,"\\n\\r]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
    return s;
  }};
  const header = CSV_COLUMNS.map(esc).join(",");
  const lines = ds.rows.map(row => CSV_COLUMNS.map(c => esc(row[c])).join(","));
  const csv = [header].concat(lines).join("\\n");
  const blob = new Blob([csv], {{ type: "text/csv;charset=utf-8;" }});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = ds.filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}}

document.addEventListener("DOMContentLoaded", () => {{
  document.querySelectorAll(".export-btn[data-csv-key]").forEach(btn => {{
    btn.addEventListener("click", () => exportCSV(btn.getAttribute("data-csv-key")));
  }});
}});
</script>
</body>
</html>"""

    with open(htmlPath, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Done. Saved economics time-series to {htmlPath}")
    webbrowser.open(htmlPath)
    return htmlPath
