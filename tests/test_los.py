## Unit tests for los.py normalization + backfill helpers.
## Run with: python -m pytest tests/test_los.py -v

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import pytest

import shalehavenscripts.los as los


class TestStripInvisible:
    """Regression tests: _stripInvisible must preserve internal whitespace.
    Stripping regular ASCII space destroys multi-word values everywhere
    they flow (Operator, Well Name, Property Name, backfill labels)."""

    def test_preserves_internal_whitespace(self):
        assert los._stripInvisible("AETHON ENERGY OPERATING LLC") == "AETHON ENERGY OPERATING LLC"
        assert los._stripInvisible("Drilly Idol A15501LH") == "Drilly Idol A15501LH"
        assert los._stripInvisible("Flybar State 1WB") == "Flybar State 1WB"

    def test_strips_leading_trailing_whitespace(self):
        assert los._stripInvisible("  Drilly Idol  ") == "Drilly Idol"

    def test_strips_invisible_chars(self):
        # NBSP, ZWSP, BOM, ZWJ should all be removed
        assert los._stripInvisible("Drilly\u00a0Idol") == "DrillyIdol"  # NBSP has no regular space
        assert los._stripInvisible("\ufeffClase Azul 1H\u200b") == "Clase Azul 1H"

    def test_none_and_nan(self):
        assert los._stripInvisible(None) == ""
        assert los._stripInvisible(np.nan) == ""


class TestIsBlank:
    def test_none_and_nan(self):
        assert los._isBlank(None)
        assert los._isBlank(np.nan)
        assert los._isBlank(pd.NA)
        assert los._isBlank(pd.NaT)

    def test_empty_and_whitespace(self):
        assert los._isBlank("")
        assert los._isBlank("   ")
        assert los._isBlank("\t\n  ")

    def test_invisible_chars(self):
        # NBSP, ZWSP, BOM, ZWJ/ZWNJ
        assert los._isBlank("\u00a0")
        assert los._isBlank("\u200b")
        assert los._isBlank("\ufeff")
        assert los._isBlank("\u200c\u200d")

    def test_nan_strings(self):
        assert los._isBlank("nan")
        assert los._isBlank("NaN")
        assert los._isBlank("none")
        assert los._isBlank("<NA>")
        assert los._isBlank("NaT")

    def test_real_values_not_blank(self):
        assert not los._isBlank("Drilly Idol A15501LH")
        assert not los._isBlank("171651.01")
        assert not los._isBlank(0)
        assert not los._isBlank("0")


class TestNormalizeAfeKey:
    def test_none_and_nan(self):
        assert los.normalizeAfeKey(None) is None
        assert los.normalizeAfeKey(np.nan) is None
        assert los.normalizeAfeKey("") is None
        assert los.normalizeAfeKey("   ") is None

    def test_passthrough_when_clean(self):
        assert los.normalizeAfeKey("DD.22.32703") == "DD.22.32703"
        assert los.normalizeAfeKey("AZUL1H-1") == "AZUL1H-1"
        assert los.normalizeAfeKey("24032") == "24032"

    def test_strip_prefixes(self):
        assert los.normalizeAfeKey("100*DD.22.32703") == "DD.22.32703"
        assert los.normalizeAfeKey("XX-24032") == "24032"
        assert los.normalizeAfeKey("MM-FOO") == "FOO"

    def test_strip_capex_suffixes(self):
        assert los.normalizeAfeKey("DD.22.32703.CAP") == "DD.22.32703"
        assert los.normalizeAfeKey("DD.22.32703.CMP") == "DD.22.32703"
        assert los.normalizeAfeKey("DD.22.32703.DRL") == "DD.22.32703"

    def test_strip_chained_suffixes(self):
        # spec example: DD.22.32703.CAP.CMP -> DD.22.32703
        assert los.normalizeAfeKey("DD.22.32703.CAP.CMP") == "DD.22.32703"

    def test_numeric_input(self):
        # pandas reads bare numbers from Excel as floats
        assert los.normalizeAfeKey(171651.01) == "171651.01"
        assert los.normalizeAfeKey(24032) == "24032"


class TestNormalizePropertyKey:
    def test_none_and_nan(self):
        assert los.normalizePropertyKey(None) is None
        assert los.normalizePropertyKey(np.nan) is None

    def test_comma_split_keeps_only_first_segment(self):
        assert los.normalizePropertyKey(
            "FLYBAR STATE 1WB, SECTION 45, BLOCK 57"
        ) == "FLYBARSTATE1WB"

    def test_uppercase_and_alnum_only(self):
        assert los.normalizePropertyKey("Flybar State 1WB") == "FLYBARSTATE1WB"
        assert los.normalizePropertyKey("DJS Fed 31-36-12NH") == "DJSFED313612NH"
        assert los.normalizePropertyKey("DJS FED 31-36-12 NH") == "DJSFED313612NH"

    def test_punctuation_dropped(self):
        assert los.normalizePropertyKey("Clase Azul #1H") == "CLASEAZUL1H"
        assert los.normalizePropertyKey("Clase Azul 1H") == "CLASEAZUL1H"


class TestNormalizePropertyName:
    """Regression tests. normalizePropertyName is consumed by Facts,
    Dimensions, the backfill labels, and CTB/AFE/JIB rollups — so any change
    must keep these outputs stable (or be a clear superset)."""

    def test_none_and_nan(self):
        assert los.normalizePropertyName(None) is None
        assert los.normalizePropertyName(np.nan) is None
        assert los.normalizePropertyName("") is None
        assert los.normalizePropertyName("   ") is None

    def test_uppercase_and_whitespace_collapse(self):
        assert los.normalizePropertyName("Flybar State 1WB") == "FLYBAR STATE 1WB"
        assert los.normalizePropertyName("  flybar   state   1wb  ") == "FLYBAR STATE 1WB"

    def test_real_well_names(self):
        assert los.normalizePropertyName("Drilly Idol A15501LH") == "DRILLY IDOL A15501LH"
        assert los.normalizePropertyName("Clase Azul 1H") == "CLASE AZUL 1H"
        assert los.normalizePropertyName("Nichols-Trulson 156-90-10-14H-1") == "NICHOLS-TRULSON 156-90-10-14H-1"

    def test_synthetic_labels(self):
        # fallback labels used by backfill / last-mile safety
        assert los.normalizePropertyName("(AFE-only 171651.01)") == "(AFE-ONLY 171651.01)"
        assert los.normalizePropertyName("(Unmatched AFE 24032)") == "(UNMATCHED AFE 24032)"


class TestNormalizeOwnerName:
    def test_collapse_commas_to_spaces(self):
        a = los.normalizeOwnerName("SHALEHAVEN PARTNERS 2025, LP")
        b = los.normalizeOwnerName("SHALEHAVEN PARTNERS 2025 LP")
        assert a == b == "SHALEHAVEN PARTNERS 2025 LP"

    def test_none_and_nan(self):
        assert los.normalizeOwnerName(None) is None
        assert los.normalizeOwnerName(np.nan) is None


class TestPropertyKeyAliases:
    """Clase Azul merges: CLASEAZUL1/2 must resolve to CLASEAZUL1H/2H."""

    def test_afe_side_rewrite(self):
        afe = pd.DataFrame({
            "Well Name": ["Clase Azul 1", "Clase Azul 2", "Flybar State 1WB"],
            "Property Key": ["CLASEAZUL1", "CLASEAZUL2", "FLYBARSTATE1WB"],
            "Property Name (Normalized)": ["CLASE AZUL 1", "CLASE AZUL 2", "FLYBAR STATE 1WB"],
        })
        out = los._applyPropertyKeyAliases(afe, "Well Name")
        assert list(out["Property Key"]) == ["CLASEAZUL1H", "CLASEAZUL2H", "FLYBARSTATE1WB"]
        assert list(out["Well Name"]) == ["Clase Azul 1H", "Clase Azul 2H", "Flybar State 1WB"]
        assert out.loc[0, "Property Name (Normalized)"] == "CLASE AZUL 1H"

    def test_jib_side_rewrite(self):
        jib = pd.DataFrame({
            "Property Name": ["Clase Azul 1", "Flybar State 1WB"],
            "Property Key": ["CLASEAZUL1", "FLYBARSTATE1WB"],
            "Property Name (Normalized)": ["CLASE AZUL 1", "FLYBAR STATE 1WB"],
        })
        out = los._applyPropertyKeyAliases(jib, "Property Name")
        assert out.loc[0, "Property Key"] == "CLASEAZUL1H"
        assert out.loc[0, "Property Name"] == "Clase Azul 1H"

    def test_no_op_when_no_matches(self):
        afe = pd.DataFrame({
            "Well Name": ["Flybar State 1WB"],
            "Property Key": ["FLYBARSTATE1WB"],
            "Property Name (Normalized)": ["FLYBAR STATE 1WB"],
        })
        before = afe.copy()
        out = los._applyPropertyKeyAliases(afe, "Well Name")
        pd.testing.assert_frame_equal(out, before)


class TestJibAfeToWellOverride:
    """Manual JIB_AFE_TO_WELL overrides must fire regardless of whether the
    JIB row's Property Name was blank or populated (e.g. a typo'd name).

    These tests inject their own JIB_AFE_TO_WELL via monkeypatch so they are
    independent of whatever entries are in the live production dict."""

    def test_fires_on_blank_property_name(self, monkeypatch):
        monkeypatch.setattr(los, "JIB_AFE_TO_WELL", {"TESTAFE-1": "Test Well 1H"})
        afe = pd.DataFrame({
            "AFE Key": ["DD.22.32703"],
            "AFE Key Raw": ["DD.22.32703"],
            "Well Name": ["Test Well 1H"],
            "Property Key": ["TESTWELL1H"],
            "Property Name (Normalized)": ["TEST WELL 1H"],
        })
        jib = pd.DataFrame({
            "AFE Key": ["TESTAFE-1"],
            "AFE Key Raw": ["TESTAFE-1"],
            "Property Name": [None],
            "Property Key": [None],
            "Property Name (Normalized)": [None],
        })
        out = los._backfillJibPropertyNames(jib.copy(), afe)
        assert out.loc[0, "Property Name"] == "Test Well 1H"
        assert out.loc[0, "Property Key"] == "TESTWELL1H"

    def test_fires_on_populated_typo_property_name(self, monkeypatch):
        # The override must apply EVEN when Property Name is already populated
        # (source-data typo case: "CLAUSE AZUL #1H" arriving in JIB).
        monkeypatch.setattr(los, "JIB_AFE_TO_WELL", {"TESTAFE-1": "Test Well 1H"})
        afe = pd.DataFrame({
            "AFE Key": ["DD.22.32703"],
            "AFE Key Raw": ["DD.22.32703"],
            "Well Name": ["Test Well 1H"],
            "Property Key": ["TESTWELL1H"],
            "Property Name (Normalized)": ["TEST WELL 1H"],
        })
        jib = pd.DataFrame({
            "AFE Key": ["TESTAFE-1"],
            "AFE Key Raw": ["TESTAFE-1"],
            "Property Name": ["TYPO'D WELL NAME"],
            "Property Key": ["TYPODWELLNAME"],
            "Property Name (Normalized)": ["TYPO'D WELL NAME"],
        })
        out = los._backfillJibPropertyNames(jib.copy(), afe)
        assert out.loc[0, "Property Name"] == "Test Well 1H"
        assert out.loc[0, "Property Key"] == "TESTWELL1H"

    def test_override_stamps_label_when_target_missing_from_afe(self, monkeypatch):
        # If the target Well Name doesn't exist in the AFE master, the
        # override still stamps the label (so Power BI never sees a blank)
        # and derives Property Key / Normalized from the label.
        monkeypatch.setattr(los, "JIB_AFE_TO_WELL", {"TESTAFE-1": "Phantom Well 1H"})
        afe = pd.DataFrame({
            "AFE Key": ["DD.22.32703"],
            "AFE Key Raw": ["DD.22.32703"],
            "Well Name": ["Some Other Well"],
            "Property Key": ["SOMEOTHERWELL"],
            "Property Name (Normalized)": ["SOME OTHER WELL"],
        })
        jib = pd.DataFrame({
            "AFE Key": ["TESTAFE-1"],
            "AFE Key Raw": ["TESTAFE-1"],
            "Property Name": [None],
            "Property Key": [None],
            "Property Name (Normalized)": [None],
        })
        out = los._backfillJibPropertyNames(jib.copy(), afe)
        assert out.loc[0, "Property Name"] == "Phantom Well 1H"
        assert out.loc[0, "Property Key"] == "PHANTOMWELL1H"

    def test_unmapped_key_falls_through_to_synthetic_label(self, monkeypatch):
        monkeypatch.setattr(los, "JIB_AFE_TO_WELL", {})
        afe = pd.DataFrame({
            "AFE Key": ["DD.22.32703"],
            "AFE Key Raw": ["DD.22.32703"],
            "Well Name": ["Some Well"],
            "Property Key": ["SOMEWELL"],
            "Property Name (Normalized)": ["SOME WELL"],
        })
        jib = pd.DataFrame({
            "AFE Key": ["UNKNOWN999"],
            "AFE Key Raw": ["UNKNOWN999"],
            "Property Name": [None],
            "Property Key": [None],
            "Property Name (Normalized)": [None],
        })
        out = los._backfillJibPropertyNames(jib.copy(), afe)
        assert out.loc[0, "Property Name"] == "(Unmatched AFE UNKNOWN999)"


class TestOperatorMap:
    """The operator map builds legal_name -> clean_folder via four passes.
    Pass 4 (OPERATOR_TO_PROJECT manual overrides) must win over the three
    automatic passes."""

    def test_manual_override_beats_automatic_match(self, monkeypatch):
        monkeypatch.setattr(
            los, "OPERATOR_TO_PROJECT", {"AETHON ENERGY OPERATING LLC": "Aethon Energy"}
        )
        afe = pd.DataFrame({
            "Property Key": ["WELL1"],
            "AFE Key": ["DD.22.001"],
            "Folder": ["Some Other Folder"],
        })
        jib = pd.DataFrame({
            "Property Key": ["WELL1"],
            "AFE Key": ["DD.22.001"],
            "Operator": ["AETHON ENERGY OPERATING LLC"],
        })
        opMap = los.buildOperatorMap(afe, jib)
        # Pass 1 would map to "Some Other Folder" via shared Property Key,
        # but pass 4 override replaces it with "Aethon Energy".
        assert opMap["AETHON ENERGY OPERATING LLC"] == "Aethon Energy"

    def test_manual_override_fills_gap_when_no_automatic_match(self, monkeypatch):
        monkeypatch.setattr(
            los, "OPERATOR_TO_PROJECT", {"AETHON ENERGY OPERATING LLC": "Aethon Energy"}
        )
        afe = pd.DataFrame({
            "Property Key": ["WELL1"],
            "AFE Key": ["DD.22.001"],
            "Folder": ["Unrelated Folder"],
        })
        jib = pd.DataFrame({
            "Property Key": ["WELL2"],
            "AFE Key": ["DD.22.002"],
            "Operator": ["AETHON ENERGY OPERATING LLC"],
        })
        opMap = los.buildOperatorMap(afe, jib)
        assert opMap["AETHON ENERGY OPERATING LLC"] == "Aethon Energy"

    def test_override_matches_comma_and_period_variations(self, monkeypatch):
        # Real-world operator strings vary: commas, periods, mixed case.
        # Pass 4 must be tolerant to all three while still mapping the
        # EXACT jib string so .map() resolves it.
        monkeypatch.setattr(
            los, "OPERATOR_TO_PROJECT", {"AETHON ENERGY OPERATING LLC": "Aethon Energy"}
        )
        afe = pd.DataFrame({"Property Key": [], "AFE Key": [], "Folder": []})
        jib = pd.DataFrame({
            "Property Key": ["W1", "W2", "W3", "W4"],
            "AFE Key":      ["A1", "A2", "A3", "A4"],
            "Operator": [
                "AETHON ENERGY OPERATING LLC",
                "AETHON ENERGY OPERATING, LLC",
                "AETHON ENERGY OPERATING L.L.C.",
                "Aethon Energy Operating LLC",
            ],
        })
        opMap = los.buildOperatorMap(afe, jib)
        # Each JIB string (exactly as it appears) must map to "Aethon Energy"
        for op in jib["Operator"]:
            assert opMap.get(op) == "Aethon Energy", f"no match for {op!r}"


class TestNormalizeOperatorKey:
    def test_handles_common_variations(self):
        canonical = los._normalizeOperatorKey("AETHON ENERGY OPERATING LLC")
        assert los._normalizeOperatorKey("AETHON ENERGY OPERATING, LLC") == canonical
        assert los._normalizeOperatorKey("AETHON ENERGY OPERATING L.L.C.") == canonical
        assert los._normalizeOperatorKey("Aethon Energy Operating LLC") == canonical
        assert los._normalizeOperatorKey("  AETHON  ENERGY   OPERATING LLC  ") == canonical

    def test_blank_returns_empty(self):
        assert los._normalizeOperatorKey(None) == ""
        assert los._normalizeOperatorKey("") == ""
        assert los._normalizeOperatorKey("   ") == ""


class TestCanonicalNamingInvariant:
    """After the full pipeline runs, every row with the same Property Key must
    show the same Property Name. Regression test for the Ballard Petroleum
    bug: Facts had 'Chuck Fed 22-31-18SH' on AFE rows and 'CHUCK FED 22-31-18
    SH' on JIB rows, splitting the slicer and orphaning ~$842K of JIB cost."""

    def test_mixed_casing_collapses_to_one_property_name(self, tmp_path, monkeypatch):
        # minimal AFE and JIB inputs where the same well appears with
        # different casings on the two sides
        afe_path = tmp_path / "afe.xlsx"
        jib_path = tmp_path / "jib.xlsx"
        pd.DataFrame({
            "AFE Number": ["4078"],
            "Well Name": ["Chuck Fed 22-31-18SH"],
            "Bucketing": ["Drilling"],
            "Tax": [None],
            "Description": ["AFE line"],
            "Gross Cost": [100000.0],
            "Working Interest": [0.05],
            "Net Cost": [5000.0],
            "Folder": ["Ballard Petroleum"],
            "Fund": ["2025"],
            "Company Code": ["BAL"],
        }).to_excel(afe_path, index=False)
        pd.DataFrame({
            "Operator": ["BALLARD PETROLEUM HOLDINGS LLC"],
            "Owner Name": ["SHALEHAVEN PARTNERS 2025 LP"],
            "Op AFE": ["4078"],
            "Property Name": ["CHUCK FED 22-31-18 SH"],  # different casing + extra space
            "Major Description": ["TANGIBLE CONSTRUCTION"],
            "Minor Description": [None],
            "AFE Description": ["JIB line"],
            "Gross Invoiced": [80000.0],
            "Working Interest": [0.05],
            "Net Expense": [4000.0],
            "Invoice Date": [pd.Timestamp("2025-10-01")],
            "Activity Month": [pd.Timestamp("2025-10-31")],
            "Detail Line Notation": [None],
            "Property Code": [None],
            "Invoice Number": [None],
        }).to_excel(jib_path, index=False)

        facts, dims = los.generateAfeActualReport(
            str(afe_path), str(jib_path), str(tmp_path)
        )

        # Both rows must share the same Property Name + Property Key
        ballard_facts = facts[facts["Project"] == "Ballard Petroleum"]
        assert len(ballard_facts) == 2
        assert ballard_facts["Property Name"].nunique() == 1, \
            f"expected one canonical Property Name, got {ballard_facts['Property Name'].unique()}"
        assert ballard_facts["Property Name (Normalized)"].nunique() == 1
        # AFE side's spelling should win
        assert ballard_facts["Property Name"].iloc[0] == "Chuck Fed 22-31-18SH"
        # Dimensions should show only one well
        assert len(dims[dims["Project"] == "Ballard Petroleum"]) == 1


class TestParseJibMajorDescription:
    """Parser splits 'Major Description' into (Tax, Category) pairs."""

    def test_intangible_drilling(self):
        assert los.parseJibMajorDescription("Intangible Drilling") == ("Intangible", "Drilling")

    def test_tangible_facility(self):
        assert los.parseJibMajorDescription("Tangible Facility") == ("Tangible", "Facility")

    def test_upper_case_with_suffix(self):
        assert los.parseJibMajorDescription("INTANGIBLE DRILLING COSTS") == ("Intangible", "Drilling")
        assert los.parseJibMajorDescription("TANGIBLE CONSTRUCTION COSTS") == ("Tangible", "Facility")

    def test_abbreviated_forms(self):
        assert los.parseJibMajorDescription("INTANGIBLE COMPL COST") == ("Intangible", "Completion")
        assert los.parseJibMajorDescription("Intangible Completio") == ("Intangible", "Completion")

    def test_capital_well_work_wins_over_facility(self):
        # 'Facilty CWW' contains both FACIL and CWW — CWW must win
        assert los.parseJibMajorDescription("Intangible Facilty CWW") == ("Intangible", "Capital Well Work")
        assert los.parseJibMajorDescription("Tangible Capital Well Work") == ("Tangible", "Capital Well Work")

    def test_tax_fallback_for_opex_rows(self):
        # LOE / OPEX variants default to (Intangible, Overhead) — the user
        # prefers one non-CAPEX bucket rather than splitting into OPEX.
        for src in ("Lease Operating Expenses", "Lease Operating Expe",
                    "LEASE OP EXPENSES (JIB850)", "LEASE OPERATING EXPENSE",
                    "Operating Expense", "Lease Operations"):
            tax, cat = los.parseJibMajorDescription(src)
            assert tax == "Intangible", f"{src!r} -> {tax!r}"
            assert cat == "Overhead", f"{src!r} -> {cat!r}"

    def test_tax_fallback_for_exploratory(self):
        assert los.parseJibMajorDescription("EXPLORATORY/APPRAISAL DRILL") == ("Intangible", "Drilling")
        assert los.parseJibMajorDescription("EXPLORATORY/APPRAISAL COMPLETE") == ("Intangible", "Completion")

    def test_tax_fallback_for_equipment_and_facilities(self):
        assert los.parseJibMajorDescription("Equipment Costs") == ("Tangible", "Equipment")
        assert los.parseJibMajorDescription("EXPLORE/APPRAISAL PRODUCTION FACILITIES") == ("Tangible", "Facility")
        # Other Current Assets: tax-tagged but no category pin -> Overhead via catch-all
        assert los.parseJibMajorDescription("Other Current Assets") == ("Tangible", "Overhead")

    def test_tax_fallback_for_overhead(self):
        assert los.parseJibMajorDescription("Comp Generated Copas Overhead") == ("Intangible", "Overhead")

    def test_tax_fallback_for_afe_expenditures(self):
        # No Minor passed -> catch-all kicks in and Category defaults to Overhead.
        assert los.parseJibMajorDescription("AFE Expenditures") == ("Intangible", "Overhead")

    def test_non_expense_rows_stay_none(self):
        # Cash Call / Revenue aren't expense categories — no inference
        assert los.parseJibMajorDescription("Cash Call") == (None, None)
        assert los.parseJibMajorDescription("Revenue") == (None, None)

    def test_safe_is_environmental_expense(self):
        # SAFE in operator JIBs = safety/environmental expense (OPEX), not acronym.
        # Single-row example: Friesian 3 Federal Com 1H, $118 environmental charge.
        # Tax-tagged rows with no category pin default to Overhead via catch-all.
        assert los.parseJibMajorDescription("SAFE") == ("Intangible", "Overhead")

    def test_blank_inputs(self):
        assert los.parseJibMajorDescription(None) == (None, None)
        assert los.parseJibMajorDescription("") == (None, None)
        assert los.parseJibMajorDescription("   ") == (None, None)

    def test_minor_description_fills_category_when_major_is_generic(self):
        # Hunt Oil "AFE Expenditures" has no category keyword; real signal
        # is in Minor Description.
        assert los.parseJibMajorDescription("AFE Expenditures", "Drllng Fluids-Prod&S") == ("Intangible", "Drilling")
        assert los.parseJibMajorDescription("AFE Expenditures", "Eqpmnt-Facility Equ")  == ("Intangible", "Facility")
        assert los.parseJibMajorDescription("AFE Expenditures", "Csd Hole Wirelne Ser") == ("Intangible", "Completion")
        assert los.parseJibMajorDescription("AFE Expenditures", "Administrative OH")    == ("Intangible", "Overhead")
        assert los.parseJibMajorDescription("AFE Expenditures", "ENVIRONMENTAL EXPENSE") == ("Intangible", "Overhead")
        assert los.parseJibMajorDescription("AFE Expenditures", "Lease Brokerage Comm") == ("Intangible", "Land")
        assert los.parseJibMajorDescription("AFE Expenditures", "Equipment - Wellhead") == ("Intangible", "Facility")
        assert los.parseJibMajorDescription("AFE Expenditures", "Equipment - Casing")   == ("Intangible", "Completion")

    def test_major_category_wins_over_minor(self):
        # If Major Description already pins a category, Minor is ignored.
        assert los.parseJibMajorDescription("Intangible Drilling", "Eqpmnt-Facility Equ") == ("Intangible", "Drilling")

    def test_minor_without_tax_still_returns_category(self):
        assert los.parseJibMajorDescription("SAFE", "ENVIRONMENTAL EXPENSE") == ("Intangible", "Overhead")

    def test_unclassifiable_tax_tagged_rows_fall_back_to_overhead(self):
        # Ambiguous Minor Descriptions on tax-tagged rows default to Overhead.
        assert los.parseJibMajorDescription("AFE Expenditures", "Contract Labor")      == ("Intangible", "Overhead")
        assert los.parseJibMajorDescription("AFE Expenditures", "Miscellaneous expens") == ("Intangible", "Overhead")
        assert los.parseJibMajorDescription("AFE Expenditures", "Inspection Services")  == ("Intangible", "Overhead")
        assert los.parseJibMajorDescription("AFE Expenditures", "Airfare")              == ("Intangible", "Overhead")
        assert los.parseJibMajorDescription("AFE Expenditures", "Some Unknown Thing")   == ("Intangible", "Overhead")

    def test_cash_call_and_revenue_stay_uncategorized(self):
        # Non-expense rows don't get force-bucketed to Overhead.
        assert los.parseJibMajorDescription("Cash Call", "Anything")    == (None, None)
        assert los.parseJibMajorDescription("Revenue",   "Anything")    == (None, None)


class TestOwnerInheritance:
    """AFE rows inherit Owner Name from JIB rows sharing the same Property
    Key. When multiple owners exist for a single well, the owner with the
    largest absolute Net Expense wins."""

    def test_afe_inherits_sole_owner_from_jib(self, tmp_path):
        afe_path = tmp_path / "afe.xlsx"
        jib_path = tmp_path / "jib.xlsx"
        pd.DataFrame({
            "AFE Number": ["4078", "4079"],
            "Well Name": ["Well A", "Well B"],
            "Bucketing": ["Drilling", "Drilling"],
            "Tax": [None, None],
            "Description": ["AFE A", "AFE B"],
            "Gross Cost": [100.0, 200.0],
            "Working Interest": [0.05, 0.05],
            "Net Cost": [5.0, 10.0],
            "Folder": ["Ballard Petroleum", "Ballard Petroleum"],
            "Fund": ["2025", "2025"],
            "Company Code": ["BAL", "BAL"],
        }).to_excel(afe_path, index=False)
        pd.DataFrame({
            "Operator": ["BALLARD PETROLEUM HOLDINGS LLC"] * 2,
            "Owner Name": ["SHALEHAVEN PARTNERS 2025 LP"] * 2,
            "Op AFE": ["4078", "4079"],
            "Property Name": ["Well A", "Well B"],
            "Major Description": ["x", "x"],
            "Minor Description": [None, None],
            "AFE Description": ["y", "y"],
            "Gross Invoiced": [50.0, 80.0],
            "Working Interest": [0.05, 0.05],
            "Net Expense": [2.5, 4.0],
            "Invoice Date": [pd.Timestamp("2025-10-01")] * 2,
            "Activity Month": [pd.Timestamp("2025-10-31")] * 2,
            "Detail Line Notation": [None, None],
            "Property Code": [None, None],
            "Invoice Number": [None, None],
        }).to_excel(jib_path, index=False)

        facts, _ = los.generateAfeActualReport(
            str(afe_path), str(jib_path), str(tmp_path)
        )
        afe_rows = facts[facts["Source"] == "AFE"]
        # Every AFE row must carry the inherited owner
        assert afe_rows["Owner Name"].isna().sum() == 0
        assert (afe_rows["Owner Name"] == "SHALEHAVEN PARTNERS 2025 LP").all()

    def test_dominant_owner_wins_when_well_has_multiple(self, tmp_path):
        afe_path = tmp_path / "afe.xlsx"
        jib_path = tmp_path / "jib.xlsx"
        pd.DataFrame({
            "AFE Number": ["4078"],
            "Well Name": ["Well A"],
            "Bucketing": ["Drilling"],
            "Tax": [None],
            "Description": ["AFE A"],
            "Gross Cost": [100.0],
            "Working Interest": [0.05],
            "Net Cost": [5.0],
            "Folder": ["Ballard Petroleum"],
            "Fund": ["2025"],
            "Company Code": ["BAL"],
        }).to_excel(afe_path, index=False)
        # Two owners on JIB; Owner X has larger abs Net Expense (80) than
        # Owner Y (20) -> X wins.
        pd.DataFrame({
            "Operator": ["BALLARD PETROLEUM HOLDINGS LLC"] * 2,
            "Owner Name": ["OWNER X", "OWNER Y"],
            "Op AFE": ["4078", "4078"],
            "Property Name": ["Well A", "Well A"],
            "Major Description": ["x", "x"],
            "Minor Description": [None, None],
            "AFE Description": ["y", "y"],
            "Gross Invoiced": [1000.0, 500.0],
            "Working Interest": [0.08, 0.02],
            "Net Expense": [-80.0, 20.0],   # abs: X=80, Y=20
            "Invoice Date": [pd.Timestamp("2025-10-01")] * 2,
            "Activity Month": [pd.Timestamp("2025-10-31")] * 2,
            "Detail Line Notation": [None, None],
            "Property Code": [None, None],
            "Invoice Number": [None, None],
        }).to_excel(jib_path, index=False)

        facts, _ = los.generateAfeActualReport(
            str(afe_path), str(jib_path), str(tmp_path)
        )
        afe_row = facts[facts["Source"] == "AFE"].iloc[0]
        assert afe_row["Owner Name"] == "OWNER X"


class TestBackfillConservation:
    """Row-count and amount conservation invariants for the backfill pass."""

    def test_row_count_preserved(self):
        afe = pd.DataFrame({
            "AFE Key": ["A1"],
            "AFE Key Raw": ["A1"],
            "Well Name": ["Well 1"],
            "Property Key": ["WELL1"],
            "Property Name (Normalized)": ["WELL 1"],
        })
        jib = pd.DataFrame({
            "AFE Key": ["A1", "X99"],
            "AFE Key Raw": ["A1", "X99"],
            "Property Name": [None, None],
            "Property Key": [None, None],
            "Property Name (Normalized)": [None, None],
        })
        out = los._backfillJibPropertyNames(jib.copy(), afe)
        assert len(out) == 2
