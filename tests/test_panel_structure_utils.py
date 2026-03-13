"""
Tests for literature-guided panel-structure helper logic.

Focus:
- institution presence-pattern classification
- identifier-linkage diagnostics with and without OPEID-like columns
- classification-stability summaries on small synthetic panels
"""
from __future__ import annotations

import pandas as pd

from helpers import load_script_module


panel_utils = load_script_module("panel_structure_utils", "Scripts/panel_structure_utils.py")


def test_classify_presence_pattern_distinguishes_always_present_and_intermittent() -> None:
    years = [2021, 2022, 2023, 2024]

    always = panel_utils.classify_presence_pattern([2021, 2022, 2023, 2024], years)
    assert always["pattern"] == "always_present"
    assert always["possible_selection_risk"] is False

    intermittent = panel_utils.classify_presence_pattern([2021, 2023, 2024], years)
    assert intermittent["pattern"] == "intermittent_gap"
    assert intermittent["missing_internal_years"] == 1
    assert intermittent["possible_selection_risk"] is True


def test_build_identifier_linkage_summary_gracefully_handles_missing_opeid() -> None:
    df = pd.DataFrame(
        [
            {"year": 2022, "UNITID": 100654},
            {"year": 2023, "UNITID": 100663},
        ]
    )
    out = panel_utils.build_identifier_linkage_summary(df, [2022, 2023])
    assert out.iloc[0]["record_type"] == "unavailable"
    assert "skipped gracefully" in out.iloc[0]["notes"]


def test_build_identifier_linkage_summary_flags_multi_opeid_cases() -> None:
    df = pd.DataFrame(
        [
            {"year": 2022, "UNITID": 100654, "OPEID": "00100200"},
            {"year": 2023, "UNITID": 100654, "OPEID": "00100300"},
            {"year": 2023, "UNITID": 100663, "OPEID": "00100300"},
        ]
    )
    out = panel_utils.build_identifier_linkage_summary(df, [2022, 2023])
    overall = out[out["record_type"] == "overall"].iloc[0]
    assert overall["unitids_with_multiple_opeid"] == 1
    assert overall["opeids_with_multiple_unitids_any_year"] == 1


def test_build_classification_stability_summary_counts_changed_institutions() -> None:
    df = pd.DataFrame(
        [
            {"year": 2022, "UNITID": 100654, "CONTROL": 1},
            {"year": 2023, "UNITID": 100654, "CONTROL": 2},
            {"year": 2022, "UNITID": 100663, "CONTROL": 1},
            {"year": 2023, "UNITID": 100663, "CONTROL": 1},
        ]
    )
    out = panel_utils.build_classification_stability_summary(df, ["CONTROL"])
    row = out[out["record_type"] == "institution_change"].iloc[0]
    assert row["variable"] == "CONTROL"
    assert row["units_observed"] == 2
    assert row["units_changed"] == 1
