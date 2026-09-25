from __future__ import annotations

import json

import pandas as pd
import pytest

from source_metadata_corrections import (
    apply_source_metadata_corrections, exact_table_dictionary_rows, load_registry,
    normalize_number, verify_source_files,
)
from helpers import load_script_module


def pell_frame():
    return pd.DataFrame([
        {"year": 2023, "source_file": "SFA_P", "access_table_name": "SFA2223_P2",
         "source_file_label": "SFA2223_P2", "varname": "UPGRNTN", "varnumber": "00070306",
         "varTitle": "Pell recipients", "variable_id": "nces:SFA_P:00070306:UPGRNTN"},
        {"year": 2023, "source_file": "SFA_P", "access_table_name": "SFA2223_P2",
         "source_file_label": "SFA2223_P2", "varname": "SCUGFFP", "varnumber": "00070296",
         "varTitle": "Share", "variable_id": "nces:SFA_P:00070296:SCUGFFP"},
        {"year": 2022, "source_file": "SFA_P", "access_table_name": "SFA2122_P2",
         "source_file_label": "SFA2122_P2", "varname": "UPGRNTN", "varnumber": "00070306",
         "varTitle": "Pell recipients", "variable_id": "nces:SFA_P:00070306:UPGRNTN"},
    ])


def test_exact_correction_preserves_original_identity_and_unrelated_rows():
    before = pell_frame()
    after = apply_source_metadata_corrections(before, verify_inputs=False)
    assert after.loc[0, "resolved_physical_table"] == "SFA2223_P1"
    assert after.loc[0, "access_table_name"] == "SFA2223_P1"
    assert json.loads(after.loc[0, "original_metadata_json"]) == before.iloc[0].to_dict()
    assert after.loc[0, "variable_id"] == before.loc[0, "variable_id"]
    pd.testing.assert_frame_equal(after.loc[1:, before.columns], before.loc[1:])
    pd.testing.assert_frame_equal(apply_source_metadata_corrections(after, verify_inputs=False), after)
    assert "not included" in after.loc[0, "imputation_flag_availability"]


def test_no_blanket_table_replacement_or_unverified_input(tmp_path):
    frame = pell_frame()
    with pytest.raises(ValueError, match="does not match input"):
        apply_source_metadata_corrections(frame, root=tmp_path)
    before = frame.copy()
    before.loc[0, "varnumber"] = "123"
    after = apply_source_metadata_corrections(before, root=tmp_path)
    pd.testing.assert_frame_equal(before, after)


def test_registry_audit_is_individual_and_absent_variable_not_corrected():
    registry = load_registry()
    assert len(registry["corrections"]) == 343
    assert not {"SCFA2ND", "SCUGFFP"} & {r["varname"] for r in registry["corrections"]}
    for row in registry["corrections"]:
        assert row["varname"] in registry["physical_tables"][row["resolved_table"]]["columns"]
        assert row["varname"] not in registry["physical_tables"][row["original_table"]]["columns"]


def test_unnamed_codebook_correction_requires_unique_number(tmp_path):
    registry = load_registry()
    row = next(r for r in registry["corrections"] if r["varname"] == "UPGRNTN")
    registry["corrections"] = [row, {**row, "varname": "OTHER", "evidence_id": "other"}]
    registry["physical_tables"][row["resolved_table"]]["columns"].append("OTHER")
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(registry))
    frame = pell_frame().iloc[:1].copy()
    frame["varname"] = ""
    with pytest.raises(ValueError, match="Ambiguous unnamed"):
        apply_source_metadata_corrections(frame, registry_path=path, codebook=True, verify_inputs=False)


def test_number_normalization():
    assert normalize_number(pd.NA) == ""
    assert normalize_number(float("nan")) == ""
    assert normalize_number(70306.0) == "70306"
    assert normalize_number("00070306") == "70306"


def test_existing_conflicting_resolution_is_rejected():
    frame = apply_source_metadata_corrections(pell_frame(), verify_inputs=False)
    frame.loc[0, "resolved_physical_table"] = "SFA2223_P2"
    with pytest.raises(ValueError, match="Conflicting existing correction"):
        apply_source_metadata_corrections(frame, verify_inputs=False)


def test_harmonize_cannot_borrow_shared_canonical_family():
    frame = pell_frame().iloc[:2]
    module = load_script_module("harmonize_exact_metadata", "Scripts/04_harmonize.py")
    assert module.select_dict_source(frame, "SFA_P", "SFA2223_P1").empty
    with pytest.raises(SystemExit, match="physical table/variable metadata mismatch"):
        module.select_dict_source(frame, "SFA_P", "SFA2223_P1", physical_columns=["UNITID", "UPGRNTN"])
    resolved = apply_source_metadata_corrections(frame, verify_inputs=False)
    selected = module.select_dict_source(resolved, "SFA_P", "SFA2223_P1", physical_columns=["UNITID", "UPGRNTN"])
    assert selected.varname.tolist() == ["UPGRNTN"]


def test_same_named_variable_in_distinct_tables_is_not_an_ambiguity():
    frame = pell_frame().iloc[:1]
    other = frame.assign(access_table_name="SFA2223_P1", varnumber="99999")
    selected = exact_table_dictionary_rows(pd.concat([frame, other]), "SFA_P", "SFA2223_P1", ["UPGRNTN"])
    assert selected.varnumber.tolist() == ["99999"]
