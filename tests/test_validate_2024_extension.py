import importlib.util
from pathlib import Path

import pandas as pd
import pytest

SPEC = importlib.util.spec_from_file_location(
    "validate_2024_extension", Path(__file__).resolve().parents[1] / "Scripts/QA_QC/29_validate_2024_extension.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_free_text_newlines_are_preserved_and_whitespace_only_is_missing():
    result = MODULE.source_values(pd.Series([" x\n ", "\t", "None", "-2"]), "free text")
    assert result.iloc[0] == "x\n"
    assert pd.isna(result.iloc[1])
    assert result.iloc[2] == "None"
    assert result.iloc[3] == "-2"


def fixture(tmp_path):
    root = tmp_path / "sources"
    table_dir = root / "Raw_Access_Databases/2024/tables_csv"
    table_dir.mkdir(parents=True)
    metadata = table_dir.parent / "metadata"
    metadata.mkdir()
    raw = pd.DataFrame({"UNITID": [475477, 111111], "PRCH_F": [3, -2], "FORM_F": [-1, 3],
                        "F3EQUITR": [48, 20], "F3SALRPC": [64, 70]})
    raw.to_csv(table_dir / "FIN2024.csv", index=False)
    pd.DataFrame([{"table_name": "FIN2024", "csv_path": "tables_csv/FIN2024.csv"}]).to_csv(metadata / "table_inventory.csv", index=False)
    mapping = pd.DataFrame([{"access_table_name": "FIN2024", "varname": name, "analysis_column": name,
                             "variable_id": "nces:FIN:" + name,
                             "logical_type": "categorical string" if name in {"PRCH_F", "FORM_F"} else "float"}
                            for name in raw if name != "UNITID"])
    mapping.to_csv(tmp_path / "mapping.csv", index=False)
    policy = pd.DataFrame([
        {"year_start": 2024, "year_end": 2024, "flag": "PRCH_F", "code": 3, "action": "null",
         "target_columns": "F3EQUITR", "row_predicate": '{"FORM_F":-1}', "rule_id": "partial"},
        {"year_start": 2024, "year_end": 2024, "flag": "PRCH_F", "code": -2, "action": "retain",
         "target_columns": "", "row_predicate": '{"FORM_F":3}', "rule_id": "retain"}])
    policy.to_csv(tmp_path / "policy.csv", index=False)
    wide = raw.copy()
    wide["year"] = 2024
    for name in ["PRCH_F", "FORM_F"]:
        wide[name] = wide[name].astype(str)
    for name in ["F3EQUITR", "F3SALRPC"]:
        wide[name] = wide[name].astype(float)
    wide.to_parquet(tmp_path / "wide.parquet", index=False)
    clean = wide.copy()
    clean.loc[0, "F3EQUITR"] = None
    clean.to_parquet(tmp_path / "clean.parquet", index=False)
    pd.DataFrame([{"UNITID": 475477, "column": "F3EQUITR", "rule_id": "partial", "year": 2024,
                   "action": "null", "old_value": "48.0", "new_value": None}]).to_parquet(tmp_path / "actions.parquet", index=False)
    return [root, tmp_path / "wide.parquet", tmp_path / "clean.parquet", tmp_path / "mapping.csv", tmp_path / "policy.csv", tmp_path / "actions.parquet"]


def test_independent_source_and_cleaning_reconciliation(tmp_path):
    paths = fixture(tmp_path)
    result = MODULE.validate(*paths)
    assert result["compared_panel_cells_including_missing"] == 8
    assert result["verified_cleaning_actions"] == 1
    assert result["raw_to_wide_discrepancies"] == 0
    assert result["rows"] == 2
    assert result["columns"] == 6


def test_detects_value_corruption_despite_valid_keys(tmp_path):
    paths = fixture(tmp_path)
    wide = pd.read_parquet(paths[1])
    wide.loc[1, "F3SALRPC"] = 71
    wide.to_parquet(paths[1], index=False)
    with pytest.raises(ValueError, match="Raw-to-wide mismatch"):
        MODULE.validate(*paths)


def test_detects_extra_cleaning_or_incomplete_action_ledger(tmp_path):
    paths = fixture(tmp_path)
    clean = pd.read_parquet(paths[2])
    clean.loc[0, "F3SALRPC"] = None
    clean.to_parquet(paths[2], index=False)
    with pytest.raises(ValueError, match="Unexpected cleaning"):
        MODULE.validate(*paths)
    paths = fixture(tmp_path / "other")
    actions = pd.read_parquet(paths[5])
    actions.iloc[:0].to_parquet(paths[5], index=False)
    with pytest.raises(ValueError, match="ledger differs"):
        MODULE.validate(*paths)
