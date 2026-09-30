import csv
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pandas as pd
import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "Scripts"))
from extension_2024_metadata import attach_2024_provenance
from prepare_2024_pipeline import CONTRACT_ID, digest, prepare, validate_sources


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def source_fixture(tmp_path):
    repository = tmp_path / "repo"
    root = tmp_path / "sources"
    write_csv(repository / "contracts/extension_2024/table_grain.csv", [{
        "access_table_pattern": "SFA2324", "unit_key_columns": "UNITID"}])
    write_csv(repository / "contracts/extension_2024/source_columns.csv", [
        {"access_table_pattern": "SFA2324", "column_name": name}
        for name in ["UNITID", "UPGRNTN"]])
    write_csv(root / "Raw_Access_Databases/2024/metadata/table_inventory.csv", [{
        "table_name": "SFA2324", "table_role": "data", "has_unitid": "true",
        "csv_path": "tables_csv/SFA2324.csv", "row_count_csv": "2"}])
    table = root / "Raw_Access_Databases/2024/tables_csv/SFA2324.csv"
    write_csv(table, [{"UNITID": "1", "UPGRNTN": "5"}, {"UNITID": "2", "UPGRNTN": "8"}])
    return repository, root, table


def test_preflight_rejects_unknown_column_and_conflicting_key(tmp_path):
    repo, root, table = source_fixture(tmp_path)
    assert validate_sources(root, repo)["tables"][0]["rows"] == 2
    write_csv(table, [{"UNITID": "1", "UPGRNTN": "5", "NEW": "9"}])
    with pytest.raises(ValueError, match="physical columns changed"):
        validate_sources(root, repo)
    write_csv(table, [{"UNITID": "1", "UPGRNTN": "5"}, {"UNITID": "1", "UPGRNTN": "8"}])
    with pytest.raises(ValueError, match="conflicting duplicate source key"):
        validate_sources(root, repo)


def test_pell_and_cost_use_exact_relocated_identities():
    mapping = pd.read_csv(REPOSITORY / "contracts/extension_2024/variable_mapping.csv", keep_default_na=False)
    pell = mapping[mapping.varname.isin(["UPGRNTN", "UPGRNTT"])]
    assert set(pell.access_table_name) == {"SFA2324"}
    assert set(pell.previous_variable_id) == {"nces:SFA_P:00070306:UPGRNTN", "nces:SFA_P:00070421:UPGRNTT"}
    tuition = mapping[mapping.varname.eq("TUITION1")].iloc[0]
    assert tuition.analysis_column == "TUITION1"
    assert tuition.access_table_name == "COST1_2024"
    assert tuition.previous_variable_id == "nces:IC_AY:00011616:TUITION1"
    # SFA is reused as a source-family name in 2024. Its 2004-08 identities
    # must not steal the current 2023 SFA_P column merely because IDs recur.
    aid = mapping[mapping.varname.eq("AGRNT_A")].iloc[0]
    assert aid.analysis_column == "AGRNT_A__SFA_P__00070321__516B07129244"
    assert aid.previous_variable_id == "nces:SFA_P:00070321:AGRNT_A"
    assert aid.decision == "exact_2023_number_name_relocated"
    assert not mapping.analysis_column.duplicated().any()
    assert not mapping.variable_id.duplicated().any()
    columns = pd.read_csv(REPOSITORY / "contracts/extension_2024/source_columns.csv", keep_default_na=False)
    assert set(columns.loc[columns.column_name.eq("UNITID"), "role"]) == {"identifier"}
    assert set(columns.role) <= {"identifier", "dimension", "measure", "excluded"}


def test_missing_finance_form_partial_child_retains_expenses():
    policy = pd.read_csv(REPOSITORY / "contracts/extension_2024/prch_policy.csv", dtype=str, keep_default_na=False)
    rule = policy[(policy.flag == "PRCH_F") & (policy.code == "3") & (policy.row_predicate == '{"FORM_F":-1}')].iloc[0]
    assert rule.action == "null"
    assert rule.form == "not_reported"
    assert "F3EQUITR" in rule.target_columns.split("|")
    assert "F3SALRPC" not in rule.target_columns.split("|")
    # The actual source case has no finance-form row and only these two derived
    # cells. Its expense ratio survives and its equity ratio is nulled.
    actual = {"UNITID": 475477, "F3EQUITR": 48, "F3SALRPC": 64}
    for column in rule.target_columns.split("|"):
        if column in actual:
            actual[column] = None
    assert actual == {"UNITID": 475477, "F3EQUITR": None, "F3SALRPC": 64}
    sfa = policy[(policy.flag == "PRCH_SFA") & (policy.action == "null")].iloc[0]
    assert "SCFA2ND" in sfa.target_columns.split("|")
    assert not {"PARTVT", "PO9", "DOD"} & set(sfa.target_columns.split("|"))


def test_table_release_status_and_literal_none_are_preserved(tmp_path):
    write_csv(tmp_path / "Raw_Access_Databases/2024/metadata/table_inventory.csv", [{
        "table_name": "HD2024", "source_release_type": "Final", "source_release_date": "2026-09-08",
        "source_url": "https://nces.ed.gov/example.zip", "source_archive_sha256": "a" * 64,
        "source_member_sha256": "b" * 64}])
    write_csv(tmp_path / "Raw_Access_Databases/2024/tables_csv/tables24.csv", [{
        "TableName": "HD2024", "YearCoverage": "Fall 2024"}])
    frame = pd.DataFrame([{"year": 2024, "access_table_name": "HD2024", "release_type": "Provisional", "valuelabel": "None"}])
    result = attach_2024_provenance(frame, root=tmp_path)
    assert result.iloc[0].release_type == "Final"
    assert result.iloc[0].original_release_type == "Provisional"
    assert result.iloc[0].valuelabel == "None"
    assert result.iloc[0].source_table_reference_period == "Fall 2024"
    frame.loc[0, "access_table_name"] = "UNREVIEWED2024"
    with pytest.raises(ValueError, match="absent physical tables"):
        attach_2024_provenance(frame, root=tmp_path)


def test_prepared_pipeline_preserves_historical_policy_bytes(tmp_path):
    output = tmp_path / "pipeline"
    receipt = prepare(output, REPOSITORY)
    original = subprocess.check_output(["git", "show", receipt["base_commit"] + ":contracts/prch_policy.csv"], cwd=REPOSITORY)
    assert (output / "contracts/prch_policy.csv").read_bytes().startswith(original)
    assert receipt["contract_id"] == CONTRACT_ID
    assert all(digest(output / row["path"]) == row["sha256"] for row in receipt["pipeline_scripts"])
    assert "keep_default_na=False" in (output / "Scripts/03_dictionary_ingest.py").read_text()
    # Normal release-mode loaders must accept every extended contract.
    code = """
from wide_build_common import load_analysis_schema_contract, load_discrete_family_contract
from prch_policy import load_policy_contract
from pathlib import Path
root=Path.cwd()
assert len(load_analysis_schema_contract(root/'contracts/analysis_schema.csv')) == 2091
assert len(load_discrete_family_contract(root/'contracts/discrete_families.csv')) == 297
load_policy_contract(root/'contracts/prch_policy.csv', release_mode=True)
"""
    import os
    env = dict(os.environ, PYTHONPATH=str(output / "Scripts"))
    subprocess.run([sys.executable, "-c", code], cwd=output, env=env, check=True, capture_output=True)


@pytest.mark.parametrize("years,mode,allowed", [
    ([2023], "release", False), ([2023, 2024], "release", False),
    ([2024], "development", False), ([2024], "release", True),
])
def test_extension_cleaner_is_locked_to_2024_release(tmp_path, years, mode, allowed):
    output = tmp_path / "pipeline"
    prepare(output, REPOSITORY)
    panel = tmp_path / "panel.parquet"
    pd.DataFrame({"UNITID": [1] * len(years), "year": years}).to_parquet(panel)
    result = subprocess.run([
        sys.executable, str(output / "Scripts/07_clean_panel.py"),
        "--input", str(panel), "--output", str(tmp_path / "clean.parquet"),
        "--dictionary", str(tmp_path / "unused.parquet"), "--policy-mode", mode,
        "--log-file", str(tmp_path / "clean.log"),
    ], text=True, capture_output=True)
    message = result.stdout + result.stderr
    assert result.returncode != 0
    if allowed:
        # The authorized scope proceeds to the unchanged missing-PRCH gate.
        assert "no PRCH flag columns" in message
    else:
        assert "requires exactly year 2024 and release policy mode" in message
    assert not (tmp_path / "clean.parquet").exists()
