"""The source-value audit must distinguish source errors from documented cleaning."""
import csv
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from helpers import load_script_module


audit = load_script_module("sfa_value_audit", "Scripts/QA_QC/23_verify_sfa_values.py")


def test_schema_resolution_searches_all_tables_and_reports_absent_fields() -> None:
    tables = audit.schema_tables("CREATE TABLE [A] (\n[VALUE] Double\n);\nCREATE TABLE [B] (\n[VALUE] Double\n);")
    metadata = [{"TableName": "SFA_P1", "VarName": "VALUE", "VarNumber": "1"},
                {"TableName": "SFA_P1", "VarName": "ABSENT", "VarNumber": "2"}]
    rows = {row["varname"]: row for row in audit.source_table_audit(metadata, tables)}
    assert rows["VALUE"]["mapping_status"] == "ambiguous_physical_identity"
    assert rows["VALUE"]["physical_candidates"] == "A|B"
    assert rows["ABSENT"]["mapping_status"] == "absent"


def test_source_values_missingness_and_prch_explanations(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "schema.sql").write_text("CREATE TABLE [SFA2223_P1] (\n[UNITID] Long Integer,\n[UPGRNTN] Double\n);\n")
    with (source / "varTable23.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["TableName", "VarName", "VarNumber", "ImputationVar"])
        writer.writerow(["SFA2223_P2", "UPGRNTN", "1", "XUPGRNTN"])
    (source / "sfa2223_p1.csv").write_text("UNITID,UPGRNTN\n100,1\n101,\n102,0\n")
    (source / "release_readme.txt").write_text("Imputation flags are not included in this release.")
    wide, clean, lineage, actions = [tmp_path / (name + ".parquet") for name in ("wide", "clean", "lineage", "actions")]
    pq.write_table(pa.table({"year": [2023] * 3, "UNITID": [100, 101, 102], "UPGRNTN": [1.0, None, 0.0]}), wide)
    pq.write_table(pa.table({"year": [2023] * 3, "UNITID": [100, 101, 102], "UPGRNTN": [None, None, 0.0]}), clean)
    pq.write_table(pa.Table.from_pylist([{"year": 2023, "analysis_column": "UPGRNTN", "varname": "UPGRNTN", "source_varnumber": "00000001",
                                         "access_table_name": "sfa2223_p1", "source_file": "SFA_P", "transformation_id": "identity", "lineage_role": "direct"}]), lineage)
    pq.write_table(pa.Table.from_pylist([{"year": 2023, "UNITID": 100, "column": "UPGRNTN", "old_value": "1", "new_value": "",
                                         "flag": "PRCH_SFA", "code": 2, "action": "null"}]), actions)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"status": "complete", "artifacts": [
        {"name": "panel_clean.parquet", "sha256": hashlib.sha256(clean.read_bytes()).hexdigest()},
        {"name": "actions.parquet", "sha256": hashlib.sha256(actions.read_bytes()).hexdigest()},
    ]}))
    summary = audit.run_audit(source, wide, clean, lineage, actions, manifest, tmp_path / "report")
    assert summary["comparison_status_counts"] == {"pass": 1}
    assert summary["matching_nonmissing_values"] == 2
    assert summary["missingness_mismatches"] == 0
    assert summary["raw_to_clean_changes"] == 1
    assert summary["unexplained_clean_changes"] == 0
    assert summary["clean_hash_matches_prch_manifest"]
    assert summary["physical_xupgrnt_columns"] == []
    # Turning a source zero into missing must be a mismatch, never normalized away.
    (source / "sfa2223_p1.csv").write_text("UNITID,UPGRNTN\n100,1\n101,\n102,\n")
    pq.write_table(pa.table({"year": [2023] * 3, "UNITID": [100, 101, 102], "UPGRNTN": [2.0, None, 0.0]}), clean)
    bad = audit.run_audit(source, wide, clean, lineage, actions, manifest, tmp_path / "bad_report")
    assert bad["raw_source_mismatches"] == 1
    assert bad["missingness_mismatches"] == 1
    assert bad["unexplained_clean_changes"] == 1
    assert not bad["clean_hash_matches_prch_manifest"]
