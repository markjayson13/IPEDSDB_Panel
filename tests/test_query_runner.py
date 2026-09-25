"""
Tests for the saved-query runner and its inspection-view bootstrap.

Focus:
- saved-query discovery
- non-invasive DuckDB inspection bootstrap over standard outputs
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from helpers import load_script_module, run_script


query_runner = load_script_module("run_saved_query", "Scripts/run_saved_query.py")


def write_parquet(path: Path, rows: list[dict]) -> None:
    if not rows:
        raise ValueError("rows must be non-empty for this helper")
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = {key: [row.get(key) for row in rows] for key in rows[0]}
    pq.write_table(pa.table(columns), path)


def test_resolve_saved_query_path_accepts_stem(tmp_path: Path) -> None:
    sql_path = tmp_path / "01_example_query.sql"
    sql_path.write_text("select 1;\n", encoding="utf-8")

    resolved = query_runner.resolve_saved_query_path(tmp_path, "01_example_query")

    assert resolved == sql_path.resolve()


def test_bootstrap_artifact_views_reads_build_db_and_outputs(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    layout = query_runner.ensure_data_layout(root)

    write_parquet(
        layout.panels / "panel_clean_analysis_2023_2023.parquet",
        [{"year": 2023, "UNITID": 100654}],
    )
    write_parquet(
        layout.panels / "2023-2023" / "panel_long_varnum_2023_2023.parquet",
        [
            {
                "year": 2023,
                "UNITID": 100654,
                "varname": "INSTNM",
                "value": "Example U",
                "varnumber": "00000001",
                "source_file": "HD",
            }
        ],
    )
    write_parquet(
        layout.dictionary / "dictionary_lake.parquet",
        [
            {
                "year": 2023,
                "varnumber": "00000001",
                "varname": "INSTNM",
                "varTitle": "Institution name",
                "longDescription": "Institution name",
                "source_file": "HD",
            }
        ],
    )
    write_parquet(
        layout.dictionary / "dictionary_codes.parquet",
        [
            {
                "year": 2023,
                "varnumber": "00000001",
                "varname": "CONTROL",
                "codevalue": "1",
                "valuelabel": "Public",
                "source_file": "HD",
            }
        ],
    )
    release_inventory = layout.checks / "download_qc" / "release_inventory.csv"
    release_inventory.parent.mkdir(parents=True, exist_ok=True)
    with release_inventory.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["year", "academic_year_label", "release_type", "release_date_text", "download_status"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "year": 2023,
                "academic_year_label": "2023-24",
                "release_type": "Final",
                "release_date_text": "March 2026",
                "download_status": "downloaded",
            }
        )

    build_db_path = layout.build / "ipedsdb_build.duckdb"
    con_build = duckdb.connect(str(build_db_path))
    con_build.execute("CREATE SCHEMA IF NOT EXISTS meta")
    con_build.execute("CREATE SCHEMA IF NOT EXISTS qa")
    con_build.execute(
        """
        CREATE TABLE meta.build_runs (
            run_id BIGINT,
            started_at TIMESTAMP,
            input_path VARCHAR,
            dictionary_path VARCHAR,
            years_spec VARCHAR,
            lane_split BOOLEAN,
            exclude_vars VARCHAR,
            typed_output BOOLEAN,
            persist_duckdb BOOLEAN,
            config_json VARCHAR
        )
        """
    )
    con_build.execute(
        """
        INSERT INTO meta.build_runs VALUES
        (1, CURRENT_TIMESTAMP, 'in.parquet', 'dictionary.parquet', '2023:2023', TRUE, '', TRUE, TRUE, '{"duckdb_memory_limit":"8GB"}')
        """
    )
    con_build.execute(
        """
        CREATE TABLE qa.cast_report (
            year INTEGER,
            "column" VARCHAR,
            non_empty_tokens BIGINT,
            parsed_numeric_tokens BIGINT,
            failed_parse_tokens BIGINT
        )
        """
    )
    con_build.execute('INSERT INTO qa.cast_report (year, "column", non_empty_tokens, parsed_numeric_tokens, failed_parse_tokens) VALUES (2023, \'FTE\', 10, 9, 1)')
    con_build.close()

    con = duckdb.connect()
    manifest = query_runner.bootstrap_artifact_views(
        con,
        root=root,
        years_spec="2023:2023",
        duckdb_path=build_db_path,
    )

    assert manifest["attached_build_db"] == "true"
    assert con.execute("SELECT COUNT(*) FROM inspect.panel_clean").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM inspect.build_runs").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM inspect.release_inventory").fetchone()[0] == 1
    assert con.execute("SELECT failed_parse_tokens FROM inspect.cast_report").fetchone()[0] == 1


def query_fixture(tmp_path: Path, sql: str) -> tuple[Path, Path, Path]:
    root = tmp_path / "data"
    layout = query_runner.ensure_data_layout(root)
    write_parquet(layout.panels / "panel_clean_analysis_2023_2023.parquet",
                  [{"year": 2023, "UNITID": 100654, "CONTROL": 1}, {"year": 2023, "UNITID": 100655, "CONTROL": 2}])
    write_parquet(layout.dictionary / "dictionary_lake.parquet",
                  [{"year": 2023, "source_file": "HD", "source_table": "HD2023", "varname": "CONTROL", "varnumber": "1",
                    "varTitle": "Institution control", "longDescription": "Control classification.", "DataType": "disc"}])
    write_parquet(layout.dictionary / "dictionary_codes.parquet", [
        {"year": 2023, "source_file": "HD", "source_table": "HD2023", "varname": "CONTROL", "varnumber": "1",
         "codevalue": str(code), "valuelabel": label} for code, label in ((1, "Public"), (2, "Private"))])
    query_path = tmp_path / "query.sql"
    query_path.write_text(sql, encoding="utf-8")
    return root, query_path, tmp_path / "results"


def run_query(root: Path, query: Path, output: Path, *extra):
    return run_script("Scripts/run_saved_query.py", query, "--root", root, "--years", "2023:2023",
                      "--output-dir", output, *extra)


def test_sql_plain_projection_has_verified_labels_and_provenance(tmp_path: Path) -> None:
    root, query, output = query_fixture(tmp_path, "SELECT year, UNITID, CONTROL FROM inspect.panel_clean WHERE CONTROL = 1")
    result = run_query(root, query, output, "--format", "parquet", "--export-mode", "panel")
    assert result.returncode == 0, result.stdout
    metadata_path = next(output.glob("*/result.parquet.metadata.json"))
    metadata = json.loads(metadata_path.read_text())
    variable = next(v for v in metadata["variables"] if v["name"] == "CONTROL")
    assert variable["label"] == "Institution control"
    assert variable["value_labels"] == [{"value": "1", "label": "Public"}, {"value": "2", "label": "Private"}]
    assert variable["query_provenance"]["status"] == "verified_direct"
    assert metadata["readiness_status"] == "complete"
    assert metadata["panel_keys"] == ["UNITID", "year"]
    assert metadata["source_fingerprints"]["clean_path"]["sha256"]
    assert metadata["query_source_sha256"]
    schema = pq.read_schema(metadata_path.parent / "result.parquet")
    assert schema.field("CONTROL").metadata[b"label"] == b"Institution control"
    assert pq.read_table(metadata_path.parent / "result.parquet").num_rows == 1


def test_sql_computed_existing_name_never_inherits_raw_meaning(tmp_path: Path) -> None:
    root, query, output = query_fixture(tmp_path, "SELECT year, UNITID, CONTROL * 2 AS CONTROL FROM inspect.panel_clean")
    result = run_query(root, query, output)
    assert result.returncode == 0, result.stdout
    metadata = json.loads(next(output.glob("*/result.csv.metadata.json")).read_text())
    variable = next(v for v in metadata["variables"] if v["name"] == "CONTROL")
    assert variable["label"] == "CONTROL"
    assert variable["value_labels"] == []
    assert variable["source_metadata"] == []
    assert variable["query_provenance"]["status"] == "unverified_expression"
    assert metadata["readiness_status"] == "incomplete"
    assert "CONTROL * 2 AS CONTROL" in metadata["query_sql"]
    rejected = run_query(root, query, tmp_path / "strict", "--export-mode", "panel")
    assert rejected.returncode != 0
    assert "Metadata is incomplete" in rejected.stdout
    assert not list((tmp_path / "strict").glob("*/result.csv"))


def test_sql_renamed_keys_cannot_claim_panel_identity(tmp_path: Path) -> None:
    root, query, output = query_fixture(tmp_path, "SELECT year, CONTROL AS UNITID FROM inspect.panel_clean")
    result = run_query(root, query, output)
    assert result.returncode == 0, result.stdout
    metadata = json.loads(next(output.glob("*/result.csv.metadata.json")).read_text())
    unitid = next(v for v in metadata["variables"] if v["name"] == "UNITID")
    assert unitid["metadata_origin"] == "unverified_sql_expression"
    assert unitid["description"] == ""
    assert metadata["artifact_kind"] == "query_diagnostic"
    assert metadata["panel_keys"] == []
    assert metadata["readiness_status"] == "not_applicable"


def test_diagnostic_sql_preserves_schema_query_and_no_fake_panel_keys(tmp_path: Path) -> None:
    root, query, output = query_fixture(tmp_path, "SELECT COUNT(*) AS row_count FROM inspect.panel_clean")
    result = run_query(root, query, output)
    assert result.returncode == 0, result.stdout
    metadata_path = next(output.glob("*/result.csv.metadata.json"))
    metadata = json.loads(metadata_path.read_text())
    assert metadata["artifact_kind"] == "query_diagnostic"
    assert metadata["panel_keys"] == []
    assert metadata["readiness_status"] == "not_applicable"
    assert metadata["variables"][0]["storage_type"] == "int64"
    assert (metadata_path.parent / "query.sql").read_text().strip() == query.read_text()
    assert "diagnostic query result" in (metadata_path.parent / "result.csv.README.txt").read_text()


def test_query_runner_prefers_v2_panel_and_its_own_dictionary(tmp_path: Path) -> None:
    root, query, output = query_fixture(tmp_path, "SELECT year, UNITID, CONTROL FROM inspect.panel_clean")
    v2_panel = root / "Panels/v2/panel_clean_prch_2023_2023.parquet"
    v2_panel.parent.mkdir()
    (root / "Panels/panel_clean_analysis_2023_2023.parquet").rename(v2_panel)
    v2_dictionary = root / "Dictionary/v2"
    v2_dictionary.mkdir()
    for name in ("dictionary_lake.parquet", "dictionary_codes.parquet"):
        (root / "Dictionary" / name).rename(v2_dictionary / name)
    write_parquet(root / "Dictionary/dictionary_lake.parquet", [{"year": 2023, "varname": "CONTROL",
                  "varTitle": "Wrong legacy label", "source_file": "LEGACY"}])
    result = run_query(root, query, output, "--export-mode", "panel")
    assert result.returncode == 0, result.stdout
    metadata = json.loads(next(output.glob("*/result.csv.metadata.json")).read_text())
    variable = next(v for v in metadata["variables"] if v["name"] == "CONTROL")
    assert variable["label"] == "Institution control"
    assert metadata["source_panel"] == str(v2_panel)
    assert metadata["metadata_sources"]["dictionary"] == str(v2_dictionary / "dictionary_lake.parquet")
