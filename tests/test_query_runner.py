"""
Tests for the saved-query runner and its inspection-view bootstrap.

Focus:
- saved-query discovery
- non-invasive DuckDB inspection bootstrap over standard outputs
"""
from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from helpers import load_script_module


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
            column VARCHAR,
            non_empty_tokens BIGINT,
            parsed_numeric_tokens BIGINT,
            failed_parse_tokens BIGINT
        )
        """
    )
    con_build.execute("INSERT INTO qa.cast_report VALUES (2023, 'FTE', 10, 9, 1)")
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
