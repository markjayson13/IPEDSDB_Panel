#!/usr/bin/env python3
"""
Analyst utility: run a saved SQL query against the build database and standard artifacts.

Reads:
- `Queries/*.sql`
- the persisted DuckDB build database when it exists
- panel, dictionary, and release-inventory outputs under `IPEDSDB_ROOT`

Writes:
- `Checks/query_results/<timestamp>_<query_name>/result.{csv|parquet}`
- a copied `query.sql`
- a small `query_run.json` manifest and `preview.txt`

Focus:
- repeatable query results
- lightweight query history
- Data Wrangler friendly CSV exports
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import duckdb

from access_build_utils import ensure_data_layout, parse_years, repo_root
from duckdb_build_utils import copy_query_to_parquet, sql_quote, write_query_csv


def queries_root() -> Path:
    return repo_root() / "Queries"


def slugify_label(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip()).strip("_").lower()
    return text or "query"


def resolve_saved_query_path(base_dir: Path, query_arg: str) -> Path:
    candidate = Path(query_arg)
    search_paths = []
    if candidate.exists():
        search_paths.append(candidate)
    if not candidate.is_absolute():
        search_paths.extend(
            [
                base_dir / candidate,
                base_dir / f"{candidate}.sql",
            ]
        )
    for path in search_paths:
        if path.exists() and path.is_file():
            return path.resolve()
    raise FileNotFoundError(f"Unable to resolve saved query: {query_arg}")


def list_saved_queries(base_dir: Path) -> list[Path]:
    return sorted(path for path in base_dir.glob("*.sql") if path.is_file())


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_query_sql(sql: str) -> str:
    return re.sub(r";+\s*$", "", str(sql or "").strip())


def relation_exists(con: duckdb.DuckDBPyConnection, qualified_name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {qualified_name} LIMIT 1")
        return True
    except duckdb.Error:
        return False


def create_or_replace_view(con: duckdb.DuckDBPyConnection, view_name: str, query: str) -> None:
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {query}")


def create_empty_view(con: duckdb.DuckDBPyConnection, view_name: str, columns: list[tuple[str, str]]) -> None:
    select_sql = ", ".join(f"CAST(NULL AS {dtype}) AS {name}" for name, dtype in columns)
    create_or_replace_view(con, view_name, f"SELECT {select_sql} WHERE 1 = 0")


def bootstrap_artifact_views(
    con: duckdb.DuckDBPyConnection,
    *,
    root: Path,
    years_spec: str,
    duckdb_path: Path,
) -> dict[str, str]:
    layout = ensure_data_layout(root)
    years = parse_years(years_spec)
    start_year, end_year = years[0], years[-1]
    con.execute("CREATE SCHEMA IF NOT EXISTS inspect")

    attached_build_db = duckdb_path.exists()
    if attached_build_db:
        con.execute(f"ATTACH {sql_quote(str(duckdb_path))} AS build_db (READ_ONLY)")

    long_path = layout.panels / f"{start_year}-{end_year}" / f"panel_long_varnum_{start_year}_{end_year}.parquet"
    wide_path = layout.panels / f"panel_wide_analysis_{start_year}_{end_year}.parquet"
    clean_path = layout.panels / f"panel_clean_analysis_{start_year}_{end_year}.parquet"
    dict_lake_path = layout.dictionary / "dictionary_lake.parquet"
    dict_codes_path = layout.dictionary / "dictionary_codes.parquet"
    release_inventory_path = layout.checks / "download_qc" / "release_inventory.csv"

    file_views = {
        "inspect.panel_long": (
            long_path,
            [
                ("year", "INTEGER"),
                ("UNITID", "BIGINT"),
                ("varname", "VARCHAR"),
                ("value", "VARCHAR"),
                ("varnumber", "VARCHAR"),
                ("source_file", "VARCHAR"),
            ],
        ),
        "inspect.panel_wide": (
            wide_path,
            [
                ("year", "INTEGER"),
                ("UNITID", "BIGINT"),
            ],
        ),
        "inspect.panel_clean": (
            clean_path,
            [
                ("year", "INTEGER"),
                ("UNITID", "BIGINT"),
            ],
        ),
        "inspect.dictionary_lake": (
            dict_lake_path,
            [
                ("year", "INTEGER"),
                ("varnumber", "VARCHAR"),
                ("varname", "VARCHAR"),
                ("varTitle", "VARCHAR"),
                ("longDescription", "VARCHAR"),
                ("source_file", "VARCHAR"),
            ],
        ),
        "inspect.dictionary_codes": (
            dict_codes_path,
            [
                ("year", "INTEGER"),
                ("varnumber", "VARCHAR"),
                ("varname", "VARCHAR"),
                ("codevalue", "VARCHAR"),
                ("valuelabel", "VARCHAR"),
                ("source_file", "VARCHAR"),
            ],
        ),
    }

    for view_name, (path, empty_columns) in file_views.items():
        if path.exists():
            create_or_replace_view(con, view_name, f"SELECT * FROM read_parquet({sql_quote(str(path))})")
        else:
            create_empty_view(con, view_name, empty_columns)

    if release_inventory_path.exists():
        create_or_replace_view(
            con,
            "inspect.release_inventory",
            f"SELECT * FROM read_csv_auto({sql_quote(str(release_inventory_path))}, HEADER=TRUE)",
        )
    else:
        create_empty_view(
            con,
            "inspect.release_inventory",
            [
                ("year", "INTEGER"),
                ("academic_year_label", "VARCHAR"),
                ("release_type", "VARCHAR"),
                ("release_date_text", "VARCHAR"),
                ("download_status", "VARCHAR"),
            ],
        )

    db_views = {
        "inspect.build_runs": (
            "build_db.meta.build_runs",
            [
                ("run_id", "BIGINT"),
                ("started_at", "TIMESTAMP"),
                ("input_path", "VARCHAR"),
                ("dictionary_path", "VARCHAR"),
                ("years_spec", "VARCHAR"),
                ("lane_split", "BOOLEAN"),
                ("exclude_vars", "VARCHAR"),
                ("typed_output", "BOOLEAN"),
                ("persist_duckdb", "BOOLEAN"),
                ("config_json", "VARCHAR"),
            ],
        ),
        "inspect.scalar_conflicts": (
            "build_db.qa.scalar_conflicts",
            [
                ("UNITID", "BIGINT"),
                ("year", "INTEGER"),
                ("varname", "VARCHAR"),
                ("value", "VARCHAR"),
                ("varnumber", "VARCHAR"),
                ("source_file", "VARCHAR"),
                ("distinct_values", "BIGINT"),
            ],
        ),
        "inspect.cast_report": (
            "build_db.qa.cast_report",
            [
                ("year", "INTEGER"),
                ("column", "VARCHAR"),
                ("non_empty_tokens", "BIGINT"),
                ("parsed_numeric_tokens", "BIGINT"),
                ("failed_parse_tokens", "BIGINT"),
            ],
        ),
        "inspect.wide_year_summary": (
            "build_db.qa.wide_year_summary",
            [
                ("year", "INTEGER"),
                ("rows", "BIGINT"),
                ("vars", "BIGINT"),
                ("non_empty_values", "BIGINT"),
                ("fill_rate", "DOUBLE"),
                ("dup_rows", "BIGINT"),
            ],
        ),
        "inspect.target_lineage": (
            "build_db.qa.target_lineage",
            [
                ("varname", "VARCHAR"),
                ("final_in_all_targets", "BOOLEAN"),
                ("removed_as_anti_garbage", "BOOLEAN"),
            ],
        ),
    }

    for view_name, (source_name, empty_columns) in db_views.items():
        if attached_build_db and relation_exists(con, source_name):
            create_or_replace_view(con, view_name, f"SELECT * FROM {source_name}")
        else:
            create_empty_view(con, view_name, empty_columns)

    return {
        "long_path": str(long_path),
        "wide_path": str(wide_path),
        "clean_path": str(clean_path),
        "dictionary_lake_path": str(dict_lake_path),
        "dictionary_codes_path": str(dict_codes_path),
        "release_inventory_path": str(release_inventory_path),
        "duckdb_path": str(duckdb_path),
        "attached_build_db": "true" if attached_build_db else "false",
    }


def build_preview_text(con: duckdb.DuckDBPyConnection, query: str, preview_rows: int) -> str:
    preview_df = con.execute(f"SELECT * FROM ({query}) q LIMIT {int(preview_rows)}").fetchdf()
    if preview_df.empty:
        return "[preview] query returned zero rows\n"
    return preview_df.to_string(index=False) + "\n"


def parse_args() -> argparse.Namespace:
    default_root = os.environ.get("IPEDSDB_ROOT", "/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("query", nargs="?", help="Saved query name or path. Example: 01_clean_panel_rows_by_year")
    ap.add_argument("--root", default=default_root, help="External IPEDSDB_ROOT")
    ap.add_argument("--years", default="2004:2023", help='Year span used to locate standard panel outputs, e.g. "2004:2023"')
    ap.add_argument("--duckdb-path", default=None, help="Optional override for the persisted DuckDB build database")
    ap.add_argument("--output-dir", default=None, help="Where query-result run folders should be written")
    ap.add_argument("--format", choices=["csv", "parquet"], default="csv", help="Result file format")
    ap.add_argument("--name", default=None, help="Optional label override for the output folder")
    ap.add_argument("--preview-rows", type=int, default=20, help="Rows to include in preview.txt")
    ap.add_argument("--list", action="store_true", help="List saved queries and exit")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    qroot = queries_root()
    qroot.mkdir(parents=True, exist_ok=True)

    if args.list:
        for path in list_saved_queries(qroot):
            print(path.relative_to(qroot.parent))
        return
    if not args.query:
        raise SystemExit("Provide a saved query name/path, or pass --list.")

    query_path = resolve_saved_query_path(qroot, args.query)
    query_sql = normalize_query_sql(query_path.read_text(encoding="utf-8"))
    if not query_sql:
        raise SystemExit(f"Saved query is empty: {query_path}")

    layout = ensure_data_layout(args.root)
    duckdb_path = Path(args.duckdb_path) if args.duckdb_path else (layout.build / "ipedsdb_build.duckdb")
    output_dir = Path(args.output_dir) if args.output_dir else (layout.checks / "query_results")
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    query_name = slugify_label(args.name or query_path.stem)
    run_dir = output_dir / f"{stamp}_{query_name}"
    run_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    source_manifest = bootstrap_artifact_views(con, root=layout.root, years_spec=args.years, duckdb_path=duckdb_path)

    result_path = run_dir / f"result.{args.format}"
    query_copy_path = run_dir / "query.sql"
    preview_path = run_dir / "preview.txt"
    manifest_path = run_dir / "query_run.json"

    query_copy_path.write_text(query_sql + "\n", encoding="utf-8")
    row_count = int(con.execute(f"SELECT COUNT(*) FROM ({query_sql}) q").fetchone()[0])
    if args.format == "csv":
        write_query_csv(con, query_sql, str(result_path))
    else:
        copy_query_to_parquet(con, query_sql, str(result_path))
    preview_path.write_text(build_preview_text(con, query_sql, args.preview_rows), encoding="utf-8")

    manifest = {
        "generated_at": datetime.now().isoformat(),
        "query_name": query_name,
        "query_path": str(query_path),
        "query_sha256": file_sha256(query_copy_path),
        "result_format": args.format,
        "result_path": str(result_path),
        "row_count": row_count,
        "preview_path": str(preview_path),
        "years_spec": args.years,
        "root": str(layout.root),
        "output_dir": str(run_dir),
        "attached_sources": source_manifest,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"query: {query_path}")
    print(f"result: {result_path}")
    print(f"preview: {preview_path}")
    print(f"manifest: {manifest_path}")
    print(f"rows: {row_count}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
