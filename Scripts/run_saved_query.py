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
from datetime import datetime, timezone
from pathlib import Path
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from access_build_utils import DEFAULT_IPEDSDB_ROOT, ensure_data_layout, parse_years, repo_root
from duckdb_build_utils import copy_query_to_parquet, sql_quote
from export_metadata import build_export_metadata, discover_export_metadata
from export_integrity import (apply_observation_validation, assert_source_unchanged, export_code_provenance,
                              require_export_ready, scan_panel, source_fingerprint, validate_observed_codes)
from panel_export import prepare_format_metadata, write_excel, write_sidecars, write_stata, write_stream


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
    v2_clean = layout.panels / "v2" / f"panel_clean_prch_{start_year}_{end_year}.parquet"
    v2_wide = layout.panels / "v2" / f"panel_wide_analysis_{start_year}_{end_year}.parquet"
    versioned = v2_clean.is_file() or v2_wide.is_file()
    if versioned:
        clean_path, wide_path = v2_clean, v2_wide
        long_path = layout.panels / "v2" / f"panel_long_scalar_{start_year}_{end_year}.parquet"
        dict_lake_path = layout.dictionary / "v2/dictionary_lake.parquet"
        dict_codes_path = layout.dictionary / "v2/dictionary_codes.parquet"
    lineage_path = discover_export_metadata(None, "Checks/wide_qc/qc_column_lineage.csv", clean_path, root)
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
        "column_lineage_path": str(lineage_path) if lineage_path else "",
        "panel_layout": "v2" if versioned else "legacy",
        "release_inventory_path": str(release_inventory_path),
        "duckdb_path": str(duckdb_path),
        "attached_build_db": "true" if attached_build_db else "false",
    }


def verified_projection(con, query: str, result_schema: pa.Schema, sources: dict) -> tuple[Path | None, dict[str, str], dict]:
    """Prove plain field identity from DuckDB's parser, never from output names alone.

    Joins, CTEs, set operations, aliases, aggregation, and star rewrites are
    intentionally outside this proof. They need an explicit metadata definition.
    """
    try:
        parsed = json.loads(con.execute("SELECT json_serialize_sql(?)", [query]).fetchone()[0])
        statements = parsed.get("statements", [])
        if parsed.get("error") or len(statements) != 1:
            return None, {}, {"status": "unverified", "reason": "SQL parse unavailable"}
        node = statements[0]["node"]
    except (duckdb.Error, ValueError, KeyError, TypeError):
        return None, {}, {"status": "unverified", "reason": "SQL parser unavailable"}
    source = node.get("from_table", {})
    eligible = (node.get("type") == "SELECT_NODE" and source.get("type") == "BASE_TABLE"
                and source.get("schema_name", "").lower() == "inspect"
                and not source.get("catalog_name") and not source.get("column_name_alias")
                and not node.get("cte_map", {}).get("map") and not node.get("group_expressions")
                and not node.get("group_sets") and not node.get("having") and not node.get("qualify")
                and node.get("aggregate_handling") == "STANDARD_HANDLING")
    source_key = {"panel_clean": "clean_path", "panel_wide": "wide_path"}.get(source.get("table_name", "").lower())
    if not eligible or not source_key:
        return None, {}, {"status": "unverified", "reason": "Query is not a direct standard-panel projection", "parsed_select": node.get("select_list", [])}
    source_path = Path(sources[source_key])
    if not source_path.is_file():
        return None, {}, {"status": "unverified", "reason": "Source panel unavailable"}
    source_schema = pq.read_schema(source_path)
    source_names = {name.upper(): name for name in source_schema.names}
    if len(source_names) != len(source_schema) or len({name.upper() for name in result_schema.names}) != len(result_schema):
        return source_path, {}, {"status": "unverified", "reason": "Case-ambiguous field names require explicit metadata"}
    expressions = node.get("select_list", [])
    expanded = []
    for expr in expressions:
        if expr.get("class") == "STAR":
            if any(expr.get(key) for key in ("exclude_list", "qualified_exclude_list", "replace_list", "rename_list", "columns", "expr")):
                return source_path, {}, {"status": "unverified", "reason": "Star transformation requires explicit metadata", "parsed_select": expressions}
            expanded.extend({"class": "COLUMN_REF", "column_names": [name], "alias": ""} for name in source_schema.names)
        else:
            expanded.append(expr)
    if len(expanded) != len(result_schema):
        return source_path, {}, {"status": "unverified", "reason": "Projection layout could not be proven", "parsed_select": expressions}
    verified = {}
    field_provenance = {}
    for field, expr in zip(result_schema, expanded):
        names = expr.get("column_names", [])
        source_name = source_names.get(str(names[-1]).upper()) if names else None
        direct = expr.get("class") == "COLUMN_REF" and not expr.get("alias") and source_name and field.name.upper() == source_name.upper()
        if direct:
            verified[field.name] = source_name
        field_provenance[field.name] = {"status": "verified_direct" if direct else "unverified_expression",
                                       "source_column": source_name if direct else None, "expression": expr}
    return source_path, verified, {"status": "verified" if len(verified) == len(result_schema) else "partial",
                                  "source_relation": f"inspect.{source['table_name']}", "fields": field_provenance}


def query_export_metadata(con, query: str, dataset, sources: dict, *, root: Path, mode: str) -> tuple[dict, dict]:
    schema = dataset.schema
    source_path, verified, provenance = verified_projection(con, query, schema, sources)
    source_schema = pq.read_schema(source_path) if source_path else None
    dictionary = Path(sources["dictionary_lake_path"])
    codes = Path(sources["dictionary_codes_path"])
    lineage = discover_export_metadata(None, "Checks/wide_qc/qc_column_lineage.csv",
                                       source_path or Path(sources["clean_path"]), root)
    paths = {"dictionary": dictionary if dictionary.is_file() else None,
             "codes": codes if codes.is_file() else None, "lineage": lineage}
    verified_keys = {name.upper(): name for name in verified if name.upper() in {"UNITID", "YEAR"}}
    panel_mode = mode != "diagnostic" and set(verified_keys) == {"UNITID", "YEAR"}
    if mode == "panel" and not panel_mode:
        raise ValueError("Panel export requires verified, unrenamed UNITID and year projections from a standard panel.")
    if panel_mode:
        scan = scan_panel(dataset, schema.names, panel_keys=(verified_keys["UNITID"], verified_keys["YEAR"]))
        years = scan["years"]
    else:
        row_count = 0
        null_counts = {name: 0 for name in schema.names}
        for batch in dataset.to_batches(batch_size=100000):
            row_count += batch.num_rows
            for name, column in zip(schema.names, batch.columns):
                null_counts[name] += column.null_count
        scan = {"row_count": row_count, "years": [], "null_counts": null_counts}
        years = []
        # A direct year may scope individual definitions even in a diagnostic
        # result, but it does not establish an institution-year panel.
        if "YEAR" in verified_keys:
            year_values = dataset.to_table(columns=[verified_keys["YEAR"]]).column(0).to_pylist()
            scoped_years = set()
            for value in year_values:
                try:
                    if value is not None and not isinstance(value, bool) and int(value) == value and int(value) > 0:
                        scoped_years.add(int(value))
                except (TypeError, ValueError, OverflowError):
                    continue
            years = sorted(scoped_years)
    fields, renamed = [], {}
    for index, field in enumerate(schema):
        if field.name in verified:
            original = source_schema.field(verified[field.name])
            fields.append(field.with_metadata(original.metadata))
        else:
            temporary_name = f"__sql_expression_{index}"
            fields.append(pa.field(temporary_name, field.type))
            renamed[temporary_name] = field.name
    metadata = build_export_metadata(pa.schema(fields), years, paths["dictionary"], paths["codes"], paths["lineage"])
    for variable in metadata["variables"]:
        original_name = variable["name"]
        if original_name in renamed:
            variable.update({"name": renamed[original_name], "label": renamed[original_name], "description": "",
                             "metadata_status": "incomplete", "metadata_origin": "unverified_sql_expression",
                             "comparability_status": "unknown", "source_metadata": [], "value_labels": [],
                             "value_label_records": [], "resolved_value_label_records": [], "lineage_records": [],
                             "imputation_parent_metadata": [], "code_identity_candidates": [],
                             "metadata_availability": {"dictionary": False, "codes": False}})
            variable["semantic_metadata"] = {
                key: {"status": "unknown", "value": None, "values": [], "missing_source_records": 0}
                for key in variable.get("semantic_metadata", {})
            }
            variable["unknown_semantic_fields"] = list(variable["semantic_metadata"])
            metadata["issues"].append({"severity": "warning", "code": "sql_expression_metadata_unknown",
                                       "variable": renamed[original_name],
                                       "message": "Computed, renamed, or unverified SQL output requires an explicit metadata definition; raw-variable meaning was not inherited."})
        variable["query_provenance"] = provenance.get("fields", {}).get(variable["name"], {"status": "unverified_expression"})
        variable["null_count"] = scan["null_counts"][variable["name"]]
    for issue in metadata["issues"]:
        issue["variable"] = renamed.get(issue.get("variable"), issue.get("variable"))
    metadata.update({"schema_version": "1.1", "artifact_kind": "panel_extract" if panel_mode else "query_diagnostic",
                     "query_provenance": provenance, "query_sql": query,
                     "source_panel": str(source_path.resolve()) if source_path else None,
                     "created_utc": datetime.now(timezone.utc).isoformat(),
                     "row_count": scan["row_count"], "column_count": len(schema),
                     "panel_keys": [verified_keys["UNITID"], verified_keys["YEAR"]] if panel_mode else [],
                     "metadata_sources": {key: str(path.resolve()) if path else None for key, path in paths.items()},
                     "missing_values": "SQL result nulls remain missing. SQL may transform observations; consult query.sql and per-field provenance.",
                     "export_code": export_code_provenance()})
    if renamed:
        metadata["metadata_status"] = "incomplete"
    if panel_mode:
        checks = validate_observed_codes(dataset, metadata, year_col=verified_keys["YEAR"])
        apply_observation_validation(metadata, scan, checks)
    else:
        metadata["observation_validation"] = {"status": "not_applicable", "reason": "Diagnostic SQL result; no panel key or analyst-readiness claim."}
        metadata["readiness_status"] = "not_applicable"
    return metadata, paths


def build_preview_text(con: duckdb.DuckDBPyConnection, query: str, preview_rows: int) -> str:
    preview_df = con.execute(f"SELECT * FROM ({query}) q LIMIT {int(preview_rows)}").fetchdf()
    if preview_df.empty:
        return "[preview] query returned zero rows\n"
    return preview_df.to_string(index=False) + "\n"


def parse_args() -> argparse.Namespace:
    default_root = os.environ.get("IPEDSDB_ROOT", str(DEFAULT_IPEDSDB_ROOT))
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("query", nargs="?", help="Saved query name or path. Example: 01_clean_panel_rows_by_year")
    ap.add_argument("--root", default=default_root, help="External IPEDSDB_ROOT")
    ap.add_argument("--years", default="2004:2023", help='Year span used to locate standard panel outputs, e.g. "2004:2023"')
    ap.add_argument("--duckdb-path", default=None, help="Optional override for the persisted DuckDB build database")
    ap.add_argument("--output-dir", default=None, help="Where query-result run folders should be written")
    ap.add_argument("--format", choices=["csv", "parquet", "dta", "xlsx"], default="csv", help="Result file format")
    ap.add_argument("--export-mode", choices=["auto", "panel", "diagnostic"], default="auto",
                    help="Auto recognizes direct panel keys; panel enforces analyst readiness; diagnostic makes no panel claim")
    ap.add_argument("--require-ready", action="store_true", help="Require full metadata and panel observation validation")
    ap.add_argument("--allow-incomplete-metadata", action="store_true", help="Explicit draft escape for --export-mode panel")
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
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    query_name = slugify_label(args.name or query_path.stem)
    run_dir = output_dir / f"{stamp}_{query_name}"
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        source_manifest = bootstrap_artifact_views(con, root=layout.root, years_spec=args.years, duckdb_path=duckdb_path)
        source_paths = {name: Path(path) for name, path in source_manifest.items()
                        if name.endswith("_path") and path and Path(path).exists()}
        source_paths["query_path"] = query_path
        fingerprints = {name: source_fingerprint(path) for name, path in source_paths.items()}
        with tempfile.TemporaryDirectory(prefix=".ipeds-query-", dir=output_dir) as staging:
            staging_path = Path(staging)
            intermediate = staging_path / "query_materialized.parquet"
            copy_query_to_parquet(con, query_sql, str(intermediate))
            dataset = ds.dataset(intermediate, format="parquet")
            metadata, _ = query_export_metadata(con, query_sql, dataset, source_manifest,
                                                root=layout.root, mode=args.export_mode)
            metadata.update({"format": args.format, "source_fingerprints": fingerprints,
                             "query_source_sha256": fingerprints["query_path"]["sha256"]})
            prepare_format_metadata(metadata, dataset.schema, args.format)
            if args.require_ready or (args.export_mode == "panel" and not args.allow_incomplete_metadata):
                require_export_ready(metadata)
            staged_result = staging_path / f"result.{args.format}"
            if args.format in {"csv", "parquet"}:
                write_stream(dataset, dataset.schema.names, None, 100000, staged_result, args.format, dataset.schema, metadata)
            elif args.format == "xlsx":
                write_excel(dataset, dataset.schema.names, None, 100000, staged_result, metadata)
            else:
                write_stata(dataset.to_table(), staged_result, metadata)
            write_sidecars(staged_result, metadata)
            query_copy = staging_path / "query.sql"
            query_copy.write_text(query_sql + "\n", encoding="utf-8")
            # Preview the same materialized observations that are exported.
            preview = con.execute(f"SELECT * FROM read_parquet({sql_quote(str(intermediate))}) LIMIT {max(0, args.preview_rows)}").fetchdf()
            (staging_path / "preview.txt").write_text(preview.to_string(index=False) + "\n" if not preview.empty else "[preview] query returned zero rows\n", encoding="utf-8")
            result_path = run_dir / staged_result.name
            manifest = {
                "generated_at": datetime.now(timezone.utc).isoformat(), "query_name": query_name,
                "query_path": str(query_path), "query_sha256": file_sha256(query_copy),
                "result_format": args.format, "result_path": str(result_path),
                "row_count": metadata["row_count"], "preview_path": str(run_dir / "preview.txt"),
                "years_spec": args.years, "root": str(layout.root), "output_dir": str(run_dir),
                "attached_sources": source_manifest, "source_fingerprints": fingerprints,
                "metadata_status": metadata["metadata_status"], "readiness_status": metadata["readiness_status"],
                "artifact_kind": metadata["artifact_kind"], "data_sha256": metadata["data_sha256"],
            }
            (staging_path / "query_run.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            for name, path in source_paths.items():
                assert_source_unchanged(path, fingerprints[name])
            intermediate.unlink()
            # Publish the entire run directory together; no successful-looking
            # partial result folder is left behind after validation failures.
            staging_path.rename(run_dir)
        print(f"query: {query_path}")
        print(f"result: {result_path}")
        print(f"manifest: {run_dir / 'query_run.json'}")
        print(f"rows: {metadata['row_count']}; readiness={metadata['readiness_status']}")
    finally:
        con.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
