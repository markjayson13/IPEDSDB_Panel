#!/usr/bin/env python3
"""Compare one release's freshly extracted Access SFA tables with wide/clean panels.

All inputs are read-only. Metadata table claims are checked against the complete
MDB schema; original values are matched through year-scoped value lineage.
Missing source fields are reported explicitly rather than synthesized. DuckDB
uses two threads, a bounded memory budget, and a temporary spill directory.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
from pathlib import Path
import re
import tempfile

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def canonical(value):
    """Treat source blanks as missing and compare numeric values without rounding."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        number = Decimal(text)
        if number.is_finite():
            return number
    except InvalidOperation:
        pass
    return text


def schema_tables(text: str) -> dict[str, set[str]]:
    return {name.upper(): {column.upper() for column in re.findall(r"^\s*\[([^\]]+)\]", body, re.M)}
            for name, body in re.findall(r"CREATE\s+TABLE\s+\[([^\]]+)\]\s*\((.*?)\);", text, re.S | re.I)}


def source_table_audit(metadata: list[dict], tables: dict[str, set[str]]) -> list[dict]:
    rows = []
    for record in metadata:
        declared = str(record["TableName"]).upper()
        if not declared.startswith("SFA"):
            continue
        variable = str(record["VarName"]).upper()
        targets = sorted(table for table, columns in tables.items() if variable in columns)
        physical = targets[0] if len(targets) == 1 else ""
        status = ("absent" if not targets else "ambiguous_physical_identity" if len(targets) > 1 else
                  "consistent" if declared == physical else "unique_physical_mismatch")
        rows.append({"varname": variable, "varnumber": str(int(record["VarNumber"])),
                     "declared_table": declared, "physical_table": physical,
                     "physical_candidates": "|".join(targets), "mapping_status": status,
                     "declared_imputationvar": record.get("ImputationVar", "")})
    return sorted(rows, key=lambda row: (row["varname"], row["varnumber"]))


def read_csv_records(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_csv(path: Path, rows: list[dict]) -> None:
    columns = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def number_id(value) -> str:
    return str(int(str(value)))


def run_audit(source_dir: Path, wide_path: Path, clean_path: Path, lineage_path: Path,
              actions_path: Path, manifest_path: Path, out_dir: Path, year: int = 2023) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    source_paths = {path.stem.upper(): path for path in source_dir.glob("*.csv")}
    metadata_path = next(path for path in source_dir.glob("*.csv") if path.stem.upper().startswith("VARTABLE"))
    schema_path = source_dir / "schema.sql"
    metadata = read_csv_records(metadata_path)
    tables = schema_tables(schema_path.read_text())
    audit = source_table_audit(metadata, tables)
    if not tables or not audit:
        raise ValueError("The original schema or SFA metadata is empty; no source verification is possible.")
    target_tables = sorted({row["physical_table"] for row in audit if row["physical_table"]})
    input_paths = {"wide_panel": wide_path, "clean_panel": clean_path, "value_lineage": lineage_path,
                   "prch_actions": actions_path, "prch_manifest": manifest_path,
                   "source_metadata": metadata_path, "source_schema": schema_path}
    for table in target_tables:
        if table in source_paths:
            input_paths[f"source_table:{table}"] = source_paths[table]
    database = next(iter(source_dir.glob("*.accdb")), None)
    if database:
        input_paths["original_access_database"] = database
    snapshots = {key: (path.stat().st_size, path.stat().st_mtime_ns) for key, path in input_paths.items()}
    identities = {key: {"path": str(path.resolve()), "bytes": snapshots[key][0], "mtime_ns": snapshots[key][1],
                       "sha256": hash_file(path) if key != "value_lineage" else None}
                  for key, path in input_paths.items()}
    print("[audit] source and panel hashes collected", flush=True)

    with tempfile.TemporaryDirectory(prefix="sfa-value-audit-") as temp:
        con = duckdb.connect(config={"threads": 2, "memory_limit": "256MB", "temp_directory": temp})
        try:
            # The filter/projection is applied in DuckDB before the distinct
            # metadata result crosses into Python; do not load the lineage lake.
            lineage = con.execute("""SELECT DISTINCT analysis_column, varname, source_varnumber,
                access_table_name, source_file, transformation_id, lineage_role
                FROM read_parquet(?) WHERE year = ? AND upper(access_table_name) LIKE 'SFA%'""",
                [str(lineage_path), year]).to_arrow_table().to_pylist()
            write_csv(out_dir / "value_lineage_mapping.csv", lineage)
            lookup = defaultdict(list)
            for row in lineage:
                lookup[(str(row["varname"]).upper(), number_id(row["source_varnumber"]))].append(row)
            columns = sorted({row["analysis_column"] for row in lineage})
            raw = pq.read_table(wide_path, columns=["year", "UNITID", *columns], filters=[("year", "=", year)]).to_pandas().set_index("UNITID")
            clean = pq.read_table(clean_path, columns=["year", "UNITID", *columns], filters=[("year", "=", year)]).to_pandas().set_index("UNITID")
            if raw.index.has_duplicates or clean.index.has_duplicates or raw.index.isna().any() or clean.index.isna().any():
                raise ValueError("Wide/clean panel has duplicate or missing institution-year keys.")
            if set(raw.index) != set(clean.index):
                raise ValueError("Wide and clean institution-year universes differ.")
            con.register("sfa_columns", pa.table({"column": columns}))
            actions = con.execute('''SELECT a.* FROM read_parquet(?) a
                WHERE year = ? AND "column" IN (SELECT "column" FROM sfa_columns)''',
                [str(actions_path), year]).to_arrow_table().to_pylist()
        finally:
            con.close()
    print(f"[audit] {len(lineage)} mapped columns, {len(raw)} panel rows, {len(actions)} SFA cleaning actions", flush=True)

    source_data = {}
    for table in target_tables:
        if table not in source_paths:
            continue
        frame = pd.read_csv(source_paths[table], dtype=str, keep_default_na=False)
        frame.columns = frame.columns.str.upper()
        frame["UNITID"] = frame["UNITID"].astype(int)
        if frame["UNITID"].duplicated().any():
            raise ValueError(f"Original source table has duplicate UNITID: {table}")
        source_data[table] = frame.set_index("UNITID")
    action_lookup = defaultdict(list)
    for row in actions:
        action_lookup[(int(row["UNITID"]), row["column"])].append(row)
    comparisons, mismatches = [], []
    for record in audit:
        result = dict(record)
        variable = record["varname"]
        matches = lookup[(variable, record["varnumber"])]
        result["lineage_mapping_count"] = len(matches)
        result["analysis_column"] = "|".join(sorted({row["analysis_column"] for row in matches}))
        if not record["physical_table"]:
            result["comparison_status"] = "source_field_absent" if record["mapping_status"] == "absent" else "ambiguous_physical_identity"
            comparisons.append(result)
            continue
        if len(matches) != 1 or record["physical_table"] not in source_data:
            result["comparison_status"] = "unresolved_panel_mapping"
            comparisons.append(result)
            continue
        mapping = matches[0]
        column = mapping["analysis_column"]
        result["lineage_physical_table_matches"] = str(mapping["access_table_name"]).upper() == record["physical_table"]
        result["lineage_transformation"] = mapping["transformation_id"]
        source_values = source_data[record["physical_table"]][variable].map(canonical).to_dict()
        raw_values = raw[column].map(canonical).to_dict()
        clean_values = clean[column].map(canonical).to_dict()
        source_keys, panel_keys = set(source_values), set(raw_values)
        keys = source_keys | panel_keys
        result.update({"source_rows": len(source_keys), "panel_rows": len(panel_keys),
                       "source_nonmissing": sum(value is not None for value in source_values.values()),
                       "source_missing": sum(value is None for value in source_values.values()),
                       "raw_nonmissing": sum(value is not None for value in raw_values.values()),
                       "clean_nonmissing": sum(value is not None for value in clean_values.values()),
                       "source_keys_absent_from_panel": len(source_keys - panel_keys),
                       "raw_values_outside_source_universe": sum(raw_values[key] is not None for key in panel_keys - source_keys)})
        raw_mismatch = missing_mismatch = nonmissing_match = clean_changes = explained = 0
        for key in sorted(keys):
            source_value, raw_value, clean_value = source_values.get(key), raw_values.get(key), clean_values.get(key)
            if source_value != raw_value:
                raw_mismatch += 1
                missing_mismatch += int((source_value is None) != (raw_value is None))
                mismatches.append({"year": year, "UNITID": key, "varname": variable, "analysis_column": column,
                                   "kind": "source_vs_raw", "source_value": str(source_value), "raw_value": str(raw_value), "clean_value": str(clean_value)})
            elif source_value is not None:
                nonmissing_match += 1
            if raw_value != clean_value:
                clean_changes += 1
                valid_actions = [action for action in action_lookup[(key, column)]
                                 if canonical(action["old_value"]) == raw_value
                                 and canonical(action["new_value"]) == clean_value
                                 and str(action["action"]).lower() == "null"
                                 and str(action["flag"]).upper().startswith("PRCH")]
                if valid_actions:
                    explained += 1
                else:
                    mismatches.append({"year": year, "UNITID": key, "varname": variable, "analysis_column": column,
                                       "kind": "unexplained_clean_change", "source_value": str(source_value), "raw_value": str(raw_value), "clean_value": str(clean_value)})
        result.update({"matching_nonmissing_values": nonmissing_match, "raw_source_mismatches": raw_mismatch,
                       "raw_source_missingness_mismatches": missing_mismatch,
                       "raw_to_clean_changes": clean_changes, "changes_explained_by_prch_actions": explained,
                       "unexplained_clean_changes": clean_changes - explained})
        result["comparison_status"] = ("pass" if raw_mismatch == 0 and clean_changes == explained
                                       and not result["source_keys_absent_from_panel"]
                                       and result["lineage_physical_table_matches"] else "fail")
        comparisons.append(result)

    write_csv(out_dir / "affected_variable_comparison.csv", comparisons)
    write_csv(out_dir / "mismatches.csv", mismatches or [{"kind": "none", "message": "No source/value or unexplained cleaning mismatches."}])
    write_csv(out_dir / "relevant_prch_actions.csv", actions or [{"action": "none", "message": "No PRCH actions for mapped SFA columns in the selected year."}])
    manifest = json.loads(manifest_path.read_text())
    expected_artifacts = {row["name"]: row for row in manifest.get("artifacts", [])}
    clean_manifest_match = expected_artifacts.get("panel_clean.parquet", {}).get("sha256") == identities["clean_panel"]["sha256"]
    actions_manifest_match = expected_artifacts.get(actions_path.name, {}).get("sha256") == identities["prch_actions"]["sha256"]
    readme_path = source_dir / "release_readme.txt"
    readme_text = re.sub(r"\s+", " ", readme_path.read_text()) if readme_path.exists() else ""
    flag_evidence = [match.group(0) for match in re.finditer(r"[^.]{0,150}imputation[^.]{0,220}\.", readme_text, re.I)]
    flags = sorted(name for columns in tables.values() for name in columns if name.startswith("XUPGRNT"))
    direction_counts = Counter(f"{row['declared_table']}->{row['physical_table']}" for row in audit if row["mapping_status"] == "unique_physical_mismatch")
    summary = {"created_utc": datetime.now(timezone.utc).isoformat(), "year": year,
               "audit_script_sha256": hash_file(Path(__file__)),
               "source_identity_basis": "Fresh Access CSV exports and complete mdb-schema output; variable names checked against all physical tables.",
               "inputs": identities, "mapping_status_counts": dict(Counter(row["mapping_status"] for row in audit)),
               "mismatch_directions": dict(direction_counts), "comparison_status_counts": dict(Counter(row["comparison_status"] for row in comparisons)),
               "compared_variables": sum(row["comparison_status"] in {"pass", "fail"} for row in comparisons),
               "matching_nonmissing_values": sum(row.get("matching_nonmissing_values", 0) for row in comparisons),
               "raw_source_mismatches": sum(row.get("raw_source_mismatches", 0) for row in comparisons),
               "missingness_mismatches": sum(row.get("raw_source_missingness_mismatches", 0) for row in comparisons),
               "raw_to_clean_changes": sum(row.get("raw_to_clean_changes", 0) for row in comparisons),
               "unexplained_clean_changes": sum(row.get("unexplained_clean_changes", 0) for row in comparisons),
               "prch_actions_in_scope": len(actions), "prch_manifest_status": manifest.get("status"),
               "clean_hash_matches_prch_manifest": clean_manifest_match,
               "actions_hash_matches_prch_manifest": actions_manifest_match,
               "pell": {row["varname"]: row for row in comparisons if row["varname"] in {"UPGRNTN", "UPGRNTP", "UPGRNTT", "UPGRNTA"}},
               "absent_source_fields": [row for row in comparisons if row["mapping_status"] == "absent"],
               "physical_xupgrnt_columns": flags, "readme_imputation_evidence": flag_evidence,
               "limitations": ["Value-lineage file content hash is not recomputed; its path, bytes, timestamp stability and bounded projected mappings are recorded.",
                               "Native Access extraction itself is an upstream input step; this audit reads the fresh exported tables and schema."]}
    for key, path in input_paths.items():
        if (path.stat().st_size, path.stat().st_mtime_ns) != snapshots[key]:
            raise ValueError(f"Input changed while auditing: {path}")
    summary["input_timestamps_stable"] = True
    incomplete = sum(count for status, count in summary["comparison_status_counts"].items()
                     if status not in {"pass", "source_field_absent"})
    summary["present_source_fields_verified"] = (summary["compared_variables"] > 0 and incomplete == 0
                                                  and clean_manifest_match and actions_manifest_match)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({key: summary[key] for key in ("mapping_status_counts", "mismatch_directions", "comparison_status_counts", "raw_source_mismatches", "missingness_mismatches", "raw_to_clean_changes", "unexplained_clean_changes", "clean_hash_matches_prch_manifest")}, indent=2), flush=True)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source-dir", "wide", "clean", "value-lineage", "prch-actions", "prch-manifest", "out-dir"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--year", type=int, default=2023)
    args = parser.parse_args()
    summary = run_audit(args.source_dir, args.wide, args.clean, args.value_lineage,
                        args.prch_actions, args.prch_manifest, args.out_dir, args.year)
    if not summary["present_source_fields_verified"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
