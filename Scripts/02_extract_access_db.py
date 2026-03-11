#!/usr/bin/env python3
"""
Stage 02: extract each yearly Access database and inventory its tables.

Reads:
- `Raw_Access_Databases/<year>/downloads/*.zip`
- yearly `manifest.csv`

Writes:
- `Raw_Access_Databases/<year>/extracted_db/*`
- `Raw_Access_Databases/<year>/tables_csv/*`
- `Raw_Access_Databases/<year>/metadata/*`
- `Raw_Access_Databases/<year>/qc/*`
- `Checks/extract_qc/*`
"""
from __future__ import annotations

import argparse
import csv
import shutil
import zipfile
from pathlib import Path

import pandas as pd

from access_build_utils import (
    can_serve_metadata_role_from_capabilities,
    canonical_source_file,
    classify_table_role,
    compute_file_metadata,
    csv_header_and_rowcount,
    decode_tool_output,
    ensure_data_layout,
    maybe_mdb_count_binary,
    normalize_table_name,
    normalize_text_key,
    parse_years,
    require_mdb_tools,
    run_checked,
    safe_table_filename,
    table_role_capabilities,
)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=None, help="External IPEDSDB_ROOT")
    ap.add_argument("--years", default="2004:2023")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--fail-on-missing-metadata", action=argparse.BooleanOptionalAction, default=True)
    return ap.parse_args()


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def unzip_archive(zip_path: Path, extracted_dir: Path, force: bool) -> None:
    if force and extracted_dir.exists():
        shutil.rmtree(extracted_dir)
    extracted_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as archive:
        archive.extractall(extracted_dir)


def find_access_db(extracted_dir: Path) -> Path:
    candidates = sorted([*extracted_dir.rglob("*.mdb"), *extracted_dir.rglob("*.accdb")])
    if len(candidates) != 1:
        raise SystemExit(f"Expected exactly one Access DB in {extracted_dir}, found {len(candidates)}")
    return candidates[0]


def maybe_access_row_count(db_path: Path, table_name: str) -> str:
    binary = maybe_mdb_count_binary()
    if not binary:
        return ""
    try:
        result = run_checked([binary, str(db_path), table_name])
    except Exception:  # noqa: BLE001
        return ""
    return decode_tool_output(result.stdout).strip().splitlines()[0] if result.stdout else ""


def export_table(db_path: Path, table_name: str, destination: Path, force: bool) -> str:
    if destination.exists() and not force:
        return "skipped_existing"
    destination.parent.mkdir(parents=True, exist_ok=True)
    result = run_checked(["mdb-export", str(db_path), table_name])
    destination.write_text(decode_tool_output(result.stdout), encoding="utf-8", newline="\n")
    return "exported"


def main() -> None:
    args = parse_args()
    require_mdb_tools()
    layout = ensure_data_layout(args.root)
    years = parse_years(args.years)
    extract_qc_dir = layout.checks / "extract_qc"
    extract_qc_dir.mkdir(parents=True, exist_ok=True)

    global_inventory_rows: list[dict] = []
    global_failures: list[dict] = []

    for year in years:
        year_dir = layout.raw_access / str(year)
        manifest_path = year_dir / "manifest.csv"
        if not manifest_path.exists():
            raise SystemExit(f"Missing manifest for year {year}: {manifest_path}")
        manifest = pd.read_csv(manifest_path, dtype=str).fillna("")
        if manifest.empty:
            raise SystemExit(f"Empty manifest for year {year}: {manifest_path}")
        manifest_row = manifest.iloc[0].to_dict()
        zip_path = year_dir / "downloads" / manifest_row["access_filename"]
        if not zip_path.exists():
            raise SystemExit(f"Missing downloaded Access archive for year {year}: {zip_path}")

        extracted_dir = year_dir / "extracted_db"
        tables_dir = year_dir / "tables_csv"
        metadata_dir = year_dir / "metadata"
        qc_dir = year_dir / "qc"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        qc_dir.mkdir(parents=True, exist_ok=True)

        unzip_archive(zip_path, extracted_dir, args.force)
        db_path = find_access_db(extracted_dir)
        _, db_sha = compute_file_metadata(db_path)
        schema_sql = decode_tool_output(run_checked(["mdb-schema", str(db_path), "access"]).stdout)
        (metadata_dir / "table_schema.sql").write_text(schema_sql, encoding="utf-8")
        tables_output = decode_tool_output(run_checked(["mdb-tables", "-1", str(db_path)]).stdout)
        table_names = [line.strip() for line in tables_output.splitlines() if line.strip()]
        if not table_names:
            raise SystemExit(f"No tables found in Access DB for year {year}: {db_path}")

        inventory_rows: list[dict] = []
        column_rows: list[dict] = []
        row_count_rows: list[dict] = []
        failure_rows: list[dict] = []
        for table_name in table_names:
            csv_path = tables_dir / f"{safe_table_filename(table_name)}.csv"
            status = "failed"
            try:
                status = export_table(db_path, table_name, csv_path, args.force)
                header, row_count = csv_header_and_rowcount(csv_path)
                access_row_count = maybe_access_row_count(db_path, table_name)
                role = classify_table_role(table_name, header)
                capabilities = table_role_capabilities(table_name, header)
                has_unitid = any(normalize_text_key(col) == "unitid" for col in header)
                _, csv_sha = compute_file_metadata(csv_path)
                inventory_rows.append(
                    {
                        "year": year,
                        "access_db_file": db_path.name,
                        "table_name": table_name,
                        "normalized_table_name": canonical_source_file(table_name),
                        "table_role": role,
                        "csv_path": str(csv_path.relative_to(year_dir)),
                        "has_unitid": has_unitid,
                        "row_count_csv": row_count,
                        "column_count": len(header),
                        "csv_sha256": csv_sha,
                        "export_status": status,
                        "has_varnumber": bool(capabilities["has_varnumber"]),
                        "has_varname": bool(capabilities["has_varname"]),
                        "has_vartitle": bool(capabilities["has_vartitle"]),
                        "has_longdesc": bool(capabilities["has_longdesc"]),
                        "has_codevalue": bool(capabilities["has_code"]),
                        "has_valuelabel": bool(capabilities["has_label"]),
                        "has_imputation_markers": bool(capabilities["has_imputation"]),
                    }
                )
                row_count_rows.append(
                    {
                        "year": year,
                        "table_name": table_name,
                        "row_count_csv": row_count,
                        "row_count_access": access_row_count,
                        "row_count_match": str(access_row_count) == str(row_count) if access_row_count != "" else "",
                    }
                )
                for idx, col_name in enumerate(header, start=1):
                    column_rows.append(
                        {
                            "year": year,
                            "table_name": table_name,
                            "normalized_table_name": canonical_source_file(table_name),
                            "ordinal_position": idx,
                            "column_name": col_name,
                            "column_name_normalized": normalize_table_name(col_name),
                        }
                    )
                global_inventory_rows.append(inventory_rows[-1])
            except Exception as exc:  # noqa: BLE001
                failure_rows.append({"year": year, "table_name": table_name, "error": str(exc)})
                global_failures.append(failure_rows[-1])

        inventory_fields = [
            "year",
            "access_db_file",
            "table_name",
            "normalized_table_name",
            "table_role",
            "csv_path",
            "has_unitid",
            "row_count_csv",
            "column_count",
            "csv_sha256",
            "export_status",
            "has_varnumber",
            "has_varname",
            "has_vartitle",
            "has_longdesc",
            "has_codevalue",
            "has_valuelabel",
            "has_imputation_markers",
        ]
        write_csv(metadata_dir / "table_inventory.csv", inventory_rows, inventory_fields)
        write_csv(
            metadata_dir / "table_columns.csv",
            column_rows,
            [
                "year",
                "table_name",
                "normalized_table_name",
                "ordinal_position",
                "column_name",
                "column_name_normalized",
            ],
        )
        write_csv(
            metadata_dir / "row_count_report.csv",
            row_count_rows,
            ["year", "table_name", "row_count_csv", "row_count_access", "row_count_match"],
        )
        write_csv(qc_dir / "tables_with_unitid.csv", [row for row in inventory_rows if row["has_unitid"]], inventory_fields)
        write_csv(qc_dir / "zero_row_tables.csv", [row for row in inventory_rows if int(row["row_count_csv"]) == 0], inventory_fields)
        ambiguous_rows: list[dict] = []
        for role in ("metadata_varlist", "metadata_description", "metadata_codes", "metadata_imputation"):
            candidates = [row for row in inventory_rows if row["table_role"] == role]
            if len(candidates) > 1:
                ambiguous_rows.extend(candidates)
        write_csv(qc_dir / "ambiguous_metadata_tables.csv", ambiguous_rows, inventory_fields)
        write_csv(qc_dir / "extract_failures.csv", failure_rows, ["year", "table_name", "error"])

        if args.fail_on_missing_metadata:
            required_roles = {"metadata_varlist", "metadata_description", "metadata_codes"}
            found_roles = set()
            for row in inventory_rows:
                capabilities = {
                    "has_varnumber": str(row.get("has_varnumber", "")).lower() in {"true", "1"},
                    "has_varname": str(row.get("has_varname", "")).lower() in {"true", "1"},
                    "has_vartitle": str(row.get("has_vartitle", "")).lower() in {"true", "1"},
                    "has_longdesc": str(row.get("has_longdesc", "")).lower() in {"true", "1"},
                    "has_code": str(row.get("has_codevalue", "")).lower() in {"true", "1"},
                    "has_label": str(row.get("has_valuelabel", "")).lower() in {"true", "1"},
                    "has_imputation": str(row.get("has_imputation_markers", "")).lower() in {"true", "1"},
                }
                for role in required_roles:
                    if can_serve_metadata_role_from_capabilities(role, capabilities):
                        found_roles.add(role)
            missing_roles = sorted(required_roles - found_roles)
            if missing_roles:
                raise SystemExit(f"Missing required metadata table roles for year {year}: {missing_roles}")

        manifest["extracted_access_db_file"] = db_path.name
        manifest["extracted_access_db_sha256"] = db_sha
        manifest.to_csv(manifest_path, index=False)
        print(f"[year {year}] extracted Access DB: {db_path.name} tables={len(inventory_rows)}")

    write_csv(
        extract_qc_dir / "table_inventory_all_years.csv",
        global_inventory_rows,
        [
            "year",
            "access_db_file",
            "table_name",
            "normalized_table_name",
            "table_role",
            "csv_path",
            "has_unitid",
            "row_count_csv",
            "column_count",
            "csv_sha256",
            "export_status",
            "has_varnumber",
            "has_varname",
            "has_vartitle",
            "has_longdesc",
            "has_codevalue",
            "has_valuelabel",
            "has_imputation_markers",
        ],
    )
    write_csv(extract_qc_dir / "extract_failures.csv", global_failures, ["year", "table_name", "error"])
    if global_failures:
        raise SystemExit("Access extraction failures encountered; inspect extract_failures.csv")


if __name__ == "__main__":
    main()
