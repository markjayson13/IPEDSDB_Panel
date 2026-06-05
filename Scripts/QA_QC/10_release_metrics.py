#!/usr/bin/env python3
"""
QA 10: compute compact release-validation metrics for manuscript and archive use.

Reads:
- generated `IPEDSDB_ROOT` artifacts

Writes:
- `release_inventory_summary.csv`
- `extract_inventory_summary.csv`
- `dictionary_summary.csv`
- `panel_file_summary.csv`
- `prch_summary.csv`
- `panel_structure_metrics.csv`
- `wide_qc_summary.csv`
- `table_release_validation_metrics_filled.csv`

This script is intentionally a reporter, not a gate. The release gate remains
`08_acceptance_audit.py`.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from access_build_utils import DEFAULT_IPEDSDB_ROOT


def parse_years(spec: str) -> list[int]:
    if ":" in spec:
        start, end = spec.split(":", maxsplit=1)
        return list(range(int(start), int(end) + 1))
    return [int(x) for x in spec.split(",") if x.strip()]


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def parquet_rows_cols(path: Path) -> tuple[int | None, int | None]:
    if not path.exists():
        return None, None
    meta = pq.ParquetFile(path).metadata
    return int(meta.num_rows), int(meta.num_columns)


def parquet_row_count(path: Path) -> int | None:
    rows, _ = parquet_rows_cols(path)
    return rows


def file_sha256(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def first_present(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def summarize_release_inventory(root: Path, years: list[int]) -> pd.DataFrame:
    path = root / "Checks" / "download_qc" / "release_inventory.csv"
    df = read_csv(path)
    if df.empty:
        return pd.DataFrame([{"artifact": str(path), "present": False}])
    year_col = first_present(df, ["year", "collection_year", "data_year"])
    release_col = first_present(df, ["release", "release_type", "release_norm", "status"])
    if year_col:
        df = df[pd.to_numeric(df[year_col], errors="coerce").isin(years)].copy()
    final_count = None
    if release_col:
        release_norm = df[release_col].fillna("").astype(str).str.lower()
        final_count = int(release_norm.str.contains("final").sum())
    return pd.DataFrame(
        [
            {
                "artifact": str(path),
                "present": True,
                "rows": int(len(df)),
                "years_requested": len(years),
                "years_observed": int(pd.to_numeric(df[year_col], errors="coerce").nunique()) if year_col else None,
                "final_release_rows": final_count,
            }
        ]
    )


def summarize_extract_inventory(root: Path, years: list[int]) -> pd.DataFrame:
    path = root / "Checks" / "extract_qc" / "table_inventory_all_years.csv"
    df = read_csv(path)
    if df.empty:
        return pd.DataFrame([{"artifact": str(path), "present": False}])
    if "year" in df.columns:
        df = df[pd.to_numeric(df["year"], errors="coerce").isin(years)].copy()
    has_unitid_col = first_present(df, ["has_unitid", "contains_unitid"])
    with_unitid = None
    if has_unitid_col:
        with_unitid = int(df[has_unitid_col].astype(str).str.lower().isin({"true", "1", "yes"}).sum())
    return pd.DataFrame(
        [
            {
                "artifact": str(path),
                "present": True,
                "tables_exported": int(len(df)),
                "tables_with_unitid": with_unitid,
            }
        ]
    )


def summarize_dictionary(root: Path, years: list[int]) -> pd.DataFrame:
    dict_path = root / "Dictionary" / "dictionary_lake.parquet"
    codes_path = root / "Dictionary" / "dictionary_codes.parquet"
    dict_rows = parquet_row_count(dict_path)
    codes_rows = parquet_row_count(codes_path)
    unique_vars = None
    source_files = None
    if dict_path.exists():
        cols = [c for c in ["year", "varname", "source_file"] if c in pq.ParquetFile(dict_path).schema.names]
        df = pd.read_parquet(dict_path, columns=cols)
        if "year" in df.columns:
            df = df[pd.to_numeric(df["year"], errors="coerce").isin(years)].copy()
        if "varname" in df.columns:
            unique_vars = int(df["varname"].dropna().astype(str).str.upper().nunique())
        if "source_file" in df.columns:
            source_files = int(df["source_file"].dropna().astype(str).str.upper().nunique())
    return pd.DataFrame(
        [
            {
                "dictionary_artifact": str(dict_path),
                "dictionary_present": dict_path.exists(),
                "dictionary_rows": dict_rows,
                "dictionary_code_label_rows": codes_rows,
                "unique_varnames": unique_vars,
                "unique_source_files": source_files,
            }
        ]
    )


def summarize_panel_files(root: Path, years: list[int]) -> pd.DataFrame:
    start, end = years[0], years[-1]
    files = {
        "long_panel": root / "Panels" / f"{start}-{end}" / f"panel_long_varnum_{start}_{end}.parquet",
        "wide_panel": root / "Panels" / f"panel_wide_analysis_{start}_{end}.parquet",
        "clean_panel": root / "Panels" / f"panel_clean_analysis_{start}_{end}.parquet",
    }
    rows = []
    for label, path in files.items():
        n_rows, n_cols = parquet_rows_cols(path)
        rows.append(
            {
                "panel": label,
                "path": str(path),
                "present": path.exists(),
                "rows": n_rows,
                "columns": n_cols,
                "sha256": file_sha256(path),
            }
        )
    return pd.DataFrame(rows)


def summarize_prch(root: Path) -> pd.DataFrame:
    path = root / "Checks" / "prch_qc" / "prch_clean_summary.csv"
    df = read_csv(path)
    if df.empty:
        return pd.DataFrame([{"artifact": str(path), "present": False}])
    child_col = first_present(df, ["child_rows_cleaned", "child_rows"])
    review_col = first_present(df, ["review_only_rows"])
    target_col = first_present(df, ["target_columns"])
    return pd.DataFrame(
        [
            {
                "artifact": str(path),
                "present": True,
                "rows": int(len(df)),
                "child_rows_cleaned": int(pd.to_numeric(df[child_col], errors="coerce").fillna(0).sum()) if child_col else None,
                "review_only_rows": int(pd.to_numeric(df[review_col], errors="coerce").fillna(0).sum()) if review_col else None,
                "flag_years_with_targets": int((pd.to_numeric(df[target_col], errors="coerce").fillna(0) > 0).sum()) if target_col else None,
            }
        ]
    )


def summarize_wide_qc(root: Path) -> pd.DataFrame:
    rows = []
    for name in ["qc_target_lineage.csv", "qc_column_lineage.csv", "qc_scalar_conflicts.csv", "qc_anti_garbage_failures.csv", "qc_cast_report.csv"]:
        path = root / "Checks" / "wide_qc" / name
        df = read_csv(path)
        rows.append({"artifact": str(path), "name": name, "present": path.exists(), "rows": int(len(df)) if not df.empty else 0 if path.exists() else None})
    return pd.DataFrame(rows)


def summarize_panel_structure(root: Path) -> pd.DataFrame:
    files = [
        root / "Checks" / "panel_qc" / "panel_structure_summary.csv",
        root / "Checks" / "panel_qc" / "identifier_linkage_summary.csv",
        root / "Checks" / "panel_qc" / "classification_stability_summary.csv",
        root / "Checks" / "panel_qc" / "finance_comparability_summary.csv",
    ]
    rows = []
    for path in files:
        df = read_csv(path)
        rows.append({"artifact": str(path), "present": path.exists(), "rows": int(len(df)) if not df.empty else None})
    return pd.DataFrame(rows)


def acceptance_pass_count(root: Path) -> tuple[int | None, int | None]:
    path = root / "Checks" / "acceptance_qc" / "acceptance_summary.csv"
    df = read_csv(path)
    if df.empty:
        return None, None
    cols = {c.lower(): c for c in df.columns}
    passed_col = first_present(df, ["passed", "pass", "ok"])
    required_col = first_present(df, ["required"])
    if passed_col:
        passed = int(df[passed_col].astype(str).str.lower().isin({"true", "1", "yes", "pass", "passed"}).sum())
        total = int(len(df[df[required_col].astype(str).str.lower().isin({"true", "1", "yes"})])) if required_col else int(len(df))
        return passed, total
    if "status" in cols:
        col = cols["status"]
        passed = int(df[col].astype(str).str.lower().isin({"pass", "passed", "ok"}).sum())
        return passed, int(len(df))
    return None, int(len(df))


def build_validation_table(
    release_df: pd.DataFrame,
    extract_df: pd.DataFrame,
    dict_df: pd.DataFrame,
    panel_df: pd.DataFrame,
    prch_df: pd.DataFrame,
    wide_qc_df: pd.DataFrame,
    structure_df: pd.DataFrame,
    root: Path,
) -> pd.DataFrame:
    panel_lookup = {row["panel"]: row for row in panel_df.to_dict("records")}
    wide = panel_lookup.get("wide_panel", {})
    clean = panel_lookup.get("clean_panel", {})
    long = panel_lookup.get("long_panel", {})
    accepted, acceptance_total = acceptance_pass_count(root)
    raw_clean_delta = None
    if wide.get("rows") is not None and clean.get("rows") is not None:
        raw_clean_delta = int(clean["rows"]) - int(wide["rows"])
    structure_present = int(structure_df["present"].sum()) if not structure_df.empty and "present" in structure_df else None
    wide_qc_rows = {row["name"]: row["rows"] for row in wide_qc_df.to_dict("records")} if not wide_qc_df.empty else {}

    rows = [
        ("1. Release-stage validation", "Years included", release_df.get("years_observed", pd.Series([None])).iat[0], "years", "Checks/download_qc/release_inventory.csv", "Canonical release window"),
        ("1. Release-stage validation", "Final releases included", release_df.get("final_release_rows", pd.Series([None])).iat[0], "releases", "Checks/download_qc/release_inventory.csv", "Final-only default"),
        ("2. Access extraction", "Access tables exported", extract_df.get("tables_exported", pd.Series([None])).iat[0], "tables", "Checks/extract_qc/table_inventory_all_years.csv", "All extracted tables across years"),
        ("2. Access extraction", "Tables with UNITID", extract_df.get("tables_with_unitid", pd.Series([None])).iat[0], "tables", "Checks/extract_qc/table_inventory_all_years.csv", "Tables eligible for harmonization"),
        ("3. Dictionary coverage", "Dictionary rows", dict_df.get("dictionary_rows", pd.Series([None])).iat[0], "rows", "Dictionary/dictionary_lake.parquet", "Variable-year metadata rows"),
        ("3. Dictionary coverage", "Dictionary code-label rows", dict_df.get("dictionary_code_label_rows", pd.Series([None])).iat[0], "rows", "Dictionary/dictionary_codes.parquet", "Code-label rows"),
        ("3. Dictionary coverage", "Unique varnames", dict_df.get("unique_varnames", pd.Series([None])).iat[0], "variables", "Dictionary/dictionary_lake.parquet", "Distinct output metadata names"),
        ("4. Long-panel integrity", "Long-panel rows", long.get("rows"), "rows", "Panels/2004-2023/panel_long_varnum_2004_2023.parquet", "Stitched long panel"),
        ("5. Wide-build contract", "Wide-panel rows", wide.get("rows"), "rows", "Panels/panel_wide_analysis_2004_2023.parquet", "One row per UNITID-year"),
        ("5. Wide-build contract", "Wide-panel columns", wide.get("columns"), "columns", "Panels/panel_wide_analysis_2004_2023.parquet", "Analysis schema width"),
        ("5. Wide-build contract", "Scalar conflict rows", wide_qc_rows.get("qc_scalar_conflicts.csv"), "rows", "Checks/wide_qc/qc_scalar_conflicts.csv", "Should be zero or documented"),
        ("5. Wide-build contract", "Target lineage rows", wide_qc_rows.get("qc_target_lineage.csv"), "rows", "Checks/wide_qc/qc_target_lineage.csv", "Column target transformation evidence"),
        ("5. Wide-build contract", "Column lineage rows", wide_qc_rows.get("qc_column_lineage.csv"), "rows", "Checks/wide_qc/qc_column_lineage.csv", "Source lineage consumed by PRCH cleaning"),
        ("6. PRCH cleaning", "Raw vs clean row delta", raw_clean_delta, "rows", "Wide and clean panel parquet", "Should be zero"),
        ("6. PRCH cleaning", "Child rows cleaned", prch_df.get("child_rows_cleaned", pd.Series([None])).iat[0], "rows", "Checks/prch_qc/prch_clean_summary.csv", "Sum across observed PRCH flags"),
        ("7. Panel structure", "Structure diagnostic files present", structure_present, "files", "Checks/panel_qc", "Panel structure/linkage/comparability diagnostics"),
        ("8. Release reproducibility", "Acceptance checks passed", accepted, "checks", "Checks/acceptance_qc/acceptance_summary.csv", f"Total checks: {acceptance_total}"),
        ("8. Release reproducibility", "Final clean panel SHA-256", clean.get("sha256", ""), "hash", "Panels/panel_clean_analysis_2004_2023.parquet", "For archive manifest"),
    ]
    return pd.DataFrame(rows, columns=["section", "metric", "value", "units", "source", "notes"])


def main() -> None:
    default_root = Path(os.environ.get("IPEDSDB_ROOT", str(DEFAULT_IPEDSDB_ROOT)))
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", default=str(default_root), help="External IPEDSDB_ROOT")
    p.add_argument("--years", default="2004:2023")
    p.add_argument("--out-dir", required=True)
    args = p.parse_args()

    root = Path(args.root).expanduser()
    out_dir = Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    years = parse_years(args.years)

    release_df = summarize_release_inventory(root, years)
    extract_df = summarize_extract_inventory(root, years)
    dict_df = summarize_dictionary(root, years)
    panel_df = summarize_panel_files(root, years)
    prch_df = summarize_prch(root)
    wide_qc_df = summarize_wide_qc(root)
    structure_df = summarize_panel_structure(root)

    outputs = {
        "release_inventory_summary.csv": release_df,
        "extract_inventory_summary.csv": extract_df,
        "dictionary_summary.csv": dict_df,
        "panel_file_summary.csv": panel_df,
        "prch_summary.csv": prch_df,
        "wide_qc_summary.csv": wide_qc_df,
        "panel_structure_metrics.csv": structure_df,
    }
    for name, df in outputs.items():
        df.to_csv(out_dir / name, index=False)

    table = build_validation_table(release_df, extract_df, dict_df, panel_df, prch_df, wide_qc_df, structure_df, root)
    table.to_csv(out_dir / "table_release_validation_metrics_filled.csv", index=False)
    print(f"Wrote release metrics to {out_dir}")


if __name__ == "__main__":
    main()
