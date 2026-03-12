#!/usr/bin/env python3
"""
QA 08: audit the generated IPEDSDB_ROOT artifacts against core acceptance criteria.

Reads:
- final panel artifacts under `Panels/`
- dictionary and QA summaries under `Checks/`
- release inventory under `Checks/download_qc/`

Writes:
- `Checks/acceptance_qc/acceptance_summary.csv`
- `Checks/acceptance_qc/acceptance_summary.md`

Focus:
- exact requested year coverage
- row and key preservation
- zero unresolved dictionary and panel QA failures
- readable, reusable acceptance status for the live build
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from access_build_utils import ensure_data_layout, parse_years


def parse_args() -> argparse.Namespace:
    data_root = Path(os.environ.get("IPEDSDB_ROOT", "/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling"))
    checks_root = data_root / "Checks"
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", default=str(data_root), help="External IPEDSDB_ROOT")
    p.add_argument("--years", default="2004:2023", help='Expected year span, e.g. "2004:2023"')
    p.add_argument("--out-dir", default=str(checks_root / "acceptance_qc"), help="Acceptance output directory")
    return p.parse_args()


def build_row(
    check_name: str,
    passed: bool,
    *,
    value: object = "",
    expected: object = "",
    details: str = "",
) -> dict[str, object]:
    return {
        "check_name": check_name,
        "status": "PASS" if passed else "FAIL",
        "passed": bool(passed),
        "value": value,
        "expected": expected,
        "details": details,
    }


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path)


def series_or_default(df: pd.DataFrame, column: str, default: str = "") -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series([default] * len(df), index=df.index, dtype="string")


def year_coverage(path: Path) -> list[int]:
    dataset = ds.dataset(str(path), format="parquet")
    values = dataset.to_table(columns=["year"]).column(0)
    return sorted(int(v) for v in pc.unique(values).to_pylist() if v is not None)


def null_or_blank_key_counts(path: Path, key_cols: list[str]) -> dict[str, int]:
    dataset = ds.dataset(str(path), format="parquet")
    counts = {col: 0 for col in key_cols}
    for batch in dataset.to_batches(columns=key_cols, batch_size=200_000):
        schema_names = list(batch.schema.names)
        for col in key_cols:
            arr = batch.column(schema_names.index(col))
            counts[col] += int(arr.null_count)
            if pa.types.is_string(arr.type) or pa.types.is_large_string(arr.type):
                counts[col] += int(pc.sum(pc.equal(pc.utf8_trim_whitespace(arr), "")).as_py() or 0)
    return counts


def duplicate_key_count(path: Path, key_cols: list[str]) -> int:
    dataset = ds.dataset(str(path), format="parquet")
    df = dataset.to_table(columns=key_cols).to_pandas()
    return int(df.duplicated(subset=key_cols).sum())


def collect_acceptance_rows(root: Path, years: list[int]) -> list[dict[str, object]]:
    layout = ensure_data_layout(root)
    start, end = years[0], years[-1]
    expected_years = years

    long_path = layout.panels / f"{start}-{end}" / f"panel_long_varnum_{start}_{end}.parquet"
    wide_path = layout.panels / f"panel_wide_analysis_{start}_{end}.parquet"
    clean_path = layout.panels / f"panel_clean_analysis_{start}_{end}.parquet"
    dict_path = layout.dictionary / "dictionary_lake.parquet"
    dict_qc_path = layout.checks / "dictionary_qc" / "dictionary_qaqc_summary.csv"
    panel_qc_path = layout.checks / "panel_qc" / "panel_qa_summary.csv"
    panel_cov_path = layout.checks / "panel_qc" / "panel_qa_coverage_matrix.csv"
    disc_summary_path = layout.checks / "disc_qc" / "disc_conflicts_summary_all_years.csv"
    release_inventory_path = layout.checks / "download_qc" / "release_inventory.csv"

    rows: list[dict[str, object]] = []

    required_paths = {
        "dictionary_lake": dict_path,
        "panel_long": long_path,
        "panel_wide": wide_path,
        "panel_clean": clean_path,
        "dictionary_qaqc_summary": dict_qc_path,
        "panel_qa_summary": panel_qc_path,
        "panel_qa_coverage_matrix": panel_cov_path,
        "release_inventory": release_inventory_path,
    }
    for label, path in required_paths.items():
        rows.append(
            build_row(
                f"exists:{label}",
                path.exists(),
                value=str(path),
                expected="file exists",
                details="" if path.exists() else "Required artifact is missing.",
            )
        )

    per_year_missing = [
        year for year in expected_years if not (layout.cross_sections / f"panel_long_varnum_{year}.parquet").exists()
    ]
    rows.append(
        build_row(
            "cross_sections:per_year_long_files",
            not per_year_missing,
            value=",".join(str(y) for y in expected_years),
            expected="all requested years present",
            details="" if not per_year_missing else f"Missing per-year long files for: {per_year_missing}",
        )
    )

    if release_inventory_path.exists():
        release_df = safe_read_csv(release_inventory_path)
        release_df["year"] = pd.to_numeric(series_or_default(release_df, "year"), errors="coerce").astype("Int64")
        release_df["release_type"] = series_or_default(release_df, "release_type").fillna("").astype(str).str.strip().str.lower()
        final_years = sorted(int(y) for y in release_df.loc[release_df["release_type"] == "final", "year"].dropna().tolist())
        final_ok = final_years == expected_years
        rows.append(
            build_row(
                "release_inventory:final_year_coverage",
                final_ok,
                value=",".join(str(y) for y in final_years),
                expected=",".join(str(y) for y in expected_years),
                details="" if final_ok else "Final release years did not match the expected analysis window.",
            )
        )
        if "download_status" in release_df.columns:
            release_df["download_status"] = series_or_default(release_df, "download_status").fillna("").astype(str).str.strip().str.lower()
            requested_release_rows = release_df[release_df["year"].isin(expected_years)]
            download_ok = requested_release_rows["download_status"].isin({"downloaded", "existing"}).all()
            rows.append(
                build_row(
                    "release_inventory:download_status",
                    bool(download_ok),
                    value=",".join(sorted(set(requested_release_rows["download_status"].astype(str)))),
                    expected="downloaded/existing only",
                    details="" if download_ok else "One or more requested years were not marked downloaded/existing.",
                )
            )
        else:
            rows.append(
                build_row(
                    "release_inventory:download_status",
                    True,
                    value="column absent",
                    expected="column optional",
                    details="release_inventory.csv does not include download_status; downstream artifact existence is used instead.",
                )
            )

    if long_path.exists():
        long_years = year_coverage(long_path)
        long_nulls = null_or_blank_key_counts(long_path, ["year", "UNITID", "varnumber", "source_file"])
        rows.append(
            build_row(
                "panel_long:year_coverage",
                long_years == expected_years,
                value=",".join(str(y) for y in long_years),
                expected=",".join(str(y) for y in expected_years),
                details="" if long_years == expected_years else "Stitched long panel year coverage mismatch.",
            )
        )
        rows.append(
            build_row(
                "panel_long:key_nulls",
                all(v == 0 for v in long_nulls.values()),
                value=str(long_nulls),
                expected="all zero",
                details="" if all(v == 0 for v in long_nulls.values()) else "Long panel contains null or blank key fields.",
            )
        )

    wide_rows = None
    clean_rows = None
    if wide_path.exists():
        wide_dataset = ds.dataset(str(wide_path), format="parquet")
        wide_rows = int(wide_dataset.count_rows())
        wide_years = year_coverage(wide_path)
        wide_dup = duplicate_key_count(wide_path, ["UNITID", "year"])
        rows.append(
            build_row(
                "panel_wide:year_coverage",
                wide_years == expected_years,
                value=",".join(str(y) for y in wide_years),
                expected=",".join(str(y) for y in expected_years),
                details="" if wide_years == expected_years else "Wide panel year coverage mismatch.",
            )
        )
        rows.append(
            build_row(
                "panel_wide:duplicate_unitid_year",
                wide_dup == 0,
                value=wide_dup,
                expected=0,
                details="" if wide_dup == 0 else "Wide panel contains duplicate UNITID-year rows.",
            )
        )

    if clean_path.exists():
        clean_dataset = ds.dataset(str(clean_path), format="parquet")
        clean_rows = int(clean_dataset.count_rows())
        clean_years = year_coverage(clean_path)
        clean_dup = duplicate_key_count(clean_path, ["UNITID", "year"])
        rows.append(
            build_row(
                "panel_clean:year_coverage",
                clean_years == expected_years,
                value=",".join(str(y) for y in clean_years),
                expected=",".join(str(y) for y in expected_years),
                details="" if clean_years == expected_years else "Clean panel year coverage mismatch.",
            )
        )
        rows.append(
            build_row(
                "panel_clean:duplicate_unitid_year",
                clean_dup == 0,
                value=clean_dup,
                expected=0,
                details="" if clean_dup == 0 else "Clean panel contains duplicate UNITID-year rows.",
            )
        )

    if wide_rows is not None and clean_rows is not None:
        rows.append(
            build_row(
                "panel_clean:row_preservation",
                wide_rows == clean_rows,
                value=clean_rows,
                expected=wide_rows,
                details="" if wide_rows == clean_rows else "Raw and cleaned panel row counts differ.",
            )
        )

    if dict_qc_path.exists():
        dict_summary = safe_read_csv(dict_qc_path).iloc[0].to_dict()
        for field in ["duplicate_rows", "source_file_conflicts", "varnumber_collisions", "unmapped_rows", "needs_review_rows"]:
            value = int(dict_summary.get(field, 0) or 0)
            rows.append(
                build_row(
                    f"dictionary_qaqc:{field}",
                    value == 0,
                    value=value,
                    expected=0,
                    details="" if value == 0 else f"Dictionary QA reported nonzero {field}.",
                )
            )

    if panel_qc_path.exists():
        panel_summary = safe_read_csv(panel_qc_path).iloc[0].to_dict()
        suspicious = int(panel_summary.get("suspicious_flags", 0) or 0)
        raw_rows_value = int(panel_summary.get("raw_rows", 0) or 0)
        clean_rows_value = int(panel_summary.get("clean_rows", 0) or 0)
        rows.append(
            build_row(
                "panel_qaqc:suspicious_flags",
                suspicious == 0,
                value=suspicious,
                expected=0,
                details="" if suspicious == 0 else "Panel QA reported unresolved suspicious PRCH flags.",
            )
        )
        rows.append(
            build_row(
                "panel_qaqc:raw_clean_rows_match",
                raw_rows_value == clean_rows_value,
                value=clean_rows_value,
                expected=raw_rows_value,
                details="" if raw_rows_value == clean_rows_value else "Panel QA reported row-count mismatch.",
            )
        )

    if panel_cov_path.exists():
        coverage = safe_read_csv(panel_cov_path)
        suspicious_cov = int((coverage.get("status", pd.Series(dtype=str)).astype(str) == "suspicious").sum())
        rows.append(
            build_row(
                "panel_qaqc:coverage_matrix_suspicious_rows",
                suspicious_cov == 0,
                value=suspicious_cov,
                expected=0,
                details="" if suspicious_cov == 0 else "Coverage matrix still has suspicious flag rows.",
            )
        )

    if disc_summary_path.exists():
        disc_summary = safe_read_csv(disc_summary_path)
        high_signal = int(pd.to_numeric(disc_summary.get("high_signal"), errors="coerce").fillna(0).astype(int).sum()) if not disc_summary.empty else 0
        rows.append(
            build_row(
                "disc_qaqc:high_signal_groups",
                high_signal == 0,
                value=high_signal,
                expected=0,
                details="" if high_signal == 0 else "Discrete conflict summary still contains high-signal groups.",
            )
        )

    return rows


def render_markdown(rows: list[dict[str, object]], years: list[int]) -> str:
    passed = sum(1 for row in rows if row["passed"])
    failed = len(rows) - passed
    lines = [
        "# Acceptance Audit",
        "",
        f"- Years: `{years[0]}:{years[-1]}`",
        f"- Checks passed: `{passed}`",
        f"- Checks failed: `{failed}`",
        "",
        "| Check | Status | Value | Expected | Details |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            f"| `{row['check_name']}` | {row['status']} | `{row['value']}` | `{row['expected']}` | {row['details']} |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    root = Path(args.root).expanduser()
    years = parse_years(args.years)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = collect_acceptance_rows(root, years)
    df = pd.DataFrame(rows)
    csv_path = out_dir / "acceptance_summary.csv"
    md_path = out_dir / "acceptance_summary.md"
    df.to_csv(csv_path, index=False)
    md_path.write_text(render_markdown(rows, years), encoding="utf-8")

    failed = int((~df["passed"].astype(bool)).sum())
    print(f"Wrote acceptance audit: {csv_path}")
    print(f"Wrote acceptance audit: {md_path}")
    if failed:
        raise SystemExit(f"Acceptance audit failed: {failed} checks failed")
    print("Acceptance audit passed")


if __name__ == "__main__":
    main()
