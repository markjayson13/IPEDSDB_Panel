#!/usr/bin/env python3
"""
QA 01: compare the PRCH-cleaned wide panel against the raw wide panel.

Reads:
- raw stitched wide parquet
- cleaned stitched wide parquet
- `Checks/prch_qc/prch_clean_columns.csv` when present

Writes:
- `Checks/panel_qc/panel_qa_summary.csv`
- `Checks/panel_qc/panel_qa_coverage_matrix.csv`
- `Checks/panel_qc/panel_qa_by_flag_code.csv`

Focus:
- year coverage preservation
- row preservation
- full PRCH flag coverage, not just finance
- targeted non-null comparisons after PRCH cleaning
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

import pandas as pd
import pyarrow.dataset as ds

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from prch_policy import classify_flag_code, cleaned_child_codes, get_policy, review_only_codes


COVERAGE_COLUMNS = [
    "flag",
    "observed_codes",
    "child_codes",
    "review_only_codes",
    "policy_target_source_files",
    "policy_target_source_prefixes",
    "child_rows_raw",
    "child_rows_clean",
    "review_rows_raw",
    "review_rows_clean",
    "target_columns",
    "raw_target_nonnull",
    "clean_target_nonnull",
    "review_target_nonnull_raw",
    "review_target_nonnull_clean",
    "status",
    "status_reason",
    "rationale",
]

CODE_COLUMNS = [
    "flag",
    "code",
    "policy_bucket",
    "raw_rows",
    "clean_rows",
    "raw_target_nonnull",
    "clean_target_nonnull",
]


def setup_logging(log_path: str | None) -> None:
    if not log_path:
        return
    log_file = Path(log_path)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    f = log_file.open("a", buffering=1)

    class Tee:
        def __init__(self, *streams):
            self.streams = streams

        def write(self, data):
            for s in self.streams:
                s.write(data)

        def flush(self):
            for s in self.streams:
                s.flush()

    sys.stdout = Tee(sys.stdout, f)
    sys.stderr = Tee(sys.stderr, f)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--raw", required=True, help="Raw stitched wide parquet")
    p.add_argument("--clean", required=True, help="PRCH cleaned wide parquet")
    data_root = Path(os.environ.get("IPEDSDB_ROOT", "/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling"))
    checks_root = data_root / "Checks"
    p.add_argument("--out-dir", default=str(checks_root / "panel_qc"), help="QA output directory")
    p.add_argument("--prch-qc-dir", default=str(checks_root / "prch_qc"), help="PRCH QC dir (for prch_clean_columns.csv)")
    p.add_argument("--flag", default="ALL", help='Flag to validate, comma list, or "ALL"')
    p.add_argument("--flag-child", default="AUTO", help='Child codes for a single-flag run, or "AUTO" to use repo policy')
    p.add_argument("--batch-rows", type=int, default=10_000, help="Batch size for targeted non-null QA")
    p.add_argument("--year-sep", default="|", help="Separator for year lists in CSV")
    p.add_argument("--excel-text", action=argparse.BooleanOptionalAction, default=True, help="Prefix year lists with apostrophe for Excel text")
    p.add_argument("--log-file", default=str(checks_root / "logs" / "01_panel_qa.log"), help="Optional log file path")
    return p.parse_args()


def format_years(years, sep: str, excel_text: bool) -> str:
    years_sorted = sorted(int(y) for y in years if pd.notna(y))
    s = sep.join(str(y) for y in years_sorted)
    if excel_text:
        return "'" + s
    return s


def resolve_child_codes(flag: str, value: str) -> set[int]:
    if (value or "").strip().upper() == "AUTO":
        return cleaned_child_codes(flag)
    return {int(x) for x in value.split(",") if x.strip()}


def resolve_requested_flags(flag_arg: str, raw_schema_names: list[str], clean_schema_names: list[str]) -> list[str]:
    available = sorted(
        {
            col.upper()
            for col in set(raw_schema_names) | set(clean_schema_names)
            if str(col).upper().startswith("PRCH")
        }
    )
    if not available:
        return []
    spec = (flag_arg or "ALL").strip().upper()
    if spec in {"ALL", "*", "AUTO"}:
        return available
    requested = [part.strip().upper() for part in spec.split(",") if part.strip()]
    return [flag for flag in requested if flag in available]


def load_target_columns_map(prch_qc_dir: Path, schema_names: list[str]) -> dict[str, list[str]]:
    qc_path = prch_qc_dir / "prch_clean_columns.csv"
    if not qc_path.exists():
        return {}
    qc = pd.read_csv(qc_path, dtype=str).fillna("")
    if "flag" not in qc.columns or "column" not in qc.columns:
        return {}
    schema_set = set(schema_names)
    out: dict[str, list[str]] = {}
    for flag, frame in qc.groupby(qc["flag"].astype(str).str.upper()):
        cols = sorted({str(col).strip() for col in frame["column"].tolist() if str(col).strip() in schema_set})
        out[flag] = cols
    return out


def aggregate_target_nonnull_by_code(dataset: ds.Dataset, flag: str, target_cols: list[str], batch_rows: int) -> dict[int, dict[str, int]]:
    if flag not in dataset.schema.names:
        return {}
    scan_cols = [flag] + [c for c in target_cols if c in dataset.schema.names]
    out: dict[int, dict[str, int]] = {}
    for batch in dataset.to_batches(columns=scan_cols, batch_size=batch_rows):
        df = batch.to_pandas()
        flag_num = pd.to_numeric(df[flag], errors="coerce")
        valid_codes = flag_num.dropna()
        if valid_codes.empty:
            continue
        code_counts = valid_codes.astype(int).value_counts()
        for code, cnt in code_counts.items():
            code_int = int(code)
            record = out.setdefault(code_int, {"rows": 0, "target_nonnull": 0})
            record["rows"] += int(cnt)
            mask = flag_num == code_int
            if target_cols and mask.any():
                record["target_nonnull"] += int(df.loc[mask, target_cols].notna().sum().sum())
    return out


def determine_flag_status(
    *,
    child_rows_raw: int,
    review_rows_raw: int,
    target_columns: int,
    clean_target_nonnull: int,
) -> tuple[str, str]:
    if child_rows_raw > 0 and target_columns == 0:
        return "no_targets", "Child rows were observed but no target columns were configured for this flag."
    if child_rows_raw > 0 and clean_target_nonnull > 0:
        return "suspicious", "Targeted child-row cells remained non-null after cleaning."
    if child_rows_raw > 0:
        return "cleaned", "Observed child rows were cleaned across the targeted column family."
    if review_rows_raw > 0:
        return "review_only", "Only review-only codes were observed for this flag."
    if target_columns == 0:
        return "no_targets", "No target columns were configured and no child rows were observed."
    return "cleaned", "No child rows were observed for this flag."


def build_coverage_row(
    *,
    flag: str,
    child_codes: set[int],
    review_codes: set[int],
    target_cols: list[str],
    raw_code_stats: dict[int, dict[str, int]],
    clean_code_stats: dict[int, dict[str, int]],
) -> dict[str, object]:
    observed_codes = sorted(set(raw_code_stats) | set(clean_code_stats))
    child_rows_raw = sum(raw_code_stats.get(code, {}).get("rows", 0) for code in child_codes)
    child_rows_clean = sum(clean_code_stats.get(code, {}).get("rows", 0) for code in child_codes)
    raw_target_nonnull = sum(raw_code_stats.get(code, {}).get("target_nonnull", 0) for code in child_codes)
    clean_target_nonnull = sum(clean_code_stats.get(code, {}).get("target_nonnull", 0) for code in child_codes)
    review_rows_raw = sum(raw_code_stats.get(code, {}).get("rows", 0) for code in review_codes)
    review_rows_clean = sum(clean_code_stats.get(code, {}).get("rows", 0) for code in review_codes)
    review_target_nonnull_raw = sum(raw_code_stats.get(code, {}).get("target_nonnull", 0) for code in review_codes)
    review_target_nonnull_clean = sum(clean_code_stats.get(code, {}).get("target_nonnull", 0) for code in review_codes)
    status, reason = determine_flag_status(
        child_rows_raw=child_rows_raw,
        review_rows_raw=review_rows_raw,
        target_columns=len(target_cols),
        clean_target_nonnull=clean_target_nonnull,
    )
    policy = get_policy(flag)
    return {
        "flag": flag,
        "observed_codes": ",".join(str(code) for code in observed_codes),
        "child_codes": ",".join(str(code) for code in sorted(child_codes)),
        "review_only_codes": ",".join(str(code) for code in sorted(review_codes)),
        "policy_target_source_files": ",".join(policy.target_source_files),
        "policy_target_source_prefixes": ",".join(policy.target_source_prefixes),
        "child_rows_raw": child_rows_raw,
        "child_rows_clean": child_rows_clean,
        "review_rows_raw": review_rows_raw,
        "review_rows_clean": review_rows_clean,
        "target_columns": len(target_cols),
        "raw_target_nonnull": raw_target_nonnull,
        "clean_target_nonnull": clean_target_nonnull,
        "review_target_nonnull_raw": review_target_nonnull_raw,
        "review_target_nonnull_clean": review_target_nonnull_clean,
        "status": status,
        "status_reason": reason,
        "rationale": policy.rationale,
    }


def build_code_rows(
    flag: str,
    raw_code_stats: dict[int, dict[str, int]],
    clean_code_stats: dict[int, dict[str, int]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for code in sorted(set(raw_code_stats) | set(clean_code_stats)):
        rows.append(
            {
                "flag": flag,
                "code": code,
                "policy_bucket": classify_flag_code(flag, code),
                "raw_rows": raw_code_stats.get(code, {}).get("rows", 0),
                "clean_rows": clean_code_stats.get(code, {}).get("rows", 0),
                "raw_target_nonnull": raw_code_stats.get(code, {}).get("target_nonnull", 0),
                "clean_target_nonnull": clean_code_stats.get(code, {}).get("target_nonnull", 0),
            }
        )
    return rows


def main() -> None:
    args = parse_args()
    setup_logging(args.log_file)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    raw = ds.dataset(args.raw, format="parquet")
    cln = ds.dataset(args.clean, format="parquet")

    raw_years = raw.to_table(columns=["year"]).to_pandas()["year"].unique()
    cln_years = cln.to_table(columns=["year"]).to_pandas()["year"].unique()
    raw_rows = raw.count_rows()
    cln_rows = cln.count_rows()
    raw_size = os.path.getsize(args.raw) if os.path.exists(args.raw) else 0
    cln_size = os.path.getsize(args.clean) if os.path.exists(args.clean) else 0

    flags = resolve_requested_flags(args.flag, raw.schema.names, cln.schema.names)
    if not flags:
        raise SystemExit("No PRCH flags were found to validate.")

    target_cols_map = load_target_columns_map(Path(args.prch_qc_dir), raw.schema.names)
    coverage_rows: list[dict[str, object]] = []
    code_rows: list[dict[str, object]] = []

    for flag in flags:
        child_codes = resolve_child_codes(flag, args.flag_child if len(flags) == 1 else "AUTO")
        review_codes = review_only_codes(flag)
        target_cols = target_cols_map.get(flag, [])
        raw_code_stats = aggregate_target_nonnull_by_code(raw, flag, target_cols, args.batch_rows)
        clean_code_stats = aggregate_target_nonnull_by_code(cln, flag, target_cols, args.batch_rows)
        coverage_rows.append(
            build_coverage_row(
                flag=flag,
                child_codes=child_codes,
                review_codes=review_codes,
                target_cols=target_cols,
                raw_code_stats=raw_code_stats,
                clean_code_stats=clean_code_stats,
            )
        )
        code_rows.extend(build_code_rows(flag, raw_code_stats, clean_code_stats))

    coverage_df = pd.DataFrame(coverage_rows, columns=COVERAGE_COLUMNS)
    code_df = pd.DataFrame(code_rows, columns=CODE_COLUMNS)

    status_counts = coverage_df["status"].value_counts().to_dict() if not coverage_df.empty else {}
    summary = pd.DataFrame(
        [
            {
                "raw_years": format_years(raw_years, args.year_sep, args.excel_text),
                "clean_years": format_years(cln_years, args.year_sep, args.excel_text),
                "raw_years_min": min([int(y) for y in raw_years if pd.notna(y)], default=None),
                "raw_years_max": max([int(y) for y in raw_years if pd.notna(y)], default=None),
                "raw_years_count": len([y for y in raw_years if pd.notna(y)]),
                "clean_years_min": min([int(y) for y in cln_years if pd.notna(y)], default=None),
                "clean_years_max": max([int(y) for y in cln_years if pd.notna(y)], default=None),
                "clean_years_count": len([y for y in cln_years if pd.notna(y)]),
                "raw_rows": raw_rows,
                "clean_rows": cln_rows,
                "raw_size_bytes": raw_size,
                "clean_size_bytes": cln_size,
                "flags_evaluated": len(coverage_df),
                "flags_with_child_rows": int((coverage_df["child_rows_raw"] > 0).sum()) if not coverage_df.empty else 0,
                "cleaned_flags": int(status_counts.get("cleaned", 0)),
                "no_target_flags": int(status_counts.get("no_targets", 0)),
                "review_only_flags": int(status_counts.get("review_only", 0)),
                "suspicious_flags": int(status_counts.get("suspicious", 0)),
                "suspicious_flag_list": ",".join(coverage_df.loc[coverage_df["status"] == "suspicious", "flag"].tolist()),
                "no_target_flag_list": ",".join(coverage_df.loc[coverage_df["status"] == "no_targets", "flag"].tolist()),
                "review_only_flag_list": ",".join(coverage_df.loc[coverage_df["status"] == "review_only", "flag"].tolist()),
            }
        ]
    )

    summary_path = out_dir / "panel_qa_summary.csv"
    coverage_path = out_dir / "panel_qa_coverage_matrix.csv"
    code_path = out_dir / "panel_qa_by_flag_code.csv"

    summary.to_csv(summary_path, index=False)
    coverage_df.to_csv(coverage_path, index=False)
    code_df.to_csv(code_path, index=False)

    print("raw years:", sorted(raw_years))
    print("clean years:", sorted(cln_years))
    print("raw rows:", raw_rows)
    print("clean rows:", cln_rows)
    print("flags evaluated:", len(coverage_df))
    print("status counts:", status_counts)
    print("Wrote", summary_path)
    print("Wrote", coverage_path)
    print("Wrote", code_path)


if __name__ == "__main__":
    main()
