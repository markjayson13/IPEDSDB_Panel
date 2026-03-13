#!/usr/bin/env python3
"""
Stage 07: apply PRCH parent/child cleaning to the stitched wide panel.

Reads:
- `Panels/panel_wide_analysis_*.parquet`
- `Dictionary/dictionary_lake.parquet`

Writes:
- `Panels/panel_clean_analysis_*.parquet`
- `Checks/prch_qc/*`

Policy:
- keep all `UNITID-year` rows
- if a `PRCH_*` flag marks a child observation, null only the affected
  component-family columns
- for Finance, treat `PRCH_F=2,3,4,5` as child rows; retain `PRCH_F=6`
  as a partial review-only case

Open this file when you want to see the actual row-preserving parent/child cleaning logic used to produce the final released panel.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
import pathlib

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.parquet as pq

from prch_policy import (
    classify_flag_code,
    cleaned_child_codes,
    policy_rows,
    review_only_codes,
    targets_source_file,
)


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
    p.add_argument("--input", required=True, help="Input stitched wide parquet")
    p.add_argument("--output", required=True, help="Output cleaned parquet")
    p.add_argument("--dictionary", required=True, help="dictionary_lake.parquet")
    p.add_argument("--qc-dir", default=None, help="Write QC summaries here")
    p.add_argument("--batch-rows", type=int, default=100_000, help="Batch size for streaming")
    p.add_argument("--log-every", type=int, default=50, help="Log progress every N batches")
    p.add_argument("--drop-imputation-flags", action=argparse.BooleanOptionalAction, default=False, help="Drop X* imputation columns")
    data_root = pathlib.Path(os.environ.get("IPEDSDB_ROOT", "/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling"))
    logs_root = data_root / "Checks" / "logs"
    p.add_argument("--log-file", default=str(logs_root / "07_clean_panel.log"), help="Optional log file path")
    return p.parse_args()


def mode(series: pd.Series) -> str:
    s = series.dropna()
    if s.empty:
        return ""
    return s.mode().iat[0]


def build_var_source_map(dictionary_path: Path) -> dict[str, str]:
    df = pd.read_parquet(dictionary_path, columns=["varname", "source_file"])
    df["varname"] = df["varname"].fillna("").astype(str).str.strip().str.upper()
    df["source_file"] = df["source_file"].fillna("").astype(str).str.strip()
    df = df[df["varname"] != ""]
    return df.groupby("varname")["source_file"].agg(mode).to_dict()


def main() -> None:
    args = parse_args()
    setup_logging(args.log_file)
    in_path = Path(args.input)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    var_to_source = build_var_source_map(Path(args.dictionary))

    dataset = ds.dataset(str(in_path), format="parquet")
    if "year" not in dataset.schema.names:
        raise SystemExit("Input must contain a 'year' column for PRCH cleaning.")

    # Guard: refuse to run if only a single year is detected
    years: set[int] = set()
    for batch in dataset.to_batches(columns=["year"], batch_size=min(args.batch_rows, 200_000)):
        uniq = pc.unique(batch.column(0)).to_pylist()
        for v in uniq:
            if v is None:
                continue
            years.add(int(v))
    if len(years) == 1:
        yr = next(iter(years))
        raise SystemExit(
            f"Refusing to run: input appears to contain only one year ({yr}). "
            "Provide the full stitched panel to avoid a single-year cleaned output."
        )
    years_sorted = sorted(years)
    all_cols = dataset.schema.names
    prch_flags = [c for c in all_cols if c.upper().startswith("PRCH")]

    # Build column lists per flag from the shared PRCH policy.
    flag_cols: dict[str, list[str]] = {f: [] for f in prch_flags}
    for col in all_cols:
        if col in prch_flags:
            continue
        sf = var_to_source.get(col.upper(), "")
        for flag in prch_flags:
            if targets_source_file(flag, sf):
                flag_cols[flag].append(col)

    # de-duplicate columns per flag but retain zero-target flags for QA coverage.
    flag_cols = {k: sorted(set(v)) for k, v in flag_cols.items()}

    qc_child_counts: dict[tuple[int, str], int] = {}
    qc_review_counts: dict[tuple[int, str], int] = {}
    qc_code_counts: dict[tuple[int, str, int], int] = {}

    writer = None
    batch_idx = 0
    rows_processed = 0
    years_seen: set[int] = set()
    last_log = time.time()
    target_schema = dataset.schema
    for y in years_sorted:
        print(f"[year] start {y}")
        year_rows = 0
        scanner = dataset.scanner(filter=ds.field("year") == y, batch_size=args.batch_rows)
        for batch in scanner.to_batches():
            df = batch.to_pandas()
            batch_idx += 1
            rows_processed += len(df)
            year_rows += len(df)
            for flag in prch_flags:
                if flag not in df.columns:
                    continue
                flag_num = pd.to_numeric(df[flag], errors="coerce")
                valid_codes = flag_num.dropna()
                if not valid_codes.empty:
                    code_counts = valid_codes.astype(int).value_counts()
                    for code, cnt in code_counts.items():
                        key = (int(y), flag, int(code))
                        qc_code_counts[key] = qc_code_counts.get(key, 0) + int(cnt)

                child_mask = flag_num.isin(cleaned_child_codes(flag))
                review_mask = flag_num.isin(review_only_codes(flag))
                if child_mask.any():
                    counts = df.loc[child_mask, "year"].value_counts()
                    for yy, cnt in counts.items():
                        key = (int(yy), flag)
                        qc_child_counts[key] = qc_child_counts.get(key, 0) + int(cnt)

                if review_mask.any():
                    review_counts = df.loc[review_mask, "year"].value_counts()
                    for yy, cnt in review_counts.items():
                        key = (int(yy), flag)
                        qc_review_counts[key] = qc_review_counts.get(key, 0) + int(cnt)

                cols = flag_cols.get(flag, [])
                if child_mask.any() and cols:
                    df.loc[child_mask, cols] = pd.NA

            table = pa.Table.from_pandas(df, preserve_index=False)
            # Align schema to avoid mismatches across years/batches
            for field in target_schema:
                if field.name not in table.column_names:
                    table = table.append_column(field.name, pa.nulls(table.num_rows, type=field.type))
            table = table.select([f.name for f in target_schema])
            try:
                table = table.cast(target_schema, safe=False)
            except Exception:
                # If casting fails, still write aligned columns; Parquet will store as-is.
                pass
            if args.drop_imputation_flags:
                drop_cols = [c for c in table.column_names if c.upper().startswith("X")]
                if drop_cols:
                    table = table.drop(drop_cols)
            if writer is None:
                schema_to_write = table.schema if args.drop_imputation_flags else target_schema
                writer = pq.ParquetWriter(out_path, schema_to_write, compression="snappy")
            writer.write_table(table)

            if args.log_every and batch_idx % args.log_every == 0:
                now = time.time()
                if now - last_log >= 1:
                    print(
                        f"[progress] batches={batch_idx} rows={rows_processed:,} "
                        f"current_year={y} year_rows={year_rows:,}"
                    )
                    sys.stdout.flush()
                    last_log = now
        print(f"[year] done {y} rows={year_rows:,}")

    if writer:
        writer.close()
        print(f"Wrote cleaned panel: {out_path}")

    if args.qc_dir:
        qc_dir = Path(args.qc_dir)
        qc_dir.mkdir(parents=True, exist_ok=True)
        policy_by_flag = {row["flag"]: row for row in policy_rows(prch_flags)}
        rows = []
        for y in years_sorted:
            for flag in prch_flags:
                policy = policy_by_flag.get(flag, {"child_codes_applied": "", "review_only_codes": ""})
                rows.append(
                    {
                        "year": y,
                        "flag": flag,
                        "child_codes_applied": policy["child_codes_applied"],
                        "review_only_codes": policy["review_only_codes"],
                        "target_columns": len(flag_cols.get(flag, [])),
                        "has_target_columns": bool(flag_cols.get(flag, [])),
                        "child_rows_cleaned": qc_child_counts.get((y, flag), 0),
                        "review_only_rows": qc_review_counts.get((y, flag), 0),
                    }
                )
        pd.DataFrame(rows).to_csv(qc_dir / "prch_clean_summary.csv", index=False)
        # also record which columns were cleaned per flag
        col_rows = []
        for flag, cols in flag_cols.items():
            for c in cols:
                col_rows.append({"flag": flag, "column": c})
        pd.DataFrame(col_rows).to_csv(qc_dir / "prch_clean_columns.csv", index=False)
        pd.DataFrame(policy_rows(prch_flags)).to_csv(qc_dir / "prch_flag_policy.csv", index=False)
        code_rows = []
        for (year, flag, code), cnt in sorted(qc_code_counts.items()):
            code_rows.append(
                {
                    "year": year,
                    "flag": flag,
                    "code": code,
                    "policy_bucket": classify_flag_code(flag, code),
                    "target_columns": len(flag_cols.get(flag, [])),
                    "has_target_columns": bool(flag_cols.get(flag, [])),
                    "rows": cnt,
                }
            )
        pd.DataFrame(code_rows).to_csv(qc_dir / "prch_flag_code_counts.csv", index=False)
        print(f"QC written to {qc_dir}")


if __name__ == "__main__":
    main()
    
