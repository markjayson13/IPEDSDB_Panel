#!/usr/bin/env python3
"""
QA 21: reconcile selected panel metrics against external benchmarks.

Reads:
- clean panel parquet
- contracts/external_benchmarks.csv

Writes:
- Checks/external_benchmarks/external_benchmark_reconciliation.csv
- Checks/external_benchmarks/external_benchmark_reconciliation.md
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from access_build_utils import DEFAULT_IPEDSDB_ROOT, data_layout, parse_years, repo_root

REQUIRED_COLUMNS = {
    "benchmark_id",
    "year",
    "metric",
    "column",
    "expected_value",
    "tolerance_abs",
    "tolerance_rel",
    "source",
    "notes",
}


def parse_args() -> argparse.Namespace:
    root = Path(os.environ.get("IPEDSDB_ROOT", str(DEFAULT_IPEDSDB_ROOT)))
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", default=str(root), help="External IPEDSDB_ROOT")
    p.add_argument("--years", default="2004:2023")
    p.add_argument("--panel", default=None)
    p.add_argument("--benchmarks", default=str(repo_root() / "contracts" / "external_benchmarks.csv"))
    p.add_argument("--out-dir", default=None)
    p.add_argument("--require-benchmarks", action=argparse.BooleanOptionalAction, default=False)
    return p.parse_args()


def numeric(value: object, default: float = 0.0) -> float:
    text = str(value if value is not None else "").strip()
    if not text:
        return default
    return float(text)


def load_benchmarks(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"Benchmark file does not exist: {path}")
    df = pd.read_csv(path, dtype=str).fillna("")
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise SystemExit(f"Benchmark file missing columns: {', '.join(sorted(missing))}")
    return df[df["benchmark_id"].astype(str).str.strip() != ""].copy()


def metric_value(panel: pd.DataFrame, metric: str, column: str) -> float:
    metric = metric.strip().lower()
    column = column.strip()
    if metric == "panel_rows":
        return float(len(panel))
    if metric == "distinct_unitid":
        return float(panel["UNITID"].nunique())
    if column not in panel.columns:
        raise KeyError(column)
    series = panel[column]
    if metric == "nonnull_count":
        return float(series.notna().sum())
    numeric_series = pd.to_numeric(series, errors="coerce")
    if metric == "sum":
        return float(numeric_series.sum(skipna=True))
    if metric == "mean":
        return float(numeric_series.mean(skipna=True))
    raise ValueError(f"Unsupported metric: {metric}")


def render_markdown(rows: list[dict[str, object]]) -> str:
    lines = [
        "# External Benchmark Reconciliation",
        "",
        "| Benchmark | Status | Year | Metric | Column | Actual | Expected | Source |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| {benchmark_id} | {status} | {year} | {metric} | {column} | {actual_value} | {expected_value} | {source} |".format(
                **{key: str(value) for key, value in row.items()}
            )
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    root = Path(args.root).expanduser()
    layout = data_layout(root)
    years = parse_years(args.years)
    panel_path = Path(args.panel).expanduser() if args.panel else layout.panels / f"panel_clean_analysis_{years[0]}_{years[-1]}.parquet"
    out_dir = Path(args.out_dir).expanduser() if args.out_dir else layout.checks / "external_benchmarks"
    out_dir.mkdir(parents=True, exist_ok=True)
    benchmarks = load_benchmarks(Path(args.benchmarks).expanduser())

    if benchmarks.empty:
        rows = [
            {
                "benchmark_id": "no_benchmarks_configured",
                "status": "REVIEW",
                "year": "",
                "metric": "",
                "column": "",
                "actual_value": "",
                "expected_value": "",
                "delta": "",
                "tolerance_abs": "",
                "tolerance_rel": "",
                "source": str(args.benchmarks),
                "notes": "Add official external benchmarks before treating this release as externally reconciled.",
            }
        ]
        pd.DataFrame(rows).to_csv(out_dir / "external_benchmark_reconciliation.csv", index=False)
        (out_dir / "external_benchmark_reconciliation.md").write_text(render_markdown(rows), encoding="utf-8")
        if args.require_benchmarks:
            raise SystemExit("No external benchmarks configured.")
        print(f"Wrote external benchmark reconciliation to {out_dir}")
        return 0

    if not panel_path.exists():
        raise SystemExit(f"Panel file does not exist: {panel_path}")
    panel = pd.read_parquet(panel_path)
    rows: list[dict[str, object]] = []
    failures = 0
    for _, bench in benchmarks.iterrows():
        year_text = str(bench["year"]).strip()
        subset = panel
        if year_text:
            subset = panel[panel["year"].astype(str) == year_text]
        expected = numeric(bench["expected_value"])
        tol_abs = numeric(bench["tolerance_abs"])
        tol_rel = numeric(bench["tolerance_rel"])
        try:
            actual = metric_value(subset, str(bench["metric"]), str(bench["column"]))
            delta = actual - expected
            allowed = max(tol_abs, abs(expected) * tol_rel)
            passed = abs(delta) <= allowed
            status = "PASS" if passed else "FAIL"
            if not passed:
                failures += 1
        except Exception as exc:
            actual = ""
            delta = ""
            status = "FAIL"
            failures += 1
            bench["notes"] = f"{bench['notes']} ({exc})".strip()
        rows.append(
            {
                "benchmark_id": bench["benchmark_id"],
                "status": status,
                "year": year_text,
                "metric": bench["metric"],
                "column": bench["column"],
                "actual_value": actual,
                "expected_value": expected,
                "delta": delta,
                "tolerance_abs": tol_abs,
                "tolerance_rel": tol_rel,
                "source": bench["source"],
                "notes": bench["notes"],
            }
        )

    pd.DataFrame(rows).to_csv(out_dir / "external_benchmark_reconciliation.csv", index=False)
    (out_dir / "external_benchmark_reconciliation.md").write_text(render_markdown(rows), encoding="utf-8")
    print(f"Wrote external benchmark reconciliation to {out_dir}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
