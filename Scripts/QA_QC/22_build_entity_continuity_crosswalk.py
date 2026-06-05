#!/usr/bin/env python3
"""
QA 22: build a UNITID-level entity-continuity and join-risk table.

Reads:
- clean panel parquet

Writes:
- Checks/entity_continuity/entity_continuity_crosswalk.csv
- Checks/entity_continuity/entity_continuity_summary.csv
- Checks/entity_continuity/entity_continuity_summary.md
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

from access_build_utils import DEFAULT_IPEDSDB_ROOT, data_layout, parse_years


def parse_args() -> argparse.Namespace:
    root = Path(os.environ.get("IPEDSDB_ROOT", str(DEFAULT_IPEDSDB_ROOT)))
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", default=str(root), help="External IPEDSDB_ROOT")
    p.add_argument("--years", default="2004:2023")
    p.add_argument("--panel", default=None)
    p.add_argument("--out-dir", default=None)
    return p.parse_args()


def find_column(columns: list[str], candidates: list[str]) -> str:
    by_upper = {col.upper(): col for col in columns}
    for candidate in candidates:
        if candidate.upper() in by_upper:
            return by_upper[candidate.upper()]
    return ""


def nonblank_values(series: pd.Series) -> list[str]:
    values = []
    for value in series.dropna().astype(str):
        clean = value.strip()
        if clean and clean.lower() not in {"nan", "none", "<na>", "na", "nat"}:
            values.append(clean)
    return sorted(set(values))


def has_internal_gap(years: list[int]) -> bool:
    if len(years) < 2:
        return False
    return len(years) != (max(years) - min(years) + 1)


def render_summary(summary: pd.DataFrame) -> str:
    lines = ["# Entity Continuity Summary", "", "| Metric | Value |", "| --- | --- |"]
    for _, row in summary.iterrows():
        lines.append(f"| {row['metric']} | {row['value']} |")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    root = Path(args.root).expanduser()
    layout = data_layout(root)
    years = parse_years(args.years)
    panel_path = Path(args.panel).expanduser() if args.panel else layout.panels / f"panel_clean_analysis_{years[0]}_{years[-1]}.parquet"
    out_dir = Path(args.out_dir).expanduser() if args.out_dir else layout.checks / "entity_continuity"
    out_dir.mkdir(parents=True, exist_ok=True)
    if not panel_path.exists():
        raise SystemExit(f"Panel file does not exist: {panel_path}")

    panel = pd.read_parquet(panel_path)
    columns = list(panel.columns)
    unitid_col = find_column(columns, ["UNITID"])
    year_col = find_column(columns, ["year", "YEAR"])
    if not unitid_col or not year_col:
        raise SystemExit("Panel must include UNITID and year columns.")
    opeid_col = find_column(columns, ["OPEID", "OPEIDB"])
    opeid6_col = find_column(columns, ["OPEID6", "OPEID_6"])
    prch_cols = [col for col in columns if col.upper().startswith("PRCH")]

    rows: list[dict[str, object]] = []
    for unitid, group in panel.groupby(unitid_col, dropna=False):
        observed_years = sorted(int(year) for year in pd.to_numeric(group[year_col], errors="coerce").dropna().unique())
        opeids = nonblank_values(group[opeid_col]) if opeid_col else []
        opeid6s = nonblank_values(group[opeid6_col]) if opeid6_col else []
        prch_hits: list[str] = []
        for col in prch_cols:
            values = nonblank_values(group[col])
            childish = [value for value in values if value not in {"0", "1"}]
            if childish:
                prch_hits.append(f"{col}:{'|'.join(childish)}")
        risk_flags = []
        if len(opeids) > 1:
            risk_flags.append("multi_opeid")
        if len(opeid6s) > 1:
            risk_flags.append("multi_opeid6")
        if prch_hits:
            risk_flags.append("parent_child_flag_observed")
        if has_internal_gap(observed_years):
            risk_flags.append("internal_year_gap")
        if len(observed_years) == 1:
            risk_flags.append("single_year_observation")
        rows.append(
            {
                "UNITID": unitid,
                "first_year": min(observed_years) if observed_years else "",
                "last_year": max(observed_years) if observed_years else "",
                "years_observed_count": len(observed_years),
                "years_observed": "|".join(str(year) for year in observed_years),
                "has_internal_gap": has_internal_gap(observed_years),
                "opeid_values": "|".join(opeids),
                "opeid_count": len(opeids),
                "opeid6_values": "|".join(opeid6s),
                "opeid6_count": len(opeid6s),
                "prch_review_values": ";".join(prch_hits),
                "join_risk_flags": "|".join(risk_flags),
                "join_risk_level": "review" if risk_flags else "standard",
            }
        )

    crosswalk = pd.DataFrame(rows).sort_values(["join_risk_level", "UNITID"], ascending=[False, True])
    crosswalk.to_csv(out_dir / "entity_continuity_crosswalk.csv", index=False)
    summary = pd.DataFrame(
        [
            {"metric": "unitids", "value": len(crosswalk)},
            {"metric": "review_unitids", "value": int((crosswalk["join_risk_level"] == "review").sum())},
            {"metric": "multi_opeid_unitids", "value": int((crosswalk["opeid_count"] > 1).sum())},
            {"metric": "multi_opeid6_unitids", "value": int((crosswalk["opeid6_count"] > 1).sum())},
            {"metric": "parent_child_flag_unitids", "value": int(crosswalk["prch_review_values"].astype(str).ne("").sum())},
            {"metric": "internal_gap_unitids", "value": int(crosswalk["has_internal_gap"].sum())},
        ]
    )
    summary.to_csv(out_dir / "entity_continuity_summary.csv", index=False)
    (out_dir / "entity_continuity_summary.md").write_text(render_summary(summary), encoding="utf-8")
    print(f"Wrote entity continuity outputs to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
