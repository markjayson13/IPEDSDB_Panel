#!/usr/bin/env python3
"""Independently compare every 2024 scalar source cell and PRCH cleaning action.

This validator reads physical CSVs and the two wide outputs directly. It does
not call harmonization, pivot, or cleaning implementation functions and does
not take pipeline QC success as proof of data equality.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


def digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def equal_cells(left: pd.Series, right: pd.Series) -> pd.Series:
    return (left.isna() & right.isna()) | left.eq(right).fillna(False)


def checked_panel(path: Path) -> pd.DataFrame:
    data = pd.read_parquet(path)
    if data[["UNITID", "year"]].isna().any().any() or data.duplicated(["UNITID", "year"]).any():
        raise ValueError(f"Missing or duplicate institution-year key: {path}")
    if set(data.year) != {2024}:
        raise ValueError(f"Expected only year 2024: {path}")
    return data.set_index("UNITID").sort_index()


def source_values(values: pd.Series, logical_type: str) -> pd.Series:
    original = values.astype(str)
    missing = original.str.strip().eq("")
    # The preserved wide contract trims ordinary spaces, while retaining tabs
    # and newlines in nonblank free text. A wholly whitespace token is missing.
    values = original.str.strip(" ").mask(missing, "")
    if logical_type == "float":
        # Python float performs a correctly rounded conversion independently of
        # the pipeline's DuckDB cast. Negative source codes stay negative.
        result = values.map(lambda value: float(value) if value else np.nan)
        if not np.isfinite(result.dropna()).all():
            raise ValueError("Nonfinite numeric source token")
        return result
    if logical_type in {"categorical string", "free text", "identifier string"}:
        return values.replace("", None)
    raise ValueError(f"Unreviewed 2024 logical type: {logical_type}")


def validate(source_root: Path, wide_path: Path, clean_path: Path, mapping_path: Path,
             policy_path: Path, actions_path: Path) -> dict:
    mapping = pd.read_csv(mapping_path, dtype=str, keep_default_na=False)
    if mapping.analysis_column.duplicated().any() or mapping.variable_id.duplicated().any():
        raise ValueError("Ambiguous source-to-analysis mapping")
    wide, clean = checked_panel(wide_path), checked_panel(clean_path)
    columns = set(mapping.analysis_column) | {"year"}
    if set(wide) != columns or set(clean) != columns:
        raise ValueError("Wide schema does not exactly match reviewed scalar mappings")
    if not wide.index.equals(clean.index):
        raise ValueError("Cleaning changed the institution-year spine")
    year_dir = source_root / "Raw_Access_Databases/2024"
    inventory = pd.read_csv(year_dir / "metadata/table_inventory.csv", dtype=str, keep_default_na=False)
    inventory["table_key"] = inventory.table_name.str.upper().str.strip()
    if inventory.table_key.duplicated().any():
        raise ValueError("Duplicate physical table in inventory")
    inventory = inventory.set_index("table_key")
    source_receipts, pell = [], []
    source_ids, compared_cells, reported_cells = set(), 0, 0
    for table, rows in mapping.groupby("access_table_name", sort=True):
        path = year_dir / inventory.loc[table, "csv_path"]
        raw = pd.read_csv(path, dtype=str, keep_default_na=False)
        raw.columns = raw.columns.str.upper().str.strip()
        raw = raw.drop_duplicates()
        raw["UNITID"] = raw.UNITID.str.strip()
        if not raw.UNITID.str.fullmatch(r"[0-9]+").all():
            raise ValueError(f"Invalid raw institution key: {table}")
        raw["UNITID"] = raw.UNITID.astype("int64")
        if raw.UNITID.duplicated().any():
            raise ValueError(f"Conflicting raw scalar rows: {table}")
        raw = raw.set_index("UNITID")
        if not set(raw.index) <= set(wide.index):
            raise ValueError(f"Source institutions were dropped: {table}")
        source_ids.update(raw.index)
        for row in rows.itertuples():
            expected = source_values(raw[row.varname], row.logical_type).reindex(wide.index)
            actual = wide[row.analysis_column]
            same = equal_cells(expected, actual)
            if not same.all():
                bad = same.index[~same][:5].tolist()
                raise ValueError(f"Raw-to-wide mismatch: {table}.{row.varname} -> {row.analysis_column}; UNITID={bad}")
            compared_cells += len(expected)
            reported_cells += int(expected.notna().sum())
            if row.varname in {"UPGRNTN", "UPGRNTT"}:
                pell.append({"variable": row.varname, "physical_table": table,
                             "source_rows": len(raw), "nonmissing": int(expected.notna().sum()),
                             "missing_in_panel": int(expected.isna().sum()), "source_sum": float(expected.sum()),
                             "raw_to_wide_discrepancies": 0})
        source_receipts.append({"table": table, "rows": len(raw), "mapped_columns": len(rows),
                                "path": str(path.resolve()), "sha256": digest(path)})
    if source_ids != set(wide.index):
        raise ValueError("Wide institution spine differs from the union of scalar source institutions")

    # Independently implement the reviewed equality predicates and exact target
    # lists. No component-prefix expansion or implicit finance form is allowed.
    rules = pd.read_csv(policy_path, dtype=str, keep_default_na=False)
    rules = rules[(rules.year_start.astype(int) <= 2024) & (rules.year_end.astype(int) >= 2024)]
    expected_clean = wide.copy()
    coverage = {flag: pd.Series(0, index=wide.index) for flag in wide if flag.startswith("PRCH")}
    expected_actions = {}
    for rule in rules.itertuples():
        if rule.flag not in coverage:
            raise ValueError(f"Policy references absent flag: {rule.flag}")
        mask = pd.to_numeric(wide[rule.flag], errors="raise").eq(int(rule.code))
        for column, value in json.loads(rule.row_predicate).items():
            if isinstance(value, (int, float)):
                mask &= pd.to_numeric(wide[column], errors="raise").eq(value)
            else:
                mask &= wide[column].eq(value).fillna(False)
        coverage[rule.flag] += mask.astype(int)
        if rule.action == "retain":
            continue
        if rule.action != "null":
            raise ValueError(f"Unknown policy action: {rule.action}")
        for column in filter(None, rule.target_columns.split("|")):
            if column not in wide or column in {"UNITID", "year"} or column.startswith("PRCH"):
                raise ValueError(f"Invalid exact cleaning target: {column}")
            changed = mask & wide[column].notna()
            for unitid in wide.index[changed]:
                key = (int(unitid), column)
                if key in expected_actions:
                    raise ValueError(f"Overlapping cleaning rules at {key}")
                expected_actions[key] = rule.rule_id
            expected_clean.loc[mask, column] = None
    for flag, counts in coverage.items():
        observed = wide[flag].notna() & wide[flag].astype(str).str.strip().ne("")
        if not counts[observed].eq(1).all() or not counts[~observed].eq(0).all():
            raise ValueError(f"Uncovered or multiply classified PRCH observation: {flag}")
    for column in wide:
        if not equal_cells(expected_clean[column], clean[column]).all():
            raise ValueError(f"Unexpected cleaning value or missingness change: {column}")

    actions = pd.read_parquet(actions_path)
    if actions.duplicated(["UNITID", "column"]).any():
        raise ValueError("Duplicate cleaning action ledger cell")
    actual_actions = {(int(row.UNITID), row.column): row.rule_id for row in actions.itertuples()}
    if actual_actions != expected_actions:
        raise ValueError("Cleaning action ledger differs from independently resolved policy")
    types = mapping.set_index("analysis_column").logical_type
    for row in actions.itertuples():
        before = source_values(pd.Series([row.old_value]), types[row.column]).iloc[0]
        if row.year != 2024 or row.action != "null" or pd.notna(row.new_value):
            raise ValueError("Invalid cleaning action value/status")
        if before != wide.at[row.UNITID, row.column] or pd.notna(clean.at[row.UNITID, row.column]):
            raise ValueError("Cleaning ledger before/after values differ from panel")
    for item in pell:
        column = mapping.loc[mapping.varname.eq(item["variable"]), "analysis_column"].iloc[0]
        item["cleaning_changes"] = int((~equal_cells(wide[column], clean[column])).sum())
        item["clean_sum"] = float(clean[column].sum())
    files = {"wide": wide_path, "clean": clean_path, "mapping": mapping_path, "policy": policy_path, "actions": actions_path}
    return {"status": "passed", "year": 2024, "rows": len(wide), "columns": len(wide.columns)+1,
            "scalar_variables": len(mapping), "compared_panel_cells_including_missing": compared_cells,
            "reported_source_cells": reported_cells, "raw_to_wide_discrepancies": 0,
            "unexpected_cleaning_changes": 0, "verified_cleaning_actions": len(expected_actions),
            "normalization": "All-whitespace source tokens are missing; trim ordinary surrounding spaces; retain other text; numeric values use finite double conversion; negative codes remain distinct.",
            "pell": pell, "scalar_source_tables": source_receipts,
            "artifacts": {name: {"path": str(path.resolve()), "sha256": digest(path)} for name, path in files.items()}}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ["source-root", "wide", "clean", "mapping", "policy", "actions", "output"]:
        parser.add_argument("--" + field, required=True, type=Path)
    args = parser.parse_args()
    result = validate(args.source_root, args.wide, args.clean, args.mapping, args.policy, args.actions)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: result[key] for key in ["status", "rows", "columns", "scalar_variables", "compared_panel_cells_including_missing", "verified_cleaning_actions", "pell"]}, indent=2))
