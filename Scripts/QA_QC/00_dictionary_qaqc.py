#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


CANONICAL_SOURCE_FILES = {
    "HD",
    "IC",
    "IC_AY",
    "IC_PY",
    "IC_CAMPUSES",
    "IC_PCCAMPUSES",
    "ADM",
    "AL",
    "C_A",
    "C_B",
    "C_C",
    "CDEP",
    "COST",
    "EAP",
    "EFA",
    "EFA_DIST",
    "EFB",
    "EFC",
    "EFCP",
    "EFFY",
    "EFFY_DIST",
    "EFIA",
    "EFIB",
    "EFIC",
    "EFID",
    "F_F",
    "F_FA",
    "F_FA_F",
    "F_FA_G",
    "GR",
    "GR200",
    "GR_PELL_SSL",
    "OM",
    "SAL_A",
    "SAL_A_LT",
    "SAL_B",
    "SAL_FACULTY",
    "SAL_IS",
    "S_ABD",
    "S_CN",
    "S_F",
    "S_G",
    "S_IS",
    "S_NH",
    "S_OC",
    "S_SIS",
    "SFA",
    "SFAV",
    "DRVADM",
    "DRVAL",
    "DRVC",
    "DRVEF",
    "DRVEF12",
    "DRVF",
    "DRVGR",
    "DRVHR",
    "DRVIC",
    "DRVOM",
    "KEYS",
}


def parse_args() -> argparse.Namespace:
    default_root = Path("/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=str(default_root), help="External IPEDSDB_ROOT")
    return ap.parse_args()


def years_text(series: pd.Series) -> str:
    vals = sorted({str(int(v)) for v in series.dropna().tolist()})
    return "'" + "|".join(vals) if vals else ""


def normalize_varnumber(value: object) -> str:
    txt = str(value or "").strip()
    if txt.lower() in {"", "nan", "none", "<na>", "na", "nat"}:
        return ""
    return txt.zfill(8) if txt.isdigit() else txt


def main() -> None:
    args = parse_args()
    root = Path(args.root)
    checks_dir = root / "Checks" / "dictionary_qc"
    checks_dir.mkdir(parents=True, exist_ok=True)

    lake_path = root / "Dictionary" / "dictionary_lake.parquet"
    codes_path = root / "Dictionary" / "dictionary_codes.parquet"
    inventory_all_path = root / "Checks" / "extract_qc" / "table_inventory_all_years.csv"
    candidate_path = checks_dir / "dictionary_metadata_candidates.csv"

    if not lake_path.exists():
        raise SystemExit(f"Missing dictionary lake: {lake_path}")
    lake = pd.read_parquet(lake_path)
    for col in (
        "year",
        "source_file",
        "varnumber",
        "varname",
        "longDescription",
        "metadata_source",
        "access_table_name",
    ):
        if col not in lake.columns:
            lake[col] = ""
    lake["varnumber"] = lake["varnumber"].map(normalize_varnumber)
    lake["source_file"] = lake["source_file"].fillna("").astype(str).str.strip()
    lake["varname"] = lake["varname"].fillna("").astype(str).str.upper().str.strip()
    lake["access_table_name"] = lake["access_table_name"].fillna("").astype(str).str.strip()

    duplicates = lake.groupby(["year", "source_file", "varnumber", "varname"]).size().reset_index(name="n_rows")
    duplicates = duplicates[duplicates["n_rows"] > 1].copy()
    duplicates.to_csv(checks_dir / "dictionary_duplicates.csv", index=False)

    source_conflicts = (
        lake[lake["access_table_name"] != ""]
        .groupby(["year", "access_table_name"])["source_file"]
        .agg(lambda s: sorted(set(x for x in s if x)))
        .reset_index(name="source_files")
    )
    source_conflicts["n_source_files"] = source_conflicts["source_files"].str.len()
    source_conflicts = source_conflicts[source_conflicts["n_source_files"] > 1].copy()
    source_conflicts["source_files"] = source_conflicts["source_files"].map("|".join)
    source_conflicts.to_csv(checks_dir / "source_file_conflicts.csv", index=False)

    missing_desc = (
        lake.groupby(["year", "source_file"], as_index=False)
        .agg(
            rows=("varname", "size"),
            missing_long_description=("longDescription", lambda s: int((s.fillna("").astype(str).str.strip() == "").sum())),
        )
    )
    missing_desc["missing_long_description_rate"] = missing_desc["missing_long_description"] / missing_desc["rows"]
    missing_desc.to_csv(checks_dir / "missing_descriptions_by_source_year.csv", index=False)

    source_years = (
        lake.groupby("source_file", as_index=False)
        .agg(
            years=("year", years_text),
            rows=("varname", "size"),
        )
        .sort_values("source_file")
    )
    source_years.to_csv(checks_dir / "source_file_years.csv", index=False)

    collisions = (
        lake.groupby(["year", "source_file", "varnumber"])["varname"]
        .agg(lambda s: sorted(set(v for v in s if v)))
        .reset_index(name="varnames")
    )
    collisions["n_varnames"] = collisions["varnames"].str.len()
    collisions = collisions[collisions["n_varnames"] > 1].copy()
    collisions["varnames"] = collisions["varnames"].map("|".join)
    collisions.to_csv(checks_dir / "varnumber_varname_collisions.csv", index=False)

    unmapped_rows = lake[~lake["source_file"].isin(CANONICAL_SOURCE_FILES)].copy()
    unmapped_rows.to_csv(checks_dir / "unmapped_metadata_tables.csv", index=False)

    if inventory_all_path.exists():
        inventory_all = pd.read_csv(inventory_all_path, dtype=str).fillna("")
        inventory_all["normalized_table_name"] = inventory_all["normalized_table_name"].fillna("").astype(str)
        metadata_inventory = inventory_all[inventory_all["table_role"].str.startswith("metadata")].copy()
        inventory_unmapped = metadata_inventory[~metadata_inventory["normalized_table_name"].isin(CANONICAL_SOURCE_FILES)].copy()
        inventory_unmapped.to_csv(checks_dir / "unmapped_inventory_metadata_tables.csv", index=False)

    if candidate_path.exists():
        candidate_df = pd.read_csv(candidate_path, dtype=str).fillna("")
        candidate_df.to_csv(checks_dir / "dictionary_metadata_candidates_snapshot.csv", index=False)

    summary_rows = [
        {
            "lake_rows": len(lake),
            "lake_source_files": lake["source_file"].replace("", pd.NA).nunique(dropna=True),
            "duplicate_rows": len(duplicates),
            "source_file_conflicts": len(source_conflicts),
            "varnumber_collisions": len(collisions),
            "unmapped_rows": len(unmapped_rows),
        }
    ]
    if codes_path.exists():
        codes = pd.read_parquet(codes_path)
        summary_rows[0]["code_rows"] = len(codes)
    pd.DataFrame(summary_rows).to_csv(checks_dir / "dictionary_qaqc_summary.csv", index=False)
    print(f"Wrote dictionary QA/QC to {checks_dir}")


if __name__ == "__main__":
    main()
