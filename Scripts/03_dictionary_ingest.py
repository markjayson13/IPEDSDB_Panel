#!/usr/bin/env python3
"""
Stage 03: build stitched metadata dictionaries from extracted Access metadata.

Reads:
- yearly `metadata/table_inventory.csv`
- exported metadata CSV tables under `Raw_Access_Databases/<year>/tables_csv/`

Writes:
- `Dictionary/dictionary_lake.parquet` and `.csv`
- `Dictionary/dictionary_codes.parquet` and `.csv`
- `Checks/dictionary_qc/*`
"""
from __future__ import annotations

import argparse
import csv
import zlib
from pathlib import Path

import pandas as pd

from access_build_utils import (
    DATA_TABLE_CANDIDATES,
    IMPUTATION_CANDIDATES,
    LONG_DESC_CANDIDATES,
    VALUE_LABEL_CANDIDATES,
    VAR_NAME_CANDIDATES,
    VAR_NUMBER_CANDIDATES,
    VAR_TITLE_CANDIDATES,
    CODE_VALUE_CANDIDATES,
    canonical_source_file,
    clean_source_label,
    can_serve_metadata_role_from_capabilities,
    ensure_data_layout,
    normalize_text_key,
    normalize_varnumber,
    parse_years,
    pick_column,
)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=None, help="External IPEDSDB_ROOT")
    ap.add_argument("--years", default="2004:2023")
    return ap.parse_args()


def load_manifest_year_info(year_dir: Path) -> dict[str, str]:
    manifest = pd.read_csv(year_dir / "manifest.csv", dtype=str).fillna("")
    if manifest.empty:
        raise SystemExit(f"Empty manifest: {year_dir / 'manifest.csv'}")
    return manifest.iloc[0].to_dict()


def choose_candidates(inventory: pd.DataFrame, role: str) -> pd.DataFrame:
    def row_capabilities(row: pd.Series) -> dict[str, bool]:
        return {
            "has_varnumber": str(row.get("has_varnumber", "")).lower() in {"true", "1"},
            "has_varname": str(row.get("has_varname", "")).lower() in {"true", "1"},
            "has_vartitle": str(row.get("has_vartitle", "")).lower() in {"true", "1"},
            "has_longdesc": str(row.get("has_longdesc", "")).lower() in {"true", "1"},
            "has_code": str(row.get("has_codevalue", "")).lower() in {"true", "1"},
            "has_label": str(row.get("has_valuelabel", "")).lower() in {"true", "1"},
            "has_imputation": str(row.get("has_imputation_markers", "")).lower() in {"true", "1"},
        }

    capability_mask = inventory.apply(lambda row: can_serve_metadata_role_from_capabilities(role, row_capabilities(row)), axis=1)
    subset = inventory[capability_mask | (inventory["table_role"] == role)].copy()
    if subset.empty:
        return subset

    def score_row(row: pd.Series) -> tuple[int, int, str]:
        name = normalize_text_key(row["table_name"])
        score = 0
        if row.get("table_role") == role:
            score += 100
        if role == "metadata_varlist" and any(tok in name for tok in ("varlist", "layout", "variable")):
            score += 20
        if role == "metadata_description" and any(tok in name for tok in ("descript", "vartable", "layout")):
            score += 20
        if role == "metadata_codes" and any(tok in name for tok in ("valueset", "frequenc", "code", "label")):
            score += 20
        if role == "metadata_imputation" and "imput" in name:
            score += 20
        return (score, int(row.get("row_count_csv", 0) or 0), str(row["table_name"]))

    scored = sorted(subset.to_dict("records"), key=score_row, reverse=True)
    return pd.DataFrame(scored)


def normalize_access_table_name(raw_value: str, metadata_table_name: str) -> str:
    value = str(raw_value or "").strip()
    if value:
        return value
    return metadata_table_name


def append_synthetic_imputation_rows(lake: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if lake.empty:
        return lake, 0
    df = lake.copy()
    df["varname"] = df["varname"].fillna("").astype(str).str.upper().str.strip()
    df["imputationvar"] = df["imputationvar"].fillna("").astype(str).str.upper().str.strip()
    df.loc[df["imputationvar"].isin({"", "NAN", "NONE", "<NA>", "NAT"}), "imputationvar"] = ""
    ref = df[df["imputationvar"] != ""].copy()
    if ref.empty:
        return df, 0

    existing = set(
        zip(
            pd.to_numeric(df["year"], errors="coerce").fillna(-1).astype(int),
            df["source_file"].fillna("").astype(str),
            df["varname"].fillna("").astype(str).str.upper(),
        )
    )
    used_varnumbers = {
        normalize_varnumber(v)
        for v in df["varnumber"].fillna("").astype(str)
        if normalize_varnumber(v) not in {"", "00000000"}
    }

    rows = []
    grouped = (
        ref.groupby(["year", "source_file", "imputationvar"], as_index=False)
        .agg(
            anchor_varnumber=("varnumber", "first"),
            access_table_name=("access_table_name", "first"),
            academic_year_label=("academic_year_label", "first"),
            release_type=("release_type", "first"),
        )
    )
    for rec in grouped.to_dict("records"):
        key = (int(rec["year"]), str(rec["source_file"]), str(rec["imputationvar"]).upper())
        if key in existing:
            continue
        anchor = normalize_varnumber(rec.get("anchor_varnumber", ""))
        crc = zlib.crc32("|".join(map(str, key)).encode("utf-8")) & 0xFFFFFFFF
        if anchor.isdigit() and anchor not in {"", "00000000"}:
            start = int(anchor) + 1 + int(crc % 11)
        else:
            start = 85_000_000 + int(crc % 10_000_000)
        while True:
            candidate = f"{start:08d}"
            if candidate not in used_varnumbers:
                used_varnumbers.add(candidate)
                break
            start += 1
        rows.append(
            {
                "year": int(rec["year"]),
                "varnumber": candidate,
                "varname": str(rec["imputationvar"]).upper(),
                "varTitle": f"Imputation flag for {rec['imputationvar']}",
                "longDescription": "Synthetic dictionary row generated from imputationvar mapping.",
                "DataType": "",
                "format": "",
                "Fieldwidth": "",
                "imputationvar": "",
                "source_file": rec["source_file"],
                "source_file_label": rec["source_file"],
                "access_table_name": rec["access_table_name"],
                "metadata_table_name": "",
                "academic_year_label": rec["academic_year_label"],
                "release_type": rec["release_type"],
                "metadata_source": "synthetic_imputation",
            }
        )

    if not rows:
        return df, 0
    out = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
    out = out.drop_duplicates(subset=["year", "source_file", "varnumber", "varname"]).reset_index(drop=True)
    return out, len(rows)


def append_unitid_metadata_rows(lake: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if lake.empty:
        return lake, 0
    df = lake.copy()
    years = sorted(pd.to_numeric(df["year"], errors="coerce").dropna().astype(int).unique().tolist())
    existing = set(
        zip(
            pd.to_numeric(df["year"], errors="coerce").fillna(-1).astype(int),
            df["source_file"].fillna("").astype(str),
            df["varname"].fillna("").astype(str).str.upper(),
        )
    )
    rows = []
    for year in years:
        key = (year, "KEYS", "UNITID")
        if key in existing:
            continue
        academic_year_label = f"{year}-{(year + 1) % 100:02d}"
        rows.append(
            {
                "year": year,
                "varnumber": "00000001",
                "varname": "UNITID",
                "varTitle": "Institution identifier (panel key)",
                "longDescription": "IPEDS institution identifier used as the panel key. This row is metadata-only.",
                "DataType": "integer",
                "format": "",
                "Fieldwidth": "",
                "imputationvar": "",
                "source_file": "KEYS",
                "source_file_label": "Panel key metadata",
                "access_table_name": "KEYS",
                "metadata_table_name": "KEYS",
                "academic_year_label": academic_year_label,
                "release_type": "Final",
                "metadata_source": "synthetic_unitid",
            }
        )
    if not rows:
        return df, 0
    out = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
    out = out.drop_duplicates(subset=["year", "source_file", "varnumber", "varname"]).reset_index(drop=True)
    return out, len(rows)


def append_nearest_year_source_backfill_rows(
    lake: pd.DataFrame,
    *,
    layout,
    years: list[int],
    source_file: str,
    metadata_source: str,
) -> tuple[pd.DataFrame, int]:
    if lake.empty:
        return lake, 0

    df = lake.copy()
    source_norm = str(source_file or "").strip().upper()
    donor_pool = df[
        (df["source_file"].fillna("").astype(str).str.upper() == source_norm)
        & (~df["metadata_source"].fillna("").astype(str).str.startswith("synthetic_"))
    ].copy()
    if donor_pool.empty:
        return df, 0

    donor_years = sorted(pd.to_numeric(donor_pool["year"], errors="coerce").dropna().astype(int).unique().tolist())
    if not donor_years:
        return df, 0

    rows: list[pd.DataFrame] = []
    total_rows = 0
    for year in years:
        has_source_rows = not df[
            (pd.to_numeric(df["year"], errors="coerce") == year)
            & (df["source_file"].fillna("").astype(str).str.upper() == source_norm)
        ].empty
        if has_source_rows:
            continue

        year_dir = layout.raw_access / str(year)
        inventory_path = year_dir / "metadata" / "table_inventory.csv"
        manifest_path = year_dir / "manifest.csv"
        if not inventory_path.exists() or not manifest_path.exists():
            continue

        inventory = pd.read_csv(inventory_path, dtype=str).fillna("")
        data_match = inventory[
            (inventory["table_role"].astype(str).str.lower() == "data")
            & (inventory["normalized_table_name"].fillna("").astype(str).str.upper() == source_norm)
        ].copy()
        if data_match.empty:
            continue
        data_match["row_count_csv_num"] = pd.to_numeric(data_match["row_count_csv"], errors="coerce").fillna(0).astype(int)
        data_match = data_match[data_match["row_count_csv_num"] > 0].copy()
        if data_match.empty:
            continue

        donor_year = min(donor_years, key=lambda donor: (abs(donor - year), donor))
        donor_rows = donor_pool[pd.to_numeric(donor_pool["year"], errors="coerce") == donor_year].copy()
        if donor_rows.empty:
            continue

        manifest_info = load_manifest_year_info(year_dir)
        access_table_name = str(data_match.sort_values(["row_count_csv_num", "table_name"], ascending=[False, True]).iloc[0]["table_name"])
        donor_rows["year"] = year
        donor_rows["source_file_label"] = clean_source_label(access_table_name)
        donor_rows["access_table_name"] = access_table_name
        donor_rows["metadata_table_name"] = metadata_source
        donor_rows["academic_year_label"] = manifest_info["academic_year_label"]
        donor_rows["release_type"] = manifest_info["release_type"]
        donor_rows["metadata_source"] = metadata_source
        rows.append(donor_rows)
        total_rows += len(donor_rows)

    if not rows:
        return df, 0
    out = pd.concat([df] + rows, ignore_index=True)
    out = out.drop_duplicates(subset=["year", "source_file", "varnumber", "varname"], keep="first").reset_index(drop=True)
    return out, total_rows


def build_description_maps(desc_candidates: pd.DataFrame, year_dir: Path) -> dict[tuple[str, str], str]:
    desc_map: dict[tuple[str, str], str] = {}
    for rec in desc_candidates.to_dict("records"):
        csv_path = year_dir / rec["csv_path"]
        if not csv_path.exists():
            continue
        df = pd.read_csv(csv_path, dtype=str).fillna("")
        varnum_col = pick_column(df.columns, VAR_NUMBER_CANDIDATES)
        varname_col = pick_column(df.columns, VAR_NAME_CANDIDATES)
        access_table_col = pick_column(df.columns, DATA_TABLE_CANDIDATES)
        long_col = pick_column(df.columns, LONG_DESC_CANDIDATES)
        if long_col is None:
            continue
        for row in df.to_dict("records"):
            table_name = normalize_access_table_name(row.get(access_table_col, ""), rec["table_name"])
            key_pairs = [
                (normalize_text_key(table_name), normalize_varnumber(row.get(varnum_col, ""))),
                (normalize_text_key(table_name), str(row.get(varname_col, "")).strip().upper()),
                ("", normalize_varnumber(row.get(varnum_col, ""))),
                ("", str(row.get(varname_col, "")).strip().upper()),
            ]
            value = str(row.get(long_col, "")).strip()
            if not value:
                continue
            for key in key_pairs:
                if key[1] and key not in desc_map:
                    desc_map[key] = value
    return desc_map


def main() -> None:
    args = parse_args()
    layout = ensure_data_layout(args.root)
    dictionary_qc_dir = layout.checks / "dictionary_qc"
    dictionary_qc_dir.mkdir(parents=True, exist_ok=True)
    years = parse_years(args.years)
    candidate_rows: list[dict] = []
    lake_rows: list[dict] = []
    code_rows: list[dict] = []

    for year in years:
        year_dir = layout.raw_access / str(year)
        inventory_path = year_dir / "metadata" / "table_inventory.csv"
        if not inventory_path.exists():
            raise SystemExit(f"Missing table inventory for year {year}: {inventory_path}")
        manifest_info = load_manifest_year_info(year_dir)
        inventory = pd.read_csv(inventory_path, dtype=str).fillna("")
        varlist_candidates = choose_candidates(inventory, "metadata_varlist")
        desc_candidates = choose_candidates(inventory, "metadata_description")
        code_candidates = choose_candidates(inventory, "metadata_codes")
        imputation_candidates = choose_candidates(inventory, "metadata_imputation")

        for role_name, frame in (
            ("metadata_varlist", varlist_candidates),
            ("metadata_description", desc_candidates),
            ("metadata_codes", code_candidates),
            ("metadata_imputation", imputation_candidates),
        ):
            for order, row in enumerate(frame.to_dict("records"), start=1):
                candidate_rows.append(
                    {
                        "year": year,
                        "role": role_name,
                        "candidate_rank": order,
                        "table_name": row["table_name"],
                        "csv_path": row["csv_path"],
                    }
                )

        desc_map = build_description_maps(desc_candidates, year_dir)

        for rec in varlist_candidates.to_dict("records"):
            csv_path = year_dir / rec["csv_path"]
            if not csv_path.exists():
                continue
            df = pd.read_csv(csv_path, dtype=str).fillna("")
            varnum_col = pick_column(df.columns, VAR_NUMBER_CANDIDATES)
            varname_col = pick_column(df.columns, VAR_NAME_CANDIDATES)
            vartitle_col = pick_column(df.columns, VAR_TITLE_CANDIDATES)
            data_type_col = pick_column(df.columns, {"datatype", "data type", "data_type"})
            format_col = pick_column(df.columns, {"format"})
            fieldwidth_col = pick_column(df.columns, {"fieldwidth", "field width", "field_width", "width"})
            imp_col = pick_column(df.columns, IMPUTATION_CANDIDATES)
            access_table_col = pick_column(df.columns, DATA_TABLE_CANDIDATES)
            if varname_col is None:
                continue
            for row in df.to_dict("records"):
                varname = str(row.get(varname_col, "")).strip().upper()
                if not varname or varname in {"NAN", "NONE", "<NA>", "NAT", "UNITID"}:
                    continue
                varnumber = normalize_varnumber(row.get(varnum_col, "")) if varnum_col else ""
                if varnumber in {"", "00000000"}:
                    continue
                access_table_name = normalize_access_table_name(row.get(access_table_col, ""), rec["table_name"])
                source_file = canonical_source_file(access_table_name)
                long_desc = (
                    desc_map.get((normalize_text_key(access_table_name), varnumber), "")
                    or desc_map.get((normalize_text_key(access_table_name), varname), "")
                    or desc_map.get(("", varnumber), "")
                    or desc_map.get(("", varname), "")
                )
                lake_rows.append(
                    {
                        "year": year,
                        "varnumber": varnumber,
                        "varname": varname,
                        "varTitle": str(row.get(vartitle_col, "")).strip() if vartitle_col else "",
                        "longDescription": long_desc,
                        "DataType": str(row.get(data_type_col, "")).strip() if data_type_col else "",
                        "format": str(row.get(format_col, "")).strip() if format_col else "",
                        "Fieldwidth": str(row.get(fieldwidth_col, "")).strip() if fieldwidth_col else "",
                        "imputationvar": str(row.get(imp_col, "")).strip().upper() if imp_col else "",
                        "source_file": source_file,
                        "source_file_label": clean_source_label(access_table_name),
                        "access_table_name": access_table_name,
                        "metadata_table_name": rec["table_name"],
                        "academic_year_label": manifest_info["academic_year_label"],
                        "release_type": manifest_info["release_type"],
                        "metadata_source": "access_varlist",
                    }
                )

        for rec in code_candidates.to_dict("records"):
            csv_path = year_dir / rec["csv_path"]
            if not csv_path.exists():
                continue
            df = pd.read_csv(csv_path, dtype=str).fillna("")
            varnum_col = pick_column(df.columns, VAR_NUMBER_CANDIDATES)
            varname_col = pick_column(df.columns, VAR_NAME_CANDIDATES)
            code_col = pick_column(df.columns, CODE_VALUE_CANDIDATES)
            label_col = pick_column(df.columns, VALUE_LABEL_CANDIDATES)
            title_col = pick_column(df.columns, VAR_TITLE_CANDIDATES)
            access_table_col = pick_column(df.columns, DATA_TABLE_CANDIDATES)
            if code_col is None or label_col is None:
                continue
            for row in df.to_dict("records"):
                access_table_name = normalize_access_table_name(row.get(access_table_col, ""), rec["table_name"])
                code_rows.append(
                    {
                        "year": year,
                        "varnumber": normalize_varnumber(row.get(varnum_col, "")) if varnum_col else "",
                        "varname": str(row.get(varname_col, "")).strip().upper() if varname_col else "",
                        "codevalue": str(row.get(code_col, "")).strip(),
                        "valuelabel": str(row.get(label_col, "")).strip(),
                        "varTitle": str(row.get(title_col, "")).strip() if title_col else "",
                        "source_file": canonical_source_file(access_table_name),
                        "source_file_label": clean_source_label(access_table_name),
                        "access_table_name": access_table_name,
                        "metadata_table_name": rec["table_name"],
                        "academic_year_label": manifest_info["academic_year_label"],
                        "release_type": manifest_info["release_type"],
                        "source": "access_codes",
                        "is_imputation_label": False,
                        "label_scope": "regular",
                    }
                )

        for rec in imputation_candidates.to_dict("records"):
            csv_path = year_dir / rec["csv_path"]
            if not csv_path.exists():
                continue
            df = pd.read_csv(csv_path, dtype=str).fillna("")
            code_col = pick_column(df.columns, CODE_VALUE_CANDIDATES)
            label_col = pick_column(df.columns, VALUE_LABEL_CANDIDATES)
            access_table_col = pick_column(df.columns, DATA_TABLE_CANDIDATES)
            if code_col is None or label_col is None:
                continue
            for row in df.to_dict("records"):
                access_table_name = normalize_access_table_name(row.get(access_table_col, ""), rec["table_name"])
                code_rows.append(
                    {
                        "year": year,
                        "varnumber": "",
                        "varname": "",
                        "codevalue": str(row.get(code_col, "")).strip(),
                        "valuelabel": str(row.get(label_col, "")).strip(),
                        "varTitle": "",
                        "source_file": canonical_source_file(access_table_name),
                        "source_file_label": clean_source_label(access_table_name),
                        "access_table_name": access_table_name,
                        "metadata_table_name": rec["table_name"],
                        "academic_year_label": manifest_info["academic_year_label"],
                        "release_type": manifest_info["release_type"],
                        "source": "access_imputation",
                        "is_imputation_label": True,
                        "label_scope": "imputation_variable",
                    }
                )

    candidate_df = pd.DataFrame(candidate_rows)
    candidate_df.to_csv(dictionary_qc_dir / "dictionary_metadata_candidates.csv", index=False)

    lake = pd.DataFrame(lake_rows)
    if lake.empty:
        raise SystemExit("Dictionary ingest produced no rows.")
    lake["varname"] = lake["varname"].fillna("").astype(str).str.upper().str.strip()
    lake["imputationvar"] = lake["imputationvar"].fillna("").astype(str).str.upper().str.strip()
    lake.loc[lake["imputationvar"].isin({"NAN", "NONE", "<NA>", "NAT"}), "imputationvar"] = ""
    lake = lake.drop_duplicates(subset=["year", "source_file", "varnumber", "varname"], keep="first").reset_index(drop=True)
    lake, lt9_backfill_count = append_nearest_year_source_backfill_rows(
        lake,
        layout=layout,
        years=years,
        source_file="SAL_A_LT",
        metadata_source="synthetic_backfill_salary_lt9",
    )
    lake, synth_imp_count = append_synthetic_imputation_rows(lake)
    lake, unitid_count = append_unitid_metadata_rows(lake)
    lake.to_parquet(layout.dictionary / "dictionary_lake.parquet", index=False)
    lake.to_csv(layout.dictionary / "dictionary_lake.csv", index=False)

    codes = pd.DataFrame(code_rows)
    if codes.empty:
        codes = pd.DataFrame(
            columns=[
                "year",
                "varnumber",
                "varname",
                "codevalue",
                "valuelabel",
                "varTitle",
                "source_file",
                "source_file_label",
                "access_table_name",
                "metadata_table_name",
                "academic_year_label",
                "release_type",
                "source",
                "is_imputation_label",
                "label_scope",
            ]
        )
    codes = codes.drop_duplicates().reset_index(drop=True)
    codes.to_parquet(layout.dictionary / "dictionary_codes.parquet", index=False)
    codes.to_csv(layout.dictionary / "dictionary_codes.csv", index=False)

    with (dictionary_qc_dir / "synthetic_rows_summary.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "synthetic_salary_lt9_backfill_rows",
                "synthetic_imputation_rows",
                "synthetic_unitid_rows",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "synthetic_salary_lt9_backfill_rows": lt9_backfill_count,
                "synthetic_imputation_rows": synth_imp_count,
                "synthetic_unitid_rows": unitid_count,
            }
        )

    print(f"Wrote dictionary lake rows={len(lake):,}")
    print(f"Wrote dictionary codes rows={len(codes):,}")


if __name__ == "__main__":
    main()
