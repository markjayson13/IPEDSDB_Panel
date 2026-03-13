#!/usr/bin/env python3
"""
Stage 04: convert exported yearly data tables into harmonized long parquet.

Reads:
- yearly exported data CSV tables
- `Dictionary/dictionary_lake.parquet`
- yearly `manifest.csv` and `metadata/table_inventory.csv`

Writes:
- `Cross_sections/panel_long_varnum_<year>.parquet`
- optional `Cross_sections/parts_<year>/part_*.parquet`
- `Checks/harmonize_qc/*`
- `Checks/release_qc/*`

Open this file when you want to understand how exported yearly CSV tables become the harmonized long panel and why a table was selected or excluded.
"""
from __future__ import annotations

import argparse
import csv
import pathlib
import shutil
import sys
from typing import Iterable

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from access_build_utils import ensure_data_layout, normalize_varnumber, parse_years


HARMONIZE_SUMMARY_COLUMNS = [
    "year",
    "table_name",
    "source_file",
    "selected",
    "exclude_reason",
    "matched_cols",
    "output_rows",
    "dictionary_rows",
    "source_match_rows",
    "access_table_match_rows",
]

MISSING_UNITID_COLUMNS = [
    "year",
    "file",
    "stage",
    "dropped_rows_missing_UNITID",
    "rows_before",
    "rows_after",
]


def setup_logging(log_path: str | None) -> None:
    if not log_path:
        return
    log_file = pathlib.Path(log_path)
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
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=None, help="External IPEDSDB_ROOT")
    ap.add_argument("--years", default="2004:2023")
    ap.add_argument("--output-dir", default=None)
    ap.add_argument("--parts-dir-base", default=None)
    ap.add_argument("--chunksize", type=int, default=50_000)
    ap.add_argument("--value-cols-per-chunk", type=int, default=250)
    ap.add_argument("--dedupe", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--final-dedupe", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--dedupe-priority", default="HD,IC,IC_AY,IC_PY,ADM,AL,C_A,C_B,C_C,CDEP,COST,EAP,EFA,EFA_DIST,EFB,EFC,EFCP,EFFY,EFFY_DIST,EFIA,F_F,F_FA,F_FA_F,F_FA_G,GR,GR200,GR_PELL_SSL,OM,SAL_A,SAL_A_LT,SAL_B,SAL_FACULTY,SAL_IS,S_ABD,S_CN,S_F,S_G,S_IS,S_NH,S_OC,S_SIS,SFA,SFAV")
    ap.add_argument("--dedupe-temp-dir", default=None, help="Optional temp directory for DuckDB dedupe spill files")
    ap.add_argument("--dedupe-max-temp-gib", type=int, default=None, help="Optional DuckDB max temp directory size in GiB")
    ap.add_argument("--dedupe-threads", type=int, default=1, help="DuckDB threads to use during final dedupe")
    ap.add_argument("--release-strict", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--log-file", default=None)
    return ap.parse_args()


def read_table_iter(fp: pathlib.Path, chunksize: int = 50_000):
    attempts = (
        dict(dtype=str, low_memory=False, index_col=False, chunksize=chunksize),
        dict(dtype=str, engine="python", on_bad_lines="skip", index_col=False, chunksize=chunksize),
        dict(dtype=str, engine="python", encoding="latin1", on_bad_lines="skip", index_col=False, chunksize=chunksize),
    )
    last_err = None
    for kwargs in attempts:
        try:
            reader = pd.read_csv(fp, **kwargs)
            first = next(reader, None)
            if first is None:
                return
            cols = [str(c).strip().upper() for c in first.columns]
            if "UNITID" in cols:
                unitid_col = first.columns[cols.index("UNITID")]
                if pd.to_numeric(first[unitid_col], errors="coerce").notna().sum() == 0:
                    raise ValueError("suspicious parse: UNITID present but no numeric values in first chunk")
            yield first
            for chunk in reader:
                yield chunk
            return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            continue
    print(f"[warn] failed to read {fp}: {last_err}")
    return


def chunk_cols(cols: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(cols), size):
        yield cols[i : i + size]


def select_dict_source(dict_year: pd.DataFrame, source_file: str, access_table_name: str) -> pd.DataFrame:
    source_norm = str(source_file or "").strip().upper()
    access_norm = str(access_table_name or "").strip().upper()
    subset = dict_year[
        (dict_year["source_file"].fillna("").astype(str).str.upper() == source_norm)
        | (dict_year["access_table_name"].fillna("").astype(str).str.upper() == access_norm)
    ].copy()
    if subset.empty and source_norm:
        subset = dict_year[dict_year["source_file"].fillna("").astype(str).str.upper() == source_norm].copy()
    if subset.empty:
        return subset
    subset["metadata_source"] = subset["metadata_source"].fillna("").astype(str)
    subset = subset.sort_values(["varname", "metadata_source", "metadata_table_name", "varnumber"]).drop_duplicates(["varname"], keep="first")
    return subset


def write_parquet_parts(out_path: pathlib.Path, frames: Iterable[pd.DataFrame], parts_dir: pathlib.Path) -> None:
    if parts_dir.exists():
        shutil.rmtree(parts_dir)
    parts_dir.mkdir(parents=True, exist_ok=True)
    idx = 0
    for chunk in frames:
        if chunk.empty:
            continue
        pq.write_table(pa.Table.from_pandas(chunk, preserve_index=False), parts_dir / f"part_{idx:05d}.parquet", compression="snappy")
        idx += 1
    if idx == 0:
        return
    tmp_out = out_path.with_suffix(out_path.suffix + ".tmp")
    if tmp_out.exists():
        tmp_out.unlink()
    writer = None
    for part in sorted(parts_dir.glob("part_*.parquet")):
        pf = pq.ParquetFile(part)
        for batch in pf.iter_batches():
            if writer is None:
                writer = pq.ParquetWriter(tmp_out, batch.schema, compression="snappy")
            writer.write_batch(batch)
    if writer:
        writer.close()
        tmp_out.replace(out_path)


def sql_quote(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def empty_missing_unitid_frame(rows: list[dict] | None = None) -> pd.DataFrame:
    return pd.DataFrame(rows or [], columns=MISSING_UNITID_COLUMNS)


def empty_harmonize_summary_frame(rows: list[dict] | None = None) -> pd.DataFrame:
    return pd.DataFrame(rows or [], columns=HARMONIZE_SUMMARY_COLUMNS)


def dictionary_match_counts(dict_year: pd.DataFrame, source_file: str, access_table_name: str) -> tuple[int, int]:
    source_norm = str(source_file or "").strip().upper()
    access_norm = str(access_table_name or "").strip().upper()
    source_match_rows = 0
    access_match_rows = 0
    if source_norm:
        source_match_rows = int(
            (
                dict_year["source_file"].fillna("").astype(str).str.upper() == source_norm
            ).sum()
        )
    if access_norm:
        access_match_rows = int(
            (
                dict_year["access_table_name"].fillna("").astype(str).str.upper() == access_norm
            ).sum()
        )
    return source_match_rows, access_match_rows


def classify_exclude_reason(
    dict_source: pd.DataFrame,
    source_match_rows: int,
    access_match_rows: int,
    *,
    source_row_count: int = 0,
) -> str:
    if int(source_row_count) == 0:
        return "source_table_has_zero_rows"
    if not dict_source.empty:
        return "dictionary_rows_found_no_column_overlap"
    if access_match_rows > 0:
        return "dictionary_rows_found_for_access_table_but_source_selection_failed"
    if source_match_rows > 0:
        return "dictionary_rows_found_for_source_file_but_access_table_missing"
    if source_match_rows == 0:
        return "missing_dictionary_rows_for_source_file"
    return "no_dictionary_source_match"


def dedupe_long_panel(
    out_path: pathlib.Path,
    priority_list: list[str],
    *,
    temp_dir: pathlib.Path,
    max_temp_gib: int | None = None,
    threads: int = 1,
) -> None:
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(f"PRAGMA temp_directory={sql_quote(str(temp_dir))}")
        con.execute(f"SET threads={max(int(threads), 1)}")
        con.execute("SET preserve_insertion_order=false")
        if max_temp_gib is None:
            free_bytes = shutil.disk_usage(temp_dir).free
            max_temp_gib = max(1, int((free_bytes * 0.9) // (1024**3)))
        con.execute(f"SET max_temp_directory_size='{int(max_temp_gib)}GiB'")

        priority_list = [src.strip().upper() for src in priority_list if src.strip()]
        case = "CASE"
        for i, src in enumerate(priority_list, start=1):
            src_esc = src.replace("'", "''")
            case += f" WHEN UPPER(source_file) = '{src_esc}' THEN {i}"
        case += " ELSE 999 END"

        tmp_path = out_path.with_suffix(out_path.suffix + ".dedupe.tmp")
        if tmp_path.exists():
            tmp_path.unlink()
        parts_dir = temp_dir / "dedupe_parts"
        parts_dir.mkdir(parents=True, exist_ok=True)

        source_files = [
            row[0]
            for row in con.execute(
                f"""
                SELECT DISTINCT COALESCE(CAST(source_file AS VARCHAR), '') AS source_file
                FROM read_parquet({sql_quote(str(out_path))})
                ORDER BY 1
                """
            ).fetchall()
        ]

        print(
            f"[dedupe] start file={out_path.name} source_files={len(source_files)} "
            f"temp_dir={temp_dir} max_temp={max_temp_gib}GiB threads={max(int(threads), 1)}"
        )
        for idx, source_file in enumerate(source_files, start=1):
            part_path = parts_dir / f"part_{idx:05d}.parquet"
            source_sql = sql_quote(source_file)
            con.execute(
                f"""
                COPY (
                    WITH filtered AS (
                        SELECT *
                        FROM read_parquet({sql_quote(str(out_path))})
                        WHERE COALESCE(CAST(source_file AS VARCHAR), '') = {source_sql}
                    ),
                    ranked AS (
                        SELECT *,
                               ROW_NUMBER() OVER (
                                   PARTITION BY UNITID, year, varnumber, source_file
                                   ORDER BY {case}, access_table_name, varname
                               ) AS _rn
                        FROM filtered
                    )
                    SELECT * EXCLUDE (_rn)
                    FROM ranked
                    WHERE _rn = 1
                ) TO {sql_quote(str(part_path))} (FORMAT PARQUET, COMPRESSION SNAPPY)
                """
            )
            if idx == 1 or idx == len(source_files) or idx % 5 == 0:
                label = source_file or "<blank>"
                print(f"[dedupe] processed {idx}/{len(source_files)} source_file={label}")

        writer = None
        for part in sorted(parts_dir.glob("part_*.parquet")):
            pf = pq.ParquetFile(part)
            for batch in pf.iter_batches():
                if writer is None:
                    writer = pq.ParquetWriter(tmp_path, batch.schema, compression="snappy")
                writer.write_batch(batch)
        if writer is None:
            raise SystemExit(f"[fatal] dedupe produced no parquet parts for {out_path}")
        writer.close()
        tmp_path.replace(out_path)
        print(f"[dedupe] complete file={out_path.name}")
    finally:
        con.close()
        shutil.rmtree(temp_dir, ignore_errors=True)


def main() -> None:
    args = parse_args()
    setup_logging(args.log_file)
    layout = ensure_data_layout(args.root)
    years = parse_years(args.years)
    output_dir = pathlib.Path(args.output_dir) if args.output_dir else layout.cross_sections
    parts_dir_base = pathlib.Path(args.parts_dir_base) if args.parts_dir_base else layout.cross_sections
    output_dir.mkdir(parents=True, exist_ok=True)
    (layout.checks / "harmonize_qc").mkdir(parents=True, exist_ok=True)
    (layout.checks / "release_qc").mkdir(parents=True, exist_ok=True)

    dict_path = layout.dictionary / "dictionary_lake.parquet"
    if not dict_path.exists():
        raise SystemExit(f"Missing dictionary lake: {dict_path}")
    dict_df = pd.read_parquet(dict_path)
    dict_df["varnumber"] = dict_df["varnumber"].map(normalize_varnumber)
    dict_df["varname"] = dict_df["varname"].fillna("").astype(str).str.upper().str.strip()
    dict_df["imputationvar"] = dict_df["imputationvar"].fillna("").astype(str).str.upper().str.strip()
    dict_df.loc[dict_df["imputationvar"].isin({"NAN", "NONE", "<NA>", "NAT"}), "imputationvar"] = ""

    for year in years:
        year_dir = layout.raw_access / str(year)
        manifest_path = year_dir / "manifest.csv"
        inventory_path = year_dir / "metadata" / "table_inventory.csv"
        if not manifest_path.exists() or not inventory_path.exists():
            raise SystemExit(f"Missing manifest or table inventory for year {year}")
        manifest = pd.read_csv(manifest_path, dtype=str).fillna("")
        release_type = manifest.iloc[0]["release_type"].strip().lower()
        release_summary = pd.DataFrame([{"year": year, "release_type": release_type, "allowed": release_type == "final"}])
        release_summary.to_csv(layout.checks / "release_qc" / f"release_summary_{year}.csv", index=False)
        if args.release_strict and release_type != "final":
            raise SystemExit(f"Unexpected non-final release for year {year}: {release_type}")

        inventory = pd.read_csv(inventory_path, dtype=str).fillna("")
        data_tables = inventory[
            (inventory["table_role"] == "data")
            & (inventory["has_unitid"].astype(str).str.lower().isin({"true", "1"}))
        ].copy()
        if data_tables.empty:
            raise SystemExit(f"No data tables with UNITID found for year {year}")
        dict_year = dict_df[pd.to_numeric(dict_df["year"], errors="coerce") == year].copy()
        if dict_year.empty:
            raise SystemExit(f"No dictionary rows found for year {year}")

        year_summary_rows: list[dict] = []
        na_drop_rows: list[dict] = []

        def frames() -> Iterable[pd.DataFrame]:
            for rec in data_tables.to_dict("records"):
                table_path = year_dir / rec["csv_path"]
                access_table_name = str(rec["table_name"])
                source_file = str(rec.get("normalized_table_name", "") or "")
                source_row_count = int(pd.to_numeric(pd.Series([rec.get("row_count_csv", "0")]), errors="coerce").fillna(0).iloc[0])
                source_match_rows, access_match_rows = dictionary_match_counts(dict_year, source_file, access_table_name)
                dict_source = select_dict_source(dict_year, rec.get("normalized_table_name", ""), access_table_name)
                if dict_source.empty:
                    year_summary_rows.append(
                        {
                            "year": year,
                            "table_name": access_table_name,
                            "source_file": source_file,
                            "selected": False,
                            "exclude_reason": classify_exclude_reason(
                                dict_source,
                                source_match_rows,
                                access_match_rows,
                                source_row_count=source_row_count,
                            ),
                            "matched_cols": 0,
                            "output_rows": 0,
                            "dictionary_rows": 0,
                            "source_match_rows": source_match_rows,
                            "access_table_match_rows": access_match_rows,
                        }
                    )
                    continue
                dict_vars = set(dict_source["varname"].tolist())
                table_output_rows = 0
                matched_any = False
                matched_cols_count = 0
                for df in read_table_iter(table_path, chunksize=args.chunksize):
                    if df.empty:
                        continue
                    df.columns = [str(c).strip().upper() for c in df.columns]
                    if "UNITID" not in df.columns:
                        raise SystemExit(f"[fatal] missing UNITID column in data table {access_table_name}")
                    df["UNITID"] = pd.to_numeric(df["UNITID"], errors="coerce").astype("Int64")
                    before_rows = len(df)
                    df = df.dropna(subset=["UNITID"])
                    dropped = before_rows - len(df)
                    if dropped > 0:
                        na_drop_rows.append(
                            {
                                "year": year,
                                "file": table_path.name,
                                "stage": "pre_melt",
                                "dropped_rows_missing_UNITID": int(dropped),
                                "rows_before": int(before_rows),
                                "rows_after": int(len(df)),
                            }
                        )
                    if args.release_strict and dropped > 0:
                        raise SystemExit(f"[fatal] missing UNITID rows detected in {table_path.name} (dropped={dropped})")
                    if df.empty:
                        continue
                    df = df.reset_index(drop=True)
                    df["_rowid"] = df.index.astype("int64")
                    value_cols = [c for c in df.columns if c not in {"UNITID", "_rowID", "_rowid"} and c in dict_vars]
                    if not value_cols:
                        continue
                    matched_any = True
                    matched_cols_count = max(matched_cols_count, len(value_cols))
                    for col_chunk in chunk_cols(value_cols, args.value_cols_per_chunk):
                        long = df.melt(id_vars=["UNITID", "_rowid"], value_vars=col_chunk, var_name="varname", value_name="value")
                        before_merge = len(long)
                        merged = long.merge(dict_source, on="varname", how="left", validate="m:1")
                        if len(merged) != before_merge:
                            raise SystemExit(f"[fatal] dictionary merge expanded rows for table {access_table_name}")
                        if merged["varnumber"].isna().sum() > 0:
                            raise SystemExit(f"[fatal] missing varnumber after dictionary merge for table {access_table_name}")
                        imp_cols = sorted({c for c in merged["imputationvar"].dropna().astype(str).str.upper().tolist() if c and c in df.columns})
                        if imp_cols:
                            imp_long = df[["_rowid"] + imp_cols].melt(id_vars=["_rowid"], value_vars=imp_cols, var_name="imputationvar", value_name="imputation_value")
                            merged = merged.merge(imp_long, on=["_rowid", "imputationvar"], how="left")
                        else:
                            merged["imputation_value"] = ""
                        merged["year"] = year
                        merged["access_table_name"] = access_table_name
                        table_output_rows += len(merged)
                        yield merged[
                            [
                                "year",
                                "UNITID",
                                "varname",
                                "varnumber",
                                "value",
                                "varTitle",
                                "longDescription",
                                "DataType",
                                "format",
                                "Fieldwidth",
                                "imputationvar",
                                "imputation_value",
                                "source_file",
                                "access_table_name",
                            ]
                        ]
                year_summary_rows.append(
                    {
                        "year": year,
                        "table_name": access_table_name,
                        "source_file": source_file,
                        "selected": matched_any,
                        "exclude_reason": (
                            ""
                            if matched_any
                            else classify_exclude_reason(
                                dict_source,
                                source_match_rows,
                                access_match_rows,
                                source_row_count=source_row_count,
                            )
                        ),
                        "matched_cols": matched_cols_count if matched_any else 0,
                        "output_rows": table_output_rows,
                        "dictionary_rows": int(len(dict_source)),
                        "source_match_rows": source_match_rows,
                        "access_table_match_rows": access_match_rows,
                    }
                )

        out_path = output_dir / f"panel_long_varnum_{year}.parquet"
        parts_dir = parts_dir_base / f"parts_{year}"
        write_parquet_parts(out_path, frames(), parts_dir)
        if not out_path.exists():
            raise SystemExit(f"No long parquet output was created for year {year}")
        if args.dedupe and out_path.exists():
            dedupe_tmp_dir = pathlib.Path(args.dedupe_temp_dir) if args.dedupe_temp_dir else layout.build / "harmonize_dedupe_tmp" / str(year)
            dedupe_long_panel(
                out_path,
                [x.strip() for x in args.dedupe_priority.split(",") if x.strip()],
                temp_dir=dedupe_tmp_dir,
                max_temp_gib=args.dedupe_max_temp_gib,
                threads=args.dedupe_threads,
            )
        if args.final_dedupe and args.dedupe and out_path.exists():
            pq.ParquetFile(out_path)

        empty_harmonize_summary_frame(year_summary_rows).to_csv(
            layout.checks / "harmonize_qc" / f"harmonize_summary_{year}.csv",
            index=False,
        )
        empty_missing_unitid_frame(na_drop_rows).to_csv(
            layout.checks / "harmonize_qc" / f"dropped_missing_unitid_{year}.csv",
            index=False,
        )
        print(f"[year {year}] wrote {out_path}")


if __name__ == "__main__":
    main()
