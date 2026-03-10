#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.parquet as pq


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--access-root", default="/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling")
    ap.add_argument("--flatfile-root", default="/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling")
    ap.add_argument("--years", default="2004,2014,2023")
    ap.add_argument("--access-long", default=None)
    ap.add_argument("--flat-long", default=None)
    ap.add_argument("--access-wide", default=None)
    ap.add_argument("--flat-wide", default=None)
    ap.add_argument("--summary-csv", default=None)
    ap.add_argument("--summary-md", default=None)
    return ap.parse_args()


def parse_years(spec: str) -> list[int]:
    if ":" in spec:
        start, end = spec.split(":", 1)
        return list(range(int(start), int(end) + 1))
    return [int(part.strip()) for part in spec.split(",") if part.strip()]


def sql_quote(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def query_df(sql: str) -> pd.DataFrame:
    con = duckdb.connect()
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


def qc_count(path: Path) -> int:
    if not path.exists():
        return 0
    return len(pd.read_csv(path))


def classify_mismatch(check: str, match: bool) -> str:
    if match:
        return "match"
    if check in {"long_source_file_coverage", "shared_content_diff"}:
        return "source-file mapping difference"
    if check in {"wide_common_columns", "wide_row_counts_by_year"}:
        return "expected schema source difference"
    return "potential bug"


def main() -> None:
    args = parse_args()
    years = parse_years(args.years)
    access_root = Path(args.access_root)
    flat_root = Path(args.flatfile_root)

    access_long = Path(args.access_long or access_root / "Panels" / "2004-2023" / "panel_long_varnum_2004_2023.parquet")
    flat_long = Path(args.flat_long or flat_root / "Panels" / "2004-2024" / "panel_long_varnum_2004_2024.parquet")
    access_wide = Path(args.access_wide or access_root / "Panels" / "panel_wide_analysis_2004_2023.parquet")
    flat_wide = Path(args.flat_wide or flat_root / "Panels" / "panel_wide_analysis_2004_2023.parquet")

    if not access_long.exists() or not flat_long.exists():
        raise SystemExit("Missing access or flatfile long panel input.")
    if not access_wide.exists() or not flat_wide.exists():
        raise SystemExit("Missing access or flatfile wide panel input.")

    out_csv = Path(args.summary_csv or access_root / "Checks" / "wide_qc" / "access_vs_flatfile_parity_summary.csv")
    out_md = Path(args.summary_md or access_root / "Checks" / "wide_qc" / "access_vs_flatfile_parity_summary.md")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_md.parent.mkdir(parents=True, exist_ok=True)

    year_list = ",".join(str(year) for year in years)
    rows: list[dict] = []

    def add(check: str, access_value, flat_value, match: bool) -> None:
        rows.append(
            {
                "check": check,
                "access_value": access_value,
                "flatfile_value": flat_value,
                "match": bool(match),
                "classification": classify_mismatch(check, bool(match)),
            }
        )

    long_access_counts = query_df(
        f"""
        SELECT year, COUNT(*) AS rows
        FROM read_parquet({sql_quote(str(access_long))})
        WHERE year IN ({year_list})
        GROUP BY year ORDER BY year
        """
    )
    long_flat_counts = query_df(
        f"""
        SELECT year, COUNT(*) AS rows
        FROM read_parquet({sql_quote(str(flat_long))})
        WHERE year IN ({year_list})
        GROUP BY year ORDER BY year
        """
    )
    add("long_row_counts_by_year", long_access_counts.to_json(orient="records"), long_flat_counts.to_json(orient="records"), long_access_counts.equals(long_flat_counts))

    unitid_access = query_df(
        f"""
        SELECT year, COUNT(DISTINCT UNITID) AS distinct_unitids
        FROM read_parquet({sql_quote(str(access_long))})
        WHERE year IN ({year_list})
        GROUP BY year ORDER BY year
        """
    )
    unitid_flat = query_df(
        f"""
        SELECT year, COUNT(DISTINCT UNITID) AS distinct_unitids
        FROM read_parquet({sql_quote(str(flat_long))})
        WHERE year IN ({year_list})
        GROUP BY year ORDER BY year
        """
    )
    add("long_distinct_unitid_by_year", unitid_access.to_json(orient="records"), unitid_flat.to_json(orient="records"), unitid_access.equals(unitid_flat))

    sf_access = query_df(
        f"""
        SELECT year, source_file, COUNT(*) AS rows
        FROM read_parquet({sql_quote(str(access_long))})
        WHERE year IN ({year_list})
        GROUP BY year, source_file ORDER BY year, source_file
        """
    )
    sf_flat = query_df(
        f"""
        SELECT year, source_file, COUNT(*) AS rows
        FROM read_parquet({sql_quote(str(flat_long))})
        WHERE year IN ({year_list})
        GROUP BY year, source_file ORDER BY year, source_file
        """
    )
    add("long_source_file_coverage", sf_access.to_json(orient="records"), sf_flat.to_json(orient="records"), sf_access.equals(sf_flat))

    wide_access_counts = query_df(
        f"""
        SELECT year, COUNT(*) AS rows
        FROM read_parquet({sql_quote(str(access_wide))})
        WHERE year IN ({year_list})
        GROUP BY year ORDER BY year
        """
    )
    wide_flat_counts = query_df(
        f"""
        SELECT year, COUNT(*) AS rows
        FROM read_parquet({sql_quote(str(flat_wide))})
        WHERE year IN ({year_list})
        GROUP BY year ORDER BY year
        """
    )
    add("wide_row_counts_by_year", wide_access_counts.to_json(orient="records"), wide_flat_counts.to_json(orient="records"), wide_access_counts.equals(wide_flat_counts))

    access_cols = set(pq.read_schema(access_wide).names)
    flat_cols = set(pq.read_schema(flat_wide).names)
    common_cols = sorted((access_cols & flat_cols) - {"year", "UNITID"})
    add("wide_common_columns", len(common_cols), len(access_cols ^ flat_cols), len(access_cols ^ flat_cols) == 0)

    compare_cols = ["year", "UNITID"] + common_cols
    quoted_cols = ", ".join(f'"{col}"' for col in compare_cols)
    access_only = query_df(
        f"""
        SELECT year, COUNT(*) AS rows
        FROM (
            SELECT {quoted_cols}
            FROM read_parquet({sql_quote(str(access_wide))})
            WHERE year IN ({year_list})
            EXCEPT ALL
            SELECT {quoted_cols}
            FROM read_parquet({sql_quote(str(flat_wide))})
            WHERE year IN ({year_list})
        )
        GROUP BY year ORDER BY year
        """
    )
    flat_only = query_df(
        f"""
        SELECT year, COUNT(*) AS rows
        FROM (
            SELECT {quoted_cols}
            FROM read_parquet({sql_quote(str(flat_wide))})
            WHERE year IN ({year_list})
            EXCEPT ALL
            SELECT {quoted_cols}
            FROM read_parquet({sql_quote(str(access_wide))})
            WHERE year IN ({year_list})
        )
        GROUP BY year ORDER BY year
        """
    )
    add("shared_content_diff", access_only.to_json(orient="records"), flat_only.to_json(orient="records"), access_only.empty and flat_only.empty)

    add(
        "anti_garbage_failures",
        qc_count(access_root / "Checks" / "wide_qc" / "qc_anti_garbage_failures.csv"),
        qc_count(flat_root / "Checks" / "wide_qc" / "qc_anti_garbage_failures.csv"),
        qc_count(access_root / "Checks" / "wide_qc" / "qc_anti_garbage_failures.csv")
        == qc_count(flat_root / "Checks" / "wide_qc" / "qc_anti_garbage_failures.csv"),
    )
    add(
        "scalar_conflict_rows",
        qc_count(access_root / "Checks" / "wide_qc" / "qc_scalar_conflicts.csv"),
        qc_count(flat_root / "Checks" / "wide_qc" / "qc_scalar_conflicts.csv"),
        qc_count(access_root / "Checks" / "wide_qc" / "qc_scalar_conflicts.csv")
        == qc_count(flat_root / "Checks" / "wide_qc" / "qc_scalar_conflicts.csv"),
    )

    summary = pd.DataFrame(rows)
    summary.to_csv(out_csv, index=False)

    lines = ["# Access vs Flatfile Parity", "", f"Years: {', '.join(str(y) for y in years)}", ""]
    for row in rows:
        lines.append(f"- `{row['check']}`: {'match' if row['match'] else 'mismatch'} ({row['classification']})")
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote parity summary CSV: {out_csv}")
    print(f"Wrote parity summary markdown: {out_md}")


if __name__ == "__main__":
    main()
