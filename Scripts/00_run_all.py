#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from access_build_utils import ensure_data_layout, parse_years, repo_root


SCRIPTS_DIR = Path(__file__).resolve().parent


def run(cmd: list[str], dry_run: bool) -> None:
    print("+", " ".join(cmd))
    if dry_run:
        return
    res = subprocess.run(cmd, check=False)
    if res.returncode != 0:
        raise SystemExit(res.returncode)


def main() -> None:
    repo = repo_root()
    default_root = os.environ.get("IPEDSDB_ROOT", "/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=default_root, help="External IPEDSDB_ROOT")
    ap.add_argument("--years", default="2004:2023", help='Year span, e.g. "2004:2023"')
    ap.add_argument("--skip-download", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--skip-extract", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--skip-dictionary", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--skip-harmonize", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--skip-stitch", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--skip-wide", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--run-cleaning", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--run-qaqc", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--build-custom", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--custom-vars", default=None)
    ap.add_argument("--custom-vars-file", default=None)
    ap.add_argument("--custom-output", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    layout = ensure_data_layout(args.root)
    years = parse_years(args.years)
    year_spec = f"{years[0]}:{years[-1]}"
    long_out = layout.panels / f"{years[0]}-{years[-1]}" / f"panel_long_varnum_{years[0]}_{years[-1]}.parquet"
    wide_out = layout.panels / f"panel_wide_analysis_{years[0]}_{years[-1]}.parquet"
    clean_out = layout.panels / f"panel_clean_analysis_{years[0]}_{years[-1]}.parquet"
    dict_lake = layout.dictionary / "dictionary_lake.parquet"

    if not args.skip_download:
        run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "01_download_access_databases.py"),
                "--root",
                str(layout.root),
                "--years",
                year_spec,
            ],
            args.dry_run,
        )

    if not args.skip_extract:
        run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "02_extract_access_db.py"),
                "--root",
                str(layout.root),
                "--years",
                year_spec,
            ],
            args.dry_run,
        )

    if not args.skip_dictionary:
        run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "03_dictionary_ingest.py"),
                "--root",
                str(layout.root),
                "--years",
                year_spec,
            ],
            args.dry_run,
        )

    if not args.skip_harmonize:
        run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "04_harmonize.py"),
                "--root",
                str(layout.root),
                "--years",
                year_spec,
                "--output-dir",
                str(layout.cross_sections),
                "--parts-dir-base",
                str(layout.cross_sections),
            ],
            args.dry_run,
        )

    if not args.skip_stitch:
        run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "05_stitch_long.py"),
                "--root",
                str(layout.root),
                "--years",
                year_spec,
                "--cross-sections-dir",
                str(layout.cross_sections),
                "--output",
                str(long_out),
            ],
            args.dry_run,
        )

    if not args.skip_wide:
        run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "06_build_wide_panel.py"),
                "--input",
                str(long_out),
                "--out_dir",
                str(layout.panels / "wide_analysis_parts"),
                "--years",
                year_spec,
                "--dictionary",
                str(dict_lake),
                "--lane-split",
                "--dim-sources",
                "IC_CAMPUSES,IC_PCCAMPUSES,F_FA_F,F_FA_G",
                "--dim-prefixes",
                "C_,EF,GR,GR200,SAL,S_,OM,DRV",
                "--scalar-long-out",
                str(layout.panels / "panel_long_scalar_unique.parquet"),
                "--dim-long-out",
                str(layout.panels / "panel_long_dim.parquet"),
                "--wide-analysis-out",
                str(wide_out),
                "--typed-output",
                "--drop-empty-cols",
                "--collapse-disc",
                "--drop-disc-components",
                "--qc-dir",
                str(layout.checks / "wide_qc"),
                "--disc-qc-dir",
                str(layout.checks / "disc_qc"),
                "--duckdb-path",
                str(layout.build / "ipedsdb_build.duckdb"),
                "--duckdb-temp-dir",
                str(layout.build / "duckdb_tmp"),
                "--persist-duckdb",
            ],
            args.dry_run,
        )

    if args.run_cleaning:
        run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "07_clean_panel.py"),
                "--input",
                str(wide_out),
                "--output",
                str(clean_out),
                "--dictionary",
                str(dict_lake),
                "--qc-dir",
                str(layout.checks / "prch_qc"),
            ],
            args.dry_run,
        )

    if args.build_custom:
        custom_output = args.custom_output or str(layout.panels / f"custom_panel_{years[0]}_{years[-1]}.parquet")
        cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "08_build_custom_panel.py"),
            "--input",
            str(clean_out if args.run_cleaning else wide_out),
            "--output",
            custom_output,
            "--years",
            year_spec,
        ]
        if args.custom_vars:
            cmd += ["--vars", args.custom_vars]
        if args.custom_vars_file:
            cmd += ["--vars-file", args.custom_vars_file]
        run(cmd, args.dry_run)

    if args.run_qaqc:
        run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "QA_QC" / "00_dictionary_qaqc.py"),
                "--root",
                str(layout.root),
            ],
            args.dry_run,
        )
        if args.run_cleaning:
            run(
                [
                    sys.executable,
                    str(SCRIPTS_DIR / "QA_QC" / "01_panel_qa.py"),
                    "--raw",
                    str(wide_out),
                    "--clean",
                    str(clean_out),
                    "--out-dir",
                    str(layout.checks / "panel_qc"),
                    "--prch-qc-dir",
                    str(layout.checks / "prch_qc"),
                ],
                args.dry_run,
            )

    if not args.dry_run and long_out.exists():
        dataset = ds.dataset(str(long_out), format="parquet")
        year_values = dataset.to_table(columns=["year"]).column(0)
        unique_years = sorted(int(v) for v in pc.unique(year_values).to_pylist() if v is not None)
        print(f"[summary] long panel years={unique_years[:3]}...{unique_years[-3:]}")
        pq.ParquetFile(long_out)
        print(f"[summary] stitched long panel: {long_out}")
    if not args.dry_run and wide_out.exists():
        pq.ParquetFile(wide_out)
        print(f"[summary] wide analysis panel: {wide_out}")
    if not args.dry_run and clean_out.exists():
        pq.ParquetFile(clean_out)
        print(f"[summary] clean analysis panel: {clean_out}")


if __name__ == "__main__":
    main()
