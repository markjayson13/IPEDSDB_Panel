#!/usr/bin/env python3
"""
Stage 06: build the wide institution-year analysis panel from the stitched long panel.

Reads:
- stitched long parquet from Stage 05
- `Dictionary/dictionary_lake.parquet`

Writes:
- `Panels/panel_wide_analysis_*.parquet`
- `Panels/panel_long_scalar_unique.parquet`
- `Panels/panel_long_dim.parquet`
- `Checks/wide_qc/*`
- `Checks/disc_qc/*`

DuckDB handles target discovery, lane-split planning, scalar-conflict QA,
discrete collapse, typed casting, and stitched exports.

Open this file when you want to understand how the long panel becomes the one-row-per-`UNITID-year` analysis dataset used by the cleaner and the release QA.

Method context:
- `METHODS_PANEL_CONSTRUCTION.md` explains the high-level panel design
- this script is the mechanical wide-build step, not the place where the unit of analysis is philosophically decided
"""
from __future__ import annotations

from wide_build_common import build_arg_parser, setup_logging
from wide_build_duckdb import run


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    setup_logging(args.log_file)
    run(args)


if __name__ == "__main__":
    main()
