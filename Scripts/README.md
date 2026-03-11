# Scripts Guide

This folder contains the operational pipeline for building the Access-database IPEDS panel.

## Start Here

- `00_run_all.py`: main Python orchestrator
- `01_download_access_databases.py` to `09_build_panel_dictionary.py`: ordered pipeline stages
- `run_saved_query.py`: analyst query runner for saved SQL and result exports
- `QA_QC/`: validation, parity, monitoring, and repo guards

## Stage Files

| File | Purpose |
| --- | --- |
| `00_run_all.py` | chain enabled stages into one run |
| `01_download_access_databases.py` | discover and download NCES Access releases |
| `02_extract_access_db.py` | unzip Access DBs and export tables |
| `03_dictionary_ingest.py` | build stitched dictionary artifacts |
| `04_harmonize.py` | convert exported yearly tables into long parquet |
| `05_stitch_long.py` | combine yearly long outputs |
| `06_build_wide_panel.py` | build the wide analysis panel |
| `07_clean_panel.py` | apply PRCH cleaning |
| `08_build_custom_panel.py` | make smaller user-selected extracts |
| `09_build_panel_dictionary.py` | build a dictionary for an actual panel output |
| `run_saved_query.py` | run saved SQL against the build DB and standard outputs |

## Shared Helpers

| File | Purpose |
| --- | --- |
| `access_build_utils.py` | path layout, normalization, table-role heuristics, subprocess helpers |
| `wide_build_common.py` | wide-build argument parsing and planning |
| `wide_build_duckdb.py` | main DuckDB execution engine |
| `wide_build_legacy.py` | legacy parity-oriented wide builder |
| `duckdb_build_utils.py` | shared DuckDB connection and export helpers |

## Reading Order

If you are tracing the code for the first time:

1. `00_run_all.py`
2. `01_download_access_databases.py`
3. `02_extract_access_db.py`
4. `03_dictionary_ingest.py`
5. `04_harmonize.py`
6. `06_build_wide_panel.py`
7. `07_clean_panel.py`
8. `QA_QC/README.md`
