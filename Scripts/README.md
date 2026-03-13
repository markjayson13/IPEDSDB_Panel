# Scripts Guide

This folder contains the operational pipeline for building the Access-database IPEDS panel.

If you are opening this folder for the first time, the basic idea is:

- `00_run_all.py` coordinates the run
- `01` through `09` are the ordered stages
- helper modules hold the shared logic
- `QA_QC/` is where the build gets checked, summarized, and audited

This is the working part of the repo. If `README.md` explains the system from the outside, this folder explains it from the inside.

## Start Here

- `00_run_all.py`: main Python orchestrator
- `01_download_access_databases.py` to `09_build_panel_dictionary.py`: ordered pipeline stages
- `run_saved_query.py`: analyst query runner for saved SQL and result exports
- `QA_QC/`: validation, parity, monitoring, and repo guards
- `QA_QC/08_acceptance_audit.py`: top-level pass/fail audit over the generated live artifacts
- `QA_QC/09_panel_structure_qc.py`: literature-guided structure, linkage, timing, and comparability diagnostics

## If You Are Trying To Understand One Specific Thing

| Question | Best file to open first |
| --- | --- |
| How does the whole build get chained together? | `00_run_all.py` |
| Where do the NCES files come from? | `01_download_access_databases.py` |
| How are Access tables classified and exported? | `02_extract_access_db.py` |
| How do raw metadata tables become one dictionary? | `03_dictionary_ingest.py` |
| How do yearly tables become one long panel? | `04_harmonize.py` |
| How does the long panel become the wide analysis panel? | `06_build_wide_panel.py` |
| How are parent-child rows handled? | `07_clean_panel.py` and `../METHODS_PRCH_CLEANING.md` |
| How is the whole panel-construction method justified? | `../METHODS_PANEL_CONSTRUCTION.md` |
| How do I decide whether the finished build is trustworthy? | `QA_QC/08_acceptance_audit.py` |

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
| `09_build_panel_dictionary.py` | build a dictionary for an actual panel output in `.csv` or formatted `.xlsx` |
| `run_saved_query.py` | run saved SQL against the build DB and standard outputs |
| `prch_policy.py` | shared parent-child cleaning policy used by cleaning and QA |

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

That reading order mirrors the real data flow, so it is usually the least confusing way to understand the project.

If you only have ten minutes, read `00_run_all.py`, then `07_clean_panel.py`, then `QA_QC/08_acceptance_audit.py`. That path gives you orchestration, cleaning policy, and release gating.

For the documented parent-child method, read `METHODS_PRCH_CLEANING.md`.

For the documented whole-pipeline method, read `METHODS_PANEL_CONSTRUCTION.md`.

For the top-level live-build acceptance check, run:

```bash
python Scripts/QA_QC/08_acceptance_audit.py \
  --root "$IPEDSDB_ROOT" \
  --years "2004:2023"
```
