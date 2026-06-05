# Scripts guide

This folder contains the operational pipeline for building the Access-database IPEDS panel.

Start with these files:

- `00_run_all.py` coordinates the run
- `01` through `09` are the main ordered build stages
- `10_build_variable_browser.py` is optional and only helps with variable selection
- helper modules hold shared logic
- `QA_QC/` is where the build gets checked and summarized

If `README.md` describes the repo from the outside, this folder describes the build path.

## Start here

- `00_run_all.py`: main Python orchestrator
- `01_download_access_databases.py` to `09_build_panel_dictionary.py`: ordered pipeline stages
- `10_build_variable_browser.py`: optional static HTML browser for finding real panel columns and exporting `selectedvars.txt`
- `run_saved_query.py`: analyst query runner for saved SQL and result exports
- `QA_QC/`: validation, parity, monitoring, and repo guards
- `QA_QC/08_acceptance_audit.py`: top-level pass/fail audit over the generated live artifacts
- `QA_QC/09_panel_structure_qc.py`: literature-guided structure, linkage, timing, and comparability diagnostics
- `QA_QC/12_build_release_manifest.py`: citable artifact ledger with hashes and parquet shapes
- `QA_QC/14_build_public_release_bundle.py`: deposit directory builder for verified release files
- `QA_QC/15_compare_release_to_baseline.py`: baseline release audit for schema, dictionary, year-window, and PRCH drift
- `QA_QC/16_build_datapackage.py`: Data Package metadata builder
- `QA_QC/17_build_provenance.py`: provenance metadata builder
- `QA_QC/18_public_release_guard.py`: public-release file and ownership guard
- `QA_QC/19_docs_style_guard.py`: release-facing prose guard
- `QA_QC/20_environment_report.py`: runtime and dependency report
- `QA_QC/21_external_benchmark_reconciliation.py`: configured external benchmark reconciliation
- `QA_QC/22_build_entity_continuity_crosswalk.py`: `UNITID` continuity and join-risk outputs
- `QA_QC/release_gate.sh`: release gate wrapper

## File lookup

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
| How do I package public-release files? | `QA_QC/release_gate.sh` |
| How do I compare a release with a prior one? | `QA_QC/15_compare_release_to_baseline.py` |

## Stage files

| File | Purpose |
| --- | --- |
| `00_run_all.py` | chain enabled stages into one run |
| `01_download_access_databases.py` | discover and download NCES Access releases |
| `02_extract_access_db.py` | unzip Access DBs and export tables |
| `03_dictionary_ingest.py` | build stitched dictionary artifacts |
| `04_harmonize.py` | convert exported yearly tables into long parquet and fail on undocumented dictionary ambiguity |
| `05_stitch_long.py` | combine yearly long outputs |
| `06_build_wide_panel.py` | build the wide analysis panel |
| `07_clean_panel.py` | apply PRCH cleaning |
| `08_build_custom_panel.py` | make smaller user-selected extracts |
| `09_build_panel_dictionary.py` | build a dictionary for an actual panel output in `.csv` or formatted `.xlsx` |
| `10_build_variable_browser.py` | build a self-contained HTML browser for panel vars with semantic grouping, card/table browsing, detail inspection, presets, group/family bulk actions, saved-set lifecycle, import diffing, and export artifacts |
| `run_saved_query.py` | run saved SQL against the build DB and standard outputs |
| `prch_policy.py` | shared parent-child cleaning policy used by cleaning and QA |

## Shared helpers

| File | Purpose |
| --- | --- |
| `access_build_utils.py` | path layout, normalization, table-role heuristics, subprocess helpers |
| `wide_build_common.py` | wide-build argument parsing and planning |
| `wide_build_duckdb.py` | main DuckDB execution engine |
| `wide_build_legacy.py` | legacy parity-oriented wide builder |
| `duckdb_build_utils.py` | shared DuckDB connection and export helpers |

## Reading order

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

For a short review, read `00_run_all.py`, then `07_clean_panel.py`, then `QA_QC/08_acceptance_audit.py`. That path covers orchestration, cleaning policy, and release gating.

For the documented parent-child method, read `METHODS_PRCH_CLEANING.md`.

For the documented whole-pipeline method, read `METHODS_PANEL_CONSTRUCTION.md`.

For the top-level live-build acceptance check, run:

```bash
python Scripts/QA_QC/08_acceptance_audit.py \
  --root "$IPEDSDB_ROOT" \
  --years "2004:2023"
```
