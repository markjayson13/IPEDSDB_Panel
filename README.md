# IPEDSDB_Panel

Build a research-ready unbalanced IPEDS institution-year panel from the NCES
IPEDS Access databases.

The canonical v1 output is a cleaned analysis panel for `2004:2023`, keyed by
`UNITID` and `year`, built from `Final` Access releases only.

## Data Root

All large data live outside the repo under:

```bash
export IPEDSDB_ROOT="/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling"
```

If `IPEDSDB_ROOT` is not set, the scripts default to that same path.

Expected layout under `IPEDSDB_ROOT`:

- `Raw_Access_Databases/`
- `Dictionary/`
- `Cross_sections/`
- `Panels/`
- `Checks/`
- `build/`

## System Dependency

The extraction stage requires `mdb-tools`:

```bash
which mdb-tables mdb-schema mdb-export
```

If any are missing, the extraction pipeline stops before it mutates data state.

## Setup

```bash
python3 -m pip install -r requirements.txt
```

## Full Run

```bash
bash manual_commands.sh
```

This runs:

1. download final Access archives and companion tables-doc workbooks
2. extract yearly Access databases with `mdb-tools`
3. build dictionary artifacts from exported metadata tables
4. harmonize yearly long panels
5. stitch the long panel
6. build the wide analysis panel
7. apply PRCH cleaning
8. run dictionary and panel QA summaries

Default outputs:

- `Panels/2004-2023/panel_long_varnum_2004_2023.parquet`
- `Panels/panel_wide_analysis_2004_2023.parquet`
- `Panels/panel_clean_analysis_2004_2023.parquet`

## Key Scripts

- `Scripts/00_run_all.py`
- `Scripts/01_download_access_databases.py`
- `Scripts/02_extract_access_db.py`
- `Scripts/03_dictionary_ingest.py`
- `Scripts/04_harmonize.py`
- `Scripts/05_stitch_long.py`
- `Scripts/06_build_wide_panel.py`
- `Scripts/07_clean_panel.py`
- `Scripts/08_build_custom_panel.py`
- `Scripts/09_build_panel_dictionary.py`

## QA/QC

Key QA outputs under `IPEDSDB_ROOT/Checks/`:

- `download_qc/`
- `extract_qc/`
- `dictionary_qc/`
- `harmonize_qc/`
- `wide_qc/`
- `disc_qc/`
- `prch_qc/`
- `panel_qc/`
- `logs/`

## Notes

- Only `Final` Access releases are in scope for v1.
- `2024-25` provisional is intentionally excluded.
- No script in this repo commits or pushes changes.
- Generated data should never be committed to git.
