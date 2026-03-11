# QA_QC Guide

This folder contains validation, parity, monitoring, and repository-guard scripts.

## Main QA Scripts

| File | Purpose |
| --- | --- |
| `00_dictionary_qaqc.py` | audit dictionary integrity and metadata coverage |
| `01_panel_qa.py` | compare raw vs cleaned wide panels |
| `02_access_vs_flatfile_parity.py` | compare this repo against the flat-file baseline project |
| `03_monitored_analysis_build.py` | run a monitored wide build with durable logs |
| `04_certify_analysis_build.py` | certify a monitored build against a baseline |
| `07_task_monitor_summary.py` | roll monitored-build telemetry into CSV and Markdown summaries |

## Repo Guards

| File | Purpose |
| --- | --- |
| `05_repo_size_guard.py` | fail if tracked files make the repo too large |
| `06_staged_repo_guard.py` | block staged generated artifacts and oversized files |

## Typical Use

- after a standard build: run `qc_only.sh`
- during deeper debugging: inspect `00_dictionary_qaqc.py` and `01_panel_qa.py`
- during parity work: use `02_access_vs_flatfile_parity.py`
- during long-run monitoring: use `03_monitored_analysis_build.py` and inspect `Checks/real_parity_runs/summary/`
- before committing: run the two repo guards
