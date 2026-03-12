# QA_QC Guide

This folder contains validation, parity, monitoring, and repository-guard scripts.

## Main QA Scripts

| File | Purpose |
| --- | --- |
| `00_dictionary_qaqc.py` | audit dictionary integrity, separate actionable mapping gaps from auxiliary/custom families, and summarize noncanonical sources |
| `01_panel_qa.py` | compare raw vs cleaned wide panels across every observed `PRCH_*` flag and write a coverage matrix plus per-code QA |
| `02_access_vs_flatfile_parity.py` | compare this repo against the flat-file baseline project |
| `03_monitored_analysis_build.py` | run a monitored wide build with durable logs |
| `04_certify_analysis_build.py` | certify a monitored build against a baseline |
| `08_acceptance_audit.py` | run the final artifact-level acceptance audit over the generated `IPEDSDB_ROOT` outputs |
| `07_task_monitor_summary.py` | roll monitored-build telemetry into CSV and Markdown summaries |

## Repo Guards

| File | Purpose |
| --- | --- |
| `05_repo_size_guard.py` | fail if tracked files make the repo too large |
| `06_staged_repo_guard.py` | block staged generated artifacts and oversized files |

## Typical Use

- after a standard build: run `qc_only.sh`
- during deeper debugging: inspect `Checks/dictionary_qc/noncanonical_source_categories.csv` and `Checks/panel_qc/panel_qa_coverage_matrix.csv` first
- for the final live-build pass/fail decision: inspect `Checks/acceptance_qc/acceptance_summary.csv` and `Checks/acceptance_qc/acceptance_summary.md`
- during parity work: use `02_access_vs_flatfile_parity.py`
- during long-run monitoring: use `03_monitored_analysis_build.py` and inspect `Checks/real_parity_runs/summary/`
- before committing: run the two repo guards

`qc_only.sh` now runs:

- dictionary QA
- panel QA
- acceptance audit

For repo tests in this environment, use:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q
```

For the parent-child cleaning method and Finance-code policy, read `METHODS_PRCH_CLEANING.md`.
