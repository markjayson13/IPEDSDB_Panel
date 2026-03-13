# QA_QC Guide

This folder contains validation, parity, monitoring, and repository-guard scripts.

In practice, this folder answers three different questions:

- did the build run correctly?
- does the cleaned panel still preserve the intended institution-year structure?
- is the current output good enough to treat as a release artifact?

If you are not sure where to start, do not start with the most detailed CSV. Start with the smallest summary that still answers your question.

## Main QA Scripts

| File | Purpose |
| --- | --- |
| `00_dictionary_qaqc.py` | audit dictionary integrity, separate actionable mapping gaps from auxiliary/custom families, and summarize noncanonical sources |
| `01_panel_qa.py` | compare raw vs cleaned wide panels across every observed `PRCH_*` flag and write a coverage matrix plus per-code QA |
| `02_access_vs_flatfile_parity.py` | compare this repo against the flat-file baseline project |
| `03_monitored_analysis_build.py` | run a monitored wide build with durable logs |
| `04_certify_analysis_build.py` | certify a monitored build against a baseline |
| `08_acceptance_audit.py` | run the final artifact-level acceptance audit over the generated `IPEDSDB_ROOT` outputs |
| `09_panel_structure_qc.py` | emit literature-guided diagnostics for panel structure, identifier linkage, timing, finance comparability, and classification stability |
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

## First Files To Open When Something Feels Off

1. `Checks/acceptance_qc/acceptance_summary.md`
2. `Checks/panel_qc/panel_qa_summary.csv`
3. `Checks/panel_qc/panel_structure_summary.csv`
4. `Checks/panel_qc/identifier_linkage_summary.csv`
5. `Checks/dictionary_qc/dictionary_qaqc_summary.csv`
6. `Checks/disc_qc/disc_conflicts_summary_all_years.csv`
7. only then, the year-level detail files

`qc_only.sh` now runs:

- dictionary QA
- panel QA
- panel structure QA
- acceptance audit

For repo tests in this environment, use:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q
```

For the parent-child cleaning method and Finance-code policy, read `METHODS_PRCH_CLEANING.md`.

For the literature-guided whole-panel method, read `METHODS_PANEL_CONSTRUCTION.md`.

If you only remember one thing from this folder, make it this:

- `panel_qc/` explains whether cleaning behaved correctly
- `acceptance_qc/` explains whether the finished build is ready to trust

That distinction matters. A build can finish successfully and still fail acceptance.
