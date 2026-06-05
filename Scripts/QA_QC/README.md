# QA/QC guide

This folder contains validation, parity, monitoring, and repository-guard scripts.

This folder answers three questions:

- did the build run correctly?
- does the cleaned panel still preserve the intended institution-year structure?
- is the current output good enough to treat as a release artifact?

If you are not sure where to start, do not start with the most detailed CSV. Start with the smallest summary that still answers your question.

## Main QA scripts

| File | Purpose |
| --- | --- |
| `00_dictionary_qaqc.py` | audit dictionary integrity, separate actionable mapping gaps from auxiliary/custom families, and summarize noncanonical sources |
| `01_panel_qa.py` | compare raw vs cleaned wide panels across every observed `PRCH_*` flag and write a coverage matrix plus per-code QA |
| `02_access_vs_flatfile_parity.py` | compare this repo against the flat-file baseline project |
| `03_monitored_analysis_build.py` | run a monitored wide build with durable logs |
| `04_certify_analysis_build.py` | certify a monitored build against a baseline |
| `08_acceptance_audit.py` | run the final artifact-level acceptance audit over the generated `IPEDSDB_ROOT` outputs |
| `09_panel_structure_qc.py` | emit literature-guided diagnostics for panel structure, identifier linkage, timing, finance comparability, and classification stability |
| `10_release_metrics.py` | summarize generated release evidence into manuscript/archive validation tables |
| `11_validate_panel_contract.py` | fail if `contracts/panel_spec.toml` drifts from current wide-build defaults or PRCH policy |
| `12_build_release_manifest.py` | write the citable artifact ledger with file sizes, SHA-256 hashes, parquet shapes, and repo commit metadata |
| `13_verify_release_manifest.py` | re-check a release manifest against files on disk before public deposit or after download |
| `14_build_public_release_bundle.py` | copy verified release files into a deposit directory with checksums, citation metadata, DataCite JSON, and RO-Crate metadata |
| `15_compare_release_to_baseline.py` | compare a current release against a baseline manifest and fail on schema, row-count, dictionary, year-window, or PRCH drift |
| `16_build_datapackage.py` | write table-level Data Package metadata from the release manifest |
| `17_build_provenance.py` | write build provenance with source manifest, git, runtime, package, and output digest metadata |
| `18_public_release_guard.py` | fail if public-release policy, ownership, citation, archive, or intake files are missing or inconsistent |
| `19_docs_style_guard.py` | scan release-facing prose for generated-text patterns before publication |
| `20_environment_report.py` | write Python, package, tool, lockfile, and platform metadata for the release |
| `21_external_benchmark_reconciliation.py` | compare selected panel metrics to configured external benchmark rows |
| `22_build_entity_continuity_crosswalk.py` | build `UNITID` join-risk and entity-continuity review outputs |
| `07_task_monitor_summary.py` | roll monitored-build telemetry into CSV and Markdown summaries |
| `release_gate.sh` | run the release checks, metadata builders, bundle builder, repo guards, and tests |

## Repo guards

| File | Purpose |
| --- | --- |
| `05_repo_size_guard.py` | fail if tracked files make the repo too large |
| `06_staged_repo_guard.py` | block staged generated artifacts and oversized files |
| `18_public_release_guard.py` | check public-facing ownership, license, citation, and intake files |
| `19_docs_style_guard.py` | check published prose before release |

## Typical use

- after a standard build: run `qc_only.sh`
- during deeper debugging: inspect `Checks/dictionary_qc/noncanonical_source_categories.csv` and `Checks/panel_qc/panel_qa_coverage_matrix.csv` first
- for the final live-build pass/fail decision: inspect `Checks/acceptance_qc/acceptance_summary.csv` and `Checks/acceptance_qc/acceptance_summary.md`
- during parity work: use `02_access_vs_flatfile_parity.py`
- during long-run monitoring: use `03_monitored_analysis_build.py` and inspect `Checks/real_parity_runs/summary/`
- for manuscript or archive tables: use `10_release_metrics.py` after acceptance artifacts exist
- after changing build defaults or PRCH rules: run `11_validate_panel_contract.py`
- before archiving or sharing a public build: run `release_gate.sh`
- before replacing an archived release: run `15_compare_release_to_baseline.py`
- before DOI deposit: fill `contracts/external_benchmarks.csv`, set `REQUIRE_EXTERNAL_BENCHMARKS=1`, and rerun `release_gate.sh`
- before external joins: inspect `Checks/entity_continuity/entity_continuity_crosswalk.csv`
- after changing Stage 06 or Stage 07 lineage behavior: inspect `Checks/wide_qc/qc_column_lineage.csv` and `Checks/prch_qc/prch_lineage_summary.csv`
- before committing: run the repo guards

## Files to open first

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
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q
```

For the parent-child cleaning method and Finance-code policy, read `METHODS_PRCH_CLEANING.md`.

For the literature-guided whole-panel method, read `METHODS_PANEL_CONSTRUCTION.md`.

Operational split:

- `panel_qc/` explains whether cleaning behaved correctly
- `acceptance_qc/` explains whether the finished build is release-ready

That distinction matters. A build can finish successfully and still fail acceptance.
