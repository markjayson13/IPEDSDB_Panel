# Release validation plan

This note adapts the archived `IPEDS_Paneling` paper-validation scaffold to the
current Access-native `IPEDSDB_Panel` pipeline. It is intended as a manuscript
appendix and release-checklist bridge: every claim below should be supported by
generated artifacts under `IPEDSDB_ROOT`, not by hand-entered numbers.

## 1. Release-stage validation

Claim: the canonical panel uses stable NCES final Access releases and avoids
provisional/schema-transition inputs in the default public research object.

Evidence to report:

- Years requested and years included.
- Count of final releases included.
- Count of non-final releases excluded or unavailable.
- Download manifest hashes and release labels.

Primary artifacts:

- `Checks/download_qc/release_inventory.csv`
- `Raw_Access_Databases/<year>/`
- `Checks/acceptance_qc/acceptance_summary.csv`

## 2. Access extraction and schema capture

Claim: annual Access databases are extracted into auditable table-level
artifacts before harmonization.

Evidence to report:

- Exported Access tables by year.
- Tables with `UNITID`.
- Extraction failures, if any.
- Row-count and schema-inventory coverage.

Primary artifacts:

- `Checks/extract_qc/table_inventory_all_years.csv`
- `Raw_Access_Databases/<year>/metadata/`

## 3. Dictionary and metadata coverage

Claim: variable identity is metadata-first; harmonization is keyed to Access
metadata rather than raw header matching alone.

Evidence to report:

- Total dictionary rows.
- Total code-label rows.
- Unique `varname` and `(year, source_file, varname)` coverage.
- Duplicate/conflict/unmapped failures.
- Synthetic metadata rows added, with reasons.
- Dictionary ambiguity overrides, expected to be empty unless a reviewed exception exists.

Primary artifacts:

- `Dictionary/dictionary_lake.parquet`
- `Dictionary/dictionary_codes.parquet`
- `contracts/dictionary_ambiguity_overrides.csv`
- `Checks/dictionary_qc/dictionary_qaqc_summary.csv`

## 4. Long-panel integrity

Claim: the long panel preserves provenance at the `UNITID-year-variable`
grain with source metadata retained.

Evidence to report:

- Long rows by year.
- Null counts for `year`, `UNITID`, `varnumber`, and `source_file`.
- Duplicate-key behavior at the canonical long-panel key.
- Missing-`UNITID` drops, expected to be zero under strict release mode.

Primary artifacts:

- `Panels/2004-2023/panel_long_varnum_2004_2023.parquet`
- `Checks/harmonize_qc/`
- `Checks/acceptance_qc/acceptance_summary.csv`

## 5. Wide-build and transformation contract

Claim: the wide analysis panel is produced through explicit scalar/dimension
rules, conflict gates, typed casting, discrete collapse, and target-lineage
evidence.

Evidence to report:

- Final wide rows and columns.
- Scalar conflict count.
- Anti-garbage blocked identifiers.
- Typed-cast parse failures.
- Discrete-collapse conflicts and high-signal rows.
- Target lineage rows and seeded legacy-compatibility columns.
- Column lineage rows used by PRCH cleaning.

Primary artifacts:

- `Panels/panel_wide_analysis_2004_2023.parquet`
- `Checks/wide_qc/qc_target_lineage.csv`
- `Checks/wide_qc/qc_column_lineage.csv`
- `Checks/wide_qc/qc_scalar_conflicts.csv`
- `Checks/wide_qc/qc_anti_garbage_failures.csv`
- `Checks/wide_qc/qc_cast_report.csv`
- `Checks/disc_qc/`

## 6. Parent/child cleaning validation

Claim: PRCH cleaning preserves the `UNITID-year` spine while nulling
component-family payloads for documented child-reporting cases.

Evidence to report:

- Raw-vs-clean row preservation.
- Duplicate `(UNITID, year)` keys before and after cleaning.
- PRCH flags observed.
- Child rows cleaned by flag and year.
- Review-only PRCH cases.
- Target columns cleaned by flag.

Primary artifacts:

- `Panels/panel_clean_analysis_2004_2023.parquet`
- `Checks/prch_qc/prch_clean_summary.csv`
- `Checks/prch_qc/prch_clean_columns.csv`
- `Checks/prch_qc/prch_lineage_summary.csv`
- `Checks/prch_qc/prch_flag_policy.csv`
- `Checks/panel_qc/panel_qa_summary.csv`

## 7. Panel structure and research-use diagnostics

Claim: the release treats unbalancedness, timing, classification instability,
and identifier continuity as explicit diagnostics rather than hidden defects.

Evidence to report:

- Distinct institutions.
- Always-present institutions.
- Entry, exit, intermittent-gap, and possible selection-risk counts.
- `UNITID` cases with multiple observed `OPEID` values.
- Classification stability flags.
- Component timing and finance comparability cautions.

Primary artifacts:

- `Checks/panel_qc/panel_structure_summary.csv`
- `Checks/panel_qc/entry_exit_gap_summary.csv`
- `Checks/panel_qc/identifier_linkage_summary.csv`
- `Checks/panel_qc/classification_stability_summary.csv`
- `Checks/panel_qc/component_timing_reference.csv`
- `Checks/panel_qc/finance_comparability_summary.csv`

## 8. Release reproducibility

Claim: a release is citable only when the generated outputs, code version,
runtime, source manifest, panel contract, and QA evidence are frozen together.

Evidence to report:

- Git tag or commit SHA.
- Contract identifier and contract checksum.
- Runtime versions for Python, DuckDB, PyArrow, pandas, and `mdb-tools`.
- SHA-256 hashes for downloaded Access archives and final outputs.
- Release manifest verification pass/fail counts.
- Baseline-comparison findings for schema, dictionary mappings, PRCH policy, and year coverage.
- Acceptance checks passed and required artifacts present.
- Public archive DOI or repository URL once deposited.

Primary artifacts:

- `Checks/acceptance_qc/acceptance_summary.csv`
- `Checks/acceptance_qc/acceptance_summary.md`
- `Checks/release_manifest/release_manifest.csv`
- `Checks/release_manifest/release_manifest.json`
- `Checks/release_manifest/release_manifest_summary.md`
- `Checks/release_manifest/release_manifest_verification.csv`
- `Checks/release_manifest/release_manifest_verification.md`
- `Checks/release_metadata/datapackage.json`
- `Checks/release_metadata/build_provenance.json`
- `Releases/ipedsdb_panel_2004_2023/bundle_manifest.csv`
- `Releases/ipedsdb_panel_2004_2023/SHA256SUMS.txt`
- `Releases/ipedsdb_panel_2004_2023/CITATION.cff`
- `Releases/ipedsdb_panel_2004_2023/datacite.json`
- `Releases/ipedsdb_panel_2004_2023/ro-crate-metadata.json`
- `Checks/release_compare/release_comparison.csv`
- `Checks/release_compare/release_comparison_summary.csv`
- `Checks/release_compare/release_comparison.md`
- `contracts/panel_spec.toml`

Generate the manifest and bundle after release metrics:

```bash
python3 Scripts/QA_QC/12_build_release_manifest.py \
  --root "$IPEDSDB_ROOT" \
  --years "2004:2023"

python3 Scripts/QA_QC/13_verify_release_manifest.py \
  --manifest "$IPEDSDB_ROOT/Checks/release_manifest/release_manifest.csv" \
  --root "$IPEDSDB_ROOT"

python3 Scripts/QA_QC/16_build_datapackage.py \
  --root "$IPEDSDB_ROOT" \
  --years "2004:2023"

python3 Scripts/QA_QC/17_build_provenance.py \
  --root "$IPEDSDB_ROOT"

python3 Scripts/QA_QC/14_build_public_release_bundle.py \
  --root "$IPEDSDB_ROOT" \
  --years "2004:2023" \
  --out-dir "$IPEDSDB_ROOT/Releases/ipedsdb_panel_2004_2023"

python3 Scripts/QA_QC/15_compare_release_to_baseline.py \
  --baseline-manifest "/path/to/prior/release_manifest.csv" \
  --current-manifest "$IPEDSDB_ROOT/Checks/release_manifest/release_manifest.csv" \
  --baseline-root "/path/to/prior/IPEDSDB_ROOT" \
  --current-root "$IPEDSDB_ROOT" \
  --out-dir "$IPEDSDB_ROOT/Checks/release_compare"
```

## Manuscript table template

Use `Artifacts/table_release_validation_metrics_template.csv` as the fillable
table shell. When generated artifacts are available, run:

```bash
python3 Scripts/QA_QC/10_release_metrics.py \
  --root "$IPEDSDB_ROOT" \
  --years "2004:2023" \
  --out-dir "$IPEDSDB_ROOT/Checks/release_metrics"
```

That script writes component summaries plus
`table_release_validation_metrics_filled.csv`.
