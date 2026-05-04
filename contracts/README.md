# Panel contracts

`panel_spec.toml` declares the canonical `IPEDSDB_Panel` research object outside
the stage scripts. It is a compact, versioned statement of the release window,
input source, wide-build transformation rules, PRCH cleaning policy, expected
outputs, and QA evidence.

The contract is intentionally declarative. It does not replace the stage
scripts; it gives reviewers and maintainers one place to inspect the data object
being built.

## Current contract

- `contract_id`: `ipedsdb-panel-2004-2023-access-final-v1`
- status: `release-candidate`
- release window: `2004:2023`
- source: NCES IPEDS Access databases
- release policy: final-only
- unit of analysis: `UNITID-year`
- canonical output: `Panels/panel_clean_analysis_2004_2023.parquet`
- cleaning lineage: `Checks/wide_qc/qc_column_lineage.csv`
- release manifest: `Checks/release_manifest/release_manifest.csv`
- release bundle manifest: `Releases/ipedsdb_panel_2004_2023/bundle_manifest.csv`
- release comparison: `Checks/release_compare/release_comparison.csv`
- data package: `Checks/release_metadata/datapackage.json`
- build provenance: `Checks/release_metadata/build_provenance.json`

## Validation

Run:

```bash
python3 Scripts/QA_QC/11_validate_panel_contract.py
```

The validator checks the contract against the current parser defaults and shared
PRCH policy module. It is a drift detector: if code defaults change without a
matching contract update, the check should fail.

## What belongs here

Add or revise a contract when the empirical object changes, including:

- release years or provisional/final policy
- unit of analysis or panel key
- lane-split source families or prefixes
- dictionary ambiguity overrides
- discrete-collapse behavior
- anti-garbage identifiers
- scalar conflict gates
- PRCH cleaning policy
- output artifact names
- release-manifest, verification, bundle, and baseline-comparison evidence
- known limitations or release-diff overrides

Generated data and QA files still belong under `IPEDSDB_ROOT`, not in this
directory.

## Dictionary ambiguity overrides

`dictionary_ambiguity_overrides.csv` is intentionally empty unless Stage 04 has
a known, reviewed metadata ambiguity. Stage 04 fails when more than one
dictionary row can describe the same `(year, source_file/access_table_name,
varname)` mapping. To permit an exception, add one row with:

- the relevant `year`, `source_file` or `access_table_name`, and `varname`
- one or more `selected_*` fields that identify exactly one dictionary row
- a concrete `justification`

Do not add broad or undocumented overrides. An override is part of the research
contract.
