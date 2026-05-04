# Migration notes

`IPEDSDB_Panel` is the current repository. `IPEDS_Paneling` is the predecessor project.

The current repository keeps the Access-database workflow, panel contract, release guards, public metadata, and QA scripts in one place. Generated data stays outside git under `IPEDSDB_ROOT`.

Material migration decisions:

- The current build uses NCES Access databases as the upstream source rather than flat component files.
- The canonical release window is `2004:2023` final-only Access data.
- The canonical panel spine remains `UNITID-year`.
- Stage 06 freezes lane-split settings in `contracts/panel_spec.toml`.
- `EAP` is not an exact dimensioned source in the current wide-build contract. It remains covered by PRCH policy and lineage-based cleaning rules.
- Legacy schema seeding is retained through `Artifacts/legacy_analysis_schema_seed.csv` and must be visible in wide-build QC.
- Parent-child cleaning is row-preserving and component-aware; it is not a full parent-level institutional consolidation.

Migration review files:

- `contracts/panel_spec.toml`
- `contracts/transformation_ledger.csv`
- `contracts/known_limitations.csv`
- `Scripts/QA_QC/15_compare_release_to_baseline.py`
