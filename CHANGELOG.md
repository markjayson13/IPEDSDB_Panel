# Changelog

## Unreleased

- Add a separate 2024 extension workflow with frozen official sources, exact source mappings, preserved historical values, native Stata checks, and a versioned codebook.
- Preserve actual item and revision flags in keyed, labeled source supplements.
- Quarantine the two reviewed mission-only UNITID 111111 observations from the extension, retaining their complete rows and source evidence without changing Final.
- Consolidate 204 verified source columns into 95 canonical variables in the extension, preserving year-specific definitions, a downloadable crosswalk, and an exact reconstruction of every original column.
- Add metadata companions to Stage 08 extracts and embedded Parquet labels.
- Add labeled Stata datasets and Excel workbooks with dictionaries, code labels, and export issues.
- Preserve year/source-specific definitions, original types and codes, with checks for ambiguous labels and format limits.
- Add public-release governance files for sole-maintainer ownership.
- Add code and data license files.
- Add issue templates, code ownership, and pull request intake.
- Add CI checks for tests, contract validation, public-release files, docs style, and repository size.
- Add release guards to make archive preparation repeatable.

## ipedsdb-panel-2004-2023-access-final-v1

- Build a final-only `2004:2023` unbalanced institution-year panel from NCES IPEDS Access databases.
- Add row-preserving PRCH cleaning with lineage-based targeting.
- Add release manifests, checksum verification, bundle creation, Data Package metadata, and build provenance.
