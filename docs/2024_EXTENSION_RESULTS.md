# 2024 extension results

Release identifier: `2024-extension-v1`.
Published under `/Volumes/CIRAGO/IPEDSDB_PANEL/Provisional`, which resolves to
`Releases/2024-extension-v1/`. The existing `Final/` release remains unchanged.
See the [2024 extension guide](2024_EXTENSION.md) for methods, interpretation,
source exceptions, and reproduction commands.

| Dataset | Rows | Columns |
|---|---:|---:|
| Preserved 2004-2023 input | 141,711 | 2,721 |
| Validated 2024 input, before quarantine | 6,073 | 2,093 |
| Unfiltered 2004-2024 append | 147,784 | 2,785 |
| Validated selection before consolidation | 147,782 | 2,785 |
| Validated consolidated analysis | 147,782 | 2,676 |

The append preserves all original and 2024 values and missingness; its 64 new
columns are null in historical years. The approved `mission-orphans-v1` policy
then excludes exactly `UNITID=111111` in 2019 and 2024: mission-only records
with unverified institutional identities. Both rows and the unfiltered append
remain in `Supporting/`. All 411,572,870 retained cells are unchanged.
`Final/` and raw sources remain unchanged; downstream FSA data are outside scope.

The reviewed `source-family-consolidation-v1` policy combines 204 source
columns into 95 canonical columns across 95 groups, a reduction of 109 columns.
The 2,785-column input remains preserved. The joins cover 87 complete families
and eight partial families: seven pre-2012 salary components and
`F1SYSNAM_2006_2024`. All selected members have disjoint observed-year scopes;
no selected member has a dictionary or compact-lineage year outside those
scopes. All 106 excluded source columns remain intact, including `GRRTAP`:
its 2010 definition gives a different two-year-institution cohort lag from
2009, and the discrepancy remains unresolved.

The policy preserves values and year-specific source meanings. It does not
standardize changed salary formulas, admission codes, SAT versions, or aid
reporting periods. The codebook and original-to-canonical crosswalk disclose
original names, years, reasons, and caveats. Exact reconstruction recovered all
411,572,870 cells of the 2,785-column input from the consolidated panel, with
values, missingness, row keys, and row order unchanged. Native Stata validation
and historical encoding checks also passed for the 2,676-column export.

Source review covers 52 tables and 2,655 columns; 2,091 scalar mappings
bind exact table and variable identities. Documented adjacent-year matches
handle Cost/SFA relocations without assuming constant measurement definitions.
Fall revisions carry per-table evidence; winter and spring remain provisional.
Detailed completions (`C2024_A`) retain complete provisional Access data outside
the scalar panel, with the final standalone file preserved separately. Eleven
fields retain their columns after all 65,562 source cells were verified
blank under exact source-hash, row-count, identity, and year checks.

| Check | Verified result | Package evidence |
|---|---|---|
| 2024 raw source and PRCH reconciliation | 12,698,643 cells compared, including missing; zero raw-to-wide discrepancies or unexpected cleaning changes; 16,221 actions verified | `Checks/raw_source_validation.json` |
| Historical and new-year append | Every input value and missing cell preserved; unique institution-year keys | `Supporting/*.append-validation.json` |
| Exact quarantine | Two authorized keys only; all retained and quarantined cells preserved | `Supporting/*.quarantine-validation.json` |
| Reversible column consolidation | 411,572,870 cells reconstructed exactly; 204 source columns joined in 95 groups; 2,785 input columns reduced to 2,676 without changing values or missingness | `Supporting/panel_clean_prch_2004_2024.consolidated.parquet.consolidation-validation.json` |
| 2024 metadata readiness | Strict metadata check passed; no issues | `Checks/2024_metadata/panel.parquet.metadata.json` |
| Native Stata | All 147,782 rows and 2,676 columns verified in three chunks; 2,676 variable labels and 10,930 value labels checked in the declared export representation | `Checks/native_stata/validation.json` |
| Historical Stata compatibility | All 2,721 historical variables and 10,718 category numbers checked; 204 reviewed alias changes; numeric encodings and storage conversions preserved, with unmerged aliases unchanged | `Checks/stata_history_validation.json` |

Pell fields `UPGRNTN`/`UPGRNTT` resolve to `SFA2324`: 5,575 source observations,
5,486,771 recipients, $29,140,280,733, and zero cleaning changes. Academic
reporters use the fall 2023 undergraduate cohort and institution-defined
academic year 2023-24; program reporters follow their applicable instructions.
These scopes differ from annual FSA volume. All 91 actual SFA item flags remain
separately keyed and labeled in `Sources/2024/sidecars/sfa2324_supplement.csv`.

Historical metadata remains incomplete: five description issues (missing,
incomplete, or conflicting) and 6,256 code observations lack safely resolved
definitions. The combined `.metadata.json` identifies affected variables and
years. Native Stata validation verifies stored values and declared labels;
these source limitations remain disclosed.

Fresh-checkout reproduction is also verified: all 663 tests pass without the
historical Git objects. The checksum-pinned source snapshot reproduces all 124
corresponding prepared pipeline files byte for byte; the published package and
its data checksums remain unchanged.

## Final artifact hashes

All paths below are relative to `/Volumes/CIRAGO/IPEDSDB_PANEL/Provisional/`.
The [machine-readable release record](../Artifacts/2024_extension_release.json)
includes exact paths, all canonical variable names, retained historical hashes,
and additional validation checksums.

| Artifact | SHA-256 |
|---|---|
| `manifest.json` | `3b44c2c442c41eab0d4aed0062bc74f7fe5418945ec833bb2147d6596404e1d9` |
| `panel_clean_prch_2004_2024.parquet` | `7b9935cebf6b09a504c34679eefbbeaeebf18d696db9afd3ff0e1367b9026ba4` |
| `panel_clean_prch_2004_2024.dta` | `1be8f6b3353eafec93891e7a492fcce7c9534f8a073a333c0c1352f53f0fc51d` |
| `Codebook/ipeds-panel-codebook.pdf` | `b5986a2a4fc1ee35cab4a8c1def69e9b00b7e745d64c877f917fea116c785913` |
| `Codebook/column-crosswalk.csv` | `5819df404fb3fe39fb350b0412884320bdebc2e092cf02b79efc517c2fe68324` |
| `Checks/native_stata/validation.json` | `890b26708aa75db87565bba0abdbfe6303a366c01b6091981bb3e740b8397bdb` |
| `Supporting/panel_clean_prch_2004_2024.consolidated.parquet.consolidation-validation.json` | `0d86fea1f98fe2a0e72ff6b3ddf2f65fcedd9ec4b84fa51f039c266fb52e981d` |
