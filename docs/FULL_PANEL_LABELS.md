# Full-panel export labels

Published as `Releases/full-panel-labels-v1` on 2026-09-29. The source
panel has 141,711 institution-year rows and 2,721 columns. The immutable source
is `Releases/2023-sfa-v1/Panels/v2/panel_clean_prch_2004_2023.parquet` under
`/Volumes/CIRAGO/IPEDSDB_PANEL`, with SHA-256
`e2b2d61e85452b76f5f8088f67c381bd7086cd91b2472b87755d6da731ccadde`.

The earlier full Parquet did not embed its metadata. Categorical columns such
as `CONTROL` and `SECTOR` used string storage, so the Stata writer left their
code labels in companion files. Historical labels also differed across years,
and default CSV missing-value parsing swallowed the literal label `None`.

The updated export embeds definitions and codebooks in Parquet. Stata attaches
native variable and value labels, preserving canonical integer codes and using
documented reversible encodings for other categorical strings. Export
validation verified 400 native value-label sets with 10,718 definitions: 390 preserve integer codes
stored as strings; 10 use reversible encodings. Ordinary strings remain text.
Stata represents original null strings as empty strings; companion metadata
retains source null counts and original code mappings.

Changing meanings are displayed with explicit year ranges. A changing variable
title carries `[varies by year]` and the latest observed year's title; all source
definitions remain available. These displays do not establish comparability.

## Source evidence and remaining gaps

The versioned [supplement registry](../contracts/source_metadata_corrections/2026-09-exact-year-labels-v1.json)
contains 1,553 code definitions across 36 source/year scopes from 17 same-year
official NCES dictionary archives. Its [evidence manifest](../contracts/source_metadata_corrections/2026-09-exact-year-labels-v1.evidence.json)
records source URLs, workbook locations, and hashes. Supplements fill missing
or blank entries after matching source identity; original metadata is retained.

Direct exports from the original 2008 and 2010 Access databases confirmed the
target codebook gaps. `valuesets08` contains the label `None` for code 0 of
`CUFASB` and `CUGASB`; code 38 is absent. `valuesets10` omits seven inspected
codebooks and contains later fiscal-year labels under its 2010 release.
Target rows match the retained CSVs exactly. Whole-table comparisons require
CRLF-to-LF normalization in six unrelated label rows per year.

| Original database | Independently recomputed SHA-256 |
| --- | --- |
| `IPEDS200809.accdb` | `d00a945dd777b9067df907c3cf32b7bca3c09d62c5c3b6df52d0c6acd3dd6eca` |
| `IPEDS201011.accdb` | `4742c4547095006d475ec589396c117e7987dd88e517852df862ab5eca82d81d` |

The bounded Access check is recorded in
`Evidence/original_mdb_codebook_gap_verification.json`, with receipt SHA-256
`bb14d5a46278e843f50eed0eb4b8f43199bc34595665ba273aed67cdaed45e38`.
It lists exact database-generation paths; it does not claim a new audit of
every original database.

The metadata audit reports 2,716 complete variables and five with missing
source descriptions: `F1C196`, `LINE_55`, `PCF_F_RV`, `REV_IC`, and `SFTETOTL`.
The last three also have different historical descriptions; blanks prevent
complete scoped rendering, although nonblank descriptions agree within each
year. No descriptions are invented.

These 6,256 nonmissing observations still lack a safely resolved same-year
code definition:

| Variable | Observations |
| --- | ---: |
| `ADMCON9` | 2 |
| `CUFASB` | 1 |
| `CUGASB` | 1 |
| `DEATHYR` | 229 |
| `STAT_AL` | 6,023 |

Their source values remain intact. The package must disclose these gaps and
must not claim complete metadata readiness. `--require-ready` continues to
reject affected exports. Unknown codes do not receive guessed meanings from
other years.

## Reproduction

Run from the repository with its Python environment. Use a new Work package
name for another run; publication refuses to overwrite an existing release.
The versioned supplement is applied by the exporter. Retain the checked
official archives and source audit receipts in the package's `Evidence/`
directory before publication.

```sh
ipeds_label_root=/Volumes/CIRAGO/IPEDSDB_PANEL
ipeds_label_package="$ipeds_label_root/Work/full-panel-labels-v1"
ipeds_label_source="$ipeds_label_root/Releases/2023-sfa-v1/Panels/v2/panel_clean_prch_2004_2023.parquet"
ipeds_label_stem=panel_clean_prch_2004_2023

.venv/bin/python Scripts/08_build_custom_panel.py \
  --root "$ipeds_label_root" --input "$ipeds_label_source" \
  --output "$ipeds_label_package/$ipeds_label_stem.parquet" \
  --all-vars --year-scoped-labels --log-file ""

.venv/bin/python Scripts/08_build_custom_panel.py \
  --root "$ipeds_label_root" \
  --input "$ipeds_label_package/$ipeds_label_stem.parquet" \
  --output "$ipeds_label_package/$ipeds_label_stem.dta" \
  --all-vars --log-file ""

.venv/bin/python Scripts/QA_QC/28_validate_full_stata.py \
  --input "$ipeds_label_package/$ipeds_label_stem.dta" \
  --source-panel "$ipeds_label_source" \
  --output-dir "$ipeds_label_package/Checks/native_stata"

.venv/bin/python Scripts/publish_labeled_panel.py \
  --root "$ipeds_label_root" --package "$ipeds_label_package" \
  --allow-source-gaps --apply

.venv/bin/python Scripts/organize_data_root.py \
  --root "$ipeds_label_root" --apply
```

The export commands intentionally omit `--require-ready` because of the
documented source gaps. Publication still requires exact Parquet value and
missingness equality against the original panel, matching embedded and
companion metadata, and a native Stata receipt bound to the exported files.
`--allow-source-gaps` cannot waive invalid keys, malformed codebooks, ambiguous
identities, precision loss, or failed format checks.

The publisher moves the verified package into `Releases/full-panel-labels-v1`
and updates `Final` links. It preserves `Releases/2023-sfa-v1`, its dictionary,
value lineage, and earlier SFA subset exports. The new manifest records every
package file, source fingerprints, validation results, and remaining gaps.
Historical receipts retain export-time Work or temporary paths; use
`Final/manifest.json` for current artifact paths and matching checksums.
`/Volumes/CIRAGO/FSA-IPEDS_DS` is outside this work.

Stata/BE can load at most 2,048 variables. Use a selected varlist from the full
file, or use Stata/SE or MP to load all 2,721 variables:

```stata
use UNITID year CONTROL SECTOR using "/Volumes/CIRAGO/IPEDSDB_PANEL/Final/panel_clean_prch_2004_2023.dta", clear
```

The native validator reads the same full file in chunks of at most 900 columns
and compares every row, variable label, declared value label, and missing-value
representation. A real 2,050-column fixture already passed this Stata/BE path.

## Published files and checks

Paths below are relative to `/Volumes/CIRAGO/IPEDSDB_PANEL`. The package
manifest covers all 55 release artifacts, including the retained official
source dictionaries and verification receipts.

| Artifact | SHA-256 |
| --- | --- |
| `Final/panel_clean_prch_2004_2023.parquet` | `f93cf45fe50e04250465a0b583a384737f4954cb7034f5a880f853d55827bb61` |
| `Final/panel_clean_prch_2004_2023.dta` | `37570f3a5f54a28a6ea02f1888d8e89e89cee499cc04100e36badcbd5f07386f` |
| `Releases/full-panel-labels-v1/manifest.json` | `bfadc2cc4bc0b2cbe985a637a39687183a39e0c73f759aa04b7e4a721336975d` |
| `Final/manifest.json` | `c4788db7ce5c711fb8476d8e39bd676bd26cead27593bd4231fb5aee2d20cbcd` |
| `Releases/full-panel-labels-v1/Checks/native_stata/validation.json` | `59de562ad7c489f194f084087e61c714ecaa103135df77f21053f814b81961a8` |

Native Stata checked the full file in chunks of 900, 900, 900, and 27 columns.
It verified all 2,721 variable labels and all 10,718 value-label definitions.
The checker used a temporary local copy whose checksum matched the published
Stata file; the execution receipt records this, and the temporary copy was
removed afterward.

- [x] Exact full Parquet values and missingness match the immutable source.
- [x] Native Stata validation passes for every full-panel column and row.
- [x] Data and package checksums recorded above.
- [x] `Final` links and both release manifests pass verification: all 55 labeled-release artifacts and all 441 original-release artifacts retain their recorded checksums.
- [x] 318 regression tests passed; contract, public-release, documentation, and repository-size checks passed for the published repository files.
