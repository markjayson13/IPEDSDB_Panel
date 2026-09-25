# 2023 SFA source metadata repair

Correction version: `2023-sfa-v1`. Source release: 2023-24 Final, archive
released March 2026. The SFA tables themselves record a December 2025 final
release. Panel year `2023` indexes this collection; the two Pell measures cover
July 1, 2022 through June 30, 2023.

## Root cause and source verification

The original `varTable23` inside `IPEDS202324.accdb` assigns `UPGRNTN` (70306)
and `UPGRNTT` (70421) to `SFA2223_P2`. The separately inspected table-documentation
workbook repeats those assignments. Both columns physically exist in
`sfa2223_p1`; neither exists in `sfa2223_p2`. This is an upstream dictionary
table-reference error, not a change in the meaning of Pell recipients or aid.

The original archive was extracted afresh, and `mdb-schema` and `mdb-export`
were run against that database. All 57 physical tables were searched for each
SFA variable. The fresh metadata exports agree with the cached extraction after
normalizing embedded CRLF/LF line endings; all three SFA data exports are
byte-identical. An independent HTTP 200 download from the
[official NCES archive](https://nces.ed.gov/ipeds/tablefiles/zipfiles/IPEDS_2023-24_Final.zip)
also has the same 94,499,516-byte length and SHA-256 as the locked local archive.

| Evidence | SHA-256 |
| --- | --- |
| Original and freshly downloaded archive | `5a29f4b8d0fbdd5e091015e286dbddcecdf8d423926521a8af560967cd376629` |
| Freshly extracted `IPEDS202324.accdb` | `95983414e996ffce605f3de728b86eb572500353164da98dc818b055df3c9026` |
| `IPEDS202324Tablesdoc.xlsx` | `1e3f210f8479fb66fc203d60f01efb6e6d825b316f1a36fb96ffc7dca217384a` |
| `sfa2223_p1.csv` | `b615dab10732b9e1a49310c5a78e657387e052560841020804f18ca2d89638a9` |
| `sfa2223_p2.csv` | `f4e8ed588d511e1066db2b55694e3ad187b0d50fc881af56c1a9c43d413b9305` |

The release ReadMe explicitly says item imputation status flags are omitted
from Access because of its table-column limit. `XUPGRNTN` and `XUPGRNTT` are
dictionary associations, not physical columns in this release. Their values
cannot be recovered or inferred from the Pell amounts. Survey-level response
and imputation status are distinct from item-level flags.

The existing v2 pipeline already carried 343 individually approved
`B04-ALIAS-001` source aliases, which explains why its values and physical
lineage were correct while its dictionary still exposed the upstream table
error. The current main-branch harmonizer also had a broader unsafe fallback:
shared `source_file=SFA_P` could admit metadata from either physical part before
matching on variable name. That fallback has been removed. An exact physical
table/variable match or a documented exception is now required.

## Individual audit and correction

Every SFA dictionary definition was classified independently:

| Classification | Count | Resolution |
| --- | ---: | --- |
| Declared P1, uniquely present in P2 | 246 | Individually enumerated corrections |
| Declared P2, uniquely present in P1 | 97 | Individually enumerated corrections, including Pell |
| Correct physical assignment | 19 | Unchanged: 18 SFAV variables and `SCUGFFP` |
| Absent from all physical Access tables | 1 | `SCFA2ND` (73016), unresolved source omission; no invented substitution |

The complete affected-variable list is
[`2023-sfa-v1.audit.csv`](../contracts/source_metadata_corrections/2023-sfa-v1.audit.csv).
The correction registry is
[`2023-sfa-v1.json`](../contracts/source_metadata_corrections/2023-sfa-v1.json),
and original definitions, physical-table documentation, release evidence, and
source hashes are retained in
[`2023-sfa-v1.evidence.json`](../contracts/source_metadata_corrections/2023-sfa-v1.evidence.json).
The independent download receipt is
[`2023-sfa-v1.download.json`](../contracts/source_metadata_corrections/2023-sfa-v1.download.json).

Stage 03 verifies the pinned source archive and extracted input hashes before
applying an exact year/variable name/variable number/original table correction.
Original rows survive in `original_metadata_json`; original table labels have
dedicated columns. Resolved physical table, reason, per-variable evidence ID,
registry hash, archive hash, database hash, and physical-source hash travel with
each corrected row. Canonical source families and variable IDs remain unchanged.
Synthetic flag rows are not reclassified as physical data.

The regenerated full dictionary has 66,746 rows. Exactly 343 original rows
change only their physical table and source-table label fields; every other
original field, unrelated year, and synthetic row remains unchanged. The
208,339 codebook rows are unchanged: this release's `valueSets23` has no SFA
code rows requiring these corrections.

## Validation and reproduction

Independent raw-source comparison covers all 362 present SFA variables and
1,105,214 nonmissing values. Values and missingness agree with both the
original wide and cleaned panels. No 2023 SFA cells were changed by PRCH cleaning.
Both requested Pell variables match all 5,653 source institutions exactly.

| Baseline artifact | SHA-256 |
| --- | --- |
| `Panels/v2/panel_wide_analysis_2004_2023.parquet` | `ef3926acda4674551d5a51d0b0c3c1d56b97805021483626d62f73f1e6c4a9ef` |
| `Panels/v2/panel_clean_prch_2004_2023.parquet` | `25fb22ae14d5845991b1a615a3c4c47449e1c314bccebaa553da66252f086c4b` |
| Preserved PRCH policy | `67fca57085ec759944152f6e6e38374fac75991ef2dbd0489d241641d310e4f3` |

The supplied panel belongs to the v2 pipeline, so reproduction preserves v2
commit `799a63930f97df9a3ec35be857f340ea3743afac` with the small, versioned
[`v2-pipeline.patch`](../contracts/source_metadata_corrections/v2-pipeline.patch).
The repair performs normal full Stage 03 metadata ingestion, normal scoped
Stage 04/05 harmonization, a checked incremental scalar stitch, and normal
full Stage 06/07 wide construction and PRCH cleaning. Unchanged historical
dimensioned/source-row lanes are reused after source-row equality and empty
affected-dimensioned-scope checks. This is an incremental metadata repair,
not a claim that all twenty years were downloaded and re-extracted again.

Recreate the preserved pipeline and source evidence in new staging directories:

```bash
.venv/bin/python Scripts/QA_QC/25_prepare_v2_repair_pipeline.py --output /tmp/ipeds-v2-repair-pipeline
.venv/bin/python Scripts/QA_QC/22_verify_sfa_source_metadata.py \
  --root /Volumes/CIRAGO/IPEDSDB_PANEL --output /tmp/ipeds-sfa-source-verification
```

`Scripts/QA_QC/24_rebuild_metadata_repair.py` records stage commands, code and
input hashes, protected-cell equality, full-panel equality, and PRCH action
equality. Its `--phase` interface supports a resumable staged build.
`Scripts/QA_QC/23_verify_sfa_values.py` independently compares fresh source
cells through physical value lineage with both supplied panels.

The downstream `/Volumes/CIRAGO/FSA-IPEDS_DS` is outside this repair.

## Export safeguards

Strict export readiness now checks complete source-component coverage,
unambiguous codebook identities, dictionary/codebook schema, institution-year
keys, and observed category values. Supplied units, currency, price basis,
and reference-period conventions are checked across source records. Missing
semantics remain explicitly unknown; no cross-year comparability is invented.

Stage 09 and SQL panel exports share the metadata and readiness machinery.
Computed SQL columns do not inherit source labels by their output names.
Input file or dataset-directory checksums are verified again before publication.
Failed package replacements restore previous data and companions. Multi-file
rollback is not a promise of crash-atomic visibility to concurrent readers;
completed versioned packages provide the publication boundary.

## Completed validation and corrected outputs

The regenerated wide and cleaned panels each contain 141,711 rows and 2,721
columns. Exact comparison confirms identical schemas, values, and missingness
against the supplied baseline, including every unrelated variable and year.
All 150,801,591 value-lineage rows and all 17 lineage fields are unchanged.
All 286,180 PRCH action records and the original policy are unchanged.

Exports of all 343 corrected variables plus the two keys passed exact readback
in Parquet, CSV, Stata, and Excel for all 6,163 panel institutions in 2023.
Native Stata verified every observation and all 345 variable
labels. The Stage 09 dictionary export passed strict readiness. The final
software suite passed all 169 tests. Missing measurement semantics remain
explicitly unknown; these checks do not assert undocumented comparability.

Use the versioned correction bundle at
`/Volumes/CIRAGO/IPEDSDB_PANEL/Metadata_repairs/2023-sfa-v1`.
The original canonical output files are retained as the before-repair baseline;
the corrected dictionary and panel must be used together from this new root.
The downstream FSA dataset was not modified.

Paths below are relative to that correction root. `manifest.json` records every
absolute output path, SHA-256, source dependency, and validation receipt;
`SHA256SUMS` also covers the manifest itself.

| Corrected output | SHA-256 |
| --- | --- |
| `Dictionary/v2/dictionary_lake.parquet` | `f5209f4533f675bbbd6d81448c4a7758e141bbf6cbd7b976c051c4a722474163` |
| `Dictionary/v2/dictionary_codes.parquet` | `acf5dba41e9bf69b8000471ef9e6ad65e3a8a38353756c72a24ac9263619b356` |
| `Panels/v2/panel_long_scalar_2004_2023.parquet` | `2beb87b673dac90048c073b4e3172ca6caa37f089232cceaad440b0e09e620b4` |
| `Panels/v2/panel_wide_analysis_2004_2023.parquet` | `5df2f40a4dc5d4df43a92503bdf56a9dfa96499883949757214392e1fa7a9fde` |
| `Panels/v2/panel_clean_prch_2004_2023.parquet` | `e2b2d61e85452b76f5f8088f67c381bd7086cd91b2472b87755d6da731ccadde` |
| `Checks/v2/wide_qc/qc_value_lineage.parquet` | `0bfb36726fecd155d3d231ab2dc5c8237967995e2edc58937ab082c844bca307` |

Rewritten Parquet bytes have different hashes because the pipeline reserialized
them; exact observation and lineage equality, rather than byte identity, is the
metadata-only acceptance check. Complete original metadata and the enumerated
correction records are retained under `Evidence/source_metadata_corrections`.
