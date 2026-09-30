# 2004-2024 extension

This extension reuses the verified 2004-2023 panel and processes only 2024.
`Final/` continues to identify the final-only 2004-2023 release. A validated
extension is published separately under `Provisional/`, with its complete
evidence retained in `Releases/2024-extension-v1/`. The downstream FSA volume
is outside this build's scope.

See [validation results and checksums](2024_EXTENSION_RESULTS.md) for the
published dataset paths, exact quarantine counts, and export checks.

## Sources and interpretation

The original 2024-25 Access database is provisional. Its ZIP SHA-256 is
`ef134955ba5003a07d37e46fae6286a76dca4c15767c3c4cd9086b1887896713`;
the database SHA-256 is
`98e9175e54ba719cf7f0bfcc8e043fac4d11babd1b73ba1701ebbdc23e5470f5`.
Original metadata and physical tables are preserved. Current official final
fall CSV revisions are applied only after comparisons with their original
Access values, exact source keys, and dictionaries. Winter and spring sources
remain provisional. Every table records its actual release status/date and
source/member hashes.

Detailed completions (`C2024_A`) is a separate exception: the Access table
contains 1,380,764 aggregate records absent from the standalone CSV. Replacing
it wholesale would lose data. Its dimensioned lane retains the unchanged
provisional Access table; the complete final revised CSV and key differences
are preserved separately. It supplies no institution-year panel columns.

Actual item/revision flags omitted from Access remain in keyed, labeled
`Sources/2024/sidecars/` files, including all 91 SFA item flags. Do not mistake
a blank main-panel value or a synthetic pipeline flag for the original NCES
imputation status. Where the Excel dictionary omitted `S`, the matching
official, same-table Stata import script supplies `S = Suppressed`; the
original workbook and hash-bound supplement are both preserved.

Pell recipients and dollars (`UPGRNTN`, `UPGRNTT`) resolve to `SFA2324`.
For academic reporters these refer to the fall 2023 undergraduate cohort and
institution-defined academic year 2023-24. The table's broad July-June coverage
does not establish a universal student population or make its amounts equal
to annual FSA volume. Program reporters have their applicable reporting-period
instructions. See the [official 2024 SFA form](https://nces.ed.gov/ipeds/use-the-data/download-survey-material/2024/student%20financial%20aid/package_7_16.pdf).

## Mapping and cleaning

Fresh checkouts prepare the original v2 code from the checksum-pinned
[source snapshot](../contracts/reproduction/README.md). This removes the
dependency on an old local Git commit while preserving the original scripts,
contracts, and cleaning-policy bytes.

The preserved v2 pipeline is extended through the explicit, 2024-only tables
in `contracts/extension_2024/`. It checks all 52 physical data tables and 2,655
columns. Each scalar output has an exact table, variable name, variable number,
and source identity. Cost/SFA relocations use documented adjacent-year matches;
the returning SFA source-family name cannot attach a 2024 field to a retired
2004-2008 column simply because an old identifier recurs.

Historical parent/child policy bytes remain unchanged. New 2024 rules preserve
the same meaning while reflecting the new source layout. For the documented
finance partial child with missing form metadata, the asset/liability-derived
equity ratio is nulled and the expense ratio retained; the form is never
inferred from a variable prefix. Participation indicators remain context
fields, outside aid amount/count null targets.

Eleven fields are physically blank in both original and effective 2024
sources: `CHG10AY0/1/2`, `CHG10PY0/1/2`, `PCADM_F`, `PCCOS_F`, `PCGR2_F`,
`PCGR_F`, and `PCOM_F`. All 65,562 source cells were checked. Their columns
and definitions remain in the output under an explicit source-hash, row-count,
identity, and year check. Unexpected all-null fields still fail validation.
The observed blanks do not establish why NCES left those fields empty.

Original 2004-2023 values, missingness, column order, and source records are
compared after the append. New columns must be null in historical years.
Unmerged variables retain their published Stata aliases. Consolidated variables
use the reviewed canonical names; their original numeric category encodings
and storage conversions are preserved. New string categories receive new
numbers after the historical maximum.

The extended analysis files quarantine exactly two mission-only observations:
`UNITID=111111` in 2019 and 2024. Fresh exports from the original Access
databases confirm their mission URLs, but neither year has a corresponding
directory or response-flag record. Their institutional identities remain
unverified. This is not a claim that they are confirmed fake institutions.
The user approved excluding both from the extension on September 29, 2026.

`Supporting/quarantined_mission_records.parquet` preserves both complete rows.
The unfiltered append remains in `Supporting/`; the separate analysis copy
removes only those two exact keys after checking the expected URLs and that
every other non-key field is null. A versioned policy, original-source audit
receipts, and a full retained-cell comparison accompany the files. All other
historical observations and values are unchanged. `Final/` and raw sources
are unchanged. Parquet and export metadata carry the quarantine provenance.

The reviewed `source-family-consolidation-v1` policy then consolidates columns
split by source-table moves. Its 95 groups combine 204 source columns into 95
canonical columns, reducing the analysis file by 109 columns. The analysis
shape is 147,782 rows and 2,676 columns; the 2,785-column input remains preserved.

The policy covers 87 complete variable families and eight partial joins.
Seven salary joins use names ending in `_PRE2012` and keep the later salary
formula separate. `F1SYSNAM_2006_2024` keeps the older system-name field separate.
Conflicting sports measures, changed system-membership codes, uncertain aid
periods, and other excluded source columns remain intact. `GRRTAP` remains
separate because its 2010 source definition changes the two-year-institution
cohort lag from three to six years without a verified explanation. Consolidation copies
values without recoding; it does not remove changes in definitions, category
meanings, test versions, or reporting periods within the series.

Exact original names and observed-year scopes make each join reversible.
Annual source identities and definitions remain available, including recorded
caveats. The generated codebook detail, PDF, and `column-crosswalk.csv` explain
the joins. The compact policy and compressed NCES definition/code evidence are
versioned under `contracts/harmonization/`. Full value and missingness checks
must prove exact reconstruction before the consolidated file is accepted.

## Reproduce without rebuilding history

Use the repository's Python environment and a fresh work directory with enough
space for the source snapshot, single-year intermediate files, labeled exports,
and DuckDB spill files. Use CIRAGO for `--work`, or set `--spill-root` to a
directory there; a disk-backed lineage join can need more than 20 GB of temporary
space. The tested stitch configuration uses 4 GB of RAM and one DuckDB thread.
A frozen source snapshot is verified by hashes before use. The normal
candidate command runs all five pipeline stages for 2024, appends the existing
historical release, exports labels, runs native Stata, and builds the codebook:

```bash
python Scripts/build_2024_extension.py --phase candidate \
  --root /Volumes/CIRAGO/IPEDSDB_PANEL \
  --sources /path/to/verified/2024-source-snapshot \
  --work /path/to/fresh/2024-reproduction
```

The source snapshot is created by `Scripts/prepare_2024_sources.py`; its input
audit and official download evidence are described in
[the post-2023 source audit](POST2023_READINESS.md). Once published, use the
retained `Provisional/Sources/2024` snapshot to avoid downloading or extracting
the original database again. Separate `build`, `assemble`, `consolidate`,
`export`, and `validate` phases support inspected checkpoints; none silently overwrite a
completed output.

For a replay using the release's frozen code, first copy
`Provisional/Reproduction/pipeline-2024/` to the fresh work directory as
`pipeline/`, then invoke the retained
`Provisional/Reproduction/extension-code/Scripts/build_2024_extension.py` with
the same candidate arguments. The existing prepared pipeline is reused.
The retained extension-code tree includes `Scripts/panel_consolidation.py`,
the consolidation policy, and its compressed definition/code evidence.
The runner resolves these files within that frozen tree and verifies the
policy and evidence hashes before consolidation; replay does not substitute
the current checkout's versions.
Repository-wide tests also require the full Git checkout and website
assets. Export timestamps and provenance paths can change between runs;
compare values, missingness, schema, source versions and labels, then retain
the new run's checksums.

The two reviewed mission-source audit receipts are versioned under
`contracts/source_quality/evidence/` and bound by SHA-256 in the quarantine
policy. Assembly copies and verifies them in every fresh candidate, so a replay
does not depend on manually populated work-directory checks or rerunning the
2019 source audit. The original release also retains the fresh 2019 CSV exports
and audit script under `Checks/2024/2019-mission-audit/`; the large source
database and archive remain in their original source locations.

Publication uses the fully verified package:

```bash
python Scripts/build_2024_extension.py --phase publish \
  --root /Volumes/CIRAGO/IPEDSDB_PANEL \
  --work /path/to/fresh/2024-reproduction
```

It verifies the copied package before exposing `Provisional/`, refuses to
replace an existing release, and leaves `Final/` unchanged. Full raw-value
lineage stays split between the immutable historical release and the 2024
lineage; `Metadata/value_lineage_sources.json` records both exact hashes.

## Acceptance evidence

`Checks/raw_source_validation.json` independently compares every mapped 2024
raw source cell with the pre-cleaning panel, then checks cleaning cells and
the complete action ledger against the explicit policy. The 2024-only export
must pass strict metadata readiness. The combined package retains historical
source gaps rather than inventing descriptions or category meanings.

`Supporting/*.append-validation.json` records all-cell historical and new-year
value/missingness equality before quarantine. The analysis file's
`.quarantine-validation.json` separately proves exact exclusions and complete
retained value/missingness equality. `Checks/native_stata/validation.json` covers every
row and column, native variable labels, native value labels, and the declared
Stata representation. Consolidation requires a separate exact reconstruction
check and fresh validation of the smaller exported schema. `manifest.json` binds the final datasets, codebooks,
sources, metadata and validation receipts to their hashes.
