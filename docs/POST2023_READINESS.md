# Extending the panel beyond 2023

Verified September 29, 2026 (America/Los_Angeles), against repository commit
`53dbcf313d7afe574eb4479ce43b3f910fe2dadc` and fresh official NCES downloads.
This is a source and pipeline readiness audit, not an extended panel release.
The existing `Final/` files and downstream FSA files were not modified.

The subsequent integration is documented in the [2024 extension results](2024_EXTENSION_RESULTS.md).
The audit below preserves the findings and decision made before that build.

## Decision

A 2024 extension is feasible, but it must identify provisional components and
undergo a reviewed pipeline update. The current 2004-2023 final-only release
should remain available unchanged. Do not extend it merely by changing an
end-year argument. A complete 2025 extension, including SFA/Pell, is not yet
supported by the available releases.

| Panel year | Collection | Verified availability | Recommended treatment |
| --- | --- | --- | --- |
| 2024 | 2024-25 | Complete Access package labeled Provisional, March 2026; newer final fall component files released September 8, 2026; winter and spring still provisional | Separate 2004-2024 candidate using explicitly locked component versions; retain status/date in variable-year source metadata |
| 2025 | 2025-26 | Fall provisional data released July 28, 2026; no annual Access package listed; SFA and spring components unavailable | Do not publish a complete annual extension; any early fall-only extract must disclose its component coverage |

These findings come from the live [Access inventory][access], [release
schedule][schedule], [2024 complete files][files24], [2025 complete files][files25],
and [2025-26 fall release memo][fall26]. Collection year is not a universal
measurement year: 2025 IC concerns fall 2025, while its completions and E12 data
concern 2024-25.

## Original-source verification

Downloaded the original 2024-25 Access ZIP twice (ordinary transfer and checked
HTTP ranges); both downloads have the same SHA-256. ZIP member CRC checks passed.
Freshly extracted the original database, inspected its complete physical schema,
and independently downloaded the standalone metadata workbook and SFA CSV.

| Check | Result |
| --- | --- |
| Original database | 56 physical tables, 618,156,032 bytes |
| Declared variables versus physical table columns | All 2,599 definitions match after ordinary whitespace normalization; `OMASSC4 ` has a trailing space in the dictionary |
| SFA table-reference audit | All 112 definitions in SFA2324/SFAV2324 match their declared physical tables |
| Pell source | UPGRNTN (70306) and UPGRNTT (70421) both physically and descriptively belong to SFA2324; the 2023 P1/P2 conflict does not recur |
| Access versus standalone workbook | Variable number, name, table, title and imputation-variable identities agree for every definition |
| Access SFA versus independent official CSV | Same 5,575 unique, nonmissing UNITIDs; all 91 shared non-key fields agree: 507,325 cells, zero mismatches |
| Pell comparison | Both variables agree for every one of the 5,575 institutions, including missingness |
| Source categorical-code check | 166 categorical variables and 1,001,670 nonblank cells across extracted HD/FLAGS/SFA/Cost tables; no observed code outside its exact-year codebook |

CSV comparisons align by UNITID and compare numeric lexical variants using
exact decimal conversion; they trim text padding and keep blank distinct from
nonblank. These are raw-source checks, not proof of an already constructed or
cleaned 2024 panel. They do not constitute cell-level validation of every other
2024 survey component.

The separate SFA CSV contains 91 item-status fields omitted from Access. The
original Access README documents this omission. The official [SFA dictionary][sfadict]
confirms every observed Pell status code:

| Code | Meaning | XUPGRNTN | XUPGRNTT |
| --- | --- | ---: | ---: |
| R | Reported | 5,548 | 5,558 |
| C | Analyst corrected reported value | 12 | 2 |
| Z | Implied zero | 7 | 7 |
| P | Imputed using Carry Forward procedure | 7 | 7 |
| N | Imputed using Nearest Neighbor procedure | 1 | 1 |

An extension can preserve these actual flags with their source labels. It
should not synthesize them from amounts or equate all non-reported statuses.

## Why the March Access package is not the latest complete source snapshot

Downloaded current `IC2024.zip` and `C2024_B.zip`. Each contains an original CSV
and a revised `_rv.csv`. The fresh Access tables exactly match their original
CSV fields. Compared with the final revised files, the Access payload has:

- IC2024: 19 changed data cells among 5,963 institutions and 109 shared fields.
- C2024_B: 509 changed data cells among 5,827 institutions and 40 shared fields.

The institution sets match. Comparing all original/revised CSV fields,
including their extra item-status flags, produces 20 and 595 changed cells,
respectively. These are different comparison scopes, not conflicting counts.
EFFY2024 also contains original and revised files, while EFFY2025 contains only
its provisional file; both archives passed integrity checks.

For a current extension, select the actual revised fall members and preserve
the provisional winter/spring status. An alternative is a deliberately frozen
March provisional snapshot, clearly described as such. Do not call that snapshot
the latest final 2024 data or silently mix source versions.

## Measurement and FSA linkage

The 2024 SFA table describes 2023-24 aid, with broad table coverage July 1, 2023
through June 30, 2024. This does not establish the same student population or
period for every institutional reporting type.

Directly compared the [2023 academic-reporter form][survey23] and [2024 form][survey24]:
Part B uses students enrolled in fall 2022 with institution-defined academic-year
2022-23 aid, then fall 2023 with academic-year 2023-24 aid. Program reporters have
their applicable reporting-period instructions. This qualifies the earlier
handoff's broad July-June shorthand; it is not evidence of a newly introduced
Pell cohort break in 2024. Annual FSA volume is not automatically equal to IPEDS
aid awarded to these cohorts.

2024 HD has 6,072 institutions. Its fixed-width OPEID strings require trimming
without dropping leading zeros: 6,034 valid eight-digit entries, 37 `-2`
sentinels, and one blank. There are 39 shared valid OPEID groups involving 79
UNITIDs. A blind one-to-one OPEID merge is therefore unsafe. Preserve sentinel
meaning, verify year-specific cardinality, and avoid copying a reporting unit's
entire FSA amount to each campus.

The [2024-25 survey changes][changes] move tuition/cost and net-price items into
Cost (CST). Identity comparisons locate 254 former SFA variable-number/name
identities and 205 former IC identities in 2024 Cost tables. This is a location
inventory, not an approved cross-year semantic crosswalk. The dictionary has 98
new identities and 195 prior identities absent under the same number/name pair;
these need explicit decisions, not automatic missing-value substitutions.

The [FAFSA/SAI/Pell eligibility changes][pellrules] apply to award year 2024-25,
which matters for prospective panel-year 2025 Pell. They should not be assigned
to panel-year 2024 merely because its collection label is 2024-25. Academic
Libraries retires with collection 2025-26; later absence is structural.

## Pipeline findings and minimum work

The current final dataset was reproduced through preserved v2 commit
`799a63930f97df9a3ec35be857f340ea3743afac` plus the documented repair patch and
frozen PRCH policy. The generic main runner is not a proven replacement for
that build. See [the repair reproduction record](2023_SFA_METADATA_REPAIR.md).

Executed probes establish the following:

1. Downloading/harmonization explicitly rejects provisional releases. The main
   contract and validator also hardcode 2004-2023. A 2024 contract must have a
   distinct release identity and deliberate provisional support.
2. Preserved v2 correctly rejects all tested 2024 SFA/Cost tables because they
   lack reviewed table-grain contracts. Extend source-column, grain, and PRCH
   contracts, preserving historical rules.
3. The main harmonizer blocks an analogous known P1/P2 mismatch, but entirely
   undocumented new tables/columns can be skipped. Require every physical
   source column to be included or explicitly excluded with a reason.
4. Main canonicalization produces COST, COST_FINANCIALAID, COST_NETPRICE and
   DRVCOST. Only COST is targeted by its PRCH_COS policy. Its generic DRV rule
   also classifies DRVCOST as dimensioned. These need explicit review. Actual
   FLAGS2024 has PRCH_COS=-2 for all 6,072 records, so this coverage gap is not
   evidence of current Cost child duplication.
5. Actual FLAGS2024 has one SFA parent and one child. The child has no SFA2324
   row, consistent with reporting through its parent. Preserve those semantics;
   do not assume every HD institution has a standalone SFA observation.
6. Cache reuse can pair a changed database with old extracted CSVs. Use fresh,
   versioned staging with archive/database/member hashes for each revision.
7. Older-definition backfills, optional runner QA, and fixed publication/codebook
   filenames are not sufficient extension safeguards. Exact-year metadata,
   full acceptance, and release-aware publication are required.

Before publishing: require unchanged 2004-2023 values and missingness; complete
new-year source coverage; explicit source revisions and semantic changes;
year-specific PRCH action validation; key/category/metadata checks; raw-to-panel
parity; labeled Parquet/Stata and native Stata readback; refreshed codebook; and
verified package hashes. Keep the current release immutable and separately
identify any provisional extension. No extended dataset has been published by
this audit.

## Evidence and hashes

Machine-readable results, source receipts and pipeline probes:
[`Artifacts/post2023_readiness_2026-09-29.json`](../Artifacts/post2023_readiness_2026-09-29.json).
Every declared variable's physical-table check:
[`Artifacts/post2023_variable_table_audit_2026-09-29.csv`](../Artifacts/post2023_variable_table_audit_2026-09-29.csv).
Raw downloads and temporary audit commands remain at
`/private/tmp/ipeds-post2023-audit-20260929/`; the JSON records source URLs and
digests independently of that temporary location.

| Source | SHA-256 |
| --- | --- |
| IPEDS_2024-25_Provisional.zip | `ef134955ba5003a07d37e46fae6286a76dca4c15767c3c4cd9086b1887896713` |
| IPEDS202425.accdb | `98e9175e54ba719cf7f0bfcc8e043fac4d11babd1b73ba1701ebbdc23e5470f5` |
| Standalone IPEDS202425Tablesdoc.xlsx | `e5c2197f55fb3f8853b866cde05d6d0be167c37480f9d0825ff4b7d60aaf5f6e` |
| SFA2324.zip | `1cbcfb3194e68ce1fbca17812327c37c3a20c4cd03654c934dd50d02b18060bc` |
| SFA2324_Dict.zip | `2f65a2d1887dbd5286f4ea59dfab9fc0d21f0783df9c0c3ba48f73b4636d9bab` |

[access]: https://nces.ed.gov/ipeds/use-the-data/download-access-database
[schedule]: https://nces.ed.gov/ipeds/survey-components/data-release-schedule
[files24]: https://nces.ed.gov/ipeds/datacenter/DataFiles.aspx?year=2024&surveyNumber=-1
[files25]: https://nces.ed.gov/ipeds/datacenter/DataFiles.aspx?year=2025&surveyNumber=-1
[fall26]: https://nces.ed.gov/ipeds/survey-components/release-memo?type=fall&year=2026
[sfadict]: https://nces.ed.gov/ipeds/complete-data-files/SFA2324_Dict.zip
[survey23]: https://nces.ed.gov/ipeds/use-the-data/download-survey-material/2023/student%20financial%20aid/package_7_16.pdf
[survey24]: https://nces.ed.gov/ipeds/use-the-data/download-survey-material/2024/student%20financial%20aid/package_7_16.pdf
[changes]: https://nces.ed.gov/ipeds/report-your-data/archived-changes/2024-25
[pellrules]: https://fsapartners.ed.gov/knowledge-center/fsa-handbook/2024-2025/application-and-verification-guide/ch3-student-aid-index-sai-and-pell-grant-eligibility
