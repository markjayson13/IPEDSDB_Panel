# PRCH Cleaning Method

This note documents the parent-child (`PRCH_*`) cleaning method used in `IPEDSDB_Panel` for the cleaned wide panel.

In plain terms: the repo keeps every institution-year row, but blanks the component values that clearly belong with a parent reporter instead of leaving duplicated values on the child row.

If you only remember one sentence from this note, make it this: the method preserves the institution-year spine and cleans the duplicated component payload.

## Scope

- Applies to the Access-database workflow in this repo for `2004:2023`.
- Applies after the wide panel is built, at the one-row-per-`UNITID-year` stage.
- The goal is to keep the panel unbalanced and institution-level while preventing child rows from carrying duplicated component values that are already reported with a parent.

## Operational Rule

The cleaner in `Scripts/07_clean_panel.py` does **not** drop rows. It preserves every `UNITID-year` observation and nulls only the component-family columns affected by a `PRCH_*` flag.

This design is intentional:

- component reporting can differ across IPEDS survey components
- collapsing everything to a single parent row would hide institution-level entry, exit, closure, and merger behavior
- a cleaned wide panel should still preserve row-level institutional history while preventing obvious double counting inside component families

That is the core philosophy of the method: preserve the panel spine, clean the duplicated payload.

This is meant to be practical and auditable, not magical. The method is intentionally narrow: clean what the metadata and PRCH flags clearly support, and leave the rest visible in QA rather than guessing.

## Literature And Source Support Used

### 1. `Using IPEDS for Panel Analyses: Core Concepts, Data Challenges and Empirical Applications`

Local file:

- `/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling/Paneling guide/Using IPEDS for Panel Analyses Core Concepts Data Challenges and Empirical Applications.pdf`

Relevant sections used in this repo’s method design:

- the chapter states that child-level observations should be collapsed into parent-level observations **separately for each survey component**
- it recommends creating explicit parent-child relationships when more than one `UNITID` maps to the same historical institution identifier
- it warns that blanket parent-child collapsing can be inappropriate for closures and mergers, especially when the child is collapsed into the parent for years before the relationship exists

How this repo uses that guidance:

- we treat parent-child handling as **component-specific**, not a global row-collapse rule
- we keep every `UNITID-year` row and blank only the affected component-family columns
- we avoid DCP-style blanket collapsing that would pre-collapse mergers or erase institution-level history

### 2. `Conducting research with IPEDS`

Local file:

- `/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling/Paneling guide/Conducting research with IPEDS.pdf`

Relevant section used here:

- the `Parent-child reporting` section states that a multicampus institution can report some surveys at the parent level and others at the child level
- the example given is Pennsylvania State University in `2004/05`: separate fall enrollment reporting by campus, but finance reported with University Park

How this repo uses that guidance:

- the cleaner is organized around `PRCH_*` flags and `source_file` families because reporting relationships can differ by component
- finance cleaning is allowed to differ from admissions, completions, libraries, or student-aid cleaning
- derived component families are reviewed with the same logic when their metadata indicate they are produced from a cleaned parent-child component

### 3. `IPEDS Data Collection System Glossary`

Local file:

- `/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling/Paneling guide/IPEDS Data Collection System Glossary.pdf`

Relevant definitions used here:

- `Child institution`: an institution that has some or all of its data reported by another institution
- `Parent institution`: an institution that reports some or all data for another institution
- `Institutional system`: two or more postsecondary institutions under one administrative body
- `Branch institution`: a campus beyond reasonable commuting distance that offers full programs of study

How this repo uses that guidance:

- as terminology support for the method note and QA outputs
- to distinguish parent-child reporting from other organizational relationships such as branches and systems

### 4. Local IPEDS metadata extracted by this repo

Local artifacts:

- `$IPEDSDB_ROOT/Dictionary/dictionary_codes.parquet`
- `$IPEDSDB_ROOT/Dictionary/dictionary_lake.parquet`

These are authoritative for the exact `PRCH_*` labels present in the Access-database build used by this repo. The cleaning policy is tied to those local metadata labels, not to guesswork.

## Repo Implementation

### General rule

For most `PRCH_*` flags, code `2` means the row is a child observation whose component data are reported with the parent. Those rows are cleaned by nulling the affected component-family columns.

### Completions derived variables

Completions cleaning now includes the derived completions source family `DRVC` in addition to the direct completions families `C_A`, `C_B`, `C_C`, and `CDEP`.

Reason:

- the local `DRVC` variable descriptions explicitly refer to parent-child allocation factors and derived completions totals
- leaving `DRVC` populated on `PRCH_C=2` child rows would retain completions information that should have been attributed to the parent reporting relationship

### Finance rule

Finance requires a more explicit rule than the other components.

Based on the local `PRCH_F` value labels in `dictionary_codes.parquet`, this repo now uses:

| `PRCH_F` code | Treatment | Reason |
| --- | --- | --- |
| `2` | clean child row | child record, data reported with parent campus |
| `3` | clean child row | partial child, other data reported with parent campus |
| `4` | clean child row | child record, data included with an entity that is not a postsecondary institution |
| `5` | clean child row | partial child, other data included with a non-IPEDS entity |
| `6` | keep and flag for review | partial parent/child case; the row still reports some finance information, so blanket nulling would erase valid revenues/expenses |

That means the cleaner now nulls finance-family columns for `PRCH_F in {2, 3, 4, 5}` and intentionally leaves `PRCH_F = 6` untouched.

## Why `PRCH_F = 6` Is Not Blanket-Nulled

The local metadata label for `PRCH_F = 6` is a partial parent/child case in which:

- some revenues and expenses are reported in the row
- assets and liabilities are reported with the parent

If the repo nulled all finance-family columns for code `6`, it would remove valid reported finance data. A more complete treatment would require sub-family logic inside Finance, not a blanket child-row nulling rule.

The current method therefore treats `6` as:

- not safe to leave undocumented
- not safe to blanket-null
- a review-only code that remains visible in QA outputs

## QA Expectations

The cleaning and QA scripts now use the same shared PRCH policy.

Expected QA behavior:

- `Checks/prch_qc/prch_clean_summary.csv` shows cleaned child rows and review-only rows by year and flag
- `Checks/prch_qc/prch_flag_policy.csv` records the applied code policy by flag
- `Checks/prch_qc/prch_flag_code_counts.csv` records observed PRCH codes by year and flag
- `Checks/panel_qc/panel_qa_summary.csv` records panel-level preservation and flag-level QA counts
- `Checks/panel_qc/panel_qa_coverage_matrix.csv` evaluates every observed `PRCH_*` flag, not just finance
- `Checks/panel_qc/panel_qa_by_flag_code.csv` breaks those counts out by individual PRCH code

For Finance, a healthy post-clean result should show:

- `PRCH_F` codes `2, 3, 4, 5`: targeted finance-family non-null counts drop to zero in the cleaned panel
- `PRCH_F` code `6`: targeted finance-family non-null counts remain visible and are reported as `review_only`

## Current Limitations

- This repo does **not** implement full merger-aware or acquisition-aware reassignment of `UNITID` over time.
- This repo does **not** yet split Finance into subfamilies for code `6` so that revenues/expenses and assets/liabilities can be treated differently.
- This repo uses the PRCH flags and the local Access metadata as the operational rule set for `2004:2023`; it is not a HEGIS-to-IPEDS bridge implementation.

## Practical Interpretation

Use the cleaned panel when you want:

- one row per `UNITID-year`
- component-specific parent-child handling
- an auditable record of what was nulled and why

Do **not** interpret the cleaned panel as a universal institutional collapse to parent level. It is a row-preserving, component-aware cleaning layer designed for research use, not a full organizational consolidation.

That is the safest way to talk about it in papers, memos, or handoffs: this repo produces a cleaned institution-year panel, not a single “true” consolidated institution history.

## Formal References

- Jaquette, O., & Parra, E. E. (2014). *Using IPEDS for Panel Analyses: Core Concepts, Data Challenges, and Empirical Applications.* In M. B. Paulsen (Ed.), *Higher Education: Handbook of Theory and Research* (Vol. 29, pp. 467-533). Springer. https://doi.org/10.1007/978-94-017-8005-6_11
- Jaquette, O., & Parra, E. (2016). *The Problem with the Delta Cost Project Database.* *Research in Higher Education*, 57(5), 630-651. https://doi.org/10.1007/s11162-015-9399-2
- National Center for Education Statistics. *Reporting Finance Data for Multiple Institutions.* https://nces.ed.gov/ipeds/report-your-data/data-tip-sheet-reporting-finance-data-multiple-institutions
- National Center for Education Statistics. *IPEDS Access Databases.* https://nces.ed.gov/ipeds/use-the-data/download-access-database
- Local supporting files under `/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling/Paneling guide/`, including:
  - `Using IPEDS for Panel Analyses Core Concepts Data Challenges and Empirical Applications.pdf`
  - `Conducting research with IPEDS.pdf`
  - `IPEDS Data Collection System Glossary.pdf`
