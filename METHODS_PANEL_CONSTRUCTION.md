# Panel Construction Method

This note documents the full panel-construction method used in `IPEDSDB_Panel`, not just the PRCH cleaning layer.

In plain terms: the repo builds a research-facing unbalanced `UNITID`-by-`year` panel from annual IPEDS Access databases, keeps the panel spine at the campus/institution-record level used by IPEDS, and uses QA artifacts to make the data decisions inspectable instead of hidden.

## What The Canonical Product Is

The canonical released product in this repo is:

- an unbalanced panel
- keyed by `UNITID` and `year`
- built from annual IPEDS Access databases
- restricted to `Final` Access releases for `2004:2023`
- cleaned for documented parent-child duplication without collapsing the panel to a single parent-level institution history

The panel year is the **collection-year start**. That means `2023` corresponds to the `2023-24` collection cycle label when used in the metadata, release inventory, and Access database inputs.

## Literature And Why It Matters Here

### Jaquette & Parra (2014)

This is the repo's main conceptual anchor for IPEDS panel construction.

How it is used:

- it supports treating panel construction as a research-design problem, not just a file-merging problem
- it supports keeping the unit of analysis explicit instead of pretending that all IPEDS components always describe the same institutional object
- it supports the repo's refusal to do blanket parent-child collapse across all components and all years
- it supports treating component-specific reporting differences as something to diagnose and clean, not something to smooth away automatically

Why it matters:

- IPEDS is not a naturally ready-made panel
- campus-level and parent-level reporting can differ by component
- naive longitudinal collapsing can erase closures, mergers, and reporting transitions that matter for research design

### NCES Access / Methodology / DLDT / Institutional Groupings Documentation

These materials are the repo's operational reference for how the data are structured.

How they are used:

- Access metadata tables are the source of variable titles, descriptions, code labels, and table structure
- the panel year is interpreted as the collection-year start rather than as a universal event date
- component timing is documented as a comparability issue rather than assumed away
- classification variables such as sector, level, and Carnegie are treated as time-varying diagnostics rather than permanent institutional truths

Why they matter:

- some measures describe fall snapshots
- some describe 12-month activity
- some describe fiscal-year reporting
- some describe older entering cohorts

That timing heterogeneity is why the repo now emits `component_timing_reference.csv` and classification-stability diagnostics.

### Kelchen (2019)

Kelchen is the main guide for the repo's stance on `UNITID` versus `OPEID`.

How it is used:

- `UNITID` remains the canonical panel key
- `OPEID` is treated as a diagnostic linkage field, not as an automatic replacement key
- the repo now emits identifier-linkage diagnostics instead of silently aggregating to parent level

Why it matters:

- there is no universal one-to-one mapping between `UNITID` and `OPEID`
- `OPEID` can be useful for understanding parent-child or chain structure
- automatic conversion of the whole panel to an `OPEID` unit of analysis would change the research object, not just the identifier

### Delta Cost Project Documentation And Cheslock & Jaquette (2016)

These are used as a cautionary reference, not as a build template.

How they are used:

- the repo explicitly avoids DCP-style blanket parent-child consolidation across all years
- the repo does not make the canonical output a balanced panel
- the repo does not apply automatic finance harmonization merely because a downstream panel might benefit from it

Why they matter:

- a balanced or parent-collapsed panel can be useful for some analyses
- but using that as the default released product can distort campus-level structure and make per-student interpretations less reliable in multi-campus systems

### Wooldridge / Baltagi / NISS

These references guide how the repo thinks about unbalancedness.

How they are used:

- the repo treats entry, exit, and intermittent gaps as things to diagnose explicitly
- the repo does not assume that an unbalanced panel is a defect by itself
- the repo now emits structure diagnostics for retention, gaps, and possible selection-risk patterns

Why they matter:

- unbalancedness can be substantively meaningful
- attrition and intermittent reporting can create interpretation risk even when estimation remains possible
- the right response for this repo is transparent diagnostics first, not automatic sample restriction

## How The Repo Uses These Ideas In Practice

### 1. Inputs And Release Policy

- only `Final` Access releases are used in the canonical build
- yearly manifests and release inventories make that choice explicit
- provisional releases are intentionally excluded from the release-ready panel

This follows the repo's “document the source state first” rule and keeps the build tied to stable NCES revisions.

### 2. Unit Of Analysis

- the canonical panel key is `UNITID`, `year`
- one row per `UNITID-year` is enforced in the wide and clean outputs
- parent-level linkage fields such as `OPEID` are diagnostic-only in the current release line

This keeps the released panel close to the observed IPEDS reporting unit while still allowing identifier-linkage review.

### 3. Metadata-First Harmonization

- the Access metadata tables drive variable titles, long descriptions, code labels, data types, and source-family mapping
- harmonization fails loudly when key metadata conditions are broken
- the dictionary lake is part of the integrity surface, not just a convenience file

This is how the repo converts many annual Access tables into one coherent panel without relying on undocumented manual merges.

### 4. Component-Specific Parent-Child Cleaning

- the repo keeps every `UNITID-year` row
- PRCH cleaning blanks affected component-family values rather than dropping the row
- Finance is treated with an explicit rule set because not all finance parent-child codes mean the same thing

The detailed method is in `METHODS_PRCH_CLEANING.md`.

### 5. Unbalancedness And Selection-Risk Diagnostics

The repo now emits:

- `panel_structure_summary.csv`
- `entry_exit_gap_summary.csv`
- `identifier_linkage_summary.csv`
- `classification_stability_summary.csv`
- `institution_pattern_flags.csv`

These artifacts do not change the canonical panel. They explain the structure of the canonical panel.

### 6. Timing And Comparability Diagnostics

The repo now emits:

- `component_timing_reference.csv`
- `finance_comparability_summary.csv`

These are there because a panel can be structurally clean and still require interpretation caution.

## What The Repo Intentionally Does Not Do

- It does **not** automatically convert the canonical panel to `OPEID` or parent-institution level.
- It does **not** automatically build a balanced panel as the default release product.
- It does **not** treat classification variables as fixed across all years.
- It does **not** apply undocumented finance harmonization across accounting-standard changes.
- It does **not** claim to produce a universal merger-adjusted institutional history.

Those may all be useful for specific research projects, but they are downstream design choices, not default release behavior.

## How To Describe The Dataset Correctly

The safest short description is:

> `IPEDSDB_Panel` provides a cleaned, research-facing, unbalanced `UNITID`-by-`year` panel built from final IPEDS Access databases, with component-specific parent-child handling and explicit QA diagnostics for panel structure, identifier linkage, timing, and comparability.

That phrasing is more accurate than calling it:

- a fully parent-collapsed institution panel
- a balanced longitudinal database
- a universal institutional-merger history
- a fully harmonized finance-comparability dataset

## Practical Implication

Use the canonical panel when you want:

- one row per observed `UNITID-year`
- a reproducible and inspectable build from IPEDS Access databases
- explicit QA around structure, identifiers, and parent-child reporting

Build a custom derived panel when you need:

- parent-level aggregation
- a balanced subpanel
- special finance harmonization
- analysis-specific sample restrictions

That separation is intentional. The repo's job is to produce a defensible base panel and the evidence needed to understand it, not to guess every downstream research design choice in advance.

## Formal References

- Jaquette, O., & Parra, E. E. (2014). *Using IPEDS for Panel Analyses: Core Concepts, Data Challenges, and Empirical Applications.* In M. B. Paulsen (Ed.), *Higher Education: Handbook of Theory and Research* (Vol. 29, pp. 467-533). Springer. https://doi.org/10.1007/978-94-017-8005-6_11
- Kelchen, R. (2019). *Merging Data to Facilitate Analyses.* *New Directions for Institutional Research*, 2019. https://doi.org/10.1002/ir.20298
- Jaquette, O., & Parra, E. (2016). *The Problem with the Delta Cost Project Database.* *Research in Higher Education*, 57(5), 630-651. https://doi.org/10.1007/s11162-015-9399-2
- Cheslock, J. J., & Shamekhi, Y. (2020). *Decomposing financial inequality across U.S. higher education institutions.* *Economics of Education Review*, 78, 102035. https://doi.org/10.1016/j.econedurev.2020.102035
- Wooldridge, J. M. (2010). *Econometric Analysis of Cross Section and Panel Data* (2nd ed.). MIT Press.
- Wooldridge, J. M. (2019). *Correlated random effects models with unbalanced panels.* *Journal of Econometrics*, 211(1), 137-150. https://doi.org/10.1016/j.jeconom.2018.12.010
- Baltagi, B. H. (2021). *Econometric Analysis of Panel Data* (6th ed.). Springer. https://doi.org/10.1007/978-3-030-53953-5
- National Center for Education Statistics. *IPEDS Access Databases.* https://nces.ed.gov/ipeds/use-the-data/download-access-database
- National Center for Education Statistics. *Survey Components.* https://nces.ed.gov/ipeds/survey-components
- National Center for Education Statistics. *Data Literacy/Data Use Training (DLDT).* https://nces.ed.gov/ipeds/use-the-data/dldt
- National Center for Education Statistics. *Institutional Groupings.* https://nces.ed.gov/ipeds/about-ipeds-data/institutional-groupings
