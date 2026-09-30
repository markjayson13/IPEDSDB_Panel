# Reviewed 2024 extension contracts

These decisions apply only to IPEDS collection year 2024 under
`ipedsdb-panel-2004-2024-mixed-v1`. They were reviewed during the user-authorized
integration audit, not under the historical 2026-07-24 author attestation.
They do not change the immutable 2004-2023 panel or its cleaning rules.

`prepare_2024_pipeline.py` reconstructs preserved v2 commit
`799a63930f97df9a3ec35be857f340ea3743afac`, applies the existing 2023 metadata
repair, and applies the narrow extension patch. The prepared pipeline accepts
only year 2024. It retains strict source-column, grain, source-row, mapping,
casting, and parent/child gates. The only non-final exception requires this
exact contract, year 2024, and a provisional or mixed source manifest.

- `table_grain.csv`: 52 exact physical tables. Institutional scalar tables,
  including all Cost tables and DRVCOST, remain separate from dimensioned tables.
  No physical-table wildcard is used. C2024_A remains dimensioned at
  UNITID/CIPCODE/MAJORNUM/AWLEVEL, outside the institutional scalar panel.
- `source_columns.csv`: every one of the 2,655 physical data-table columns is
  classified. Four undocumented CUSTOMCGIDS auxiliary fields are excluded from
  measures but preserved in source-row lineage. Unexpected fields fail preflight.
- `variable_mapping.csv`: 2,091 exact scalar mappings. 1,465 retain their existing
  full source identity; 562 retain a unique historical column through exact
  2023 variable-number/name identity despite a physical component move. These
  moves include Cost/SFA and ENRHSST/ENRHSST1/ENRHSST2 from IC to FLAGS. The
  remaining 64 are separate 2024 identities. Original source identities and
  annual descriptions are retained; matching identifiers do not assert uniform
  measurement definitions or reference populations.
- `analysis_schema.csv`: typed single-year analysis schema using those explicit
  mappings. Historical columns absent in 2024 are reintroduced only as missing
  when the independently validated annual panel is appended to the old release.
- `discrete_families.csv`: 297 categorical fields retained individually. No
  inferred one-hot collapse or active/inactive code interpretation is permitted.
- `prch_policy.csv`: 111 rules for exact observed annual flag/code/status or
  form combinations. Historical policy bytes remain the unchanged prefix of
  the prepared policy. Cost is governed independently from SFA in 2024. The
  new SFAV participation indicators PARTVT/PO9/DOD are retained as institutional
  context; they are not automatically blanked with financial counts/amounts.
  The sole observed SFA child (440916) has no SFA2324 or SFAV2324 source row.
- `review_evidence.json`: hashes, source definitions, and the specific missing
  finance-form case described below.

UNITID 475477 has PRCH_F=3, FORM_F=-1, STAT_F=5. It has no row in any of the three
finance-form tables; DRVF2024 contains F3EQUITR=48 and F3SALRPC=64. The equity
ratio depends on assets/liabilities, which the partial-child flag says are
reported with the parent; the salary/expense ratio depends on the child-reported
revenue/expense section. The explicit missing-form rule nulls the former and
retains the latter without inventing a reporting form. Raw values and actions
remain traceable in the normal source and cleaning lineage.

Per-table provenance records the chosen release independently. In particular,
the revised standalone C2024_A has a smaller dimensional row universe than
Access. It is preserved as a separate final supplement; the complete original
Access C2024_A remains explicitly provisional. No missing aggregate rows are
fabricated. The data table's broad YearCoverage is kept as
`source_table_reference_period`, not promoted to a universal variable cohort.
