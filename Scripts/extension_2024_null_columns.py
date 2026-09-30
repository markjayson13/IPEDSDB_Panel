"""Retain exactly reviewed, physically blank 2024 fields without dropping metadata.

This is an extension-only exception to the historical non-null evidence rule.
Every approval is bound to the physical source hash, row count, variable identity,
analysis mapping and year. Unexpected values or identities fail closed.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd


def validate_retained_null_columns(con, years: list[int], qc_dir: Path,
                                  contract_path: Path) -> int:
    if years != [2024]:
        raise SystemExit("2024 retained-null evidence requires exactly year 2024")
    approvals = pd.read_csv(contract_path, dtype=str, keep_default_na=False)
    required = {"year", "access_table_name", "source_file", "varname", "variable_id",
                "analysis_column", "source_csv_sha256", "source_rows", "reason"}
    if set(approvals) != required or approvals.empty:
        raise SystemExit("Invalid 2024 retained-null evidence contract")
    if approvals.variable_id.duplicated().any() or approvals.analysis_column.duplicated().any():
        raise SystemExit("Duplicate 2024 retained-null approval identity")
    if (set(approvals.year) != {"2024"}
            or not approvals.source_rows.str.fullmatch(r"[1-9][0-9]*").all()
            or not approvals.source_csv_sha256.str.fullmatch(r"[0-9a-f]{64}").all()
            or any(approvals[column].str.strip().eq("").any() for column in required)):
        raise SystemExit("Unbound 2024 retained-null approval")
    approvals["source_rows"] = approvals.source_rows.astype("int64")
    all_null = {row[0] for row in con.execute(
        "SELECT analysis_column FROM release_canonical_column_coverage "
        "WHERE eligible_value_rows > 0 AND non_null_value_rows = 0"
    ).fetchall()}
    unreviewed = all_null - set(approvals.analysis_column)
    if unreviewed:
        raise SystemExit(f"Unreviewed 2024 all-null columns: {sorted(unreviewed)}")
    con.register("extension_2024_retained_null_approvals", approvals)
    evidence = con.execute(r"""
        WITH source_evidence AS (
            SELECT a.analysis_column, COUNT(r.variable_id) AS source_rows,
                COUNT(*) FILTER (WHERE r.variable_id IS NOT NULL AND (
                    r.year IS DISTINCT FROM 2024
                    OR UPPER(r.access_table_name) IS DISTINCT FROM a.access_table_name
                    OR r.source_file IS DISTINCT FROM a.source_file
                    OR r.varname IS DISTINCT FROM a.varname
                    OR r.source_csv_sha256 IS DISTINCT FROM a.source_csv_sha256
                    OR r.value_status IS DISTINCT FROM 'missing'
                    OR regexp_replace(COALESCE(CAST(r.raw_value AS VARCHAR), ''), '\s', '', 'g') <> ''
                    OR regexp_replace(COALESCE(CAST(r.value AS VARCHAR), ''), '\s', '', 'g') <> ''
                )) AS source_mismatches
            FROM extension_2024_retained_null_approvals a
            LEFT JOIN release_scalar_raw r ON r.variable_id = a.variable_id
            GROUP BY a.analysis_column
        ), mapping_evidence AS (
            SELECT a.analysis_column, COUNT(m.analysis_column) AS mapped_rows,
                COUNT(*) FILTER (WHERE m.analysis_column IS NOT NULL AND (
                    m.variable_id IS DISTINCT FROM a.variable_id
                    OR m.year IS DISTINCT FROM 2024
                    OR m.transformation IS DISTINCT FROM 'identity'
                    OR m.normalized_value IS NOT NULL
                )) AS mapping_mismatches
            FROM extension_2024_retained_null_approvals a
            LEFT JOIN release_direct_mapped m ON m.analysis_column = a.analysis_column
            GROUP BY a.analysis_column
        )
        SELECT a.*, s.source_rows AS actual_source_rows, s.source_mismatches,
            m.mapped_rows, m.mapping_mismatches,
            c.eligible_value_rows, c.non_null_value_rows
        FROM extension_2024_retained_null_approvals a
        JOIN source_evidence s USING (analysis_column)
        JOIN mapping_evidence m USING (analysis_column)
        LEFT JOIN release_canonical_column_coverage c USING (analysis_column)
        ORDER BY a.analysis_column
    """).fetchdf()
    valid = (evidence.actual_source_rows.eq(evidence.source_rows)
             & evidence.mapped_rows.eq(evidence.source_rows)
             & evidence.eligible_value_rows.eq(evidence.source_rows)
             & evidence.non_null_value_rows.eq(0)
             & evidence.source_mismatches.eq(0) & evidence.mapping_mismatches.eq(0))
    evidence["evidence_status"] = valid.map({True: "retained_source_blank", False: "failed"})
    qc_dir.mkdir(parents=True, exist_ok=True)
    evidence.to_csv(qc_dir / "qc_retained_all_null_columns.csv", index=False)
    if not valid.all():
        failed = evidence.loc[~valid, "analysis_column"].tolist()
        raise SystemExit(f"2024 retained-null source evidence changed: {failed}")
    return len(evidence)
