"""Apply individually evidenced source metadata corrections without changing identities.

The source dictionary remains authoritative evidence, including its mistakes.
Corrections add provenance and resolve physical location; they never rewrite raw files.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import pandas as pd

DEFAULT_REGISTRY = Path(__file__).resolve().parents[1] / "contracts/source_metadata_corrections/2023-sfa-v1.json"


def sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def normalize_number(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".")[0]
    if not text:
        return ""
    return text.lstrip("0") or "0"


def load_registry(path: Path = DEFAULT_REGISTRY) -> dict:
    record = json.loads(path.read_text())
    if record.get("schema_version") != 1:
        raise ValueError("Unsupported source metadata correction schema")
    seen = set()
    for row in record["corrections"]:
        key = (int(row["year"]), row["varname"], normalize_number(row["varnumber"]), row["original_table"])
        if key in seen or not row.get("reason") or not row.get("evidence_id"):
            raise ValueError(f"Duplicate or undocumented source correction: {key}")
        seen.add(key)
        if row["resolved_table"] not in record["physical_tables"]:
            raise ValueError(f"Unverified correction target: {key}")
        if row["varname"] not in record["physical_tables"][row["resolved_table"]]["columns"]:
            raise ValueError(f"Variable absent from verified correction target: {key}")
    return record


def verify_source_files(root: Path, registry: dict) -> None:
    """Bind a correction to the exact release and exact extracted input bytes."""
    for item in registry["pipeline_inputs"]:
        path = root / item["path"]
        if not path.is_file() or sha256(path) != item["sha256"]:
            raise ValueError(f"Source correction evidence does not match input: {path}")


def apply_source_metadata_corrections(
    frame: pd.DataFrame, *, root: Path | None = None,
    registry_path: Path = DEFAULT_REGISTRY, codebook: bool = False,
    verify_inputs: bool = True,
) -> pd.DataFrame:
    """Resolve only exact year/table/variable/number tuples in the approved registry.

    ``codebook=True`` permits an absent varname only when that variable number has
    exactly one correction in its original table. Synthetic imputation rows are
    not physical variables and are deliberately excluded.
    """
    if frame.empty:
        return frame.copy()
    registry = load_registry(registry_path)
    years = pd.to_numeric(frame.get("year", pd.Series(dtype=object)), errors="coerce")
    applicable = [r for r in registry["corrections"] if years.eq(r["year"]).any()]
    if not applicable:
        return frame.copy()
    out = frame.copy()
    if "access_table_name" not in out or "varnumber" not in out:
        raise ValueError("Source correction requires table and variable identity")
    names = out.get("varname", pd.Series("", index=out.index)).fillna("").astype(str).str.upper()
    tables = out["access_table_name"].fillna("").astype(str).str.upper()
    numbers = out["varnumber"].map(normalize_number)
    if "metadata_correction_id" in out:
        for row in applicable:
            already = out["metadata_correction_id"].eq(row["evidence_id"])
            if already.any() and ("resolved_physical_table" not in out or
                    not out.loc[already, "resolved_physical_table"].eq(row["resolved_table"]).all() or
                    not tables.loc[already].eq(row["resolved_table"]).all()):
                raise ValueError(f"Conflicting existing correction: {row['evidence_id']}")
    applicable = [r for r in applicable if (years.eq(r["year"]) & tables.eq(r["original_table"])
                  & numbers.eq(normalize_number(r["varnumber"]))
                  & (names.eq(r["varname"]) | (codebook & names.eq("")))).any()]
    if not applicable:
        return out
    if codebook:
        for row in applicable:
            blank = years.eq(row["year"]) & tables.eq(row["original_table"]) & numbers.eq(normalize_number(row["varnumber"])) & names.eq("")
            candidates = [r for r in registry["corrections"] if (r["year"], r["original_table"], normalize_number(r["varnumber"])) == (row["year"], row["original_table"], normalize_number(row["varnumber"]))]
            if blank.any() and len(candidates) != 1:
                raise ValueError("Ambiguous unnamed codebook source correction")
    if verify_inputs:
        if root is None:
            raise ValueError("An original source root is required to verify corrections")
        verify_source_files(root, registry)
    for column in ["original_access_table_name", "original_source_file_label", "original_metadata_json",
                   "resolved_physical_table", "metadata_correction_id", "metadata_correction_reason",
                   "metadata_correction_evidence", "metadata_correction_registry_sha256", "source_archive_sha256", "source_database_sha256",
                   "source_physical_table_sha256", "source_table_reference_period", "reference_period", "reference_period_start",
                   "reference_period_end", "imputation_flag_availability"]:
        if column not in out:
            out[column] = ""
    for row in applicable:
        identity = years.eq(row["year"]) & numbers.eq(normalize_number(row["varnumber"]))
        identity &= names.eq(row["varname"]) | (codebook & names.eq(""))
        mask = identity & tables.eq(row["original_table"])
        # A second application is idempotent. Any conflicting resolution fails.
        already = identity & out["metadata_correction_id"].eq(row["evidence_id"])
        if already.any() and not out.loc[already, "resolved_physical_table"].eq(row["resolved_table"]).all():
            raise ValueError(f"Conflicting existing correction: {row['evidence_id']}")
        for index in out.index[mask]:
            original = frame.loc[index].where(pd.notna(frame.loc[index]), None).to_dict()
            out.at[index, "original_metadata_json"] = json.dumps(original, default=str, sort_keys=True)
            out.at[index, "original_access_table_name"] = original["access_table_name"]
            out.at[index, "original_source_file_label"] = original.get("source_file_label", "")
        out.loc[mask, "access_table_name"] = row["resolved_table"]
        if "source_file_label" in out:
            out.loc[mask, "source_file_label"] = row["resolved_table"]
        out.loc[mask, "resolved_physical_table"] = row["resolved_table"]
        out.loc[mask, "metadata_correction_id"] = row["evidence_id"]
        out.loc[mask, "metadata_correction_reason"] = row["reason"]
        out.loc[mask, "metadata_correction_evidence"] = registry["evidence_path"]
        out.loc[mask, "metadata_correction_registry_sha256"] = sha256(registry_path)
        out.loc[mask, "source_archive_sha256"] = registry["archive"]["sha256"]
        out.loc[mask, "source_database_sha256"] = registry["database"]["sha256"]
        out.loc[mask, "source_physical_table_sha256"] = registry["physical_tables"][row["resolved_table"]]["sha256"]
        out.loc[mask, "source_table_reference_period"] = "July 1, 2022 - June 30, 2023"
        # Net-price tables also contain lagged measures: table coverage alone
        # cannot establish every variable's measurement reference period.
        if row["varname"] in {"UPGRNTN", "UPGRNTT"}:
            out.loc[mask, "reference_period"] = "preceding academic year (July 1 to June 30)"
            out.loc[mask, "reference_period_start"] = "2022-07-01"
            out.loc[mask, "reference_period_end"] = "2023-06-30"
        out.loc[mask, "imputation_flag_availability"] = "not included in this Access release; dictionary association only"
    return out


def exact_table_dictionary_rows(
    frame: pd.DataFrame, source_file: str, access_table_name: str,
    physical_columns: list[str] | None = None,
) -> pd.DataFrame:
    """A canonical family such as SFA_P is never proof of physical table identity."""
    source = str(source_file or "").strip().upper()
    table = str(access_table_name or "").strip().upper()
    physical = frame["access_table_name"].fillna("").astype(str).str.upper()
    if "resolved_physical_table" in frame:
        resolved = frame["resolved_physical_table"].fillna("").astype(str).str.upper()
        physical = resolved.where(resolved.ne(""), physical)
    exact = frame.loc[physical.eq(table) & physical.ne("")].copy()
    if physical_columns is not None:
        names = {str(c).strip().upper() for c in physical_columns}
        exact = exact.loc[exact["varname"].str.upper().isin(names)].copy()
        family = frame["source_file"].fillna("").astype(str).str.upper().eq(source)
        candidate_names = set(frame.loc[family, "varname"].str.upper()) & names
        missing = candidate_names - set(exact["varname"].str.upper()) - {"UNITID"}
        if missing:
            raise SystemExit(
                f"[fatal] physical table/variable metadata mismatch for {table}: {sorted(missing)}. "
                "An exact table match or a versioned source metadata correction is required."
            )
    return exact
