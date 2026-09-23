"""Build lossless, year-scoped metadata for panel export formats.

The dictionary and code sidecars retain source records, including units and
release information when supplied. A single label is exposed only when the
selected source records agree; format-specific coercion belongs to writers.
"""
from __future__ import annotations

import csv
import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


KEY_METADATA = {
    "UNITID": (
        "IPEDS institution identifier",
        "Unique institution identifier assigned by IPEDS; institution key in the institution-year panel.",
    ),
    "YEAR": (
        "IPEDS reporting year",
        "IPEDS reporting year used as the year key in the institution-year panel. "
        "Reference periods for individual measures are specified in their source metadata.",
    ),
}


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _name(value: Any) -> str:
    return _text(value).upper()


def _year(value: Any) -> int | None:
    text = _text(value)
    if re.fullmatch(r"[0-9]+(?:\.0+)?", text):
        return int(text.split(".")[0])
    return None


def _number(value: Any) -> str:
    text = _text(value)
    # Dictionary readers may represent the same identifier as 00000012 or 12.0.
    if re.fullmatch(r"[0-9]+(?:\.0+)?", text):
        return str(int(text.split(".")[0]))
    return text


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _unique_records(records: list[dict]) -> list[dict]:
    seen: set[str] = set()
    result = []
    for record in records:
        fingerprint = json.dumps(record, sort_keys=True, ensure_ascii=False)
        if fingerprint not in seen:
            seen.add(fingerprint)
            result.append(record)
    return result


def _source_table(record: dict) -> str:
    return _name(record.get("source_table") or record.get("access_table_name"))


def _same_scope(left: dict, right: dict) -> bool:
    """Require an explicit year and a shared source identifier for indirect joins."""
    year = _year(left.get("year"))
    if year is None or year != _year(right.get("year")):
        return False
    compared = False
    for left_value, right_value in (
        (_name(left.get("source_file")), _name(right.get("source_file"))),
        (_source_table(left), _source_table(right)),
    ):
        if left_value and right_value:
            compared = True
            if left_value != right_value:
                return False
    return compared


def _tokens(value: Any) -> set[str]:
    return {_name(token) for token in _text(value).split("|") if _text(token)}


def build_export_metadata(
    schema: pa.Schema,
    years: list[int],
    dictionary_path: Path | None,
    codes_path: Path | None,
    lineage_path: Path | None = None,
) -> dict:
    """Return JSON-serializable definitions and diagnostics for the actual schema.

    Missing or unusable metadata is reported, never silently borrowed from a
    different year. Conflicting definitions remain in ``source_metadata`` and
    conflicting code mappings remain in ``value_label_records``. Variable
    ``metadata_status`` is ``complete`` or ``incomplete``; a complete status
    certifies the available metadata, not the observations or comparability.
    """
    selected_years = sorted(set(int(year) for year in years))
    year_set = set(selected_years)
    issues: list[dict] = []

    def issue(code: str, message: str, variable: str | None = None) -> None:
        issues.append({"severity": "warning", "code": code, "variable": variable, "message": message})

    def read_records(path: Path | None, kind: str, *, scope_years: bool) -> tuple[list[dict], bool]:
        if path is None or not Path(path).is_file():
            issue(f"{kind}_unavailable", f"{kind.replace('_', ' ').capitalize()} is unavailable.")
            return [], False
        try:
            if Path(path).suffix.lower() == ".csv":
                with Path(path).open(newline="", encoding="utf-8-sig") as handle:
                    reader = csv.DictReader(handle)
                    columns = reader.fieldnames or []
                    records = list(reader)
            else:
                table = pq.read_table(path)
                columns = table.column_names
                records = table.to_pylist()
        except Exception as exc:
            issue(f"{kind}_unreadable", f"Cannot read {kind.replace('_', ' ')}: {exc}")
            return [], False
        records = [_json_value(record) for record in records]
        if scope_years:
            if "year" not in columns:
                issue(f"{kind}_year_missing", f"{kind.replace('_', ' ').capitalize()} has no year column; records were not applied.")
                return [], False
            invalid = sum(_year(record.get("year")) is None for record in records)
            if invalid:
                issue(f"{kind}_invalid_year", f"Excluded {invalid} {kind.replace('_', ' ')} records without a valid year.")
            records = [record for record in records if _year(record.get("year")) in year_set]
            return records, not bool(invalid)
        return records, True

    embedded_variables: list[dict] = []
    for field in schema:
        raw_metadata = (field.metadata or {}).get(b"ipeds:variable")
        if raw_metadata is None:
            continue
        try:
            variable_metadata = json.loads(raw_metadata)
            if not isinstance(variable_metadata, dict) or variable_metadata.get("name") != field.name:
                raise ValueError("embedded variable name does not match its field")
            for key in ("source_metadata", "value_label_records", "imputation_parent_metadata", "lineage_records"):
                records = variable_metadata.get(key, [])
                if not isinstance(records, list) or any(not isinstance(record, dict) for record in records):
                    raise ValueError(f"embedded {key} is not a list of records")
            embedded_variables.append(variable_metadata)
        except (ValueError, TypeError, UnicodeDecodeError) as exc:
            issue("embedded_metadata_invalid", f"Cannot use embedded variable metadata: {exc}", field.name)

    def embedded_records(kind: str, keys: tuple[str, ...]) -> tuple[list[dict], bool]:
        records = _unique_records([
            record for variable in embedded_variables for key in keys for record in variable.get(key, [])
        ])
        ordinary_variables = [variable for variable in embedded_variables if _name(variable["name"]) not in KEY_METADATA]
        available = all(
            variable.get("metadata_availability", {}).get(kind, variable.get("metadata_status") == "complete")
            for variable in ordinary_variables
        )
        if not available:
            issue(f"embedded_{kind}_incomplete", f"Embedded {kind} metadata was incomplete in the source export.")
        invalid = sum(_year(record.get("year")) is None for record in records)
        if invalid:
            issue(f"embedded_{kind}_invalid_year", f"Excluded {invalid} embedded {kind} records without a valid year.")
        return [record for record in records if _year(record.get("year")) in year_set], available and not bool(invalid)

    # Keep dictionary/codebook releases together. An explicit source must not
    # silently borrow its missing companion from an older embedded package.
    use_embedded_dictionary = dictionary_path is None and codes_path is None and bool(embedded_variables)
    use_embedded_codes = use_embedded_dictionary
    if use_embedded_dictionary:
        dictionary, dictionary_ok = embedded_records("dictionary", ("source_metadata", "imputation_parent_metadata"))
    else:
        dictionary, dictionary_ok = read_records(dictionary_path, "dictionary", scope_years=True)
    if use_embedded_codes:
        codes, codes_ok = embedded_records("codes", ("value_label_records",))
    else:
        codes, codes_ok = read_records(codes_path, "codes", scope_years=True)
    lineage: list[dict] = []
    if lineage_path is not None:
        lineage, _ = read_records(lineage_path, "lineage", scope_years=False)
    elif embedded_variables:
        lineage = _unique_records([record for variable in embedded_variables for record in variable.get("lineage_records", [])])
    if not selected_years:
        issue("selected_years_empty", "No observed export years were supplied; year-specific definitions were not applied.")

    dictionary_by_name: dict[str, list[dict]] = defaultdict(list)
    imputation_parents: dict[str, list[dict]] = defaultdict(list)
    codes_by_name: dict[str, list[dict]] = defaultdict(list)
    codes_by_number: dict[tuple[int | None, str], list[dict]] = defaultdict(list)
    imputation_codes: dict[int | None, list[dict]] = defaultdict(list)
    lineage_by_name: dict[str, list[dict]] = defaultdict(list)
    for record in dictionary:
        if _name(record.get("varname")):
            dictionary_by_name[_name(record.get("varname"))].append(record)
        if _name(record.get("imputationvar")):
            imputation_parents[_name(record.get("imputationvar"))].append(record)
    for record in codes:
        if _name(record.get("varname")):
            codes_by_name[_name(record.get("varname"))].append(record)
        elif _text(record.get("label_scope")).lower() == "imputation_variable":
            imputation_codes[_year(record.get("year"))].append(record)
        elif _number(record.get("varnumber")):
            codes_by_number[(_year(record.get("year")), _number(record.get("varnumber")))].append(record)
    for record in lineage:
        if _name(record.get("output_column")):
            lineage_by_name[_name(record.get("output_column"))].append(record)

    variables = []
    for field in schema:
        start_issues = len(issues)
        name = field.name
        key = _name(name)
        lineage_records = lineage_by_name.get(key, [])
        source_names = set().union(*[_tokens(row.get("source_varnames")) for row in lineage_records]) if lineage_records else set()
        source_names = source_names or {key}
        source_files = set().union(*[
            _tokens(row.get("source_files")) | _tokens(row.get("primary_source_file"))
            for row in lineage_records
        ]) if lineage_records else set()
        source_records = [record for source_name in sorted(source_names) for record in dictionary_by_name.get(source_name, [])]
        parents = [record for source_name in sorted(source_names) for record in imputation_parents.get(source_name, [])]
        if source_files:
            source_records = [record for record in source_records if _name(record.get("source_file")) in source_files]
            parents = [record for record in parents if _name(record.get("source_file")) in source_files]
        source_records = _unique_records(source_records)
        titles = sorted({_text(record.get("varTitle")) for record in source_records} - {""})
        descriptions = sorted({_text(record.get("longDescription")) for record in source_records} - {""})
        label = titles[0] if len(titles) == 1 else name
        description = descriptions[0] if len(descriptions) == 1 else ""
        if key in KEY_METADATA:
            label, description = KEY_METADATA[key]
        else:
            if not source_records:
                issue("variable_metadata_missing", "No matching dictionary definition for the selected years and source lineage.", name)
            if len(titles) > 1:
                issue("variable_label_conflict", "Selected source definitions have different variable labels; retained in source_metadata.", name)
            elif not titles:
                issue("variable_label_missing", "No source variable label is available; the column name is used.", name)
            elif any(not _text(record.get("varTitle")) for record in source_records):
                issue("variable_label_coverage_incomplete", "Some selected source definitions have no variable label.", name)
            if len(descriptions) > 1:
                issue("variable_description_conflict", "Selected source definitions have different descriptions; retained in source_metadata.", name)
            elif not descriptions:
                issue("variable_description_missing", "No source description is available.", name)
            elif any(not _text(record.get("longDescription")) for record in source_records):
                issue("variable_description_coverage_incomplete", "Some selected source definitions have no description.", name)
            covered_years = {_year(record.get("year")) for record in source_records}
            missing_years = sorted(year_set - covered_years)
            if source_records and missing_years:
                issue("variable_year_coverage_incomplete", f"No matching dictionary definition for export years: {', '.join(map(str, missing_years))}.", name)
            if any(_text(record.get("metadata_source")).startswith("synthetic") for record in source_records):
                issue("synthetic_source_metadata", "One or more definitions were synthesized upstream; source_metadata preserves their provenance.", name)

        matched_codes: list[dict] = []
        code_scope_unproven = False
        for source_name in sorted(source_names):
            named_records = codes_by_name.get(source_name, [])
            for code in named_records:
                if source_files and _name(code.get("source_file")) not in source_files:
                    continue
                # Named codes need no number lookup, but where definitions are
                # available their source scope must agree.
                definitions = [row for row in source_records if _name(row.get("varname")) == source_name and _year(row.get("year")) == _year(code.get("year"))]
                if not any(_same_scope(code, row) for row in definitions):
                    # Keep the named record for audit, but its year/source
                    # identity is not sufficient to label the observations.
                    code_scope_unproven = True
                    issue("value_label_source_unmatched", "Named value-label records have no matching dictionary year/source scope; retained in value_label_records without a universal mapping.", name)
                matched_codes.append(code)
        for record in source_records:
            number = _number(record.get("varnumber"))
            if number:
                for code in codes_by_number.get((_year(record.get("year")), number), []):
                    if _same_scope(code, record):
                        matched_codes.append(code)
        for parent in parents:
            for code in imputation_codes.get(_year(parent.get("year")), []):
                if _same_scope(code, parent):
                    matched_codes.append(code)
        matched_codes = _unique_records(matched_codes)
        if not matched_codes and (parents or any(_text(row.get("DataType")).lower() in {"disc", "categorical", "category"} for row in source_records)):
            issue("value_labels_missing", "No matching code labels are available for a source-declared categorical or imputation variable.", name)

        def covers_definition(code_record: dict, definition: dict) -> bool:
            if not _same_scope(code_record, definition):
                return False
            code_name = _name(code_record.get("varname"))
            if code_name:
                return code_name == _name(definition.get("varname"))
            if _text(code_record.get("label_scope")).lower() == "imputation_variable":
                return any(
                    _name(parent.get("imputationvar")) in source_names
                    and _same_scope(code_record, parent)
                    and _same_scope(definition, parent)
                    for parent in parents
                )
            number = _number(definition.get("varnumber"))
            return bool(number) and _number(code_record.get("varnumber")) == number

        uncovered_definitions = [
            row for row in (source_records or parents)
            if not any(covers_definition(code_record, row) for code_record in matched_codes)
        ] if matched_codes else []
        if uncovered_definitions:
            scopes = sorted({f"{_year(row.get('year'))}/{_name(row.get('source_file')) or _source_table(row) or 'unknown source'}" for row in uncovered_definitions})
            issue("value_label_coverage_incomplete", f"Code labels are unavailable for some selected dictionary year/source scopes: {', '.join(scopes)}. No universal mapping was embedded.", name)
        mapping: dict[str, set[str]] = defaultdict(set)
        for record in matched_codes:
            value = _text(record.get("codevalue"))
            value_label = _text(record.get("valuelabel"))
            if record.get("codevalue") is None or not value_label:
                issue("value_label_incomplete", "A source code record lacks a code or label; retained in value_label_records.", name)
                continue
            mapping[value].add(value_label)
        conflicts = sorted(value for value, labels in mapping.items() if len(labels) > 1)
        if conflicts:
            issue("value_label_conflict", f"Codes have different labels across selected years or sources: {', '.join(conflicts)}. No universal mapping was embedded.", name)
            value_labels = []
        elif code_scope_unproven or uncovered_definitions:
            value_labels = []
        else:
            value_labels = [{"value": value, "label": next(iter(labels))} for value, labels in sorted(mapping.items())]

        variables.append({
            "name": name,
            "label": label,
            "description": description,
            "storage_type": str(field.type),
            "metadata_status": "complete" if len(issues) == start_issues and (key in KEY_METADATA or (dictionary_ok and codes_ok)) else "incomplete",
            "value_labels": value_labels,
            "source_metadata": source_records,
            "value_label_records": matched_codes,
            "lineage_records": lineage_records,
            "imputation_parent_metadata": _unique_records(parents),
            "metadata_origin": "controlled_panel_key" if key in KEY_METADATA else ("embedded_dictionary" if use_embedded_dictionary else "source_dictionary"),
            "metadata_availability": {"dictionary": dictionary_ok, "codes": codes_ok},
        })
    return {
        "years": selected_years,
        "variables": variables,
        "issues": _unique_records(issues),
        "metadata_status": "incomplete" if issues or any(row["metadata_status"] != "complete" for row in variables) else "complete",
        "metadata_sources": {
            "dictionary": "embedded_parquet" if use_embedded_dictionary else ("external" if dictionary_path is not None else "unavailable"),
            "codes": "embedded_parquet" if use_embedded_codes else ("external" if codes_path is not None else "unavailable"),
        },
    }
