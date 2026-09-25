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
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import duckdb


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

SOURCE_COLUMNS = {"source_file", "source_table", "access_table_name", "resolved_physical_table", "access_table_name_resolved"}
SEMANTIC_FIELDS = {
    "units": ("units", "unit", "measurement_unit"),
    "currency": ("currency", "currency_code"),
    "price_basis": ("price_basis", "priceBasis"),
    "reference_period": ("reference_period", "referencePeriod"),
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
    return _name(record.get("resolved_physical_table") or record.get("access_table_name_resolved")
                 or record.get("source_table") or record.get("access_table_name"))


def _definition_identity(record: dict) -> tuple:
    return (_year(record.get("year")), _name(record.get("source_file")),
            _source_table(record), _name(record.get("varname")))


def _schema_problems(columns: set[str], kind: str) -> list[str]:
    required = ({"year", "varname", "varTitle", "longDescription"} if kind == "dictionary"
                else {"year", "codevalue", "valuelabel"})
    missing = sorted(required - columns)
    if not SOURCE_COLUMNS.intersection(columns):
        missing.append("a source identity column")
    if kind == "codes" and not {"varname", "varnumber", "label_scope"}.intersection(columns):
        missing.append("varname, varnumber, or label_scope")
    return missing


def _has_record_identity(record: dict, kind: str) -> bool:
    if not (_name(record.get("source_file")) or _source_table(record)):
        return False
    if kind == "dictionary":
        return bool(_name(record.get("varname")))
    return bool(_name(record.get("varname")) or _number(record.get("varnumber"))
                or _text(record.get("label_scope")).lower() == "imputation_variable")


def _semantic_summary(records: list[dict], *, panel_key: bool) -> dict:
    """Describe declared measurement semantics without deriving absent facts."""
    fields = {}
    for field, aliases in SEMANTIC_FIELDS.items():
        per_record = [{_text(record.get(alias)) for alias in aliases if _text(record.get(alias))}
                      for record in records]
        values = sorted(set().union(*per_record)) if per_record else []
        missing_count = sum(not values_for_record for values_for_record in per_record)
        if panel_key:
            status = "not_applicable"
        elif len(values) > 1:
            status = "conflicting"
        elif not values or missing_count:
            status = "unknown"
        else:
            status = "consistent"
        fields[field] = {"status": status, "value": values[0] if status == "consistent" else None,
                         "values": values, "missing_source_records": missing_count}
    statuses = {value["status"] for value in fields.values()}
    status = ("not_applicable" if panel_key else "conflicting" if "conflicting" in statuses
              else "unknown" if "unknown" in statuses else "consistent")
    return {"status": status, "fields": fields,
            "unknown_fields": [field for field, value in fields.items() if value["status"] == "unknown"]}


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


def _matches_lineage(record: dict, row: dict) -> bool:
    row_year = _year(row.get("year"))
    names = _tokens(row.get("source_varnames"))
    sources = _tokens(row.get("source_files")) | _tokens(row.get("primary_source_file"))
    numbers = {_number(value) for value in _tokens(row.get("source_varnumbers"))}
    return not (
        (row_year is not None and row_year != _year(record.get("year")))
        or (names and _name(record.get("varname")) not in names)
        or (sources and _name(record.get("source_file")) not in sources)
        or (numbers and _number(record.get("varnumber")) not in numbers)
        or (_source_table(row) and _source_table(record) and _source_table(row) != _source_table(record))
    )


def discover_export_metadata(explicit: str | Path | None, relative: str,
                             input_path: Path, root: Path) -> Path | None:
    """Find companions in the input panel's layout without mixing v2 and legacy.

    Versioned panel paths select only versioned metadata. The panel's own data
    root wins over a separately configured default root; explicit paths win
    over either and must exist. Canonical value-lineage Parquet is supported
    alongside the compact column-lineage CSV.
    """
    if explicit:
        path = Path(explicit)
        if not path.is_file():
            raise ValueError(f"Metadata file does not exist: {path}")
        return path
    input_path, root = Path(input_path), Path(root)
    parts = input_path.parts
    versioned = any(parts[index:index + 2] == ("Panels", "v2") for index in range(len(parts) - 1))
    panel_roots = [parent.parent for parent in input_path.parents if parent.name == "Panels"]
    bases = list(dict.fromkeys([*panel_roots, input_path.parent, input_path.parent.parent, root]))
    target = Path(relative)
    if target.parts[:1] == ("Dictionary",):
        relatives = [Path("Dictionary") / "v2" / target.name] if versioned else [target]
    elif target.name in {"qc_column_lineage.csv", "qc_column_lineage.parquet", "qc_value_lineage.parquet"}:
        prefix = Path("Checks") / "v2" if versioned else Path("Checks")
        relatives = [prefix / "wide_qc" / "qc_column_lineage.csv",
                     prefix / "wide_qc" / "qc_column_lineage.parquet",
                     prefix / "wide_qc" / "qc_value_lineage.parquet"]
        panel_prefix = Path("Panels") / "v2" if versioned else Path("Panels")
        relatives += [panel_prefix / "wide_release/current/qc/wide_qc" / filename
                      for filename in ("qc_column_lineage.csv", "qc_column_lineage.parquet", "qc_value_lineage.parquet")]
    else:
        relatives = [target]
    for base in bases:
        for companion in relatives:
            candidate = base / companion
            if candidate.is_file():
                return candidate
    return None


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
    ``resolved_value_label_records`` contains only safely identified source
    records for observation validation. ``comparability_status`` reports
    consistency of supplied definitions/semantics; absent measurement facts
    remain explicitly ``unknown`` and are never inferred from titles.
    """
    selected_years = sorted(set(int(year) for year in years))
    year_set = set(selected_years)
    issues: list[dict] = []
    metadata_columns: dict[str, list[str]] = {}
    unapplied_metadata_records: dict[str, list[dict]] = {"dictionary": [], "codes": []}
    upstream_export_provenance: list[dict] = []

    def issue(code: str, message: str, variable: str | None = None) -> None:
        issues.append({"severity": "warning", "code": code, "variable": variable, "message": message})

    embedded_export = (schema.metadata or {}).get(b"ipeds:export")
    if embedded_export is not None:
        try:
            prior_export = json.loads(embedded_export)
            if not isinstance(prior_export, dict):
                raise ValueError("embedded export metadata is not an object")
            prior_chain = prior_export.get("upstream_export_provenance", [])
            if not isinstance(prior_chain, list) or any(not isinstance(record, dict) for record in prior_chain):
                raise ValueError("embedded upstream provenance is not a list of records")
            provenance_fields = ("schema_version", "created_utc", "format", "artifact_kind", "source_panel",
                                 "source_panel_sha256", "source_fingerprint", "exporter_provenance", "metadata_sources",
                                 "metadata_source_sha256", "metadata_source_modes", "row_count", "column_count")
            parent = {key: prior_export[key] for key in provenance_fields if key in prior_export}
            upstream_export_provenance = ([parent] if parent else []) + prior_chain
        except (ValueError, TypeError, UnicodeDecodeError) as exc:
            issue("embedded_export_provenance_invalid", f"Cannot preserve embedded export provenance: {exc}")

    def scoped_records(records: list[dict], kind: str) -> tuple[list[dict], bool]:
        selected = [record for record in records if _year(record.get("year")) in year_set]
        unidentified = [record for record in selected if not _has_record_identity(record, kind)]
        if unidentified:
            issue(f"{kind}_record_identity_missing", f"{len(unidentified)} selected {kind} records lack a variable or source identity; records were not applied.")
            unapplied_metadata_records[kind].extend(unidentified)
        return [record for record in selected if _has_record_identity(record, kind)], not bool(unidentified)

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
            metadata_columns[kind] = list(columns)
            if "year" not in columns:
                issue(f"{kind}_year_missing", f"{kind.replace('_', ' ').capitalize()} has no year column; records were not applied.")
                unapplied_metadata_records[kind].extend(records)
                return [], False
            problems = _schema_problems(set(columns), kind)
            if problems:
                issue(f"{kind}_schema_invalid", f"{kind.capitalize()} is missing required columns: {', '.join(problems)}; records were not applied.")
                unapplied_metadata_records[kind].extend(records)
                return [], False
            invalid = sum(_year(record.get("year")) is None for record in records)
            if invalid:
                issue(f"{kind}_invalid_year", f"Excluded {invalid} {kind.replace('_', ' ')} records without a valid year.")
                unapplied_metadata_records[kind].extend(record for record in records if _year(record.get("year")) is None)
            records, identities_ok = scoped_records(records, kind)
            return records, not bool(invalid) and identities_ok
        return records, True

    def read_lineage(path: Path) -> list[dict]:
        if not path.is_file() or path.suffix.lower() == ".csv":
            records, readable = read_records(path, "lineage", scope_years=False)
            if not readable:
                return []
            columns = set().union(*(record.keys() for record in records)) if records else set()
            if not records:
                with path.open(newline="", encoding="utf-8-sig") as handle:
                    columns = set(csv.DictReader(handle).fieldnames or [])
            if "output_column" not in columns or not {"source_varnames", "source_files", "primary_source_file"}.intersection(columns):
                issue("lineage_schema_invalid", "Lineage requires output_column and source identities; records were not applied.")
                return []
            return records
        try:
            columns = set(pq.read_schema(path).names)
            canonical = {"analysis_column", "year", "varname", "source_file"}.issubset(columns)
            compact = "output_column" in columns and bool({"source_varnames", "source_files", "primary_source_file"}.intersection(columns))
            if not canonical and not compact:
                issue("lineage_schema_invalid", "Unrecognized lineage schema; expected compact output_column lineage or canonical analysis_column/year/varname/source_file identities.")
                return []
            if canonical:
                projection = ['"analysis_column" AS "output_column"', '"year"',
                              '"varname" AS "source_varnames"', '"source_file" AS "source_files"']
                projection.extend(f'"{column}"' for column in sorted(SOURCE_COLUMNS - {"source_file"}) if column in columns)
                if "source_varnumber" in columns:
                    projection.append('"source_varnumber" AS "source_varnumbers"')
                output_column = "analysis_column"
            else:
                projection = ['"' + column.replace('"', '""') + '"' for column in sorted(columns)]
                output_column = "output_column"
            names = sorted({_name(name) for name in schema.names})
            predicate = (f'upper(CAST("{output_column}" AS VARCHAR)) IN ({", ".join("?" for _ in names)})' if names else "FALSE")
            params: list[Any] = [str(path), *names]
            if "year" in columns:
                predicate += (f' AND try_cast("year" AS BIGINT) IN ({", ".join("?" for _ in selected_years)})' if selected_years else " AND FALSE")
                params.extend(selected_years)
            # Canonical lineage can contain hundreds of millions of value rows.
            # Project identities and deduplicate inside a bounded engine before
            # materializing records in Python; temporary spill never uses the drive.
            with tempfile.TemporaryDirectory(prefix="ipeds-export-lineage-") as spill:
                with duckdb.connect(config={"memory_limit": "256MB", "temp_directory": spill, "threads": "2"}) as connection:
                    query = connection.execute(
                        f'SELECT DISTINCT {", ".join(projection)} FROM read_parquet(?) WHERE {predicate}', params,
                    )
                    # Keep compatibility with the minimum supported DuckDB.
                    fetch_table = getattr(query, "to_arrow_table", query.fetch_arrow_table)
                    records = fetch_table().to_pylist()
            return [_json_value(record) for record in records]
        except Exception as exc:
            issue("lineage_unreadable", f"Cannot read lineage identities: {exc}")
            return []

    embedded_variables: list[dict] = []
    for field in schema:
        raw_metadata = (field.metadata or {}).get(b"ipeds:variable")
        if raw_metadata is None:
            continue
        try:
            variable_metadata = json.loads(raw_metadata)
            if not isinstance(variable_metadata, dict) or variable_metadata.get("name") != field.name:
                raise ValueError("embedded variable name does not match its field")
            for key in ("source_metadata", "value_label_records", "imputation_parent_metadata", "lineage_records", "code_identity_candidates"):
                records = variable_metadata.get(key, [])
                if not isinstance(records, list) or any(not isinstance(record, dict) for record in records):
                    raise ValueError(f"embedded {key} is not a list of records")
            if not isinstance(variable_metadata.get("metadata_availability", {}), dict):
                raise ValueError("embedded metadata_availability is not an object")
            if not isinstance(variable_metadata.get("metadata_columns", {}), dict):
                raise ValueError("embedded metadata_columns is not an object")
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
        columns = set().union(*(record.keys() for record in records)) if records else set()
        for variable in embedded_variables:
            declared = variable.get("metadata_columns", {}).get(kind, [])
            if isinstance(declared, list) and all(isinstance(column, str) for column in declared):
                columns.update(declared)
        metadata_columns[kind] = sorted(columns)
        problems = _schema_problems(columns, kind)
        # Empty tables still need their declared schema. Availability alone
        # cannot prove that an old embedded export had a valid codebook.
        if problems and ordinary_variables:
            issue(f"embedded_{kind}_schema_invalid", f"Embedded {kind} lacks a verifiable schema: {', '.join(problems)}; records were not applied.")
            unapplied_metadata_records[kind].extend(records)
            return [], False
        invalid = sum(_year(record.get("year")) is None for record in records)
        if invalid:
            issue(f"embedded_{kind}_invalid_year", f"Excluded {invalid} embedded {kind} records without a valid year.")
        records, identities_ok = scoped_records(records, kind)
        return records, available and not bool(invalid) and identities_ok

    # Keep dictionary/codebook releases together. An explicit source must not
    # silently borrow its missing companion from an older embedded package.
    use_embedded_dictionary = dictionary_path is None and codes_path is None and bool(embedded_variables)
    use_embedded_codes = use_embedded_dictionary
    if use_embedded_dictionary:
        dictionary, dictionary_ok = embedded_records("dictionary", ("source_metadata", "imputation_parent_metadata", "code_identity_candidates"))
    else:
        dictionary, dictionary_ok = read_records(dictionary_path, "dictionary", scope_years=True)
    if use_embedded_codes:
        codes, codes_ok = embedded_records("codes", ("value_label_records",))
    else:
        codes, codes_ok = read_records(codes_path, "codes", scope_years=True)
    lineage: list[dict] = []
    if lineage_path is not None:
        lineage = read_lineage(Path(lineage_path))
    elif embedded_variables:
        lineage = _unique_records([record for variable in embedded_variables for record in variable.get("lineage_records", [])])
    if not selected_years:
        issue("selected_years_empty", "No observed export years were supplied; year-specific definitions were not applied.")

    dictionary_by_name: dict[str, list[dict]] = defaultdict(list)
    imputation_parents: dict[str, list[dict]] = defaultdict(list)
    codes_by_name: dict[str, list[dict]] = defaultdict(list)
    codes_by_number: dict[tuple[int | None, str], list[dict]] = defaultdict(list)
    definitions_by_number: dict[tuple[int | None, str], list[dict]] = defaultdict(list)
    imputation_codes: dict[int | None, list[dict]] = defaultdict(list)
    lineage_by_name: dict[str, list[dict]] = defaultdict(list)
    for record in dictionary:
        if _name(record.get("varname")):
            dictionary_by_name[_name(record.get("varname"))].append(record)
        if _name(record.get("imputationvar")):
            imputation_parents[_name(record.get("imputationvar"))].append(record)
        if _number(record.get("varnumber")):
            definitions_by_number[(_year(record.get("year")), _number(record.get("varnumber")))].append(record)
    for record in codes:
        if _name(record.get("varname")):
            codes_by_name[_name(record.get("varname"))].append(record)
        elif _text(record.get("label_scope")).lower() == "imputation_variable":
            imputation_codes[_year(record.get("year"))].append(record)
        elif _number(record.get("varnumber")):
            codes_by_number[(_year(record.get("year")), _number(record.get("varnumber")))].append(record)
    for record in lineage:
        if _name(record.get("output_column")):
            if _year(record.get("year")) is not None and _year(record.get("year")) not in year_set:
                continue
            lineage_by_name[_name(record.get("output_column"))].append(record)

    # Resolve against the full selected dictionary, before restricting it to
    # exported columns. Omitting one candidate column cannot make a join unique.
    number_candidates: dict[int, list[dict]] = {}
    for number_key, code_records in codes_by_number.items():
        for code_record in code_records:
            number_candidates[id(code_record)] = [record for record in definitions_by_number.get(number_key, [])
                                                   if _same_scope(code_record, record)]

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
        if lineage_records:
            source_records = [record for record in source_records if any(_matches_lineage(record, row) for row in lineage_records)]
            parents = [record for record in parents if any(
                _matches_lineage({**record, "varname": record.get("imputationvar")},
                                 {key: value for key, value in row.items() if key != "source_varnumbers"})
                for row in lineage_records
            )]
        source_records = _unique_records(source_records)
        lineage_incomplete = False
        if lineage_records:
            covered_names = {_name(record.get("varname")) for record in source_records}
            covered_sources = {_name(record.get("source_file")) for record in source_records}
            missing_names = sorted(source_names - covered_names)
            missing_sources = sorted(source_files - covered_sources)
            missing_scopes = []
            for row in lineage_records:
                lineage_year = _year(row.get("year"))
                for source_name in sorted(_tokens(row.get("source_varnames")) or {key}):
                    if not any(_name(record.get("varname")) == source_name and _matches_lineage(record, row)
                               for record in source_records):
                        scope = f"{lineage_year or 'selected years'}/{source_name}"
                        if _source_table(row):
                            scope += f"/{_source_table(row)}"
                        missing_scopes.append(scope)
            if missing_names or missing_sources or missing_scopes:
                lineage_incomplete = True
                issue("lineage_metadata_incomplete",
                      f"Declared lineage lacks matching definitions (variables: {', '.join(missing_names) or 'none'}; "
                      f"sources: {', '.join(missing_sources) or 'none'}; year/variables: {', '.join(missing_scopes) or 'none'}). "
                      "No universal output definition or code mapping was embedded.", name)
        titles = sorted({_text(record.get("varTitle")) for record in source_records} - {""})
        descriptions = sorted({_text(record.get("longDescription")) for record in source_records} - {""})
        label = titles[0] if len(titles) == 1 else name
        description = descriptions[0] if len(descriptions) == 1 else ""
        if lineage_incomplete:
            label, description = name, ""
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

        semantics = _semantic_summary(source_records, panel_key=key in KEY_METADATA)
        conflicting_semantics = [field for field, details in semantics["fields"].items() if details["status"] == "conflicting"]
        if conflicting_semantics:
            issue("semantic_metadata_conflict", f"Selected source definitions disagree on: {', '.join(conflicting_semantics)}. "
                  "Original measurement semantics are retained; cross-year comparability is not established.", name)

        matched_codes: list[dict] = []
        resolved_codes: list[dict] = []
        identity_candidates: list[dict] = []
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
                else:
                    resolved_codes.append(code)
                matched_codes.append(code)
        for record in source_records:
            number = _number(record.get("varnumber"))
            if number:
                for code in codes_by_number.get((_year(record.get("year")), number), []):
                    if _same_scope(code, record):
                        matched_codes.append(code)
                        candidates = number_candidates[id(code)]
                        if len({_definition_identity(candidate) for candidate in candidates}) != 1:
                            code_scope_unproven = True
                            identity_candidates.extend(candidates)
                            issue("value_label_identity_ambiguous", "An unnamed code record matches multiple source-table/variable identities; "
                                  "candidate definitions and raw labels are retained without a universal mapping.", name)
                        else:
                            resolved_codes.append(code)
        for parent in parents:
            for code in imputation_codes.get(_year(parent.get("year")), []):
                if _same_scope(code, parent):
                    matched_codes.append(code)
                    resolved_codes.append(code)
        matched_codes = _unique_records(matched_codes)
        resolved_codes = _unique_records(resolved_codes) if not lineage_incomplete else []
        declared_categorical = any(
            _text(row.get(field)).lower() in {"disc", "discrete", "categorical", "category"}
            for row in source_records for field in ("DataType", "format")
        )
        if not matched_codes and (parents or declared_categorical):
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
            if not any(covers_definition(code_record, row) for code_record in resolved_codes)
        ] if matched_codes else []
        if uncovered_definitions:
            scopes = sorted({f"{_year(row.get('year'))}/{_name(row.get('source_file')) or _source_table(row) or 'unknown source'}" for row in uncovered_definitions})
            issue("value_label_coverage_incomplete", f"Code labels are unavailable for some selected dictionary year/source scopes: {', '.join(scopes)}. No universal mapping was embedded.", name)
        mapping: dict[str, set[str]] = defaultdict(set)
        for record in resolved_codes:
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
        elif code_scope_unproven or uncovered_definitions or lineage_incomplete:
            value_labels = []
        else:
            value_labels = [{"value": value, "label": next(iter(labels))} for value, labels in sorted(mapping.items())]

        variable_status = "complete" if len(issues) == start_issues and (key in KEY_METADATA or (dictionary_ok and codes_ok)) else "incomplete"
        comparability_status = semantics["status"]
        if key not in KEY_METADATA:
            if conflicts or len(titles) > 1 or len(descriptions) > 1:
                comparability_status = "conflicting"
            elif variable_status != "complete" and comparability_status == "consistent":
                comparability_status = "unknown"
        variables.append({
            "name": name,
            "label": label,
            "description": description,
            "storage_type": str(field.type),
            "metadata_status": variable_status,
            "value_labels": value_labels,
            "source_metadata": source_records,
            "value_label_records": matched_codes,
            "resolved_value_label_records": resolved_codes,
            "code_identity_candidates": _unique_records(identity_candidates),
            "lineage_records": lineage_records,
            "imputation_parent_metadata": _unique_records(parents),
            "metadata_origin": "controlled_panel_key" if key in KEY_METADATA else ("embedded_dictionary" if use_embedded_dictionary else "source_dictionary"),
            "metadata_availability": {"dictionary": dictionary_ok, "codes": codes_ok},
            "metadata_columns": metadata_columns,
            "comparability_status": comparability_status,
            "semantic_metadata": semantics["fields"],
            "unknown_semantic_fields": semantics["unknown_fields"],
        })
    return {
        "years": selected_years,
        "variables": variables,
        "issues": _unique_records(issues),
        "metadata_status": "incomplete" if issues or any(row["metadata_status"] != "complete" for row in variables) else "complete",
        "comparability_status": ("conflicting" if any(row["comparability_status"] == "conflicting" for row in variables)
                                 else "unknown" if any(row["comparability_status"] == "unknown" for row in variables)
                                 else "consistent" if any(row["comparability_status"] == "consistent" for row in variables)
                                 else "not_applicable"),
        "unapplied_metadata_records": unapplied_metadata_records,
        "upstream_export_provenance": upstream_export_provenance,
        "metadata_sources": {
            "dictionary": "embedded_parquet" if use_embedded_dictionary else ("external" if dictionary_path is not None else "unavailable"),
            "codes": "embedded_parquet" if use_embedded_codes else ("external" if codes_path is not None else "unavailable"),
        },
    }
