"""Metadata-aware writers for analyst extracts; no cleaning or recoding is done here."""
from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import warnings
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE


# Stata reserved variable names, also rejected by pandas' Stata writer.
STATA_RESERVED = set("""aggregate array boolean break byte case catch class colvector
complex const continue default delegate delete do double else eltypedef end enum
explicit export external float for friend function global goto if in inline int
local long NULL pragma protected quad rowvector short strL typedef typename using
virtual with _all _b _coef _cons _n _N _pi _pred _rc _se _skip""".split())


def add_issue(metadata: dict, code: str, variable: str, message: str) -> None:
    metadata["issues"].append({"severity": "warning", "code": code,
                               "variable": variable, "message": message})


def stata_names(names: list[str]) -> dict[str, str]:
    """Keep legal names and give every changed name a stable, collision-safe alias."""
    legal = {n for n in names if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,31}", n)
             and n not in STATA_RESERVED}
    used = set(legal)
    result = {}
    for name in names:
        if name in legal:
            result[name] = name
            continue
        base = re.sub(r"[^A-Za-z0-9_]", "_", name)
        if not base or base[0].isdigit() or base in STATA_RESERVED:
            base = "v_" + base
        counter = 0
        while True:
            digest = hashlib.sha256(f"{name}:{counter}".encode()).hexdigest()[:8]
            candidate = f"{base[:23]}_{digest}"
            if candidate not in used:
                break
            counter += 1
        result[name] = candidate
        used.add(candidate)
    return result


def prepare_format_metadata(metadata: dict, schema: pa.Schema, fmt: str) -> None:
    names = stata_names(schema.names) if fmt == "dta" else {n: n for n in schema.names}
    for var in metadata["variables"]:
        name = var["name"]
        var["export_name"] = names[name]
        var["export_label"] = var["label"][:80] if fmt == "dta" else var["label"]
        if fmt == "dta" and "\x00" in var["export_label"]:
            raise ValueError(f"{name}: Stata variable labels cannot preserve embedded NUL characters.")
        if var["export_name"] != name:
            add_issue(metadata, "stata_name_changed", name, "Original name is retained in the dictionary.")
        if var["export_label"] != var["label"]:
            add_issue(metadata, "stata_label_shortened", name, "Full label is retained in the dictionary.")
        var["stata_value_labels"] = {}
        labels = var["value_labels"]
        if fmt != "dta" or not labels:
            continue
        dtype = schema.field(name).type
        mapping = {}
        numeric = pa.types.is_integer(dtype) or pa.types.is_floating(dtype) or pa.types.is_boolean(dtype)
        valid = numeric
        for label in labels:
            try:
                value = Decimal(label["value"])
                if not value.is_finite() or value != value.to_integral_value() or not -2147483648 <= value <= 2147483620:
                    valid = False
                    continue
                code = int(value)
                if code in mapping and mapping[code] != label["label"]:
                    valid = False
                if "\x00" in label["label"]:
                    valid = False
                mapping[code] = label["label"]
            except (InvalidOperation, ValueError):
                valid = False
        if valid:
            var["stata_value_labels"] = mapping
        else:
            add_issue(metadata, "stata_labels_sidecar_only", name,
                      "String, fractional, out-of-range or ambiguous codes cannot be Stata value labels; see value_labels.csv.")


def annotated_schema(schema: pa.Schema, metadata: dict) -> pa.Schema:
    fields = []
    for field, var in zip(schema, metadata["variables"]):
        tags = dict(field.metadata or {})
        tags.update({b"label": var["label"].encode(), b"description": var["description"].encode(),
                     b"ipeds:variable": json.dumps(var, ensure_ascii=False).encode()})
        fields.append(field.with_metadata(tags))
    # pandas metadata may describe columns removed from the extract.
    tags = {k: v for k, v in (schema.metadata or {}).items() if k != b"pandas"}
    # Full records already live on each field. Avoid duplicating a potentially
    # large multi-year dictionary inside a single schema metadata string.
    tags[b"ipeds:export"] = json.dumps({k: v for k, v in metadata.items() if k != "variables"}, ensure_ascii=False).encode()
    return pa.schema(fields, metadata=tags)


def write_stream(dataset, columns: list[str], filt, batch_rows: int, path: Path,
                 fmt: str, schema: pa.Schema, metadata: dict) -> int:
    rows = 0
    schema = annotated_schema(schema, metadata) if fmt == "parquet" else schema
    writer = (pq.ParquetWriter(path, schema, compression="snappy") if fmt == "parquet"
              else pcsv.CSVWriter(str(path), schema))
    with writer:
        for batch in dataset.to_batches(columns=columns, filter=filt, batch_size=batch_rows):
            table = pa.Table.from_batches([batch]).cast(schema)
            writer.write_table(table)
            rows += batch.num_rows
    return rows


def write_stata(table: pa.Table, path: Path, metadata: dict) -> None:
    data = {}
    for field in table.schema:
        column = table[field.name]
        dtype = field.type
        if pa.types.is_integer(dtype) or pa.types.is_boolean(dtype):
            # Check before pandas converts nullable integers into doubles.
            values = column.to_pylist()
            if any(v is not None and abs(int(v)) >= 2**53 for v in values):
                raise ValueError(f"{field.name}: integers exceed exact Stata numeric precision; use Parquet or CSV.")
            data[field.name] = pd.Series(values, dtype="Int64")
        elif pa.types.is_floating(dtype):
            data[field.name] = pd.Series(column.to_numpy(zero_copy_only=False), dtype="float64")
        elif pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
            values = column.to_pylist()
            if any(v is not None and "\x00" in v for v in values):
                raise ValueError(f"{field.name}: Stata strings cannot preserve embedded NUL characters.")
            data[field.name] = pd.Series([v if v is not None else "" for v in values], dtype=object)
            if column.null_count:
                add_issue(metadata, "stata_string_null", field.name,
                          "Stata represents missing strings as empty strings; original null count is recorded.")
        elif pa.types.is_null(dtype):
            data[field.name] = pd.Series([float("nan")] * len(column), dtype="float64")
        else:
            raise ValueError(f"{field.name}: unsupported Stata type {dtype}; use Parquet.")
    frame = pd.DataFrame(data)
    names = {v["name"]: v["export_name"] for v in metadata["variables"]}
    frame.rename(columns=names, inplace=True)
    labels = {v["export_name"]: v["export_label"] for v in metadata["variables"]}
    values = {v["export_name"]: v["stata_value_labels"] for v in metadata["variables"] if v["stata_value_labels"]}
    # Treat unexpected pandas conversion warnings as errors, not silent data loss.
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        data_label = ("IPEDS diagnostic query result; see companion metadata"
                      if metadata.get("artifact_kind") == "query_diagnostic"
                      else "IPEDS institution-year extract; see companion metadata")
        frame.to_stata(path, write_index=False, version=118 if len(frame.columns) <= 32767 else 119,
                       data_label=data_label,
                       variable_labels=labels, value_labels=values)


def excel_cell(ws, value):
    if isinstance(value, str):
        if len(value) > 32767 or ILLEGAL_CHARACTERS_RE.search(value):
            raise ValueError("Excel cell text exceeds its limit or contains an unsupported control character; use Parquet/CSV.")
        cell = WriteOnlyCell(ws, value=value)
        cell.data_type = "s"  # Source text beginning with '=' must remain text.
        return cell
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Excel cannot preserve NaN/infinity as numeric cells; use Parquet.")
    if isinstance(value, (int, float)) and value == int(value) and abs(value) >= 10**15:
        raise ValueError("Excel cannot preserve integers beyond 15 digits; use Parquet or CSV.")
    return value


def append_sheet(wb, name: str, headers: list[str], rows) -> None:
    ws = wb.create_sheet(name)
    ws.freeze_panes = "A2"
    ws.append([excel_cell(ws, value) for value in headers])
    count = 1
    for row in rows:
        count += 1
        if count > 1048576:
            raise ValueError(f"Excel sheet {name} exceeds the row limit; select fewer rows or use Parquet/CSV.")
        ws.append([excel_cell(ws, value) for value in row])


def dictionary_rows(metadata: dict) -> tuple[list[str], list[list]]:
    headers = ["column_order", "name", "export_name", "label", "description", "storage_type",
               "metadata_status", "null_count", "source_files", "years"]
    semantic_fields = ("units", "currency", "price_basis", "reference_period")
    headers += ["comparability_status"]
    headers += [column for field in semantic_fields for column in (field, field + "_status")]
    headers += ["metadata_correction_ids", "original_source_tables", "resolved_source_tables"]
    rows = []
    for i, var in enumerate(metadata["variables"], 1):
        records = var["source_metadata"]
        sources = sorted({str(r["source_file"]) for r in records if r.get("source_file")})
        years = sorted({str(r["year"]) for r in records if r.get("year") is not None})
        row = [i, var["name"], var["export_name"], var["label"], var["description"],
               var["storage_type"], var["metadata_status"], var.get("null_count", ""),
               ";".join(sources), ";".join(years), var.get("comparability_status", "unknown")]
        for field in semantic_fields:
            detail = var.get("semantic_metadata", {}).get(field, {})
            row.extend([detail.get("value") if detail.get("value") is not None else "", detail.get("status", "unknown")])
        corrections = sorted({str(record["metadata_correction_id"]) for record in records if record.get("metadata_correction_id")})
        original_tables = sorted({str(value) for record in records if
                                  (value := record.get("original_access_table_name") or record.get("access_table_name") or record.get("source_table"))})
        resolved_tables = sorted({str(value) for record in records if
                                  (value := record.get("resolved_physical_table") or record.get("access_table_name_resolved")
                                   or record.get("source_table") or record.get("access_table_name"))})
        row.extend([";".join(corrections), ";".join(original_tables), ";".join(resolved_tables)])
        rows.append(row)
    return headers, rows


def value_label_rows(metadata: dict) -> tuple[list[str], list[list]]:
    records = []
    for var in metadata["variables"]:
        for record in var["value_label_records"]:
            records.append({**record, "name": var["name"], "export_name": var["export_name"]})
    preferred = ["name", "export_name", "year", "source_file", "varname", "varnumber", "codevalue", "valuelabel"]
    headers = preferred + sorted({k for r in records for k in r} - set(preferred))
    return headers, [[r.get(k, "") for k in headers] for r in records]


def write_excel(dataset, columns: list[str], filt, batch_rows: int, path: Path, metadata: dict) -> int:
    if len(columns) > 16384 or metadata["row_count"] > 1048575:
        raise ValueError("Excel supports 16,384 columns and 1,048,575 data rows plus a header; select a smaller extract.")
    for name in columns:
        dtype = dataset.schema.field(name).type
        if not (pa.types.is_integer(dtype) or pa.types.is_floating(dtype) or pa.types.is_boolean(dtype)
                or pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_null(dtype)):
            raise ValueError(f"{name}: unsupported Excel type {dtype}; use Parquet to preserve its type and precision.")
    wb = Workbook(write_only=True)
    rows = 0
    def data_rows():
        nonlocal rows
        for batch in dataset.to_batches(columns=columns, filter=filt, batch_size=batch_rows):
            for row in zip(*(c.to_pylist() for c in batch.columns)):
                rows += 1
                yield row
    try:
        append_sheet(wb, "data", columns, data_rows())
        append_sheet(wb, "dictionary", *dictionary_rows(metadata))
        append_sheet(wb, "value_labels", *value_label_rows(metadata))
        append_sheet(wb, "about", ["property", "value"], [
            ["source_panel", metadata["source_panel"]], ["created_utc", metadata["created_utc"]],
            ["years", ", ".join(map(str, metadata["years"]))], ["rows", metadata["row_count"]],
            ["metadata_status", metadata["metadata_status"]],
            ["readiness_status", metadata.get("readiness_status", "not_assessed")],
            ["missing_values", metadata["missing_values"]],
            ["full_metadata", path.name + ".metadata.json"],
            ["note", "Codes remain codes. Definitions and code labels are on the accompanying sheets."],
        ])
        issue_headers = ["severity", "code", "variable", "message"]
        append_sheet(wb, "issues", issue_headers,
                     [[issue.get(k, "") for k in issue_headers] for issue in metadata["issues"]])
        wb.save(path)
    finally:
        wb.close()
    return rows


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_sidecars(path: Path, metadata: dict) -> None:
    for suffix, (headers, rows) in [("dictionary.csv", dictionary_rows(metadata)),
                                    ("value_labels.csv", value_label_rows(metadata))]:
        with Path(str(path) + "." + suffix).open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(headers)
            writer.writerows(rows)
    metadata["data_sha256"] = sha256_file(path)
    Path(str(path) + ".metadata.json").write_text(json.dumps(metadata, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    title = {"query_diagnostic": "IPEDS diagnostic query result", "panel_dictionary": "IPEDS panel dictionary"}.get(metadata.get("artifact_kind"), "IPEDS analyst extract")
    Path(str(path) + ".README.txt").write_text(
        f"{title}: {path.name}\n"
        f"Rows: {metadata['row_count']}; metadata status: {metadata['metadata_status']}\n"
        f"Analysis readiness: {metadata.get('readiness_status', 'not_assessed')}\n"
        "Keep this data file together with its .metadata.json, .dictionary.csv and .value_labels.csv files.\n"
        "The JSON includes original definitions, year/source-specific labels, source paths, issues, null counts and the data SHA-256.\n"
        "Missing values: " + metadata["missing_values"] + "\n"
        "Stata: open the .dta directly. Native labels apply only when unambiguous and representable. "
        "Name mappings and full labels are in the dictionary. Missing strings become empty strings.\n"
        "Excel: use the data, dictionary, value_labels, about and issues sheets. Blank and empty-string cells may be indistinguishable.\n"
        "CSV: UTF-8, comma-delimited, header row, quoted strings. Unquoted empty fields are null; quoted empty strings are text. "
        "Use the recorded storage types when importing. Do not rely on spreadsheet type inference for identifiers or codes.\n"
        "For Arrow CSV import set ConvertOptions(column_types=<types from metadata>, strings_can_be_null=True, "
        "quoted_strings_can_be_null=False, null_values=['']). This preserves literal NA and leading-zero strings.\n"
        "Negative codes and imputation flags are preserved, not recoded. No units or inflation basis are inferred. "
        "Null reasons cannot be reconstructed from a cleaned input without its cleaning QA artifacts.\n"
        "Source checksums, exporter code hashes, and observation checks are recorded when available. "
        "Ordinary package replacement errors trigger rollback to the prior files. Multi-file publication is not crash-atomic; "
        "a machine/process crash can require recovery from .ipeds-export-backup-* in the output directory. "
        "Wait for successful completion before reading a package, and verify its data checksum.\n"
        "Review metadata issues before analysis. Export validation does not certify the underlying panel or cross-year comparability.\n",
        encoding="utf-8")
