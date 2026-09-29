#!/usr/bin/env python3
"""Check a full labeled Stata export in column chunks compatible with Stata/BE.

Every chunk reads the same full .dta, then compares all observations with an
independently prepared source-Parquet projection. No source files are changed.
Passing proves representation fidelity, not completeness of source metadata.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import re
import sys

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

SCRIPTS = Path(__file__).resolve().parents[1]
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))
from panel_export import sha256_file

_spec = importlib.util.spec_from_file_location("repair_export_validator", Path(__file__).with_name("27_validate_repair_exports.py"))
_repair = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_repair)
run_checked = _repair.run_checked
sanitize_native_log = _repair.sanitize_native_log
stata_string = _repair._stata_string

PASS_MARKER = "NATIVE_STATA_FULL_EXPORT_PASS"
DEFAULT_STATA = Path("/Applications/StataNow/StataBE.app/Contents/MacOS/StataBE")


def stata_path(path: Path) -> str:
    value = str(path.resolve())
    if any(character in value for character in ('"', '`', '$', '\n', '\r', '\x00')):
        raise ValueError("Validation paths cannot contain Stata quoting or macro characters")
    return value


def column_chunks(names: list[str], keys: list[str], maximum: int = 900) -> list[list[str]]:
    if not len(keys) < maximum <= 900:
        raise ValueError("Chunk size must exceed the key count and be at most 900")
    payload = [name for name in names if name not in keys]
    width = maximum - len(keys)
    return [keys + payload[index:index + width] for index in range(0, len(payload), width)] or [keys]


def reference_column(column: pa.Array, variable: dict) -> pa.Array:
    """Independently reconstruct the declared export representation."""
    conversion = variable.get("stata_storage_conversion", "none")
    if conversion == "string_categories_to_numeric":
        records = variable.get("stata_source_code_map", [])
        codes = {record["source_code"]: record["export_code"] for record in records}
        if (not records or len(codes) != len(records) or len(set(codes.values())) != len(records)
                or any(not isinstance(token, str) or type(code) is not int or not 0 < code <= 2147483620
                       for token, code in codes.items())):
            raise ValueError(f"{variable['name']}: category encoding is not a reversible positive-integer mapping")
        values = []
        for value in column.to_pylist():
            if value is not None and value not in codes:
                raise ValueError(f"{variable['name']}: observed source code {value!r} is absent from the encoding")
            values.append(None if value is None else codes[value])
        return pa.array(values, type=pa.int64())
    if conversion == "integer_code_strings_to_numeric":
        values = []
        for value in column.to_pylist():
            if value is None:
                values.append(None)
                continue
            try:
                code = int(value)
            except (TypeError, ValueError):
                raise ValueError(f"{variable['name']}: invalid integer source code {value!r}") from None
            if not isinstance(value, str) or str(code) != value or not -2147483648 <= code <= 2147483620:
                raise ValueError(f"{variable['name']}: noncanonical integer source code {value!r}")
            values.append(code)
        return pa.array(values, type=pa.int64())
    if conversion != "none":
        raise ValueError(f"Unsupported declared Stata conversion: {conversion}")
    if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
        return pa.array([value if value is not None else "" for value in column.to_pylist()], type=pa.string())
    if pa.types.is_null(column.type):
        return pa.array([None] * len(column), type=pa.float64())
    if pa.types.is_boolean(column.type):
        return column.cast(pa.int64())
    if pa.types.is_integer(column.type):
        if any(value is not None and abs(value) >= 2**53 for value in column.to_pylist()):
            raise ValueError(f"{variable['name']}: source integers exceed exact Stata precision")
        return column
    if pa.types.is_floating(column.type):
        return column
    raise ValueError(f"Unsupported source type for {variable['name']}: {column.type}")


def write_reference(source: pq.ParquetFile, variables: list[dict], keys: list[str], path: Path,
                    all_aliases: set[str]) -> dict:
    names = [variable["name"] for variable in variables]
    references = {}
    for index, variable in enumerate(variables):
        candidate = variable["export_name"] if variable["name"] in keys else f"__ipeds_ref_{index:04d}"
        while variable["name"] not in keys and candidate in all_aliases:
            candidate = "_" + candidate
        if len(candidate) > 32:
            raise ValueError("Cannot allocate a safe native reference variable name")
        references[variable["name"]] = candidate
    nulls, string_nulls, empty_strings = {}, {}, {}
    rows = 0
    writer = None
    numeric, strings = [], []
    try:
        for batch in source.iter_batches(batch_size=4096, columns=names):
            arrays = []
            for index, variable in enumerate(variables):
                name = variable["name"]
                original = batch.column(index)
                nulls[name] = nulls.get(name, 0) + original.null_count
                is_string = pa.types.is_string(original.type) or pa.types.is_large_string(original.type)
                if is_string and variable.get("stata_storage_conversion", "none") == "none":
                    string_nulls[name] = string_nulls.get(name, 0) + original.null_count
                    empty_strings[name] = empty_strings.get(name, 0) + original.to_pylist().count("")
                arrays.append(reference_column(original, variable))
            table = pa.Table.from_arrays(arrays, names=[references[name] for name in names])
            if writer is None:
                writer = pcsv.CSVWriter(path, table.schema)
                strings = [str(index + 1) for index, field in enumerate(table.schema) if pa.types.is_string(field.type)]
                numeric = [str(index + 1) for index, field in enumerate(table.schema) if not pa.types.is_string(field.type)]
            writer.write_table(table)
            rows += batch.num_rows
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        raise ValueError("Native validation requires at least one source observation")
    return {"rows": rows, "references": references, "numeric_positions": numeric, "string_positions": strings,
            "source_null_counts": nulls, "string_nulls_represented_as_empty": string_nulls,
            "source_literal_empty_string_counts": empty_strings}


def assert_native_string(expression: str, expected: str) -> list[str]:
    """Build safe Unicode expectations without exceeding Mata's token limit."""
    commands = ['mata: __ipeds_expected = ""']
    for start in range(0, len(expected), 48):
        fragment = stata_string(expected[start:start + 48])
        commands.append(f"mata: __ipeds_expected = __ipeds_expected + ({fragment})")
    commands.append(f"mata: assert({expression} == __ipeds_expected)")
    return commands


def native_commands(data: Path, reference: Path, variables: list[dict], keys: list[str],
                    detail: dict, total_columns: int) -> list[str]:
    aliases = {variable["name"]: variable["export_name"] for variable in variables}
    key_names = " ".join(aliases[name] for name in keys)
    options = 'clear varnames(1) case(preserve) encoding("utf-8") asdouble bindquotes(strict) maxquotedrows(unlimited)'
    if detail["numeric_positions"]:
        options += " numericcols(" + " ".join(detail["numeric_positions"]) + ")"
    if detail["string_positions"]:
        options += " stringcols(" + " ".join(detail["string_positions"]) + ")"
    commands = ["clear all", "set more off", f'describe using "{stata_path(data)}", short',
                f'mata: assert(st_numscalar("r(k)") == {total_columns})',
                f'mata: assert(st_numscalar("r(N)") == {detail["rows"]})',
                f'import delimited using "{stata_path(reference)}", {options}',
                f"mata: assert(st_nobs() == {detail['rows']})", f"isid {key_names}",
                "tempfile reference", 'save "`reference\'", replace',
                f'use {" ".join(aliases.values())} using "{stata_path(data)}", clear',
                f"mata: assert(st_nobs() == {detail['rows']})", f"isid {key_names}"]
    for variable in variables:
        name = variable["export_name"]
        commands += assert_native_string(f'st_varlabel("{name}")', variable["export_label"])
        codes = variable.get("stata_value_labels", {})
        if not codes:
            commands.append(f'mata: assert(st_varvaluelabel("{name}") == "")')
        else:
            commands += ["mata:", "__ipeds_codes = .", '__ipeds_labels = ""',
                         f'st_vlload(st_varvaluelabel("{name}"), __ipeds_codes, __ipeds_labels)',
                         f"assert(rows(__ipeds_codes) == {len(codes)})", "end"]
            for code, label in codes.items():
                commands += assert_native_string(f'st_vlmap(st_varvaluelabel("{name}"), {int(code)})', label)
    commands += [f'merge 1:1 {key_names} using "`reference\'"', "assert _merge == 3"]
    for variable in variables:
        if variable["name"] not in keys:
            name = variable["export_name"]
            reference_name = detail["references"][variable["name"]]
            commands += [f"assert missing({name}) == missing({reference_name})", f"assert {name} == {reference_name}"]
    return commands + [f'display "{PASS_MARKER}"', "exit, clear"]


def validate(data: Path, metadata_path: Path, source_path: Path, output: Path,
             binary: Path = DEFAULT_STATA, chunk_size: int = 900) -> dict:
    if output.exists() and any(output.iterdir()):
        raise ValueError("Use a new or empty validation output directory")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"status": "fail", "created_utc": datetime.now(timezone.utc).isoformat(),
               "validator_sha256": sha256_file(Path(__file__)), "chunks": []}
    try:
        if not binary.is_file():
            raise ValueError(f"Native Stata executable is unavailable: {binary}")
        metadata = json.loads(metadata_path.read_text())
        fingerprints = {name: {"path": str(path.resolve()), "sha256": sha256_file(path)}
                        for name, path in (("data", data), ("metadata", metadata_path), ("source", source_path))}
        if metadata.get("data_sha256") != fingerprints["data"]["sha256"]:
            raise ValueError("The metadata checksum does not match the Stata data file")
        source = pq.ParquetFile(source_path)
        variables = metadata["variables"]
        names = [variable["name"] for variable in variables]
        aliases = [variable["export_name"] for variable in variables]
        if names != source.schema_arrow.names or len(set(aliases)) != len(names):
            raise ValueError("Metadata must cover every source column exactly once, in source order, with unique export names")
        if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,31}", name) for name in aliases):
            raise ValueError("Metadata contains an unsafe Stata variable name")
        if metadata.get("row_count") != source.metadata.num_rows or metadata.get("column_count") != len(names):
            raise ValueError("Source shape does not match export metadata")
        by_name = {variable["name"]: variable for variable in variables}
        keys = metadata.get("panel_keys", ["UNITID", "year"])
        if len(keys) != 2 or len(set(keys)) != 2 or any(key not in names for key in keys):
            raise ValueError("Metadata must identify two distinct panel key columns")
        chunks = column_chunks(names, keys, chunk_size)
        receipt.update(inputs=fingerprints, executable=str(binary.resolve()), rows=source.metadata.num_rows,
                       columns=len(names), source_metadata_status=metadata.get("metadata_status"),
                       source_readiness_status=metadata.get("readiness_status"),
                       scope="All source observations, declared export conversions, native labels, and Stata missing-value representation; does not certify source metadata completeness.")
        for index, columns in enumerate(chunks, 1):
            print(f"Native Stata chunk {index}/{len(chunks)}: {len(columns)} columns, all rows", flush=True)
            folder = output / f"chunk_{index:02d}"
            folder.mkdir()
            selected = [by_name[name] for name in columns]
            reference = folder / "reference.csv"
            detail = write_reference(source, selected, keys, reference, set(aliases))
            do_file = folder / "native_verify.do"
            do_file.write_text("\n".join(native_commands(data, reference, selected, keys, detail, len(names))) + "\n")
            run_checked([str(binary.resolve()), "-e", "do", str(do_file.resolve())], folder / "native_process.txt",
                        cwd=folder, native_output=True)
            log = folder / "native_verify.log"
            if not log.is_file() or PASS_MARKER not in sanitize_native_log(log).splitlines():
                raise ValueError(f"Native Stata did not produce a passing receipt for chunk {index}")
            for variable in selected:
                if "null_count" in variable and detail["source_null_counts"][variable["name"]] != variable["null_count"]:
                    raise ValueError(f"Source null count differs from metadata: {variable['name']}")
            receipt["chunks"].append({"status": "pass", "columns": columns, **detail,
                                       "log_sha256": sha256_file(log), "do_file_sha256": sha256_file(do_file),
                                       "reference_sha256": sha256_file(reference)})
            reference.unlink()  # Large references are reproducible; retain their hashes and do-files.
        for name, path in (("data", data), ("metadata", metadata_path), ("source", source_path)):
            if sha256_file(path) != fingerprints[name]["sha256"]:
                raise ValueError(f"Validation input changed during verification: {name}")
        receipt.update(status="pass", exact_observations_in_export_representation=True,
                       variable_labels_checked=len(names),
                       value_labels_checked=sum(len(variable.get("stata_value_labels", {})) for variable in variables))
        return receipt
    except Exception as error:
        receipt["error"] = str(error)
        raise
    finally:
        (output / "validation.json").write_text(json.dumps(receipt, indent=2) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="Full .dta export")
    parser.add_argument("--metadata", type=Path, help="Defaults to INPUT.metadata.json")
    parser.add_argument("--source-panel", required=True, type=Path, help="Original full Parquet panel")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--stata-binary", "--stata", type=Path, default=DEFAULT_STATA)
    parser.add_argument("--chunk-size", type=int, default=900)
    args = parser.parse_args()
    result = validate(args.input, args.metadata or Path(str(args.input) + ".metadata.json"), args.source_panel,
                      args.output_dir, args.stata_binary, args.chunk_size)
    print(f"Native Stata verification passed: {result['rows']:,} rows, {result['columns']:,} columns, {len(result['chunks'])} chunks")


if __name__ == "__main__":
    main()
