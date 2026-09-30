"""Append a separately verified year without rebuilding historical observations.

The input panels stay immutable. Source values and missingness are compared
again after writing, and new columns must be null throughout the historical
block. This module does not approve mappings or waive source metadata gaps.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def identity(path: Path) -> tuple:
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns


def q(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def values_equal(left: pa.Array, right: pa.Array) -> bool:
    if left.equals(right):
        return True
    equal = pc.or_(pc.fill_null(pc.equal(left, right), False),
                   pc.and_(pc.is_null(left), pc.is_null(right)))
    if pa.types.is_floating(left.type) and pa.types.is_floating(right.type):
        equal = pc.or_(equal, pc.fill_null(pc.and_(pc.is_nan(left), pc.is_nan(right)), False))
    return bool(pc.all(equal).as_py()) if len(equal) else True


def safe_column(column: pa.Array, target: pa.DataType, name: str) -> pa.Array:
    if column.type == target:
        return column
    converted = pc.cast(column, target, safe=True)
    if not pa.types.is_null(column.type):
        restored = pc.cast(converted, column.type, safe=True)
        if not values_equal(column, restored):
            raise ValueError(f"Extension cast changes source values: {name}: {column.type} -> {target}")
    return converted


def union_schema(base: pa.Schema, addition: pa.Schema) -> pa.Schema:
    for schema in (base, addition):
        if len({name.upper() for name in schema.names}) != len(schema.names):
            raise ValueError("Duplicate or case-ambiguous panel columns")
        if not {"UNITID", "year"}.issubset(schema.names):
            raise ValueError("Panel must contain UNITID and year")
    old = {field.name.upper(): field.name for field in base}
    if any(name.upper() in old and old[name.upper()] != name for name in addition.names):
        raise ValueError("Extension changes the capitalization of an existing column")
    fields = []
    for field in base:
        dtype = field.type
        if pa.types.is_null(dtype) and field.name in addition.names:
            dtype = addition.field(field.name).type
        fields.append(pa.field(field.name, dtype))
    fields.extend(pa.field(field.name, field.type) for field in addition if field.name not in base.names)
    return pa.schema(fields)


def validate_keys(path: Path, expected_years: list[int]) -> dict:
    with duckdb.connect(config={"threads": "2", "memory_limit": "256MB"}) as con:
        rows, bad, unique = con.execute(
            "SELECT count(*), count(*) FILTER (WHERE UNITID IS NULL OR year IS NULL "
            "OR NOT isfinite(try_cast(UNITID AS DOUBLE)) OR try_cast(UNITID AS DOUBLE) IS NULL "
            "OR try_cast(UNITID AS DOUBLE) <= 0 OR try_cast(UNITID AS DOUBLE) != trunc(try_cast(UNITID AS DOUBLE)) "
            "OR NOT isfinite(try_cast(year AS DOUBLE)) OR try_cast(year AS DOUBLE) IS NULL "
            "OR try_cast(year AS DOUBLE) != trunc(try_cast(year AS DOUBLE))), "
            "count(DISTINCT (UNITID, year)) FROM read_parquet(?)", [str(path)]).fetchone()
        if bad:
            raise ValueError(f"Invalid panel keys/coverage: rows={rows}, invalid_keys={bad}, unique={unique}")
        years = [int(row[0]) for row in con.execute(
            "SELECT DISTINCT year FROM read_parquet(?) ORDER BY year", [str(path)]).fetchall()]
    if bad or unique != rows or years != sorted(expected_years):
        raise ValueError(f"Invalid panel keys/coverage: rows={rows}, missing={bad}, unique={unique}, years={years}")
    return {"rows": rows, "years": years, "unique_nonmissing_keys": True}


def compare_blocks(base: Path, addition: Path, output: Path, schema: pa.Schema) -> dict:
    """Read the completed file independently of the write loop and compare all cells."""
    actual = iter(pq.ParquetFile(output).iter_batches(batch_size=2048))
    current = next(actual, None)
    checked = []
    for source in (base, addition):
        source_file = pq.ParquetFile(source)
        count = 0
        for expected in source_file.iter_batches(batch_size=2048):
            offset = 0
            while offset < expected.num_rows:
                if current is None:
                    raise ValueError("Combined panel ended before a source block")
                size = min(expected.num_rows - offset, current.num_rows)
                block = expected.slice(offset, size)
                for name in schema.names:
                    observed = current.column(schema.get_field_index(name)).slice(0, size)
                    if name not in block.schema.names:
                        if observed.null_count != size:
                            raise ValueError(f"Invented values outside a source's column scope: {name}")
                    else:
                        original = block.column(block.schema.get_field_index(name))
                        reference = safe_column(original, schema.field(name).type, name)
                        if not values_equal(reference, observed):
                            raise ValueError(f"Value or missingness changed: {source.name}, {name}, row {count + offset}")
                current = next(actual, None) if current.num_rows == size else current.slice(size)
                offset += size
            count += expected.num_rows
        checked.append({"source": str(source), "rows": count, "columns": len(source_file.schema_arrow),
                        "all_values_equal": True, "all_missingness_equal": True,
                        "out_of_scope_columns_all_null": True})
    if current is not None or next(actual, None) is not None:
        raise ValueError("Combined panel has unexpected extra rows")
    return {"status": "pass", "blocks": checked}


def append_year(base: Path, addition: Path, output: Path, *, year: int = 2024,
                base_years: list[int] | None = None, expected_base_sha256: str | None = None) -> dict:
    if output.exists() or output.resolve() in {base.resolve(), addition.resolve()}:
        raise ValueError("Extension output must be a new file, separate from both inputs")
    snapshots = {str(p): identity(p) for p in (base, addition)}
    hashes = {str(p): sha256(p) for p in (base, addition)}
    if expected_base_sha256 and hashes[str(base)] != expected_base_sha256:
        raise ValueError("Historical panel differs from its published checksum")
    history = validate_keys(base, base_years or list(range(2004, year)))
    incoming = validate_keys(addition, [year])
    if year in history["years"]:
        raise ValueError("Extension year overlaps historical data")
    readers = [pq.ParquetFile(p) for p in (base, addition)]
    base_schema, incoming_schema = (reader.schema_arrow for reader in readers)
    base_names = set(base_schema.names)
    schema = union_schema(base_schema, incoming_schema)
    output.parent.mkdir(parents=True, exist_ok=True)
    pending = output.with_name(output.name + ".pending")
    if pending.exists():
        raise ValueError("An unfinished extension output exists; inspect it before retrying")
    try:
        with pq.ParquetWriter(pending, schema, compression="zstd") as writer:
            for reader in readers:
                for batch in reader.iter_batches(batch_size=2048):
                    arrays = [safe_column(batch.column(batch.schema.get_field_index(f.name)), f.type, f.name)
                              if f.name in batch.schema.names else pa.nulls(batch.num_rows, f.type) for f in schema]
                    writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=schema))
        parity = compare_blocks(base, addition, pending, schema)
        combined = validate_keys(pending, [*history["years"], year])
        for path in (base, addition):
            if snapshots[str(path)] != identity(path) or hashes[str(path)] != sha256(path):
                raise ValueError(f"Source changed while building extension: {path}")
        pending.replace(output)
    except Exception:
        pending.unlink(missing_ok=True)
        raise
    result = {"status": "pass", "year": year, "historical": history, "new_year": incoming,
              "combined": combined, "columns": len(schema),
              "new_columns": [n for n in schema.names if n not in base_names],
              "source_sha256": hashes, "data_sha256": sha256(output), "parity": parity}
    output.with_name(output.name + ".append-validation.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def combine_metadata(base: Path, addition: Path, output: Path, *, year: int = 2024) -> dict:
    """Union complete records; never backfill new-year definitions from history."""
    if output.exists():
        raise ValueError("Combined metadata must be a new file")
    source_hashes = {str(path): sha256(path) for path in (base, addition)}
    old, new = pq.read_table(base), pq.read_table(addition)
    if year in set(old.column("year").to_pylist()) or set(new.column("year").to_pylist()) != {year}:
        raise ValueError("Metadata scopes overlap or the extension has unexpected years")
    # Dictionary/code fields have compatible physical types but new provenance
    # fields are intentionally absent from historical records.
    merged = pa.concat_tables([old, new], promote_options="permissive")
    output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(merged, output, compression="zstd")
    check = pq.read_table(output)
    for label, table, offset in (("Historical", old, 0), ("New-year", new, old.num_rows)):
        for field in table.schema:
            left = table.column(field.name)
            right = check.column(field.name).slice(offset, table.num_rows).cast(field.type)
            if not left.equals(right):
                raise ValueError(f"{label} metadata changed: {field.name}")
    if source_hashes != {str(path): sha256(path) for path in (base, addition)}:
        raise ValueError("Metadata source changed during append")
    return {"historical_records": old.num_rows, "new_records": new.num_rows,
            "historical_records_unchanged": True, "new_records_unchanged": True,
            "source_sha256": source_hashes, "sha256": sha256(output)}


def compact_lineage(paths: list[Path], output: Path) -> dict:
    if output.exists():
        raise ValueError("Compact lineage must be a new file")
    columns = ["analysis_column", "year", "varname", "source_file", "access_table_name", "source_varnumber"]
    for path in paths:
        if not set(columns).issubset(pq.read_schema(path).names):
            raise ValueError(f"Incomplete canonical lineage schema: {path}")
    output.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(config={"threads": "2", "memory_limit": "512MB"}) as con:
        union = " UNION ALL ".join(
            f"SELECT {', '.join(columns)} FROM read_parquet({q(path)})" for path in paths)
        con.execute(f"COPY (SELECT DISTINCT * FROM ({union}) ORDER BY year,analysis_column,source_file,varname) "
                    f"TO {q(output)} (FORMAT PARQUET, COMPRESSION ZSTD)")
    return {"records": pq.ParquetFile(output).metadata.num_rows, "sha256": sha256(output),
            "scope": "Exact variable/year/source identities; full raw-value lineage remains in its versioned release inputs."}
