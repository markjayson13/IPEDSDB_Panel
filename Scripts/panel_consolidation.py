"""Consolidate reviewed source families while proving every component recoverable.

The registry approves disjoint year scopes, never a priority between populated
columns. The independent readback reconstructs the entire original panel from
the canonical columns and compares its values, nulls, keys, and row order.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from panel_extension import identity, sha256, values_equal

POLICY_ID = "source-family-consolidation-v1"
PROVENANCE_KEY = b"ipeds:column_consolidation"
BATCH_SIZE = 2048
KEYS = {"UNITID", "year"}


def _policy(path: Path) -> dict:
    rule = json.loads(path.read_text())
    if (not isinstance(rule, dict) or type(rule.get("schema_version")) is not int or
            rule["schema_version"] != 1 or rule.get("policy_id") != POLICY_ID or
            not isinstance(rule.get("groups"), list) or not rule["groups"]):
        raise ValueError("Expected a version 1 source-family consolidation policy with groups")
    columns, canonicals = set(), set()
    for group in rule["groups"]:
        if not isinstance(group, dict):
            raise ValueError("Each consolidation group must be an object")
        name = group.get("canonical_name")
        if (not isinstance(name, str) or not name.strip() or name.casefold() in canonicals or
                name.casefold() in {key.casefold() for key in KEYS}):
            raise ValueError("Canonical names must be nonempty, case-unique, and separate from keys")
        canonicals.add(name.casefold())
        if (not isinstance(group.get("rationale"), str) or not group["rationale"].strip() or
                not isinstance(group.get("caveats"), (str, list)) or
                not isinstance(group.get("evidence"), dict)):
            raise ValueError(f"Consolidation group lacks rationale, caveats, or evidence: {name}")
        members = group.get("members")
        if not isinstance(members, list) or len(members) < 2:
            raise ValueError(f"Consolidation requires at least two members: {name}")
        years = set()
        for member in members:
            if not isinstance(member, dict):
                raise ValueError("Each consolidation member must be an object")
            column, scope = member.get("column"), member.get("years")
            if (not isinstance(column, str) or not column.strip() or column.casefold() in columns or
                    column.casefold() in {key.casefold() for key in KEYS}):
                raise ValueError("Source columns must occur once and keys cannot be merged")
            columns.add(column.casefold())
            if (not isinstance(scope, list) or not scope or any(type(year) is not int for year in scope) or
                    len(scope) != len(set(scope)) or years.intersection(scope)):
                raise ValueError(f"Member years must be distinct integers and disjoint within a group: {name}")
            years.update(scope)
    if columns.intersection(canonicals):
        raise ValueError("Canonical names cannot alias any source member")
    return rule


@dataclass
class _Plan:
    source_schema: pa.Schema
    output_schema: pa.Schema
    source_indices: dict
    output_indices: dict
    members: dict
    groups: list


def _plan(schema: pa.Schema, rule: dict) -> _Plan:
    names = {name.casefold() for name in schema.names}
    if len(names) != len(schema.names) or not KEYS.issubset(schema.names):
        raise ValueError("Consolidation requires case-unique columns and UNITID/year keys")
    if PROVENANCE_KEY in (schema.metadata or {}):
        raise ValueError("Source already carries column-consolidation provenance")
    members = {}
    first_members = {}
    indices = {name: index for index, name in enumerate(schema.names)}
    for group in rule["groups"]:
        canonical = group["canonical_name"]
        if canonical.casefold() in names:
            raise ValueError(f"Canonical column collides with an existing source column: {canonical}")
        source_names = [member["column"] for member in group["members"]]
        if any(name not in indices for name in source_names):
            raise ValueError(f"Consolidation source member is missing: {canonical}")
        if len({schema.field(name).type for name in source_names}) != 1:
            raise ValueError(f"Consolidation member types must match exactly: {canonical}")
        first_members[min(source_names, key=indices.get)] = canonical
        for member in group["members"]:
            members[member["column"]] = (canonical, tuple(member["years"]))
    fields = []
    for field in schema:
        if field.name in first_members:
            fields.append(field.with_name(first_members[field.name]).with_nullable(True))
        elif field.name not in members:
            fields.append(field)
    output_schema = pa.schema(fields, metadata=schema.metadata)
    return _Plan(schema, output_schema, indices,
                 {name: index for index, name in enumerate(output_schema.names)}, members, rule["groups"])


def _provenance(rule: dict, policy_hash: str, source_hash: str) -> dict:
    return {"schema_version": 1, "policy_id": rule["policy_id"], "policy_sha256": policy_hash,
            "source_sha256": source_hash,
            "groups": [{key: group[key] for key in ("canonical_name", "members", "rationale", "caveats")}
                       for group in rule["groups"]]}


def _mask(years: pa.Array, scope: tuple, cache: dict) -> pa.Array:
    key = tuple(sorted(scope))
    if key not in cache:
        cache[key] = pc.is_in(years, value_set=pa.array(key, type=years.type))
    return cache[key]


def _all_null_outside(column: pa.Array, mask: pa.Array, name: str) -> None:
    if pc.any(pc.and_(pc.is_valid(column), pc.invert(mask))).as_py():
        raise ValueError(f"Non-null value outside approved member years: {name}")


def _write(source: Path, pending: Path, plan: _Plan, metadata: dict) -> None:
    schema = plan.output_schema.with_metadata(metadata)
    with pq.ParquetWriter(pending, schema, compression="zstd") as writer:
        for batch in pq.ParquetFile(source).iter_batches(batch_size=BATCH_SIZE):
            years = batch.column(plan.source_indices["year"])
            cache, merged = {}, {}
            for group in plan.groups:
                columns = []
                occupied = pa.array([False] * batch.num_rows)
                for member in group["members"]:
                    column = batch.column(plan.source_indices[member["column"]])
                    _all_null_outside(column, _mask(years, tuple(member["years"]), cache), member["column"])
                    populated = pc.is_valid(column)
                    if pc.any(pc.and_(occupied, populated)).as_py():
                        raise ValueError(f"Two populated components overlap: {group['canonical_name']}")
                    occupied = pc.or_(occupied, populated)
                    columns.append(column)
                merged[group["canonical_name"]] = pc.coalesce(*columns)
            arrays = [merged[field.name] if field.name in merged else batch.column(plan.source_indices[field.name])
                      for field in schema]
            writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=schema))


def _aligned(source: Path, output: Path):
    """Align independent readers without assuming matching Parquet row groups."""
    observed = iter(pq.ParquetFile(output).iter_batches(batch_size=BATCH_SIZE))
    current = next(observed, None)
    for original in pq.ParquetFile(source).iter_batches(batch_size=BATCH_SIZE):
        offset = 0
        while offset < original.num_rows:
            if current is None:
                raise ValueError("Consolidated readback is missing source rows")
            size = min(original.num_rows - offset, current.num_rows)
            yield original.slice(offset, size), current.slice(0, size)
            offset += size
            current = next(observed, None) if size == current.num_rows else current.slice(size)
    if current is not None or next(observed, None) is not None:
        raise ValueError("Consolidated readback has unexpected extra rows")


def _compare(source: Path, output: Path, plan: _Plan, provenance: dict) -> dict:
    schema = pq.read_schema(output)
    if not schema.equals(plan.output_schema, check_metadata=False):
        raise ValueError("Consolidated schema, column types, or order changed")
    tagged = (schema.metadata or {}).get(PROVENANCE_KEY)
    if tagged is None or json.loads(tagged) != provenance:
        raise ValueError("Column-consolidation provenance does not match source and policy")
    metadata = dict(schema.metadata or {})
    metadata.pop(PROVENANCE_KEY)
    if metadata != dict(plan.source_schema.metadata or {}):
        raise ValueError("Consolidation changed source schema metadata")
    if any(not left.equals(right, check_metadata=True) for left, right in zip(schema, plan.output_schema)):
        raise ValueError("Consolidation changed column metadata")
    rows, year_rows = 0, Counter()
    source_counts = {name: Counter() for name in plan.members}
    component_counts = {name: Counter() for name in plan.members}
    for original, observed in _aligned(source, output):
        years = observed.column(plan.output_indices["year"])
        year_values = years.to_pylist()
        if any(year is None or isinstance(year, bool) or not isinstance(year, (int, float)) or
               not float(year).is_integer() for year in year_values):
            raise ValueError("Consolidation requires nonmissing integer-valued years")
        year_rows.update(int(year) for year in year_values)
        cache = {}
        for group in plan.groups:
            union = tuple(year for member in group["members"] for year in member["years"])
            _all_null_outside(observed.column(plan.output_indices[group["canonical_name"]]),
                              _mask(years, union, cache), group["canonical_name"])
        for name, index in plan.source_indices.items():
            expected = original.column(index)
            if name in plan.members:
                canonical, scope = plan.members[name]
                value = observed.column(plan.output_indices[canonical])
                # Invert the approved transformation, independently of the write loop.
                actual = pc.if_else(_mask(years, scope, cache), value, pa.nulls(len(value), value.type))
                for year in set(year_values):
                    year_mask = _mask(years, (int(year),), cache)
                    for counts, column in ((source_counts, expected), (component_counts, actual)):
                        populated = pc.and_(year_mask, pc.is_valid(column))
                        counts[name][int(year)] += pc.sum(pc.cast(populated, pa.int64())).as_py() or 0
            else:
                actual = observed.column(plan.output_indices[name])
            if not expected.is_null().equals(actual.is_null()) or not values_equal(expected, actual):
                raise ValueError(f"Inverse reconstruction changed source values or missingness: {name}, row {rows}")
        rows += original.num_rows
    components = [{"column": name, "canonical_name": canonical, "years": list(scope),
                   "all_values_equal": True, "all_missingness_equal": True,
                   "by_year": [{"year": year, "rows": year_rows[year],
                                "source_nonnull": source_counts[name][year],
                                "reconstructed_nonnull": component_counts[name][year]}
                               for year in sorted(year_rows)]}
                  for name, (canonical, scope) in plan.members.items()]
    return {"source_rows": rows, "output_rows": rows, "source_columns": len(plan.source_schema),
            "output_columns": len(plan.output_schema), "merged_groups": len(plan.groups),
            "reconstructed_components": len(plan.members), "cells_compared": rows * len(plan.source_schema),
            "all_source_values_equal": True, "all_source_missingness_equal": True,
            "row_keys_and_order_equal": True, "nonmerged_columns_equal": True,
            "canonical_out_of_scope_all_null": True,
            "year_rows": [{"year": year, "rows": year_rows[year]} for year in sorted(year_rows)],
            "components": components}


def _destinations(source: Path, output: Path, policy: Path, suffix: str):
    receipt = output.with_name(output.name + suffix)
    pending = output.with_name(output.name + ".pending")
    if (len({path.resolve() for path in (source, output, policy, receipt, pending)}) != 5 or
            any(path.exists() or path.is_symlink() for path in (output, receipt, pending))):
        raise ValueError("Consolidation outputs must be new files separate from source and policy")
    return receipt, pending


def verify_consolidation(source: Path, output: Path, receipt_path: Path, policy: Path) -> dict:
    """Rebind hashes/provenance and reconstruct every original source cell."""
    paths = (source, output, receipt_path, policy)
    snapshots = {path: identity(path) for path in paths}
    record = json.loads(receipt_path.read_text())
    rule = _policy(policy)
    hashes = {"source_sha256": sha256(source), "data_sha256": sha256(output), "policy_sha256": sha256(policy)}
    if record.get("status") != "pass" or any(record.get(key) != value for key, value in hashes.items()):
        raise ValueError("Consolidation receipt is not bound to current source, output, and policy")
    provenance = _provenance(rule, hashes["policy_sha256"], hashes["source_sha256"])
    if record.get("column_consolidation") != provenance:
        raise ValueError("Consolidation receipt provenance differs from source and policy")
    proof = _compare(source, output, _plan(pq.read_schema(source), rule), provenance)
    if record.get("parity") != proof:
        raise ValueError("Consolidation receipt lacks complete independent inverse parity")
    if any(identity(path) != before for path, before in snapshots.items()):
        raise ValueError("Consolidation artifacts changed during verification")
    return {"status": "pass", **hashes, "column_consolidation": provenance, "parity": proof}


def consolidate(source: Path, output: Path, policy: Path) -> dict:
    """Write a separate analysis panel only when every source component is recoverable."""
    receipt_path, pending = _destinations(source, output, policy, ".consolidation-validation.json")
    snapshots = {path: identity(path) for path in (source, policy)}
    rule = _policy(policy)
    plan = _plan(pq.read_schema(source), rule)
    source_hash, policy_hash = sha256(source), sha256(policy)
    provenance = _provenance(rule, policy_hash, source_hash)
    metadata = {**(plan.source_schema.metadata or {}), PROVENANCE_KEY: json.dumps(provenance, sort_keys=True).encode()}
    promoted = []
    try:
        output.parent.mkdir(parents=True, exist_ok=True)
        _write(source, pending, plan, metadata)
        proof = _compare(source, pending, plan, provenance)
        if (any(identity(path) != before for path, before in snapshots.items()) or
                sha256(source) != source_hash or sha256(policy) != policy_hash):
            raise ValueError("Consolidation source or policy changed during construction")
        result = {"status": "pass", "source_sha256": source_hash, "data_sha256": sha256(pending),
                  "policy_sha256": policy_hash, "column_consolidation": provenance, "parity": proof}
        pending.replace(output)
        promoted.append(output)
        promoted.append(receipt_path)
        receipt_path.write_text(json.dumps(result, indent=2) + "\n")
        return result
    except Exception:
        for path in promoted:
            path.unlink(missing_ok=True)
        raise
    finally:
        pending.unlink(missing_ok=True)


def consolidate_identities(source: Path, output: Path, policy: Path) -> dict:
    """Remap exact lineage column/year identities and retain every original record.

    Out-of-scope registered identities stay unchanged and are reported, because
    lineage alone cannot establish whether they carry meaningful panel data.
    """
    receipt_path, pending = _destinations(source, output, policy, ".consolidation-identities.json")
    snapshots = {path: identity(path) for path in (source, policy)}
    rule = _policy(policy)
    schema = pq.read_schema(source)
    if (not {"analysis_column", "year"}.issubset(schema.names) or
            "source_panel_column" in schema.names or len(set(schema.names)) != len(schema.names)):
        raise ValueError("Identity consolidation requires original unique analysis_column/year fields")
    mapping = {(member["column"], year): group["canonical_name"] for group in rule["groups"]
               for member in group["members"] for year in member["years"]}
    registered = {column for column, _ in mapping}
    hashes = {"source_sha256": sha256(source), "policy_sha256": sha256(policy)}
    indices = {name: index for index, name in enumerate(schema.names)}
    target = schema.append(schema.field("analysis_column").with_name("source_panel_column"))
    rows, mapped, unresolved = 0, 0, Counter()
    promoted = []
    try:
        output.parent.mkdir(parents=True, exist_ok=True)
        with pq.ParquetWriter(pending, target, compression="zstd") as writer:
            for batch in pq.ParquetFile(source).iter_batches(batch_size=BATCH_SIZE):
                names = batch.column(indices["analysis_column"])
                keys = list(zip(names.to_pylist(), batch.column(indices["year"]).to_pylist()))
                arrays = list(batch.columns)
                arrays[indices["analysis_column"]] = pa.array([mapping.get(key, key[0]) for key in keys], type=names.type)
                writer.write_batch(pa.RecordBatch.from_arrays([*arrays, names], schema=target))
        if not pq.read_schema(pending).equals(target, check_metadata=True):
            raise ValueError("Identity consolidation changed the lineage schema")
        for original, observed in _aligned(source, pending):
            original_names = original.column(indices["analysis_column"])
            keys = list(zip(original_names.to_pylist(), original.column(indices["year"]).to_pylist()))
            expected_names = pa.array([mapping.get(key, key[0]) for key in keys], type=original_names.type)
            if not values_equal(expected_names, observed.column(indices["analysis_column"])):
                raise ValueError("Identity consolidation changed approved column/year remapping")
            for name, index in indices.items():
                actual = observed.column(len(schema) if name == "analysis_column" else index)
                expected = original.column(index)
                if not expected.is_null().equals(actual.is_null()) or not values_equal(expected, actual):
                    raise ValueError(f"Identity consolidation changed an original field: {name}")
            mapped += sum(key in mapping for key in keys)
            unresolved.update(key for key in keys if key[0] in registered and key not in mapping)
            rows += original.num_rows
        if (any(identity(path) != before for path, before in snapshots.items()) or
                any(sha256(path) != hashes[key] for path, key in ((source, "source_sha256"), (policy, "policy_sha256")))):
            raise ValueError("Consolidation identity source or policy changed during construction")
        result = {"status": "pass", **hashes, "data_sha256": sha256(pending), "records": rows,
                  "mapped_records": mapped, "unchanged_records": rows - mapped,
                  "all_original_fields_equal": True, "all_original_missingness_equal": True,
                  "out_of_scope_registered_identities": [{"column": column, "year": year, "records": count}
                       for (column, year), count in sorted(unresolved.items(), key=lambda item: (item[0][0], str(item[0][1])))]}
        pending.replace(output)
        promoted.extend((output, receipt_path))
        receipt_path.write_text(json.dumps(result, indent=2) + "\n")
        return result
    except Exception:
        for path in promoted:
            path.unlink(missing_ok=True)
        raise
    finally:
        pending.unlink(missing_ok=True)
