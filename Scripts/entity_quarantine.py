"""Quarantine two authorized mission-only records without changing source data."""
from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from panel_extension import identity, sha256, values_equal

DEFAULT_POLICY = Path(__file__).resolve().parents[1] / "contracts/source_quality/mission-orphans-v1.json"
PROVENANCE_KEY = b"ipeds:analysis_provenance"


def _policy(path: Path) -> dict:
    policy = json.loads(path.read_text())
    records = policy.get("records", [])
    keys = [(row.get("UNITID"), row.get("year")) for row in records]
    if (policy.get("policy_id") != "mission-orphans-v1" or
            policy.get("action") != "quarantine_from_2024_extension" or
            len(records) != 2 or set(keys) != {(111111, 2019), (111111, 2024)} or
            any(not isinstance(row.get("MISSIONURL"), str) or not row["MISSIONURL"] for row in records)):
        raise ValueError("Expected the exact two authorized mission quarantine records")
    return policy


def _provenance(policy: dict, policy_hash: str, source_hash: str, quarantine: Path) -> dict:
    return {"policy_id": policy["policy_id"], "policy_sha256": policy_hash,
            "source_sha256": source_hash,
            "excluded_keys": [{"UNITID": row["UNITID"], "year": row["year"]} for row in policy["records"]],
            "reason": policy["reason"], "quarantine_filename": quarantine.name}


def _partition(source: Path, policy: dict):
    expected = {(row["UNITID"], row["year"]): row for row in policy["records"]}
    seen = {key: 0 for key in expected}
    reader = pq.ParquetFile(source)
    if not {"UNITID", "year", "MISSIONURL"}.issubset(reader.schema_arrow.names):
        raise ValueError("Mission quarantine requires UNITID, year, and MISSIONURL")
    if len(reader.schema_arrow.names) != len(set(reader.schema_arrow.names)):
        raise ValueError("Mission quarantine rejects duplicate column names")
    for batch in reader.iter_batches(batch_size=2048):
        ids = batch.column(batch.schema.get_field_index("UNITID")).to_pylist()
        years = batch.column(batch.schema.get_field_index("year")).to_pylist()
        selected = []
        for index, key in enumerate(zip(ids, years)):
            record = expected.get(key)
            selected.append(record is not None)
            if record is None:
                continue
            seen[key] += 1
            if seen[key] != 1:
                raise ValueError(f"Duplicate mission quarantine key: {key}")
            actual_url = batch.column(batch.schema.get_field_index("MISSIONURL"))[index].as_py()
            if actual_url != record["MISSIONURL"]:
                raise ValueError(f"Mission quarantine source URL changed: {key}")
            for name, column in zip(batch.schema.names, batch.columns):
                if name not in {"UNITID", "year", "MISSIONURL"} and column[index].is_valid:
                    raise ValueError(f"Mission quarantine would remove real payload: {key}, {name}")
        mask = pa.array(selected, type=pa.bool_())
        yield batch.filter(pc.invert(mask)), batch.filter(mask)
    if any(count != 1 for count in seen.values()):
        raise ValueError("Mission quarantine source lacks an exact authorized key/year")


class _Readback:
    def __init__(self, path: Path):
        self.batches = iter(pq.ParquetFile(path).iter_batches(batch_size=2048))
        self.current = next(self.batches, None)
        self.path = path

    def compare(self, expected: pa.RecordBatch) -> None:
        offset = 0
        while offset < expected.num_rows:
            if self.current is None:
                raise ValueError(f"Quarantine readback is missing rows: {self.path.name}")
            size = min(expected.num_rows - offset, self.current.num_rows)
            for name, original, observed in zip(expected.schema.names, expected.columns, self.current.columns):
                left, right = original.slice(offset, size), observed.slice(0, size)
                if not left.is_null().equals(right.is_null()) or not values_equal(left, right):
                    raise ValueError(f"Quarantine value or missingness changed: {self.path.name}, {name}")
            self.current = next(self.batches, None) if size == self.current.num_rows else self.current.slice(size)
            offset += size

    def finish(self) -> None:
        if self.current is not None or next(self.batches, None) is not None:
            raise ValueError(f"Quarantine readback has unexpected extra rows: {self.path.name}")


def _compare(source: Path, output: Path, quarantine: Path, policy: dict) -> dict:
    schema = pq.read_schema(source)
    for path in (output, quarantine):
        if not pq.read_schema(path).equals(schema, check_metadata=False):
            raise ValueError(f"Quarantine schema or column order changed: {path.name}")
    retained_reader, excluded_reader = _Readback(output), _Readback(quarantine)
    retained_rows = excluded_rows = 0
    for retained, excluded in _partition(source, policy):
        retained_reader.compare(retained)
        excluded_reader.compare(excluded)
        retained_rows += retained.num_rows
        excluded_rows += excluded.num_rows
    retained_reader.finish()
    excluded_reader.finish()
    return {"source_rows": retained_rows + excluded_rows, "retained_rows": retained_rows,
            "quarantined_rows": excluded_rows, "column_count": len(schema),
            "all_retained_values_equal": True, "all_retained_missingness_equal": True,
            "all_quarantined_values_equal": True, "all_quarantined_missingness_equal": True,
            "exact_policy_rows_only": True}


def verify_quarantine(source: Path, output: Path, quarantine: Path, receipt: dict | Path,
                      policy: Path = DEFAULT_POLICY) -> dict:
    """Bind all artifacts and independently compare the complete retained and excluded rows."""
    record = json.loads(receipt.read_text()) if isinstance(receipt, Path) else receipt
    rule = _policy(policy)
    paths = [source, output, quarantine, policy]
    snapshots = {path: identity(path) for path in paths}
    hashes = {"source_sha256": sha256(source), "data_sha256": sha256(output),
              "quarantine_sha256": sha256(quarantine), "policy_sha256": sha256(policy)}
    if record.get("status") != "pass" or any(record.get(key) != value for key, value in hashes.items()):
        raise ValueError("Quarantine receipt is not bound to the current artifacts and policy")
    provenance = _provenance(rule, hashes["policy_sha256"], hashes["source_sha256"], quarantine)
    tagged = (pq.read_schema(output).metadata or {}).get(PROVENANCE_KEY)
    if tagged is None or json.loads(tagged) != provenance or record.get("analysis_provenance") != provenance:
        raise ValueError("Quarantine analysis provenance does not match the source and policy")
    proof = _compare(source, output, quarantine, rule)
    if record.get("parity") != proof:
        raise ValueError("Quarantine receipt has incomplete full-cell parity evidence")
    if any(identity(path) != original for path, original in snapshots.items()):
        raise ValueError("Quarantine input or output changed during verification")
    return {"status": "pass", **hashes, "analysis_provenance": provenance, "parity": proof}


def quarantine_mission_records(source: Path, output: Path, quarantine: Path, *,
                               policy: Path = DEFAULT_POLICY) -> dict:
    """Create a separately labeled analysis panel and retain both complete excluded rows."""
    receipt_path = output.with_name(output.name + ".quarantine-validation.json")
    destinations = [output, quarantine, receipt_path]
    if (len({path.resolve() for path in [source, policy, *destinations]}) != 5 or
            any(path.exists() or path.is_symlink() for path in destinations)):
        raise ValueError("Quarantine outputs must be new files separate from source and policy")
    pending = [path.with_name(path.name + ".pending") for path in (output, quarantine)]
    if any(path.exists() or path.is_symlink() for path in pending):
        raise ValueError("Unfinished quarantine output exists; inspect before retrying")
    rule = _policy(policy)
    snapshots = {path: identity(path) for path in [source, policy]}
    source_hash, policy_hash = sha256(source), sha256(policy)
    provenance = _provenance(rule, policy_hash, source_hash, quarantine)
    schema = pq.read_schema(source)
    metadata = {**(schema.metadata or {}), PROVENANCE_KEY: json.dumps(provenance, sort_keys=True).encode()}
    output_schema = schema.with_metadata(metadata)
    promoted = []
    try:
        for path in pending:
            path.parent.mkdir(parents=True, exist_ok=True)
        with pq.ParquetWriter(pending[0], output_schema, compression="zstd") as retained_writer, \
                pq.ParquetWriter(pending[1], schema, compression="zstd") as excluded_writer:
            for retained, excluded in _partition(source, rule):
                if retained.num_rows:
                    retained_writer.write_batch(retained.replace_schema_metadata(metadata))
                if excluded.num_rows:
                    excluded_writer.write_batch(excluded)
        proof = _compare(source, pending[0], pending[1], rule)
        if (any(identity(path) != original for path, original in snapshots.items()) or
                sha256(source) != source_hash or sha256(policy) != policy_hash):
            raise ValueError("Quarantine source or policy changed during construction")
        result = {"status": "pass", "source_sha256": source_hash, "data_sha256": sha256(pending[0]),
                  "quarantine_sha256": sha256(pending[1]), "policy_sha256": policy_hash,
                  "analysis_provenance": provenance, "parity": proof}
        for temporary, final in zip(pending, (output, quarantine)):
            temporary.replace(final)
            promoted.append(final)
        promoted.append(receipt_path)
        receipt_path.write_text(json.dumps(result, indent=2) + "\n")
        return result
    except Exception:
        for path in promoted:
            path.unlink(missing_ok=True)
        raise
    finally:
        for path in pending:
            path.unlink(missing_ok=True)
