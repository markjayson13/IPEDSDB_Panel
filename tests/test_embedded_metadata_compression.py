"""Full embedded codebooks must remain lossless and readable with normal Arrow defaults."""
from __future__ import annotations

import hashlib
import json
import zlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import export_metadata
from export_metadata import (
    MAX_EMBEDDED_VARIABLE_BYTES,
    build_export_metadata,
    decode_embedded_variable_metadata,
    encode_embedded_variable_metadata,
)
from panel_export import annotated_schema


def variable(name="CAT", *, padding="") -> dict:
    return {
        "name": name, "label": "Category", "description": "Original category meaning.",
        "storage_type": "int64", "metadata_status": "complete",
        "source_metadata": [{"year": 2023, "source_file": "HD", "varname": name,
                             "varnumber": "1", "varTitle": "Category",
                             "longDescription": "Original category meaning.", "verbatim_note": padding}],
        "value_label_records": [{"year": 2023, "source_file": "HD", "varname": name,
                                 "varnumber": "1", "codevalue": "1", "valuelabel": "First category"}],
        "metadata_availability": {"dictionary": True, "codes": True},
    }


def tagged_field(tags: dict) -> pa.Field:
    return pa.field("CAT", pa.int64(), metadata=tags)


def test_small_legacy_json_stays_plain_and_reannotation_removes_old_compression():
    small = variable()
    tags = encode_embedded_variable_metadata(small)
    assert set(tags) == {b"ipeds:variable"}
    assert json.loads(tags[b"ipeds:variable"]) == small
    assert decode_embedded_variable_metadata(tagged_field(tags)) == small
    compressed = encode_embedded_variable_metadata(variable(padding="long note " * 10000))
    schema = annotated_schema(pa.schema([tagged_field(compressed)]), {"variables": [small]})
    assert b"ipeds:variable:zlib" not in schema.field("CAT").metadata
    assert decode_embedded_variable_metadata(schema.field("CAT")) == small


def test_large_metadata_survives_default_reader_and_offline_builder(tmp_path):
    original = variable(padding="Original wording: café, 北京,\n" * 5000)
    schema = annotated_schema(pa.schema([("CAT", pa.int64())]), {"variables": [original], "years": [2023]})
    path = tmp_path / "labeled.parquet"
    pq.write_table(pa.Table.from_arrays([pa.array([1, None])], schema=schema), path)
    restored = pq.read_table(path)
    tags = restored.schema.field("CAT").metadata
    summary = json.loads(tags[b"ipeds:variable"])
    assert summary["metadata_encoding"] == "zlib"
    assert summary["payload_key"] == "ipeds:variable:zlib"
    assert tags[b"label"] == original["label"].encode()
    assert tags[b"description"] == original["description"].encode()
    assert decode_embedded_variable_metadata(restored.schema.field("CAT")) == original
    rebuilt = build_export_metadata(restored.schema, [2023], None, None)
    assert rebuilt["metadata_status"] == "complete"
    assert rebuilt["variables"][0]["source_metadata"] == original["source_metadata"]
    assert rebuilt["variables"][0]["value_label_records"] == original["value_label_records"]


def test_full_size_schema_reads_without_raising_parquet_thrift_limits(tmp_path):
    # More than 128 MiB of logical source metadata would become an even larger
    # base64 ARROW:schema value without compression. Default readers must work.
    padding = "preserved source wording " * 90000
    variables = [variable(f"CAT{i}", padding=padding) for i in range(64)]
    assert len(padding.encode()) * len(variables) > 128 * 1024 * 1024
    schema = annotated_schema(pa.schema([(v["name"], pa.int64()) for v in variables]), {"variables": variables})
    path = tmp_path / "full_size_metadata.parquet"
    pq.write_table(pa.Table.from_arrays([pa.array([1])] * len(variables), schema=schema), path)
    restored = pq.read_table(path)
    assert path.stat().st_size < 2 * 1024 * 1024
    assert restored.num_columns == len(variables)
    for field, expected in zip(restored.schema, variables):
        assert decode_embedded_variable_metadata(field) == expected


@pytest.mark.parametrize("damage,match", [
    ("invalid", "invalid compressed"), ("truncated", "truncated"),
    ("trailing", "trailing data"), ("missing", "payload is missing"),
    ("size_short", "beyond its declared"), ("size_long", "byte length does not match"),
    ("oversized", "oversized byte length"), ("checksum", "checksum does not match"),
    ("identity", "marker identity"), ("unmarked", "unmarked"),
    ("orphan", "no marker summary"),
])
def test_compressed_metadata_corruption_is_rejected(damage, match):
    tags = encode_embedded_variable_metadata(variable(padding="source note " * 3000))
    summary = json.loads(tags[b"ipeds:variable"])
    if damage == "invalid":
        tags[b"ipeds:variable:zlib"] = b"invalid compressed bytes"
    elif damage == "truncated":
        tags[b"ipeds:variable:zlib"] = tags[b"ipeds:variable:zlib"][:-2]
    elif damage == "trailing":
        tags[b"ipeds:variable:zlib"] += b"unexpected"
    elif damage == "missing":
        tags.pop(b"ipeds:variable:zlib")
    elif damage == "size_short":
        summary["uncompressed_bytes"] -= 1
    elif damage == "size_long":
        summary["uncompressed_bytes"] += 1
    elif damage == "oversized":
        summary["uncompressed_bytes"] = MAX_EMBEDDED_VARIABLE_BYTES + 1
    elif damage == "checksum":
        summary["sha256"] = "0" * 64
    elif damage == "identity":
        summary["name"] = "WRONG"
    elif damage == "unmarked":
        summary = variable()
    tags[b"ipeds:variable"] = json.dumps(summary).encode()
    if damage == "orphan":
        tags.pop(b"ipeds:variable")
    with pytest.raises(ValueError, match=match):
        decode_embedded_variable_metadata(tagged_field(tags))
    rebuilt = build_export_metadata(pa.schema([tagged_field(tags)]), [2023], None, None)
    assert rebuilt["metadata_status"] == "incomplete"
    assert "embedded_metadata_invalid" in {issue["code"] for issue in rebuilt["issues"]}


def test_decompression_expansion_is_bounded_by_declared_size():
    payload = b"x" * (4 * 1024 * 1024)
    summary = {"name": "CAT", "metadata_encoding": "zlib", "payload_key": "ipeds:variable:zlib",
               "uncompressed_bytes": 4096, "sha256": hashlib.sha256(payload).hexdigest()}
    tags = {b"ipeds:variable": json.dumps(summary).encode(), b"ipeds:variable:zlib": zlib.compress(payload)}
    with pytest.raises(ValueError, match="beyond its declared"):
        decode_embedded_variable_metadata(tagged_field(tags))


def test_writer_rejects_a_record_exceeding_the_reader_limit(monkeypatch):
    monkeypatch.setattr(export_metadata, "MAX_EMBEDDED_VARIABLE_BYTES", 100)
    with pytest.raises(ValueError, match="exceeds"):
        encode_embedded_variable_metadata(variable())
