"""Format edge cases that can silently corrupt labels or replace a good export."""
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from panel_export import dictionary_rows, prepare_format_metadata, stata_names, write_stata
from export_integrity import PACKAGE_SUFFIXES, assert_source_unchanged, promote_package, source_fingerprint


def metadata_for(schema, label="Test label", values=None):
    return {"issues": [], "variables": [
        {"name": f.name, "label": label, "description": "Test definition", "value_labels": values or []}
        for f in schema
    ]}


def test_stata_aliases_are_stable_unique_and_labels_remain_full(tmp_path: Path) -> None:
    names = ["a" * 40, "a" * 39 + "b", "1bad name", "double", "VALID"]
    mapping = stata_names(names)
    assert mapping == stata_names(list(reversed(names)))
    assert len(set(mapping.values())) == len(names)
    assert mapping["VALID"] == "VALID"
    assert all(len(name) <= 32 for name in mapping.values())
    table = pa.table({name: [1, None] for name in names})
    label = "Unicode title 東京 " * 10
    metadata = metadata_for(table.schema, label)
    prepare_format_metadata(metadata, table.schema, "dta")
    output = tmp_path / "aliases.dta"
    write_stata(table, output, metadata)
    with pd.read_stata(output, iterator=True, convert_categoricals=False) as reader:
        assert reader.variable_labels() == {name: label[:80] for name in mapping.values()}
        assert reader.read().columns.tolist() == list(mapping.values())
    assert all(v["label"] == label for v in metadata["variables"])


def test_fractional_or_numerically_ambiguous_stata_codes_are_never_truncated() -> None:
    schema = pa.schema([("CODE", pa.float64())])
    for values in ([{"value": "1.5", "label": "Fraction"}],
                   [{"value": "1", "label": "One"}, {"value": "1.0", "label": "Different"}]):
        metadata = metadata_for(schema, values=values)
        prepare_format_metadata(metadata, schema, "dta")
        assert metadata["variables"][0]["stata_value_labels"] == {}
        assert metadata["variables"][0]["value_labels"] == values
        assert metadata["issues"][0]["code"] == "stata_labels_sidecar_only"


def test_all_null_strings_export_as_stata_missing_strings(tmp_path: Path) -> None:
    table = pa.table({"TEXT": pa.array([None, None], type=pa.string())})
    metadata = metadata_for(table.schema)
    prepare_format_metadata(metadata, table.schema, "dta")
    output = tmp_path / "strings.dta"
    write_stata(table, output, metadata)
    assert pd.read_stata(output)["TEXT"].tolist() == ["", ""]


def test_stata_rejects_nul_in_variable_label_before_silent_truncation() -> None:
    schema = pa.schema([("CODE", pa.int32())])
    metadata = metadata_for(schema, label="Before\x00After")
    with pytest.raises(ValueError, match="variable labels cannot preserve embedded NUL"):
        prepare_format_metadata(metadata, schema, "dta")


def test_portable_dictionary_surfaces_unknown_semantics_and_source_corrections() -> None:
    headers, rows = dictionary_rows({"variables": [{
        "name": "PELL", "export_name": "PELL", "label": "Pell recipients", "description": "Source definition",
        "storage_type": "double", "metadata_status": "complete", "comparability_status": "unknown",
        "semantic_metadata": {
            "units": {"status": "unknown", "value": None},
            "currency": {"status": "unknown", "value": None},
            "price_basis": {"status": "unknown", "value": None},
            "reference_period": {"status": "consistent", "value": "preceding academic year"},
        },
        "source_metadata": [{"year": 2023, "source_file": "SFA_P", "access_table_name": "SFA2223_P1",
                             "original_access_table_name": "SFA2223_P2", "resolved_physical_table": "SFA2223_P1",
                             "metadata_correction_id": "2023-sfa-v1:70306:UPGRNTN"}],
    }]})
    assert headers[:10] == ["column_order", "name", "export_name", "label", "description", "storage_type",
                            "metadata_status", "null_count", "source_files", "years"]
    row = dict(zip(headers, rows[0]))
    assert row["comparability_status"] == "unknown"
    assert row["units"] == row["currency"] == ""
    assert row["units_status"] == row["currency_status"] == "unknown"
    assert row["reference_period"] == "preceding academic year"
    assert row["reference_period_status"] == "consistent"
    assert row["original_source_tables"] == "SFA2223_P2"
    assert row["resolved_source_tables"] == "SFA2223_P1"
    assert row["metadata_correction_ids"] == "2023-sfa-v1:70306:UPGRNTN"


@pytest.mark.parametrize("existing", [True, False])
def test_package_promotion_rolls_back_when_final_replace_fails(tmp_path: Path, monkeypatch, existing: bool) -> None:
    import export_integrity
    stage = tmp_path / "staging"
    stage.mkdir()
    output = tmp_path / "extract.csv"
    staged = stage / output.name
    originals = {}
    for suffix in ("", *PACKAGE_SUFFIXES):
        Path(str(staged) + suffix).write_bytes(("new" + suffix).encode())
        if existing:
            destination = Path(str(output) + suffix)
            destination.write_bytes(("original" + suffix).encode())
            originals[destination] = destination.read_bytes()
    real_replace = export_integrity.os.replace
    def fail_final_replace(source, destination):
        if Path(source) == staged:
            raise OSError("simulated final data replacement failure")
        return real_replace(source, destination)
    monkeypatch.setattr(export_integrity.os, "replace", fail_final_replace)
    with pytest.raises(OSError, match="simulated final"):
        promote_package(staged, output)
    assert {path: path.read_bytes() for path in tmp_path.glob("extract.csv*")} == originals
    assert not list(tmp_path.glob(".ipeds-export-backup-*"))


def test_directory_source_fingerprint_detects_changed_or_added_fragments(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    for name, values in (("b.parquet", [2]), ("a.parquet", [1])):
        pq.write_table(pa.table({"year": [2023], "UNITID": values}), first / name)
        (second / name).write_bytes((first / name).read_bytes())
    fingerprint = source_fingerprint(first)
    assert fingerprint == source_fingerprint(second)
    assert [row["path"] for row in fingerprint["files"]] == ["a.parquet", "b.parquet"]
    pq.write_table(pa.table({"year": [2023], "UNITID": [3]}), first / "a.parquet")
    with pytest.raises(ValueError, match="Source changed during export"):
        assert_source_unchanged(first, fingerprint)
    pq.write_table(pa.table({"year": [2023], "UNITID": [4]}), second / "c.parquet")
    with pytest.raises(ValueError, match="Source changed during export"):
        assert_source_unchanged(second, fingerprint)
