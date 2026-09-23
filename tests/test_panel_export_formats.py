"""Format edge cases that can silently corrupt labels or replace a good export."""
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

from panel_export import prepare_format_metadata, stata_names, write_stata


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
