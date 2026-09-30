from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from panel_extension import append_year, combine_metadata, compare_blocks, safe_column, sha256, union_schema


def write(path, values):
    pq.write_table(pa.table(values), path)
    return path


def test_append_preserves_historical_strings_missingness_and_new_column_scope(tmp_path):
    base = write(tmp_path / "base.parquet", {
        "UNITID": [10, 20], "year": [2023, 2023], "OPEID": ["00100200", None],
        "PELL": [12.0, None], "RETIRED": ["None", "-2"],
    })
    new = write(tmp_path / "new.parquet", {
        "UNITID": [10, 30], "year": [2024, 2024], "OPEID": ["00100200", "00000700"],
        "PELL": [14.0, 0.0], "NEW": [4, 5],
    })
    output = tmp_path / "combined.parquet"
    result = append_year(base, new, output, base_years=[2023], expected_base_sha256=sha256(base))
    table = pq.read_table(output)
    assert table["OPEID"].to_pylist() == ["00100200", None, "00100200", "00000700"]
    assert table["NEW"].to_pylist() == [None, None, 4, 5]
    assert table["RETIRED"].to_pylist() == ["None", "-2", None, None]
    assert result["new_columns"] == ["NEW"] and result["combined"]["rows"] == 4
    # Independent verification catches a real change after successful writing.
    damaged = table.set_column(table.schema.get_field_index("PELL"), "PELL", pa.array([13.0, None, 14.0, 0.0]))
    pq.write_table(damaged, output)
    with pytest.raises(ValueError, match="Value or missingness changed"):
        compare_blocks(base, new, output, table.schema)


@pytest.mark.parametrize("year, unitid", [([2024, 2024], [10, 10]), ([2023, 2024], [10, 11]), ([2024, 2024], [10, None])])
def test_append_rejects_duplicate_missing_or_wrong_year_keys(tmp_path, year, unitid):
    base = write(tmp_path / "base.parquet", {"UNITID": [10], "year": [2023]})
    new = write(tmp_path / "new.parquet", {"UNITID": unitid, "year": year})
    output = tmp_path / "combined.parquet"
    with pytest.raises(ValueError, match="Invalid panel keys"):
        append_year(base, new, output, base_years=[2023])
    assert not output.exists()


def test_extension_refuses_wrong_baseline_hash_and_lossy_cast(tmp_path):
    base = write(tmp_path / "base.parquet", {"UNITID": [10], "year": [2023]})
    new = write(tmp_path / "new.parquet", {"UNITID": [10], "year": [2024]})
    with pytest.raises(ValueError, match="published checksum"):
        append_year(base, new, tmp_path / "out.parquet", base_years=[2023], expected_base_sha256="0" * 64)
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        safe_column(pa.array([2**53 + 1], type=pa.int64()), pa.float64(), "BIG_ID")
    with pytest.raises(ValueError, match="capitalization"):
        union_schema(pa.schema([("UNITID", pa.int64()), ("year", pa.int64()), ("A", pa.int64())]),
                     pa.schema([("UNITID", pa.int64()), ("year", pa.int64()), ("a", pa.int64())]))


def test_metadata_union_never_substitutes_a_prior_year_definition(tmp_path):
    base = write(tmp_path / "old.parquet", {"year": [2023], "varname": ["V"], "description": ["Old meaning"]})
    new = write(tmp_path / "new.parquet", {"year": [2024], "varname": ["V"], "description": [""], "release_type": ["Provisional"]})
    out = tmp_path / "all.parquet"
    result = combine_metadata(base, new, out)
    assert pq.read_table(out)["description"].to_pylist() == ["Old meaning", ""]
    assert result["historical_records_unchanged"]
    with pytest.raises(ValueError, match="overlap"):
        combine_metadata(base, base, tmp_path / "wrong.parquet")
