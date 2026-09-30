import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from entity_quarantine import DEFAULT_POLICY, PROVENANCE_KEY, quarantine_mission_records, verify_quarantine
from panel_extension import sha256, values_equal


@pytest.fixture
def files(tmp_path):
    policy = json.loads(DEFAULT_POLICY.read_text())
    urls = {row["year"]: row["MISSIONURL"] for row in policy["records"]}
    source, output, quarantine = (tmp_path / name for name in ("source.parquet", "analysis.parquet", "quarantine.parquet"))
    table = pa.table({
        "UNITID": pa.array([10, 111111, 20, 111111, 111112, 30], type=pa.int64()),
        "year": pa.array([2018, 2019, 2022, 2024, 2024, 2023], type=pa.int32()),
        "MISSIONURL": [None, urls[2019], "ordinary.example", urls[2024], urls[2024], None],
        "VALUE": pa.array([0.0, None, float("nan"), None, 12.0, None], type=pa.float64()),
        "CODE": ["", None, "None", None, "-2", None],
        "FLAG": pa.array([1, None, -2, None, None, 0], type=pa.int8()),
    }).replace_schema_metadata({b"original": b"preserve source metadata"})
    pq.write_table(table, source, row_group_size=2)
    return source, output, quarantine


def replace_value(path, column, index, value):
    table = pq.read_table(path)
    field = table.schema.field(column)
    values = table[column].to_pylist()
    values[index] = value
    pq.write_table(table.set_column(table.schema.get_field_index(column), field,
                                   pa.array(values, type=field.type)), path)


def test_exact_two_records_are_preserved_separately_and_every_other_cell_survives(files):
    source, output, quarantine = files
    original_hash = sha256(source)
    receipt = quarantine_mission_records(*files)
    assert sha256(source) == original_hash
    assert receipt["parity"]["source_rows"] == 6
    assert receipt["parity"]["retained_rows"] == 4
    assert receipt["parity"]["quarantined_rows"] == 2
    original, actual, excluded = (pq.read_table(path) for path in files)
    assert actual.schema.equals(original.schema, check_metadata=False)
    assert actual.schema.metadata[b"original"] == b"preserve source metadata"
    expected = original.take(pa.array([0, 2, 4, 5]))
    for left, right in zip(expected.columns, actual.columns):
        assert values_equal(left.combine_chunks(), right.combine_chunks())
        assert left.is_null().equals(right.is_null())
    assert excluded.equals(original.take(pa.array([1, 3]))), "Preserve the complete excluded source rows"
    assert actual["UNITID"].to_pylist() == [10, 20, 111112, 30], "An unrelated institution sharing the URL stays"
    provenance = json.loads(actual.schema.metadata[PROVENANCE_KEY])
    assert provenance == receipt["analysis_provenance"]
    assert provenance["source_sha256"] == original_hash
    assert provenance["policy_sha256"] == sha256(DEFAULT_POLICY)
    assert provenance["quarantine_filename"] == quarantine.name
    assert provenance["excluded_keys"] == [{"UNITID": 111111, "year": 2019}, {"UNITID": 111111, "year": 2024}]
    receipt_path = output.with_name(output.name + ".quarantine-validation.json")
    assert verify_quarantine(*files, receipt_path) == receipt


@pytest.mark.parametrize("index", [1, 3])
@pytest.mark.parametrize("column,value", [("VALUE", 0.0), ("CODE", ""), ("FLAG", -2)])
def test_quarantine_never_removes_a_record_with_real_payload(files, index, column, value):
    source, output, quarantine = files
    replace_value(source, column, index, value)
    before = sha256(source)
    with pytest.raises(ValueError, match="real payload"):
        quarantine_mission_records(*files)
    assert sha256(source) == before
    assert not output.exists() and not quarantine.exists()
    assert not list(source.parent.glob("*.pending"))


@pytest.mark.parametrize("index", [1, 3])
def test_changed_source_url_is_not_silently_quarantined(files, index):
    source, output, quarantine = files
    replace_value(source, "MISSIONURL", index, "a-real-institution.example")
    with pytest.raises(ValueError, match="source URL changed"):
        quarantine_mission_records(*files)
    assert not output.exists() and not quarantine.exists()


@pytest.mark.parametrize("index", [1, 3])
def test_wrong_year_cannot_match_the_quarantine_registry(files, index):
    source, output, quarantine = files
    replace_value(source, "year", index, 2020)
    with pytest.raises(ValueError, match="exact authorized key/year"):
        quarantine_mission_records(*files)
    assert not output.exists() and not quarantine.exists()


def test_duplicate_quarantine_key_is_rejected(files):
    source, output, quarantine = files
    table = pq.read_table(source)
    pq.write_table(pa.concat_tables([table, table.slice(1, 1)]), source)
    with pytest.raises(ValueError, match="Duplicate"):
        quarantine_mission_records(*files)
    assert not output.exists() and not quarantine.exists()


@pytest.mark.parametrize("index", [0, 1, 2])
def test_changed_source_or_result_fails_hash_binding(files, index):
    receipt = quarantine_mission_records(*files)
    replace_value(files[index], "MISSIONURL", 0, "Changed after approval")
    with pytest.raises(ValueError, match="not bound"):
        verify_quarantine(*files, receipt)


@pytest.mark.parametrize("scope", ["value", "missingness", "row", "quarantined_value"])
def test_independent_full_cell_readback_rejects_changes_even_with_refreshed_checksum(files, scope):
    source, output, quarantine = files
    receipt = quarantine_mission_records(*files)
    if scope == "value":
        replace_value(output, "VALUE", 0, 999.0)
    elif scope == "missingness":
        replace_value(output, "CODE", 0, None)
    elif scope == "row":
        pq.write_table(pq.read_table(output).slice(1), output)
    else:
        replace_value(quarantine, "MISSIONURL", 0, "Not the original quarantined row")
    receipt["data_sha256"] = sha256(output)
    receipt["quarantine_sha256"] = sha256(quarantine)
    with pytest.raises(ValueError, match="value or missingness changed|missing rows"):
        verify_quarantine(*files, receipt)


def test_changed_column_order_is_rejected_even_with_refreshed_checksum(files):
    source, output, quarantine = files
    receipt = quarantine_mission_records(*files)
    table = pq.read_table(output)
    pq.write_table(table.select(list(reversed(table.column_names))), output)
    receipt["data_sha256"] = sha256(output)
    with pytest.raises(ValueError, match="schema or column order"):
        verify_quarantine(*files, receipt)


@pytest.mark.parametrize("mode", ["remove", "change"])
def test_filtered_panel_must_carry_matching_analysis_provenance(files, mode):
    source, output, quarantine = files
    receipt = quarantine_mission_records(*files)
    table = pq.read_table(output)
    metadata = dict(table.schema.metadata)
    if mode == "remove":
        metadata.pop(PROVENANCE_KEY)
    else:
        provenance = json.loads(metadata[PROVENANCE_KEY])
        provenance["source_sha256"] = "0" * 64
        metadata[PROVENANCE_KEY] = json.dumps(provenance).encode()
    pq.write_table(table.replace_schema_metadata(metadata), output)
    receipt["data_sha256"] = sha256(output)
    with pytest.raises(ValueError, match="analysis provenance"):
        verify_quarantine(*files, receipt)


def test_policy_and_complete_parity_evidence_are_bound(files, tmp_path):
    receipt = quarantine_mission_records(*files)
    policy = tmp_path / "policy.json"
    policy.write_bytes(DEFAULT_POLICY.read_bytes())
    verify_quarantine(*files, receipt, policy=policy)
    changed = json.loads(policy.read_text())
    changed["reason"] = "An unapproved replacement reason"
    policy.write_text(json.dumps(changed))
    with pytest.raises(ValueError, match="not bound"):
        verify_quarantine(*files, receipt, policy=policy)
    receipt["parity"].pop("all_retained_missingness_equal")
    with pytest.raises(ValueError, match="incomplete full-cell parity"):
        verify_quarantine(*files, receipt)


def test_existing_outputs_cannot_be_overwritten(files):
    quarantine_mission_records(*files)
    before = [sha256(path) for path in files]
    with pytest.raises(ValueError, match="new files"):
        quarantine_mission_records(*files)
    assert [sha256(path) for path in files] == before
