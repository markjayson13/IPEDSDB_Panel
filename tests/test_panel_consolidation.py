import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from panel_consolidation import consolidate, consolidate_identities, verify_consolidation
from panel_extension import sha256, values_equal


PROVENANCE_KEY = b"ipeds:column_consolidation"


def receipt_path(output):
    return output.with_name(output.name + ".consolidation-validation.json")


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


@pytest.fixture
def files(tmp_path):
    source, output, policy = (tmp_path / name for name in
                              ("source.parquet", "consolidated.parquet", "policy.json"))
    years = [2019, 2024, 2020, 2021, 2022, 2023] * 834
    years = years[:5003]
    old_years = {2019, 2020}
    new_years = {2021, 2022, 2024}
    counts = [None if i % 13 == 0 else (0 if i % 7 == 0 else -2 if i % 11 == 0 else i)
              for i in range(len(years))]
    strings = [None if i % 13 == 0 else ["", "None", "-2", "ordinary", "café"][i % 5]
               for i in range(len(years))]
    floats = [None if i % 19 == 0 else float("nan") if i % 17 == 0 else i / 8.0
              for i in range(len(years))]

    def component(values, old, dtype):
        return pa.array([value if year in (old_years if old else new_years) else None
                         for year, value in zip(years, values)], type=dtype)

    table = pa.table({
        "UNITID": pa.array(range(100000, 100000 + len(years)), type=pa.int64()),
        "COUNT_OLD": component(counts, True, pa.int64()),
        "KEEP_TEXT": pa.array(["" if i % 2 else None for i in range(len(years))]),
        "TEXT_NEW": component(strings, False, pa.string()),
        "year": pa.array(years, type=pa.int32()),
        "COUNT_NEW": component(counts, False, pa.int64()),
        "FLOAT_OLD": component(floats, True, pa.float64()),
        "KEEP_BOOL": pa.array([None if i % 3 == 0 else bool(i % 2)
                               for i in range(len(years))]),
        "TEXT_OLD": component(strings, True, pa.string()),
        "FLOAT_NEW": component(floats, False, pa.float64()),
        "ALL_NULL": pa.nulls(len(years), type=pa.int16()),
    }).replace_schema_metadata({
        b"original": b"preserve source metadata",
        b"ipeds:analysis_provenance": b'{"source_policy":"prior-quarantine"}',
    })
    pq.write_table(table, source, row_group_size=701)
    groups = []
    # Neither group order nor member order matches the original column order.
    for name in ("FLOAT", "TEXT", "COUNT"):
        groups.append({
            "canonical_name": name,
            "rationale": "Synthetic same-measure family with disjoint source years.",
            "caveats": ["Retain source-year boundaries; no semantic rescaling."],
            "members": [
                {"column": name + "_NEW", "years": [2021, 2022, 2024]},
                {"column": name + "_OLD", "years": [2019, 2020]},
            ],
            "evidence": {"kind": "synthetic reviewed contract"},
        })
    write_json(policy, {"schema_version": 1, "policy_id": "source-family-consolidation-v1",
                        "groups": groups, "excluded_groups": []})
    return source, output, policy


def replace_value(path, column, index, value):
    table = pq.read_table(path)
    field = table.schema.field(column)
    values = table[column].to_pylist()
    values[index] = value
    pq.write_table(table.set_column(table.schema.get_field_index(column), field,
                                   pa.array(values, type=field.type)), path, row_group_size=997)


def refresh_output_hash(output):
    path = receipt_path(output)
    record = json.loads(path.read_text())
    record["data_sha256"] = sha256(output)
    write_json(path, record)


def assert_rejected_without_outputs(files):
    source, output, policy = files
    before = sha256(source), sha256(policy)
    with pytest.raises(ValueError):
        consolidate(source, output, policy)
    assert (sha256(source), sha256(policy)) == before
    assert not output.exists()
    assert not receipt_path(output).exists()
    assert not list(output.parent.glob("*.pending"))


def test_full_readback_reconstructs_every_component_across_multiple_batches(files):
    source, output, policy = files
    source_hash, policy_hash = sha256(source), sha256(policy)
    receipt = consolidate(source, output, policy)
    assert sha256(source) == source_hash
    assert sha256(policy) == policy_hash
    original, actual = pq.read_table(source), pq.read_table(output)
    assert actual.num_rows == original.num_rows == 5003
    assert actual.column_names == ["UNITID", "COUNT", "KEEP_TEXT", "TEXT", "year", "FLOAT",
                                   "KEEP_BOOL", "ALL_NULL"]
    for key, value in original.schema.metadata.items():
        assert actual.schema.metadata[key] == value
    for name in ("UNITID", "year", "KEEP_TEXT", "KEEP_BOOL", "ALL_NULL"):
        assert actual.schema.field(name).equals(original.schema.field(name), check_metadata=True)
        assert values_equal(original[name].combine_chunks(), actual[name].combine_chunks())
        assert original[name].is_null().equals(actual[name].is_null())
    years = original["year"].to_pylist()
    for group in json.loads(policy.read_text())["groups"]:
        canonical = actual[group["canonical_name"]]
        for member in group["members"]:
            component = original[member["column"]]
            assert canonical.type == component.type
            reconstructed = pa.array(
                [value if year in member["years"] else None
                 for year, value in zip(years, canonical.to_pylist())], type=component.type)
            assert values_equal(component.combine_chunks(), reconstructed)
            assert component.combine_chunks().is_null().equals(reconstructed.is_null())
    assert actual["FLOAT"][34].is_valid, "NaN is a source value, not Arrow null"
    assert actual["TEXT"][10].as_py() == "", "Empty strings must remain present"
    assert actual["COUNT"][7].as_py() == 0
    assert all(actual[name][5].as_py() is None for name in ("COUNT", "TEXT", "FLOAT"))
    assert receipt["status"] == "pass"
    assert receipt["source_sha256"] == source_hash
    assert receipt["data_sha256"] == sha256(output)
    assert receipt["policy_sha256"] == policy_hash
    proof = receipt["parity"]
    assert proof["source_rows"] == proof["output_rows"] == 5003
    assert proof["source_columns"] == 11
    assert proof["output_columns"] == 8
    assert proof["cells_compared"] == 5003 * 11
    for check in ("all_source_values_equal", "all_source_missingness_equal", "row_keys_and_order_equal"):
        assert proof[check] is True
    tagged = json.loads(actual.schema.metadata[PROVENANCE_KEY])
    assert tagged == receipt["column_consolidation"]
    assert tagged["source_sha256"] == source_hash
    assert tagged["policy_sha256"] == policy_hash
    assert json.loads(receipt_path(output).read_text()) == receipt
    assert verify_consolidation(source, output, receipt_path(output), policy) == receipt


@pytest.mark.parametrize("case", [
    "unsupported_schema", "wrong_policy_id", "one_member", "missing_member", "repeated_member",
    "member_used_in_two_groups", "merge_unitid", "merge_year", "existing_canonical",
    "case_insensitive_existing_canonical", "duplicate_canonical", "case_insensitive_duplicate_canonical",
    "empty_canonical", "overlapping_years", "empty_years", "noninteger_year",
])
def test_invalid_policy_is_rejected_before_any_output_is_promoted(files, case):
    source, output, policy = files
    rule = json.loads(policy.read_text())
    group = rule["groups"][0]
    if case == "unsupported_schema":
        rule["schema_version"] = 2
    elif case == "wrong_policy_id":
        rule["policy_id"] = "unreviewed-family-v2"
    elif case == "one_member":
        group["members"].pop()
    elif case == "missing_member":
        group["members"][0]["column"] = "NOT_IN_SOURCE"
    elif case == "repeated_member":
        group["members"][1]["column"] = group["members"][0]["column"]
    elif case == "member_used_in_two_groups":
        rule["groups"].append({**group, "canonical_name": "FLOAT_AGAIN"})
    elif case in ("merge_unitid", "merge_year"):
        rule["groups"][2]["members"][0]["column"] = "UNITID" if case == "merge_unitid" else "year"
    elif case == "existing_canonical":
        group["canonical_name"] = "FLOAT_OLD"
    elif case == "case_insensitive_existing_canonical":
        group["canonical_name"] = "keep_text"
    elif case == "duplicate_canonical":
        rule["groups"][1]["canonical_name"] = group["canonical_name"]
    elif case == "case_insensitive_duplicate_canonical":
        rule["groups"][1]["canonical_name"] = group["canonical_name"].lower()
    elif case == "empty_canonical":
        group["canonical_name"] = ""
    elif case == "overlapping_years":
        group["members"][0]["years"].append(2019)
    elif case == "empty_years":
        group["members"][0]["years"] = []
    elif case == "noninteger_year":
        group["members"][0]["years"][0] = "2021"
    write_json(policy, rule)
    assert_rejected_without_outputs(files)


def test_component_types_must_be_exact_without_implicit_numeric_widening(files):
    source, _, _ = files
    table = pq.read_table(source)
    index = table.schema.get_field_index("COUNT_NEW")
    pq.write_table(table.set_column(index, "COUNT_NEW", table["COUNT_NEW"].cast(pa.int32())), source)
    assert_rejected_without_outputs(files)


@pytest.mark.parametrize("column,index,value", [
    ("COUNT_OLD", 1, 0), ("TEXT_OLD", 1, ""), ("FLOAT_OLD", 1, float("nan")),
    ("COUNT_NEW", 0, -2),
])
def test_every_outside_scope_cell_must_be_physically_null(files, column, index, value):
    replace_value(files[0], column, index, value)
    assert_rejected_without_outputs(files)


@pytest.mark.parametrize("artifact", ["source", "output", "policy"])
def test_receipt_is_bound_to_each_current_artifact(files, artifact):
    source, output, policy = files
    consolidate(source, output, policy)
    if artifact == "source":
        replace_value(source, "KEEP_TEXT", 0, "changed source")
    elif artifact == "output":
        replace_value(output, "KEEP_TEXT", 0, "changed output")
    else:
        rule = json.loads(policy.read_text())
        rule["groups"][0]["rationale"] = "Unapproved replacement rationale"
        write_json(policy, rule)
    with pytest.raises(ValueError):
        verify_consolidation(source, output, receipt_path(output), policy)


@pytest.mark.parametrize("mutation", [
    "canonical_value", "canonical_null", "outside_group_year", "unmerged_value", "unmerged_missingness", "nan_to_null",
    "unitid", "year", "row_order", "missing_row", "extra_row", "column_order", "column_type",
])
def test_independent_reconstruction_rejects_changed_data_even_with_refreshed_checksum(files, mutation):
    source, output, policy = files
    consolidate(source, output, policy)
    if mutation == "canonical_value":
        replace_value(output, "COUNT", 4098, 999999)
    elif mutation == "canonical_null":
        replace_value(output, "COUNT", 13, 0)
    elif mutation == "outside_group_year":
        replace_value(output, "COUNT", 5, 0)
    elif mutation == "unmerged_value":
        replace_value(output, "KEEP_TEXT", 4097, "not the source")
    elif mutation == "unmerged_missingness":
        replace_value(output, "KEEP_TEXT", 1, None)
    elif mutation == "nan_to_null":
        replace_value(output, "FLOAT", 34, None)
    elif mutation == "unitid":
        replace_value(output, "UNITID", 4999, 999999)
    elif mutation == "year":
        replace_value(output, "year", 4098, 2024)
    else:
        table = pq.read_table(output)
        if mutation == "row_order":
            table = table.take(pa.array([1, 0, *range(2, table.num_rows)]))
        elif mutation == "missing_row":
            table = table.slice(1)
        elif mutation == "extra_row":
            table = pa.concat_tables([table, table.slice(0, 1)])
        elif mutation == "column_order":
            table = table.select(list(reversed(table.column_names)))
        elif mutation == "column_type":
            table = table.set_column(table.schema.get_field_index("COUNT"), "COUNT",
                                     table["COUNT"].cast(pa.float64()))
        pq.write_table(table, output, row_group_size=997)
    refresh_output_hash(output)
    with pytest.raises(ValueError):
        verify_consolidation(source, output, receipt_path(output), policy)


@pytest.mark.parametrize("mutation", ["remove", "change", "source_metadata"])
def test_output_metadata_is_verified_even_with_refreshed_checksum(files, mutation):
    source, output, policy = files
    consolidate(source, output, policy)
    table = pq.read_table(output)
    metadata = dict(table.schema.metadata)
    if mutation == "remove":
        metadata.pop(PROVENANCE_KEY)
    elif mutation == "change":
        provenance = json.loads(metadata[PROVENANCE_KEY])
        provenance["source_sha256"] = "0" * 64
        metadata[PROVENANCE_KEY] = json.dumps(provenance).encode()
    else:
        metadata[b"original"] = b"not the original metadata"
    pq.write_table(table.replace_schema_metadata(metadata), output)
    refresh_output_hash(output)
    with pytest.raises(ValueError):
        verify_consolidation(source, output, receipt_path(output), policy)


@pytest.mark.parametrize("mutation", ["status", "missing_parity", "changed_parity", "provenance"])
def test_receipt_requires_complete_successful_reconstruction_evidence(files, mutation):
    source, output, policy = files
    record = consolidate(source, output, policy)
    if mutation == "status":
        record["status"] = "fail"
    elif mutation == "missing_parity":
        record.pop("parity")
    elif mutation == "changed_parity":
        record["parity"]["unverified_claim"] = True
    else:
        record["column_consolidation"]["source_sha256"] = "0" * 64
    write_json(receipt_path(output), record)
    with pytest.raises(ValueError):
        verify_consolidation(source, output, receipt_path(output), policy)


@pytest.mark.parametrize("destination", ["source", "policy", "existing_output", "existing_receipt"])
def test_consolidation_cannot_overwrite_source_policy_or_existing_artifacts(files, destination):
    source, output, policy = files
    if destination == "source":
        output = source
    elif destination == "policy":
        output = policy
    elif destination == "existing_output":
        output.write_bytes(b"An existing output must survive unchanged")
    else:
        receipt_path(output).write_bytes(b"An existing receipt must survive unchanged")
    protected = [path for path in (source, policy, output, receipt_path(output)) if path.exists()]
    before = {path: sha256(path) for path in protected}
    with pytest.raises(ValueError):
        consolidate(source, output, policy)
    assert {path: sha256(path) for path in protected} == before


def test_dangling_output_symlink_is_not_replaced(files, tmp_path):
    source, output, policy = files
    target = tmp_path / "does-not-exist.parquet"
    output.symlink_to(target)
    with pytest.raises(ValueError):
        consolidate(source, output, policy)
    assert output.is_symlink()
    assert output.readlink() == target
    assert not target.exists()


@pytest.fixture
def identity_files(files):
    _, output, policy = files
    source = output.with_name("column_identities.parquet")
    destination = output.with_name("consolidated_identities.parquet")
    columns = ["COUNT_OLD", "COUNT_NEW", "FLOAT_OLD", "TEXT_NEW", "COUNT_OLD", "COUNT_OLD",
               "count_old", "UNREGISTERED", None, "COUNT_NEW", "COUNT_OLD"]
    years = [2019, 2024, 2020, 2021, 2024, 2023, 2019, 2024, 2024, None, 2024]
    count = len(columns)
    table = pa.table({
        "analysis_column": pa.array(columns, type=pa.string()),
        "year": pa.array(years, type=pa.int32()),
        "varname": pa.array(["source_" + str(i) for i in range(count)]),
        "source_file": pa.array([None if i % 3 == 0 else "SOURCE" + str(i % 2)
                                 for i in range(count)]),
        "access_table_name": pa.array(["table_" + str(i) for i in range(count)]),
        "source_varnumber": pa.array(range(count), type=pa.int64()),
        "unresolved_label": pa.array([None, "", "None", "café", *["record"] * (count - 4)]),
    }).replace_schema_metadata({b"original": b"Identity metadata must survive"})
    pq.write_table(table, source, row_group_size=3)
    return source, destination, policy


def test_identity_remap_preserves_all_rows_fields_and_unresolved_metadata(identity_files):
    source, output, policy = identity_files
    before = sha256(source), sha256(policy)
    record = consolidate_identities(source, output, policy)
    assert (sha256(source), sha256(policy)) == before
    original, actual = pq.read_table(source), pq.read_table(output)
    assert actual.column_names == [*original.column_names, "source_panel_column"]
    assert actual.num_rows == original.num_rows == 11
    assert actual.schema.metadata[b"original"] == original.schema.metadata[b"original"]
    assert actual["analysis_column"].to_pylist() == [
        "COUNT", "COUNT", "FLOAT", "TEXT", "COUNT_OLD", "COUNT_OLD", "count_old",
        "UNREGISTERED", None, "COUNT_NEW", "COUNT_OLD",
    ]
    assert actual["source_panel_column"].equals(original["analysis_column"])
    for name in original.column_names:
        assert actual.schema.field(name).equals(original.schema.field(name), check_metadata=True)
        if name != "analysis_column":
            assert actual[name].equals(original[name]), "Every original metadata field and row must survive"
    unresolved = {(row["column"], row["year"]): row["records"]
                  for row in record["out_of_scope_registered_identities"]}
    assert unresolved == {("COUNT_OLD", 2024): 2, ("COUNT_OLD", 2023): 1, ("COUNT_NEW", None): 1}
    saved = output.with_name(output.name + ".consolidation-identities.json")
    assert json.loads(saved.read_text()) == record


def test_identity_remap_rejects_an_existing_source_panel_column(identity_files):
    source, output, policy = identity_files
    table = pq.read_table(source)
    pq.write_table(table.append_column("source_panel_column", table["analysis_column"]), source)
    before = sha256(source)
    with pytest.raises(ValueError):
        consolidate_identities(source, output, policy)
    assert sha256(source) == before
    assert not output.exists()
    assert not output.with_name(output.name + ".consolidation-identities.json").exists()


@pytest.mark.parametrize("destination", ["source", "existing_output", "existing_receipt"])
def test_identity_remap_requires_new_output_artifacts(identity_files, destination):
    source, output, policy = identity_files
    if destination == "source":
        output = source
    elif destination == "existing_output":
        output.write_bytes(b"Existing identity output")
    else:
        output.with_name(output.name + ".consolidation-identities.json").write_bytes(b"Existing receipt")
    receipt = output.with_name(output.name + ".consolidation-identities.json")
    protected = [path for path in (source, policy, output, receipt) if path.exists()]
    before = {path: sha256(path) for path in protected}
    with pytest.raises(ValueError):
        consolidate_identities(source, output, policy)
    assert {path: sha256(path) for path in protected} == before


@pytest.mark.parametrize("missing", ["analysis_column", "year"])
def test_identity_remap_requires_source_column_and_year_fields(identity_files, missing):
    source, output, policy = identity_files
    table = pq.read_table(source)
    pq.write_table(table.drop([missing]), source)
    before = sha256(source)
    with pytest.raises(ValueError):
        consolidate_identities(source, output, policy)
    assert sha256(source) == before
    assert not output.exists()


def test_reviewed_registry_retains_unverified_graduation_cohort_conflict():
    from pathlib import Path
    policy = Path(__file__).parents[1] / 'contracts/harmonization/source-family-consolidation-v1.json'
    rule = json.loads(policy.read_text())
    assert 'GRRTAP' not in {group['base'] for group in rule['groups']}
    excluded = next(row for row in rule['excluded_groups'] if row['base'] == 'GRRTAP')
    assert len(excluded['columns']) == 3
    assert all(year in excluded['reason'] for year in ('2009', '2010', '2006', '2004'))
