from __future__ import annotations

import json
import zipfile

import pandas as pd
import pytest

from prepare_2024_sources import (
    canonical, compare_sources, dictionary_info, extract_zip, indexed, read_csv,
    sha256, verify_snapshot,
)


def frames():
    access = pd.DataFrame({"UNITID": ["1", "2"], "OPEID": ["00100200", "00012300"], "PELL": ["0", ""], "NAME": ["None", "College"]})
    original = access.copy()
    original["PELL"] = ["0.0", ""]
    revised = original.copy()
    revised.loc[1, "PELL"] = "30"
    revised["XPELL"] = ["Z", "R"]
    return access, original, revised


def test_full_coverage_and_exact_revision_count():
    result = compare_sources(*frames(), ["UNITID"], {"UNITID", "PELL"})
    assert result["access_vs_original_changed_cells"] == 0
    assert result["access_vs_effective_changes"] == {"PELL": 1}
    assert result["extra_columns"] == ["XPELL"]


def test_no_identifier_numeric_coercion():
    a, o, r = frames()
    r.loc[0, "OPEID"] = "100200"
    result = compare_sources(a, o, r, ["UNITID"], {"UNITID", "PELL"})
    assert result["access_vs_effective_changes"]["OPEID"] == 1
    assert canonical("01.0101", False) != canonical("1.0101", False)


def test_read_csv_preserves_leading_zeros_and_literal_none(tmp_path):
    path = tmp_path / "source.csv"
    path.write_text(' unitid ,opeid,name,amount\n1,00100200,None,\n')
    result = read_csv(path)
    assert result.iloc[0].to_dict() == {"UNITID": "1", "OPEID": "00100200", "NAME": "None", "AMOUNT": ""}


def test_missing_original_column_is_rejected():
    a, o, r = frames()
    with pytest.raises(ValueError, match="omits Access columns"):
        compare_sources(a, o, r.drop(columns="PELL"), ["UNITID"], {"UNITID", "PELL"})


def test_changed_row_grain_is_rejected():
    a, o, r = frames()
    r.loc[1, "UNITID"] = "3"
    with pytest.raises(ValueError, match="key sets differ"):
        compare_sources(a, o, r, ["UNITID"], {"UNITID", "PELL"})


def test_duplicate_and_missing_keys_rejected():
    a, _, _ = frames()
    with pytest.raises(ValueError, match="Duplicate source grain"):
        indexed(pd.concat([a, a]), ["UNITID"], {"UNITID"})
    a.loc[0, "UNITID"] = ""
    with pytest.raises(ValueError, match="Missing grain key values"):
        indexed(a, ["UNITID"], {"UNITID"})


def test_dimension_keys_preserve_cip_leading_zeros():
    a = pd.DataFrame({"UNITID": ["1", "1"], "CIPCODE": ["01.0101", "1.0101"], "N": ["2", "3"]})
    assert len(indexed(a, ["UNITID", "CIPCODE"], {"UNITID", "N"})) == 2


def test_flag_associations_are_exact_and_unambiguous():
    rows = {"Varlist": [{"varName": "UPGRNTN", "DataType": "N", "imputationvar": "XUPGRNTN"}], "Imputation values": [{"CodeValue": "R", "ValueLabel": "Reported"}]}
    numeric, flags, labels = dictionary_info(rows)
    assert numeric == {"UPGRNTN"}
    assert flags == {"XUPGRNTN": "UPGRNTN"}
    assert labels == {"R": "Reported"}
    rows["Varlist"].append({"varName": "OTHER", "DataType": "N", "imputationvar": "XUPGRNTN"})
    with pytest.raises(ValueError, match="Ambiguous imputation association"):
        dictionary_info(rows)


def test_unsafe_archive_member_rejected(tmp_path):
    path = tmp_path / "bad.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("../escape.csv", "bad")
    with pytest.raises(ValueError, match="Unsafe archive member"):
        extract_zip(path, tmp_path / "out")
    assert not (tmp_path / "escape.csv").exists()


def test_existing_snapshot_detects_changed_source(tmp_path):
    path = tmp_path / "source.csv"
    path.write_text("unitid\n1\n")
    manifest = {"artifacts": [{"path": path.name, "sha256": sha256(path)}]}
    (tmp_path / "source_manifest.json").write_text(json.dumps(manifest))
    assert verify_snapshot(tmp_path) == manifest
    path.write_text("unitid\n2\n")
    with pytest.raises(ValueError, match="changed or missing"):
        verify_snapshot(tmp_path)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "not numeric"])
def test_invalid_numeric_values_fail(value):
    with pytest.raises(ValueError):
        canonical(value, True)


def test_reviewed_revised_row_addition_is_counted_separately():
    a, o, r = frames()
    r = pd.concat([r, pd.DataFrame({"UNITID": ["3"], "OPEID": ["00045600"], "PELL": ["8"], "NAME": ["New"], "XPELL": ["R"]})], ignore_index=True)
    result = compare_sources(a, o, r, ["UNITID"], {"UNITID", "PELL"}, (1, 0))
    assert (result["original_rows"], result["effective_rows"], result["common_rows"]) == (2, 3, 2)
    assert result["added_rows"] == 1
    assert result["access_vs_effective_changes"] == {"PELL": 1}
    with pytest.raises(ValueError, match="key sets differ"):
        compare_sources(a, o, r, ["UNITID"], {"UNITID", "PELL"}, (2, 0))


def test_original_csv_row_difference_cannot_hide_as_a_revision():
    a, o, r = frames()
    with pytest.raises(ValueError, match="Access and original CSV key sets differ"):
        compare_sources(a, o.iloc[:1], r, ["UNITID"], {"UNITID", "PELL"}, (1, 0))


def test_import_script_supplement_uses_only_explicit_status_block():
    from prepare_2024_sources import stata_item_status_labels
    text = '*S An unrelated comment\n*The following are the possible values for the item imputation field variables\n*R Reported\n*S Suppressed\ntab x\n*X Unrelated later comment\n'
    assert stata_item_status_labels(text) == {"R": "Reported", "S": "Suppressed"}
    assert stata_item_status_labels('*S Suppressed\n') == {}


def test_matching_blank_strings_are_not_false_revisions():
    a, o, r = frames()
    for d in [a, o, r]:
        d.loc[1, "OPEID"] = ""
        d["ALL_BLANK_TEXT"] = ""
        d["ALL_BLANK_NUMBER"] = ""
    result = compare_sources(a, o, r, ["UNITID"], {"UNITID", "PELL", "ALL_BLANK_NUMBER"})
    assert result["access_vs_original_changed_cells"] == 0
    assert result["access_vs_effective_changes"] == {"PELL": 1}
