"""Full-width native validation must cover all columns without exceeding Stata/BE."""
import json
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import pytest

from helpers import load_script_module

validator = load_script_module("full_stata_validation", "Scripts/QA_QC/28_validate_full_stata.py")


def test_native_string_expectations_are_bounded_and_preserve_unsafe_unicode_text():
    prefix = 'Quoted " value with `macro\' and $global 東京 🙂 '
    text = prefix + 'x' * (1687 - len(prefix.encode('utf-8')))
    assert len(text.encode('utf-8')) == 1687
    commands = validator.assert_native_string('st_varlabel("VALUE")', text)
    fragments = commands[1:-1]
    assert len(fragments) > 30
    assert all(1 <= command.count('uchar(') <= 48 for command in fragments)
    assert ''.join(chr(int(code)) for command in fragments for code in re.findall(r'uchar\((\d+)\)', command)) == text
    assert not any('$global' in command or '`macro' in command or '東京' in command for command in commands)
    assert commands[-1] == 'mata: assert(st_varlabel("VALUE") == __ipeds_expected)'
    assert validator.assert_native_string('st_varlabel("VALUE")', '') == [
        'mata: __ipeds_expected = ""', 'mata: assert(st_varlabel("VALUE") == __ipeds_expected)']


def test_full_width_chunks_fit_even_after_reference_merge():
    names = ["year", "UNITID"] + [f"v{index}" for index in range(2719)]
    chunks = validator.column_chunks(names, ["UNITID", "year"])
    assert len(chunks) == 4
    assert all(len(chunk) <= 900 and 2 * len(chunk) - 1 <= 2048 for chunk in chunks)
    assert [name for chunk in chunks for name in chunk if name not in {"UNITID", "year"}] == names[2:]


@pytest.mark.parametrize("token", ["01", "+1", "-0", "1.0", " 1", "", "2147483621"])
def test_reference_conversion_rejects_lossy_integer_strings(token):
    variable = {"name": "CONTROL", "stata_storage_conversion": "integer_code_strings_to_numeric"}
    with pytest.raises(ValueError, match="source code"):
        validator.reference_column(pa.array([token]), variable)


def test_explicit_string_encoding_preserves_distinct_tokens_and_missingness():
    variable = {"name": "TYPE", "stata_storage_conversion": "string_categories_to_numeric",
                "stata_source_code_map": [{"source_code": token, "export_code": index}
                                          for index, token in enumerate(["", "01", "1", "AL"], 1)]}
    converted = validator.reference_column(pa.array(["AL", "01", "1", "", None]), variable)
    assert converted.to_pylist() == [4, 2, 3, 1, None]
    with pytest.raises(ValueError, match="absent from the encoding"):
        validator.reference_column(pa.array(["UNKNOWN"]), variable)
    variable["stata_source_code_map"][1]["export_code"] = 1
    with pytest.raises(ValueError, match="reversible"):
        validator.reference_column(pa.array(["01"]), variable)


def fixture(tmp_path):
    source, data, metadata_path, binary = [tmp_path / name for name in ("panel.parquet", "panel.dta", "panel.dta.metadata.json", "stata")]
    table = pa.table({"year": [2023] * 3, "UNITID": [1, 2, 3], "CONTROL": ["1", "-1", None], "TEXT": [None, "", 'a"b']})
    pq.write_table(table, source)
    data.write_bytes(b"native file mocked in software tests")
    binary.touch()
    variables = [{"name": name, "export_name": name, "export_label": name, "stata_value_labels": {},
                  "stata_storage_conversion": "none"} for name in table.column_names]
    variables[2].update(stata_storage_conversion="integer_code_strings_to_numeric", stata_value_labels={1: "Public", -1: "Not reported"})
    metadata = {"data_sha256": validator.sha256_file(data), "variables": variables, "row_count": 3,
                "column_count": 4, "panel_keys": ["UNITID", "year"], "readiness_status": "incomplete"}
    metadata_path.write_text(json.dumps(metadata))
    return source, data, metadata_path, binary, variables


def test_reference_tracks_conversion_and_string_missing_limit(tmp_path):
    source, _, _, _, variables = fixture(tmp_path)
    output = tmp_path / "reference.csv"
    detail = validator.write_reference(pq.ParquetFile(source), variables, ["UNITID", "year"], output, {v["export_name"] for v in variables})
    actual = pcsv.read_csv(output)
    assert actual["__ipeds_ref_0002"].to_pylist() == [1, -1, None]
    assert detail["source_null_counts"]["CONTROL"] == 1
    assert detail["string_nulls_represented_as_empty"] == {"TEXT": 1}
    assert detail["source_literal_empty_string_counts"] == {"TEXT": 1}


def test_native_commands_read_subset_from_the_full_file(tmp_path):
    source, data, _, _, variables = fixture(tmp_path)
    reference = tmp_path / "reference.csv"
    detail = validator.write_reference(pq.ParquetFile(source), variables, ["UNITID", "year"], reference, set())
    commands = validator.native_commands(data, reference, variables, ["UNITID", "year"], detail, 2721)
    assert f'use year UNITID CONTROL TEXT using "{data}", clear' in commands
    assert 'mata: assert(st_numscalar("r(k)") == 2721)' in commands
    assert "assert missing(CONTROL) == missing(__ipeds_ref_0002)" in commands
    assert any("st_vlload" in command for command in commands)
    assert 'mata: assert(st_varvaluelabel("TEXT") == "")' in commands


def test_validation_receipt_covers_all_chunks_and_does_not_certify_incomplete_metadata(tmp_path, monkeypatch):
    source, data, metadata, binary, _ = fixture(tmp_path)
    def fake_run(command, log, *, cwd, native_output):
        assert native_output
        (cwd / "native_verify.log").write_text('Stata license: omitted\n. do native_verify.do\n' + validator.PASS_MARKER + '\n')
        log.write_text("Mock native process for unit test only\n")
    monkeypatch.setattr(validator, "run_checked", fake_run)
    result = validator.validate(data, metadata, source, tmp_path / "proof", binary, chunk_size=3)
    assert result["status"] == "pass"
    assert result["source_readiness_status"] == "incomplete"
    assert result["variable_labels_checked"] == 4
    assert result["value_labels_checked"] == 2
    assert len(result["chunks"]) == 2
    assert not list((tmp_path / "proof").rglob("reference.csv"))
    assert all("Stata license:" not in log.read_text() for log in (tmp_path / "proof").rglob("native_verify.log"))


def test_failed_native_marker_keeps_failure_receipt(tmp_path, monkeypatch):
    source, data, metadata, binary, _ = fixture(tmp_path)
    def fake_run(command, log, *, cwd, native_output):
        (cwd / "native_verify.log").write_text('Native banner\n. do native_verify.do\n. display "' + validator.PASS_MARKER + '"\nr(9);\n')
    monkeypatch.setattr(validator, "run_checked", fake_run)
    output = tmp_path / "failed"
    with pytest.raises(ValueError, match="passing receipt"):
        validator.validate(data, metadata, source, output, binary)
    assert json.loads((output / "validation.json").read_text())["status"] == "fail"
