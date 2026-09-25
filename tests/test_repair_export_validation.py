"""A publication receipt must bind checked files, not a stale native marker."""
from pathlib import Path
import json
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from helpers import load_script_module

validator = load_script_module('repair_export_validation', 'Scripts/QA_QC/27_validate_repair_exports.py')


def test_canonical_mapping_is_projected_deduplicated_and_year_scoped(tmp_path, monkeypatch):
    path = tmp_path / 'lineage.parquet'
    pq.write_table(pa.Table.from_pylist([
        {'analysis_column': 'A', 'varname': 'VALUE', 'source_varnumber': '001', 'year': 2023, 'UNITID': i}
        for i in range(20)
    ] + [{'analysis_column': 'OLD', 'varname': 'VALUE', 'source_varnumber': '001', 'year': 2022, 'UNITID': 1}]), path)
    monkeypatch.setattr(validator.pq, 'read_table', lambda *a, **k: pytest.fail('Canonical lineage must not be materialized'))
    registry = {'corrections': [{'year': 2023, 'varname': 'VALUE', 'varnumber': '1.0'}]}
    assert validator.affected_columns(pa.schema([('A', pa.float64()), ('OLD', pa.float64())]), path, registry, 2023) == ['A']


def test_native_marker_requires_executed_output_and_banner_is_removed(tmp_path, monkeypatch):
    binary = tmp_path / 'fake_stata'
    binary.touch()
    metadata = {'variables': [{'name': name, 'export_name': name, 'export_label': name, 'stata_value_labels': {}}
                              for name in ['year', 'UNITID']]}
    monkeypatch.setattr(validator, 'read_metadata', lambda path: metadata)
    log = tmp_path / 'native_verify.log'
    def fake_run(*args, **kwargs):
        log.write_text('Stata license: fixture only\nSerial number: fixture only\n. do fixture.do\n. display "NATIVE_STATA_REPAIR_EXPORT_PASS"\nr(9);\n')
    monkeypatch.setattr(validator, 'run_checked', fake_run)
    with pytest.raises(ValueError, match='did not produce a passing receipt'):
        validator.verify_native_stata(tmp_path / 'data.dta', pa.table({'year': [2023], 'UNITID': [1]}), binary, tmp_path)
    assert 'Stata license:' not in log.read_text()
    assert 'Serial number:' not in log.read_text()
    assert 'r(9)' in log.read_text()


def test_native_process_capture_omits_license_banner(tmp_path, monkeypatch):
    monkeypatch.setattr(validator.subprocess, 'run', lambda *a, **k: subprocess.CompletedProcess([], 1, 'Stata license: fixture only\nSerial number: fixture only\n', ''))
    log = tmp_path / 'native_process.txt'
    with pytest.raises(RuntimeError, match='status 1') as error:
        validator.run_checked(['fake'], log, native_output=True)
    assert 'fixture only' not in log.read_text()
    assert 'fixture only' not in str(error.value)


def test_native_batch_log_pwd_matches_requested_working_directory(tmp_path, monkeypatch):
    monkeypatch.setenv('PWD', '/wrong/inherited/shell/directory')
    captured = {}
    def fake_process(*args, **kwargs):
        captured.update(kwargs)
        return subprocess.CompletedProcess([], 0, '', '')
    monkeypatch.setattr(validator.subprocess, 'run', fake_process)
    validator.run_checked(['fake_stata'], tmp_path / 'process.txt', cwd=tmp_path, native_output=True)
    assert captured['cwd'] == tmp_path
    assert captured['env']['PWD'] == str(tmp_path.resolve())


def test_end_to_end_receipt_binds_all_formats_and_companions(tmp_path, monkeypatch):
    panel, dictionary, codes, lineage, mapping = [tmp_path / name for name in
        ('panel.parquet', 'dictionary_lake.parquet', 'dictionary_codes.parquet', 'lineage.csv', 'mapping.csv')]
    pq.write_table(pa.table({'year': [2023]*3, 'UNITID': [1, 2, 3], 'AMOUNT': [123.5, -1.0, None], 'TYPE': [1, 2, None]}), panel)
    pq.write_table(pa.Table.from_pylist([
        {'year': 2023, 'source_file': 'SFA_P', 'access_table_name': 'SFA_P1', 'varname': name, 'varnumber': number,
         'varTitle': title, 'longDescription': title, 'DataType': 'N', 'format': fmt}
        for name, number, title, fmt in [('AMOUNT', '1', 'Amount', 'Cont'), ('TYPE', '2', 'Category', 'Disc')]
    ]), dictionary)
    pq.write_table(pa.Table.from_pylist([
        {'year': 2023, 'source_file': 'SFA_P', 'access_table_name': 'SFA_P1', 'varname': name, 'varnumber': number,
         'codevalue': code, 'valuelabel': label}
        for name, number, code, label in [('AMOUNT', '1', '-1', 'Not reported'), ('TYPE', '2', '1', 'Public'), ('TYPE', '2', '2', 'Private')]
    ]), codes)
    lineage.write_text('output_column,source_varnames,source_files,access_table_name,year\nAMOUNT,AMOUNT,SFA_P,SFA_P1,2023\nTYPE,TYPE,SFA_P,SFA_P1,2023\n')
    mapping.write_text('analysis_column,varname,source_varnumber,year\nAMOUNT,AMOUNT,1,2023\nTYPE,TYPE,2,2023\n')
    registry = tmp_path / 'registry.json'
    registry.write_text(json.dumps({'corrections': [{'year': 2023, 'varname': name, 'varnumber': number} for name, number in [('AMOUNT', '1'), ('TYPE', '2')]]}))
    monkeypatch.setattr(validator, 'load_registry', lambda path: json.loads(path.read_text()))
    def fake_native(path, expected, binary, proof):
        assert expected.num_rows == 3 and expected.num_columns == 4
        (proof / 'native_fixture.txt').write_text('Native process mocked for this software integration test.\n')
        return {'status': 'pass', 'fixture_mock': True}
    monkeypatch.setattr(validator, 'verify_native_stata', fake_native)
    output = tmp_path / 'export'
    monkeypatch.setattr(sys, 'argv', ['27_validate_repair_exports.py', '--input', str(panel), '--dictionary', str(dictionary),
        '--codes', str(codes), '--lineage', str(lineage), '--variable-map', str(mapping), '--registry', str(registry), '--output-dir', str(output)])
    validator.main()
    receipt = json.loads((output / 'validation.json').read_text())
    assert receipt['status'] == 'pass'
    assert receipt['corrected_variable_count'] == 2
    assert set(receipt['formats']) == {'parquet', 'csv', 'dta', 'xlsx'}
    assert receipt['inputs']['panel']['sha256'] == validator.sha256_file(panel)
    actual_files = {str(path.resolve()) for path in output.rglob('*') if path.is_file() and path.name != 'validation.json'}
    assert {record['path'] for record in receipt['artifacts']} == actual_files
    for record in receipt['artifacts']:
        assert record['sha256'] == validator.sha256_file(Path(record['path']))
    assert len([name for name in actual_files if name.endswith('.metadata.json')]) == 5
