"""Analyst defaults stay on the final release while drafts live under Work."""
from pathlib import Path
import json
import re

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from export_metadata import discover_export_metadata
from helpers import load_script_module, run_script


@pytest.fixture
def organized_root(tmp_path):
    root = tmp_path / 'data'
    release = root / 'Releases/2023-sfa-v1'
    panel = release / 'Panels/v2/panel_clean_prch_2004_2023.parquet'
    dictionary = release / 'Dictionary/v2/dictionary_lake.parquet'
    codes = dictionary.with_name('dictionary_codes.parquet')
    lineage = release / 'Checks/v2/wide_qc/qc_value_lineage.parquet'
    for path in (panel, dictionary, lineage):
        path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({'year': [2023], 'UNITID': [100654], 'VALUE': [123.5]}), panel)
    definition = {'year': 2023, 'source_file': 'HD', 'access_table_name': 'HD2023', 'varname': 'VALUE', 'varnumber': '1',
                  'varTitle': 'Verified current value', 'longDescription': 'Verified release definition.', 'DataType': 'N', 'format': 'Cont'}
    pq.write_table(pa.Table.from_pylist([definition]), dictionary)
    pq.write_table(pa.table({name: pa.array([], typ) for name, typ in [('year', pa.int64()), ('source_file', pa.string()),
                    ('varname', pa.string()), ('codevalue', pa.string()), ('valuelabel', pa.string())]}), codes)
    pq.write_table(pa.Table.from_pylist([{'analysis_column': 'VALUE', 'year': 2023, 'varname': 'VALUE',
        'source_file': 'HD', 'access_table_name': 'HD2023', 'source_varnumber': '1'}]), lineage)
    (root / 'layout.json').write_text(json.dumps({'schema_version': 1, 'current_release': '2023-sfa-v1'}))
    final = root / 'Final'
    final.mkdir()
    (final / panel.name).symlink_to(Path('../Releases/2023-sfa-v1/Panels/v2') / panel.name)
    (final / 'Metadata').symlink_to('../Releases/2023-sfa-v1/Dictionary/v2', target_is_directory=True)
    (final / 'value_lineage.parquet').symlink_to('../Releases/2023-sfa-v1/Checks/v2/wide_qc/qc_value_lineage.parquet')
    for directory in ('Work/Dictionary/v2', 'Dictionary/v2', 'Dictionary'):
        stale = root / directory / 'dictionary_lake.parquet'
        stale.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist([{**definition, 'varTitle': 'Wrong stale metadata'}]), stale)
    stale_panel = root / 'Work/Panels/v2' / panel.name
    stale_panel.parent.mkdir(parents=True)
    pq.write_table(pa.table({'year': [2023], 'UNITID': [999999], 'VALUE': [999.0]}), stale_panel)
    return root, panel, dictionary, lineage


def test_final_symlink_discovery_is_bound_to_physical_release(organized_root):
    root, panel, dictionary, lineage = organized_root
    final_panel = root / 'Final' / panel.name
    # A stale convenience metadata link cannot change the panel's own release.
    (root / 'Final/Metadata').unlink()
    (root / 'Final/Metadata').symlink_to('../Work/Dictionary/v2', target_is_directory=True)
    assert discover_export_metadata(None, 'Dictionary/dictionary_lake.parquet', final_panel, root) == dictionary
    assert discover_export_metadata(None, 'Checks/wide_qc/qc_column_lineage.csv', final_panel, root) == lineage
    dictionary.unlink()
    assert discover_export_metadata(None, 'Dictionary/dictionary_lake.parquet', final_panel, root) is None


@pytest.mark.parametrize('script', ['08_build_custom_panel.py', '09_build_panel_dictionary.py'])
def test_export_entrypoint_defaults_use_final_and_matching_metadata(organized_root, script):
    root, panel, dictionary, lineage = organized_root
    output = root / 'Work' / (script[:2] + '.csv')
    extra = ['--vars', 'VALUE'] if script.startswith('08') else []
    result = run_script('Scripts/' + script, '--root', root, '--output', output, '--require-ready', *extra)
    assert result.returncode == 0, result.stdout
    metadata = json.loads(Path(str(output) + '.metadata.json').read_text())
    assert metadata['source_panel'] == str(panel)
    assert next(row for row in metadata['variables'] if row['name'] == 'VALUE')['label'] == 'Verified current value'
    if script.startswith('08'):
        assert (root / 'Work/Checks/logs/08_build_custom_panel.log').is_file()
        assert not (root / 'Checks').exists()


@pytest.mark.parametrize('script', ['08_build_custom_panel.py', '09_build_panel_dictionary.py', '10_build_variable_browser.py'])
def test_missing_final_never_uses_stale_work_panel(organized_root, script):
    root, panel, _, _ = organized_root
    (root / 'Final' / panel.name).unlink()
    extra = ['--vars', 'VALUE'] if script.startswith('08') else []
    output = root / 'Work/should_not_exist.csv'
    result = run_script('Scripts/' + script, '--root', root, '--output', output, *extra)
    assert result.returncode != 0
    assert 'Work outputs are drafts' in result.stdout
    assert not output.exists()


def test_browser_defaults_write_work_and_use_final_metadata(organized_root):
    root, _, _, _ = organized_root
    result = run_script('Scripts/10_build_variable_browser.py', '--root', root)
    assert result.returncode == 0, result.stdout
    output = root / 'Work/Customize_Panel/variable_browser.html'
    text = output.read_text()
    payload = json.loads(re.search(r'<script id="variable-browser-data" type="application/json">(.*?)</script>', text, flags=re.S).group(1))
    assert any(row['varTitle'] == 'Verified current value' for row in payload['variables'])
    assert not (root / 'Final/variable_browser.html').exists()


def test_sql_clean_view_defaults_to_final_and_release_metadata(organized_root):
    root, panel, dictionary, _ = organized_root
    runner = load_script_module('organized_query_runner', 'Scripts/run_saved_query.py')
    with duckdb.connect() as con:
        sources = runner.bootstrap_artifact_views(con, root=root, years_spec='2004:2023', duckdb_path=root/'Work/build/missing.duckdb')
        assert con.execute('SELECT UNITID, VALUE FROM inspect.panel_clean').fetchall() == [(100654, 123.5)]
        assert sources['clean_path'] == str(root / 'Final' / panel.name)
        assert sources['dictionary_lake_path'] == str(dictionary)
        assert con.execute('SELECT varTitle FROM inspect.dictionary_lake').fetchone()[0] == 'Verified current value'
    (root / 'Final/Metadata').unlink()
    (root / 'Final/Metadata').symlink_to('../Work/Dictionary/v2', target_is_directory=True)
    dictionary.unlink()
    with duckdb.connect() as con:
        sources = runner.bootstrap_artifact_views(con, root=root, years_spec='2004:2023', duckdb_path=root/'Work/build/missing.duckdb')
        assert sources['dictionary_lake_path'] == str(dictionary)
        assert con.execute('SELECT COUNT(*) FROM inspect.dictionary_lake').fetchone()[0] == 0
    (root / 'Final' / panel.name).unlink()
    with duckdb.connect() as con, pytest.raises(FileNotFoundError, match='Work outputs are drafts'):
        runner.bootstrap_artifact_views(con, root=root, years_spec='2004:2023', duckdb_path=root/'Work/build/missing.duckdb')


def test_plain_final_panel_cannot_borrow_unbound_release_metadata(organized_root):
    root, panel, dictionary, _ = organized_root
    final_panel = root / 'Final' / panel.name
    final_panel.unlink()
    pq.write_table(pa.table({'year': [2023], 'UNITID': [111111], 'VALUE': [5.0]}), final_panel)
    (root / 'Final/Metadata').unlink()
    (root / 'Final/value_lineage.parquet').unlink()
    (root / 'Work/Panels/v2' / panel.name).unlink()
    assert dictionary.is_file()  # A release exists, but this plain panel is not bound to it.
    runner = load_script_module('organized_query_runner_plain_final', 'Scripts/run_saved_query.py')
    with duckdb.connect() as con:
        sources = runner.bootstrap_artifact_views(con, root=root, years_spec='2004:2023', duckdb_path=root/'Work/build/missing.duckdb')
        assert sources['panel_layout'] == 'v2'
        assert con.execute('SELECT UNITID, VALUE FROM inspect.panel_clean').fetchall() == [(111111, 5.0)]
        assert sources['dictionary_lake_path'] == str(root / 'Final/Metadata/dictionary_lake.parquet')
        assert sources['dictionary_codes_path'] == str(root / 'Final/Metadata/dictionary_codes.parquet')
        assert con.execute('SELECT COUNT(*) FROM inspect.dictionary_lake').fetchone()[0] == 0
        assert con.execute('SELECT COUNT(*) FROM inspect.dictionary_codes').fetchone()[0] == 0
