"""An internally consistent re-export must not renumber historical Stata categories."""
import copy
import json

import pytest

from helpers import load_script_module

runner = load_script_module("extension_stata_history", "Scripts/build_2024_extension.py")


@pytest.fixture
def encodings(tmp_path):
    root, package = tmp_path / "root", tmp_path / "package"
    old = {"format": "dta", "variables": [
        {"name": "FISCAL_END", "export_name": "FISCAL_END", "stata_storage_conversion": "string_categories_to_numeric",
         "stata_source_code_map": [{"source_code": "Jun-30", "export_code": 1}, {"source_code": "Sep-30", "export_code": 2}]},
        {"name": "CONTROL", "export_name": "CONTROL", "stata_storage_conversion": "integer_code_strings_to_numeric",
         "stata_source_code_map": [{"source_code": "1", "export_code": 1}]},
        {"name": "UNITID", "export_name": "UNITID", "stata_storage_conversion": "none"},
    ]}
    ref = root / f"Final/{runner.BASE}.dta.metadata.json"
    runner.write_json(ref, old)
    current = copy.deepcopy(old)
    current["variables"][0]["stata_source_code_map"].append({"source_code": "Apr-30", "export_code": 3})
    current["stata_encoding_reference"] = {"sha256": runner.sha256(ref)}
    output = package / f"{runner.PANEL}.dta.metadata.json"
    runner.write_json(output, current)
    return root, package, output, current


def test_published_encoding_is_independently_preserved(encodings):
    root, package, _, _ = encodings
    proof = runner.verify_stata_history(package, root)
    assert proof["historical_variables_checked"] == 3
    assert proof["historical_category_numbers_checked"] == 3
    assert proof["all_historical_aliases_conversions_and_category_numbers_equal"]


@pytest.mark.parametrize("failure", ["reference", "missing_variable", "alias", "conversion", "renumber",
                                     "integer_renumber", "missing_token", "duplicate_token", "duplicate_code", "new_code"])
def test_internally_consistent_new_metadata_cannot_change_history(encodings, failure):
    root, package, output, current = encodings
    variable = current["variables"][0]
    codes = variable["stata_source_code_map"]
    if failure == "reference":
        current.pop("stata_encoding_reference")
    elif failure == "missing_variable":
        current["variables"].pop()
    elif failure == "alias":
        variable["export_name"] = "RENAMED"
    elif failure == "conversion":
        variable["stata_storage_conversion"] = "none"
    elif failure == "renumber":
        codes[0]["export_code"], codes[2]["export_code"] = 3, 1
    elif failure == "integer_renumber":
        current["variables"][1]["stata_source_code_map"][0]["export_code"] = 2
    elif failure == "missing_token":
        codes.pop(0)
    elif failure == "duplicate_token":
        codes.append(dict(codes[0]))
    elif failure == "duplicate_code":
        codes[2]["export_code"] = 1
    else:
        codes[2]["export_code"] = 0
    runner.write_json(output, current)
    with pytest.raises(ValueError):
        runner.verify_stata_history(package, root)


def test_documented_consolidation_allows_only_canonical_alias_change(encodings):
    root, package, output, current = encodings
    policy = package / 'Reproduction/extension-code/contracts/harmonization/source-family-consolidation-v1.json'
    runner.write_json(policy, {'groups': [{'canonical_name': 'CONTROL_CANONICAL',
                                         'members': [{'column': 'CONTROL'}]}]})
    variable = current['variables'][1]
    variable['name'] = variable['export_name'] = 'CONTROL_CANONICAL'
    runner.write_json(output, current)
    proof = runner.verify_stata_history(package, root)
    assert proof['consolidated_aliases'] == {'CONTROL': 'CONTROL_CANONICAL'}
    assert proof['all_historical_value_encodings_equal']
    assert not proof['all_historical_aliases_conversions_and_category_numbers_equal']
    variable['stata_source_code_map'][0]['export_code'] = 8
    runner.write_json(output, current)
    with pytest.raises(ValueError, match='category numbers changed'):
        runner.verify_stata_history(package, root)
