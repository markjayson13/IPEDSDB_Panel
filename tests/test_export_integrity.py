"""Observation domains distinguish numeric measurements from coded categories."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from export_integrity import apply_observation_validation, require_export_ready, validate_observed_codes
from export_metadata import build_export_metadata
from helpers import run_script


def metadata(formats: dict[int, str], codes: dict[int, list[str]]) -> dict:
    return {"years": sorted(formats), "variables": [{
        "name": "VALUE",
        "source_metadata": [{"year": year, "DataType": "N", "format": domain} for year, domain in formats.items()],
        "resolved_value_label_records": [{"year": year, "codevalue": code, "valuelabel": "Source code"}
                                         for year, values in codes.items() for code in values],
    }]}


def test_continuous_numeric_special_codes_do_not_close_the_measurement_domain() -> None:
    table = pa.table({"year": [2023] * 5, "VALUE": [125.5, -42.0, -1.0, 0.0, None]})
    result = validate_observed_codes(ds.dataset(table), metadata({2023: "Cont"}, {2023: ["-1", "-2"]}))
    assert result["issues"] == []
    assert result["unknown_code_count"] == 0
    assert result["special_code_count"] == 1
    assert result["continuous_variables"] == ["VALUE"]
    assert result["categorical_variables"] == []
    assert result["code_domain_by_year"] == {"VALUE": {"2023": "continuous"}}
    assert table["VALUE"].to_pylist() == [125.5, -42.0, -1.0, 0.0, None]


def test_discrete_numeric_categories_still_reject_unknown_values() -> None:
    table = pa.table({"year": [2023] * 3, "VALUE": [1, 2, 99]})
    result = validate_observed_codes(ds.dataset(table), metadata({2023: "Disc"}, {2023: ["1", "2"]}))
    assert result["unknown_code_count"] == 1
    assert result["categorical_variables"] == ["VALUE"]
    assert result["issues"][0]["code"] == "unknown_observed_code"
    assert result["issues"][0]["examples"] == [{"year": 2023, "value": "99"}]


def test_continuous_and_categorical_declarations_are_scoped_by_year() -> None:
    table = pa.table({"year": [2022, 2023], "VALUE": [99, 99]})
    result = validate_observed_codes(ds.dataset(table), metadata({2022: "Cont", 2023: "Disc"}, {2022: ["-1"], 2023: ["1"]}))
    assert result["unknown_code_count"] == 1
    assert result["issues"][0]["examples"] == [{"year": 2023, "value": "99"}]


def test_conflicting_source_domains_are_explicit() -> None:
    table = pa.table({"year": [2023], "VALUE": [100]})
    definitions = metadata({2023: "Cont"}, {2023: ["-1"]})
    definitions["variables"][0]["source_metadata"].append({"year": 2023, "DataType": "N", "format": "Disc"})
    result = validate_observed_codes(ds.dataset(table), definitions)
    assert result["issues"][0]["code"] == "code_domain_ambiguous"
    assert result["code_domain_by_year"]["VALUE"]["2023"] == "ambiguous"


def test_strict_stata_export_preserves_continuous_values_and_special_labels(tmp_path: Path) -> None:
    panel = tmp_path / "panel.parquet"
    dictionary = tmp_path / "dictionary.parquet"
    codes = tmp_path / "codes.parquet"
    output = tmp_path / "extract.dta"
    values = [125.5, -42.0, -1.0, 0.0]
    pq.write_table(pa.table({"year": [2023] * 4, "UNITID": [1, 2, 3, 4], "VALUE": values}), panel)
    pq.write_table(pa.Table.from_pylist([{
        "year": 2023, "source_file": "SFA", "varnumber": "12", "varname": "VALUE",
        "varTitle": "Continuous measure", "longDescription": "Source-defined continuous numeric measurement.",
        "DataType": "N", "format": "Cont",
    }]), dictionary)
    pq.write_table(pa.Table.from_pylist([
        {"year": 2023, "source_file": "SFA", "varnumber": "12", "varname": "VALUE",
         "codevalue": code, "valuelabel": label}
        for code, label in [("-1", "Not reported"), ("-2", "Not applicable")]
    ]), codes)
    result = run_script("Scripts/08_build_custom_panel.py", "--input", panel, "--output", output,
                        "--vars", "VALUE", "--dictionary", dictionary, "--codes", codes,
                        "--require-metadata", "--log-file", "", env={"IPEDSDB_ROOT": str(tmp_path)})
    assert result.returncode == 0, result.stdout
    with pd.read_stata(output, iterator=True, convert_categoricals=False) as reader:
        assert reader.value_labels()["VALUE"] == {-1: "Not reported", -2: "Not applicable"}
        assert reader.read()["VALUE"].tolist() == values
    exported = json.loads(Path(str(output) + ".metadata.json").read_text())
    assert exported["readiness_status"] == "complete"
    assert exported["observation_validation"]["checked_continuous_variables"] == ["VALUE"]
    assert exported["observation_validation"]["special_code_count"] == 1


def test_stage08_auto_discovers_versioned_dictionary_and_canonical_lineage(tmp_path: Path) -> None:
    root = tmp_path / "release"
    panel = root / "Panels/v2/panel_clean_prch_2004_2023.parquet"
    dictionary = root / "Dictionary/v2/dictionary_lake.parquet"
    codes = dictionary.with_name("dictionary_codes.parquet")
    lineage = root / "Panels/v2/wide_release/current/qc/wide_qc/qc_value_lineage.parquet"
    for path in (panel, dictionary, lineage):
        path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"year": [2023], "UNITID": [1001], "CONTROL_ALIAS": [1]}), panel)
    pq.write_table(pa.Table.from_pylist([{
        "year": 2023, "source_file": "HD", "access_table_name": "HD2023", "varnumber": "12", "varname": "CONTROL",
        "varTitle": "Institution control", "longDescription": "Control classification.", "DataType": "N", "format": "Disc",
    }]), dictionary)
    pq.write_table(pa.Table.from_pylist([{
        "year": 2023, "source_file": "HD", "access_table_name": "HD2023", "varnumber": "12",
        "codevalue": "1", "valuelabel": "Public",
    }]), codes)
    pq.write_table(pa.Table.from_pylist([{
        "analysis_column": "CONTROL_ALIAS", "year": 2023, "source_file": "HD", "access_table_name": "HD2023",
        "varname": "CONTROL", "source_varnumber": "12", "UNITID": 1001,
    }]), lineage)
    output = tmp_path / "extract.csv"
    result = run_script("Scripts/08_build_custom_panel.py", "--input", panel, "--output", output,
                        "--vars", "CONTROL_ALIAS", "--require-metadata", "--log-file", "",
                        env={"IPEDSDB_ROOT": str(tmp_path / "unrelated_root")})
    assert result.returncode == 0, result.stdout
    exported = json.loads(Path(str(output) + ".metadata.json").read_text())
    assert exported["readiness_status"] == "complete"
    assert exported["metadata_sources"]["dictionary"] == str(dictionary)
    assert exported["metadata_sources"]["lineage"] == str(lineage)
    alias = next(variable for variable in exported["variables"] if variable["name"] == "CONTROL_ALIAS")
    assert alias["label"] == "Institution control"
    assert alias["value_labels"] == [{"value": "1", "label": "Public"}]


def collapsed_sources(tmp_path: Path, domains: dict[str, list[str]], declaration: str = "Disc"):
    """Two physical components share output meaning but need not share codes."""
    panel = tmp_path / "panel.parquet"
    dictionary = tmp_path / "dictionary.parquet"
    codes = tmp_path / "codes.parquet"
    lineage = tmp_path / "lineage.parquet"
    values = [1, 2] if declaration == "Disc" else [125.5, -42.0]
    table = pa.table({"year": [2023, 2023], "UNITID": [1001, 1002], "VALUE": values})
    pq.write_table(table, panel)
    definitions = [{
        "year": 2023, "source_file": "SURVEY", "access_table_name": scope,
        "varnumber": str(number), "varname": "RAW", "DataType": "N", "format": declaration,
        "varTitle": "Shared variable title", "longDescription": "Shared description.",
    } for number, scope in enumerate(domains, 1)]
    pq.write_table(pa.Table.from_pylist(definitions), dictionary)
    pq.write_table(pa.Table.from_pylist([
        {**{key: definition[key] for key in ("year", "source_file", "access_table_name", "varnumber")},
         "codevalue": code, "valuelabel": "Shared code label"}
        for definition in definitions for code in domains[definition["access_table_name"]]
    ]), codes)
    pq.write_table(pa.Table.from_pylist([
        {"analysis_column": "VALUE", "year": 2023, "UNITID": 1001 + index,
         "source_file": "SURVEY", "access_table_name": definition["access_table_name"],
         "varname": "RAW", "source_varnumber": definition["varnumber"]}
        for index, definition in enumerate(definitions)
    ]), lineage)
    assembled = build_export_metadata(table.schema, [2023], dictionary, codes, lineage)
    assert assembled["metadata_status"] == "complete", assembled["issues"]
    return table, assembled, (panel, dictionary, codes, lineage)


def test_unequal_same_year_component_domains_cannot_pass_as_a_union(tmp_path: Path) -> None:
    table, assembled, _ = collapsed_sources(tmp_path, {"TABLE_A": ["1", "2"], "TABLE_B": ["1"]})
    # Institution 1002 originated in TABLE_B, where its observed 2 is undefined.
    # Metadata deliberately retains component identity without per-cell UNITIDs.
    check = validate_observed_codes(ds.dataset(table), assembled)
    assert [issue["code"] for issue in check["issues"]] == ["categorical_source_domain_ambiguous"]
    assert check["code_domain_by_year"]["VALUE"]["2023"] == "ambiguous"
    apply_observation_validation(assembled, {"issues": []}, check)
    assert assembled["readiness_status"] == "incomplete"
    with pytest.raises(ValueError, match="not ready"):
        require_export_ready(assembled)
    assert table["VALUE"].to_pylist() == [1, 2]


def test_identical_component_domains_allow_validation_with_normalized_numeric_codes(tmp_path: Path) -> None:
    table, assembled, _ = collapsed_sources(tmp_path, {"TABLE_A": ["1", "2"], "TABLE_B": ["1.0", "2.00"]})
    check = validate_observed_codes(ds.dataset(table), assembled)
    assert check["issues"] == []
    assert check["unknown_code_count"] == 0
    unknown = table.set_column(2, "VALUE", pa.array([1, 99]))
    check = validate_observed_codes(ds.dataset(unknown), assembled)
    assert [issue["code"] for issue in check["issues"]] == ["unknown_observed_code"]


def test_unequal_continuous_sentinel_sets_do_not_close_component_domains(tmp_path: Path) -> None:
    table, assembled, _ = collapsed_sources(tmp_path, {"TABLE_A": ["-1", "-2"], "TABLE_B": ["-1"]}, "Cont")
    check = validate_observed_codes(ds.dataset(table), assembled)
    assert check["issues"] == []
    assert check["continuous_variables"] == ["VALUE"]
    assert check["code_domain_by_year"]["VALUE"]["2023"] == "continuous"


def test_stage08_rejects_ambiguous_component_domains_and_reports_them_in_diagnostics(tmp_path: Path) -> None:
    _, _, (panel, dictionary, codes, lineage) = collapsed_sources(tmp_path, {"TABLE_A": ["1", "2"], "TABLE_B": ["1"]})
    output = tmp_path / "extract.csv"
    arguments = ("--input", panel, "--output", output, "--vars", "VALUE", "--dictionary", dictionary,
                 "--codes", codes, "--column-lineage", lineage, "--log-file", "")
    strict = run_script("Scripts/08_build_custom_panel.py", *arguments, "--require-metadata")
    assert strict.returncode != 0
    assert not output.exists()
    diagnostic = run_script("Scripts/08_build_custom_panel.py", *arguments)
    assert diagnostic.returncode == 0, diagnostic.stdout
    exported = json.loads(Path(str(output) + ".metadata.json").read_text())
    assert exported["readiness_status"] == "incomplete"
    assert "categorical_source_domain_ambiguous" in {issue["code"] for issue in exported["issues"]}
    assert pd.read_csv(output)["VALUE"].tolist() == [1, 2]
