"""QA for draft builds stays in Work and preserves explicit/legacy roots."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pandas as pd
import pytest

from helpers import REPO_ROOT, load_script_module, run_script, write_parquet


def organized(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "layout.json").write_text(json.dumps({"schema_version": 1, "current_release": "2023-sfa-v1"}))
    return root


def test_dictionary_qa_reads_work_and_leaves_current_release_alone(tmp_path: Path) -> None:
    root = organized(tmp_path / "data")
    row = {"year": 2023, "source_file": "HD", "varnumber": "1", "varname": "INSTNM",
           "longDescription": "Institution name", "access_table_name": "HD2023"}
    write_parquet(root / "Work/Dictionary/dictionary_lake.parquet", [row])
    sentinel = root / "Final/Metadata/dictionary_lake.parquet"
    sentinel.parent.mkdir(parents=True)
    sentinel.write_bytes(b"current release must not be read or overwritten")
    result = run_script("Scripts/QA_QC/00_dictionary_qaqc.py", env={"IPEDSDB_ROOT": str(root)})
    assert result.returncode == 0, result.stdout
    summary = pd.read_csv(root / "Work/Checks/dictionary_qc/dictionary_qaqc_summary.csv")
    assert summary.iloc[0].lake_rows == 1
    assert not (root / "Checks").exists()
    assert sentinel.read_bytes() == b"current release must not be read or overwritten"


@pytest.mark.parametrize("marked", [False, True])
def test_release_metrics_use_the_selected_build_layout(tmp_path: Path, marked: bool) -> None:
    module = load_script_module("qa_layout_metrics", "Scripts/QA_QC/10_release_metrics.py")
    root = organized(tmp_path) if marked else tmp_path
    work = root / "Work" if marked else root
    write_parquet(work / "Panels/panel_clean_analysis_2023_2023.parquet", [{"UNITID": 1, "year": 2023}])
    write_parquet(work / "Dictionary/dictionary_lake.parquet", [{"year": 2023, "varname": "X", "source_file": "HD"}])
    panels = module.summarize_panel_files(root, [2023]).set_index("panel")
    assert panels.loc["clean_panel", "rows"] == 1
    assert panels.loc["clean_panel", "path"] == str(work / "Panels/panel_clean_analysis_2023_2023.parquet")
    assert module.summarize_dictionary(root, [2023]).iloc[0].dictionary_rows == 1


@pytest.mark.parametrize("script,arguments,attributes", [
    ("01_panel_qa.py", ["--raw", "raw.parquet", "--clean", "clean.parquet"],
     {"out_dir": "panel_qc", "prch_qc_dir": "prch_qc", "log_file": "logs/01_panel_qa.log"}),
    ("03_monitored_analysis_build.py", ["--input", "long.parquet", "--dictionary", "dictionary.parquet"],
     {"run_dir_root": "real_parity_runs"}),
    ("07_task_monitor_summary.py", [], {"run_dir_root": "real_parity_runs"}),
])
def test_qa_default_outputs_follow_marked_environment(tmp_path: Path, monkeypatch, script, arguments, attributes) -> None:
    root = organized(tmp_path)
    monkeypatch.setenv("IPEDSDB_ROOT", str(root))
    monkeypatch.setattr(sys, "argv", [script, *arguments])
    module = load_script_module("qa_paths_" + script.split("_")[0], "Scripts/QA_QC/" + script)
    args = module.parse_args()
    for field, suffix in attributes.items():
        assert Path(getattr(args, field)) == root / "Work/Checks" / suffix
    assert not (root / "Work").exists()


def test_acceptance_output_follows_root_argument_and_explicit_override(tmp_path: Path, monkeypatch) -> None:
    root = organized(tmp_path / "chosen")
    monkeypatch.setenv("IPEDSDB_ROOT", str(tmp_path / "unused"))
    module = load_script_module("qa_acceptance_paths", "Scripts/QA_QC/08_acceptance_audit.py")
    monkeypatch.setattr(sys, "argv", ["audit", "--root", str(root)])
    assert Path(module.parse_args().out_dir) == root / "Work/Checks/acceptance_qc"
    custom = tmp_path / "explicit_output"
    monkeypatch.setattr(sys, "argv", ["audit", "--root", str(root), "--out-dir", str(custom)])
    assert Path(module.parse_args().out_dir) == custom


def test_qc_wrapper_does_not_substitute_final_for_a_missing_work_build(tmp_path: Path) -> None:
    root = organized(tmp_path / "data root with spaces")
    final = root / "Final/Metadata/dictionary_lake.parquet"
    final.parent.mkdir(parents=True)
    final.write_bytes(b"current")
    result = subprocess.run(["bash", str(REPO_ROOT / "Scripts/QA_QC/qc_only.sh")],
                            cwd=REPO_ROOT, env={**os.environ, "IPEDSDB_ROOT": str(root)},
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30)
    assert result.returncode != 0
    assert str(root / "Work/Dictionary/dictionary_lake.parquet") in result.stdout
    assert not (root / "Checks").exists()
    assert final.read_bytes() == b"current"


@pytest.mark.parametrize("marked", [False, True])
def test_release_manifest_discovers_sources_and_draft_outputs(tmp_path, marked):
    module = load_script_module("organized_release_manifest", "Scripts/QA_QC/12_build_release_manifest.py")
    root = organized(tmp_path) if marked else tmp_path
    work_prefix = "Work/" if marked else ""
    source_prefix = "Sources/" if marked else ""
    downloaded = root / f"{source_prefix}Raw_Access_Databases/2023/downloads/source.zip"
    downloaded.parent.mkdir(parents=True)
    downloaded.write_bytes(b"source")
    qc = root / f"{work_prefix}Checks/panel_qc/summary.csv"
    qc.parent.mkdir(parents=True)
    qc.write_text("passed\n")
    rows = module.expected_root_artifacts(root, [2023], True)
    paths = {relative for _, relative, _ in rows}
    assert str(downloaded.relative_to(root)) in paths
    assert str(qc.relative_to(root)) in paths
    assert f"{work_prefix}Panels/panel_clean_analysis_2023_2023.parquet" in paths
    assert f"{source_prefix}Raw_Access_Databases/2023/manifest.csv" in paths


def test_structure_audit_default_output_follows_explicit_root(tmp_path, monkeypatch):
    root = organized(tmp_path)
    module = load_script_module("organized_structure_paths", "Scripts/QA_QC/09_panel_structure_qc.py")
    monkeypatch.setattr(sys, "argv", ["audit", "--root", str(root)])
    assert Path(module.parse_args().out_dir) == root / "Work/Checks/panel_qc"
