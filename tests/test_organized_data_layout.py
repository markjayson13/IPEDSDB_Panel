"""Organized roots separate source data, draft builds, and verified analysis."""
import csv
import hashlib
import json
from pathlib import Path

import pytest

from access_build_utils import data_layout, default_final_panel, ensure_data_layout, require_data_volume
from helpers import load_script_module, run_script, write_parquet
from source_metadata_corrections import resolve_source_input, verify_source_files


def organized(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "layout.json").write_text(json.dumps({"schema_version": 1, "current_release": "2023-sfa-v1"}))
    return root


def test_organized_builds_route_to_work_without_creating_final(tmp_path: Path) -> None:
    root = organized(tmp_path / "data")
    layout = ensure_data_layout(root)
    assert layout.organized and layout.current_release == "2023-sfa-v1"
    assert layout.raw_access == root / "Sources/Raw_Access_Databases"
    for field, folder in (("dictionary", "Dictionary"), ("panels", "Panels"), ("checks", "Checks"),
                          ("cross_sections", "Cross_sections"), ("build", "build")):
        assert getattr(layout, field) == root / "Work" / folder
        assert getattr(layout, field).is_dir()
        assert not (root / folder).exists()
    assert layout.final == root / "Final"
    assert layout.releases == root / "Releases"
    assert layout.archive == root / "Archive"
    assert not layout.final.exists()
    assert not layout.releases.exists()


def test_no_marker_preserves_legacy_paths_and_panel_precedence(tmp_path: Path) -> None:
    layout = ensure_data_layout(tmp_path)
    assert not layout.organized
    assert layout.current_release is None
    assert layout.panels == tmp_path / "Panels"
    assert layout.raw_access == tmp_path / "Raw_Access_Databases"
    legacy = layout.panels / "panel_clean_analysis_2020_2023.parquet"
    legacy.write_bytes(b"legacy")
    assert default_final_panel(tmp_path, "2020:2023") == legacy
    v2 = layout.panels / "v2/panel_clean_prch_2020_2023.parquet"
    v2.parent.mkdir()
    v2.write_bytes(b"v2")
    assert default_final_panel(tmp_path, "2020:2023") == v2


def test_missing_final_does_not_fall_back_to_work_or_archive(tmp_path: Path) -> None:
    root = organized(tmp_path)
    layout = ensure_data_layout(root)
    draft = layout.panels / "panel_clean_prch_2004_2023.parquet"
    draft.write_bytes(b"draft")
    with pytest.raises(FileNotFoundError, match="Work outputs are drafts"):
        default_final_panel(root)
    published = layout.releases / "2023-sfa-v1/Panels/v2" / draft.name
    published.parent.mkdir(parents=True)
    published.write_bytes(b"verified")
    layout.final.mkdir()
    analyst = layout.final / draft.name
    analyst.symlink_to(Path("../Releases/2023-sfa-v1/Panels/v2") / draft.name)
    assert default_final_panel(root) == analyst
    assert analyst.read_bytes() == b"verified"


@pytest.mark.parametrize("marker", [{"schema_version": 2, "current_release": "2023-sfa-v1"},
                                   {"schema_version": 1, "current_release": "../../outside"},
                                   {"schema_version": 1}])
def test_invalid_layout_marker_does_not_fall_back_to_legacy(tmp_path: Path, marker: dict) -> None:
    (tmp_path / "layout.json").write_text(json.dumps(marker))
    with pytest.raises(ValueError, match="layout marker"):
        ensure_data_layout(tmp_path)
    assert not (tmp_path / "Panels").exists()
    assert not (tmp_path / "Work").exists()


def test_unmounted_volume_is_rejected_before_any_mkdir(monkeypatch) -> None:
    monkeypatch.setattr(Path, "is_mount", lambda self: False)
    def unexpected_mkdir(*args, **kwargs):
        raise AssertionError("A write was attempted before the mount guard")
    monkeypatch.setattr(Path, "mkdir", unexpected_mkdir)
    with pytest.raises(FileNotFoundError, match="not mounted"):
        ensure_data_layout("/Volumes/IPEDS_TEST_ABSENT_DRIVE/data")
    require_data_volume("/private/tmp/explicit_local_root")


def test_pinned_source_evidence_routes_to_sources_without_rewriting_registry(tmp_path: Path) -> None:
    root = organized(tmp_path)
    relative = "Raw_Access_Databases/2023/tables_csv/sfa2223_p1.csv"
    source = root / "Sources" / relative
    source.parent.mkdir(parents=True)
    source.write_bytes(b"UNITID,VALUE\n1,0\n")
    registry = {"pipeline_inputs": [{"path": relative, "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}]}
    original = json.dumps(registry, sort_keys=True)
    verify_source_files(root, registry)
    assert resolve_source_input(root, relative) == source
    assert json.dumps(registry, sort_keys=True) == original
    source.write_bytes(b"UNITID,VALUE\n1,1\n")
    with pytest.raises(ValueError, match="does not match"):
        verify_source_files(root, registry)


def test_pipeline_dry_run_routes_all_drafts_under_work(tmp_path: Path) -> None:
    root = organized(tmp_path)
    result = run_script("Scripts/00_run_all.py", "--root", root, "--years", "2023:2023", "--dry-run",
                        "--run-cleaning", "--run-qaqc", "--build-custom", "--custom-vars", "VALUE")
    assert result.returncode == 0, result.stdout
    assert str(root / "Work/Panels/panel_clean_analysis_2023_2023.parquet") in result.stdout
    assert str(root / "Work/Dictionary/dictionary_lake.parquet") in result.stdout
    assert str(root / "Work/Checks") in result.stdout
    assert str(root / "Final/") not in result.stdout
    assert "--require-ready" in result.stdout
    assert not (root / "Work").exists()


def test_qa22_reads_the_organized_source_archive(tmp_path: Path) -> None:
    audit = load_script_module("organized_sfa_source_audit", "Scripts/QA_QC/22_verify_sfa_source_metadata.py")
    root = organized(tmp_path / "data")
    archive = root / "Sources/Raw_Access_Databases/2023/downloads/IPEDS_2023-24_Final.zip"
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b"not the locked release")
    with pytest.raises(ValueError, match="Original release differs"):
        audit.audit(root, tmp_path / "audit")


def test_cleaner_default_log_stays_in_organized_work(tmp_path: Path) -> None:
    root = organized(tmp_path)
    layout = ensure_data_layout(root)
    source = layout.panels / "wide.parquet"
    target = layout.panels / "clean.parquet"
    dictionary = layout.dictionary / "dictionary.parquet"
    write_parquet(source, [{"year": year, "UNITID": 1001, "PRCH_F": 1, "VALUE": 7.0}
                           for year in (2022, 2023)])
    write_parquet(dictionary, [{"varname": "VALUE", "source_file": "F_F"}])
    result = run_script("Scripts/07_clean_panel.py", "--input", source, "--output", target,
                        "--dictionary", dictionary, "--qc-dir", layout.checks / "prch_qc",
                        env={"IPEDSDB_ROOT": str(root)})
    assert result.returncode == 0, result.stdout
    assert target.is_file()
    assert (layout.checks / "logs/07_clean_panel.log").is_file()
    assert not (root / "Checks").exists()
    assert not layout.final.exists()


def test_repair_replay_reads_archive_baseline_and_separate_sources(tmp_path: Path, monkeypatch) -> None:
    rebuild = load_script_module("organized_metadata_rebuild", "Scripts/QA_QC/24_rebuild_metadata_repair.py")
    root = organized(tmp_path / "data")
    layout = data_layout(root)
    baseline = layout.archive / "pre_metadata_repair"
    policy = baseline / "Checks/v2/prch_qc/prch_flag_policy.csv"
    policy.parent.mkdir(parents=True)
    policy.write_bytes(b"original policy\n")
    monkeypatch.setattr(rebuild, "POLICY_SHA256", hashlib.sha256(policy.read_bytes()).hexdigest())
    coverage = baseline / "Checks/v2/harmonize_qc/source_column_coverage_2023.csv"
    coverage.parent.mkdir(parents=True)
    coverage.write_bytes(b"source_table\nSFA2223_P1\n")
    for year in range(2004, 2024):
        (layout.raw_access / str(year)).mkdir(parents=True)
    year_root = layout.raw_access / "2023"
    (year_root / "manifest.csv").write_text("year\n2023\n")
    inventory = []
    for name in rebuild.SCOPE_TABLES:
        csv_path = Path("tables_csv") / f"{name.lower()}.csv"
        path = year_root / csv_path
        path.parent.mkdir(exist_ok=True)
        path.write_text("UNITID,VALUE\n1001,0\n")
        inventory.append({"table_name": name, "csv_path": str(csv_path),
                          "csv_sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "row_count_csv": "1"})
    inventory_path = year_root / "metadata/table_inventory.csv"
    inventory_path.parent.mkdir()
    with inventory_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(inventory[0]))
        writer.writeheader()
        writer.writerows(inventory)
    output = tmp_path / "replay"
    result = rebuild.prepare_inputs(root, output, None)
    assert rebuild.baseline_root(root) == baseline
    assert (output / "Raw_Access_Databases/2004").resolve() == layout.raw_access / "2004"
    assert (output / "Checks/v2/harmonize_qc" / coverage.name).read_bytes() == coverage.read_bytes()
    assert (output / "contracts/prch_policy.csv").read_bytes() == policy.read_bytes()
    assert all(str(layout.raw_access) in row["original"] for row in result["source_exports"])
    assert not layout.work.exists()


def test_historical_replay_hashes_follow_archive_relocation(tmp_path: Path) -> None:
    rebuild = load_script_module("organized_replay_hashes", "Scripts/QA_QC/24_rebuild_metadata_repair.py")
    root = organized(tmp_path)
    relative = Path("Panels/v2/panel_long_dimensioned_2004_2023.parquet")
    current = root / "Archive/pre_metadata_repair" / relative
    hashes = {str(root / relative): "a" * 64}
    assert rebuild.source_hash_for(hashes, current, root) == "a" * 64
    hashes[str(current)] = "b" * 64
    with pytest.raises(ValueError, match="checksums disagree"):
        rebuild.source_hash_for(hashes, current, root)


def test_wide_builder_default_logs_and_profile_stay_in_work(tmp_path: Path, monkeypatch) -> None:
    import wide_build_common as common
    import wide_build_duckdb as wide
    root = organized(tmp_path)
    monkeypatch.setenv("IPEDSDB_ROOT", str(root))
    args = common.build_arg_parser().parse_args(["--input", "input.parquet", "--out_dir", str(root / "Work/parts"),
                                                 "--lane-split", "--profile-year", "2023"])
    assert Path(args.log_file) == root / "Work/Checks/logs/06_build_wide_panel.log"
    runtime = common.prepare_runtime(args)
    assert Path(args.write_single) == root / "Work/Panels/panel_wide_analysis_2004_2023.parquet"
    assert wide.resolve_profile_dir(args, runtime) == root / "Work/Checks/wide_qc/sql_profiles"
    assert not (root / "Checks").exists()
