"""A folder-only migration must preserve validated bytes and baseline links."""
import hashlib
import json
from pathlib import Path

import pytest

from helpers import load_script_module

organizer = load_script_module("data_root_organizer", "Scripts/organize_data_root.py")


@pytest.fixture
def data_root(tmp_path):
    root = tmp_path / "data"
    release = root / "Metadata_repairs/2023-sfa-v1"
    names = [organizer.LINKS[organizer.PANEL], organizer.LINKS["value_lineage.parquet"],
             "Exports/affected_2023.csv"]
    names += [f"Dictionary/v2/{name}.{ext}" for name in ("dictionary_lake", "dictionary_codes")
              for ext in ("csv", "parquet")]
    entries = []
    for relative in names:
        path = release / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        content = relative.encode()
        path.write_bytes(content)
        entries.append({"path": relative, "bytes": len(content),
                        "sha256": hashlib.sha256(content).hexdigest()})
    (release / "manifest.json").write_text(json.dumps({"status": "verified", "correction_version": "2023-sfa-v1",
                                                      "artifacts": entries}))
    (root / "Raw_Access_Databases/2023").mkdir(parents=True)
    (root / "Raw_Access_Databases/2023/source.accdb").write_bytes(b"original source")
    (root / "Panels/v2").mkdir(parents=True)
    (root / "Checks/v2").mkdir(parents=True)
    (root / "Checks/v2/clean.parquet").write_bytes(b"baseline")
    (root / "Panels/v2/clean.parquet").symlink_to("../../Checks/v2/clean.parquet")
    return root


def snapshot(root):
    return {str(path.relative_to(root)): ("link", path.readlink().as_posix()) if path.is_symlink()
            else ("file", path.read_bytes()) for path in root.rglob("*") if path.is_file() or path.is_symlink()}


def test_preview_does_not_modify_any_file(data_root):
    before = snapshot(data_root)
    assert organizer.organize(data_root)["status"] == "preview"
    assert snapshot(data_root) == before
    assert not (data_root / "Final").exists()


def test_migration_preserves_release_source_and_baseline_and_is_idempotent(data_root):
    original = data_root / "Metadata_repairs/2023-sfa-v1"
    before = snapshot(original)
    baseline_inode = (data_root / "Checks/v2/clean.parquet").stat().st_ino
    result = organizer.organize(data_root, apply=True,
                                expected_manifest_sha256=organizer.sha256(original / "manifest.json"))
    assert result["status"] == "organized"
    assert result["preserved_symlinks"] == 1
    assert snapshot(data_root / "Releases/2023-sfa-v1") == before
    assert (data_root / "Sources/Raw_Access_Databases/2023/source.accdb").read_bytes() == b"original source"
    baseline = data_root / "Archive/pre_metadata_repair/Panels/v2/clean.parquet"
    assert baseline.stat().st_ino == baseline_inode
    final = data_root / "Final" / organizer.PANEL
    assert final.is_symlink()
    assert final.read_bytes() == organizer.LINKS[organizer.PANEL].encode()
    assert (data_root / "Work/Checks").is_dir()
    assert not (data_root / "Panels").exists()
    assert organizer.organize(data_root, apply=True)["status"] == "already_organized"


@pytest.mark.parametrize("failing_rename", [2, 5, 9, 11])
def test_failed_rename_restores_original_layout(data_root, monkeypatch, failing_rename):
    before = snapshot(data_root)
    original_rename = organizer.os.rename
    calls = 0

    def fail_once(source, destination):
        nonlocal calls
        calls += 1
        if calls == failing_rename:
            raise OSError("simulated replacement failure")
        return original_rename(source, destination)

    monkeypatch.setattr(organizer.os, "rename", fail_once)
    with pytest.raises(OSError, match="simulated"):
        organizer.organize(data_root, apply=True)
    assert snapshot(data_root) == before
    assert not (data_root / "layout.json").exists()
    assert not list(data_root.glob(".organize-staging-*"))


def test_corrupt_release_stops_before_any_move(data_root):
    artifact = data_root / "Metadata_repairs/2023-sfa-v1/Exports/affected_2023.csv"
    artifact.write_bytes(b"x" * artifact.stat().st_size)
    before = snapshot(data_root)
    with pytest.raises(ValueError, match="checksum mismatch"):
        organizer.organize(data_root, apply=True)
    assert snapshot(data_root) == before


def test_conflicting_target_is_preserved(data_root):
    (data_root / "Final").mkdir()
    (data_root / "Final/my_analysis.csv").write_text("user data")
    before = snapshot(data_root)
    with pytest.raises(ValueError, match="Destination already exists"):
        organizer.organize(data_root, apply=True)
    assert snapshot(data_root) == before


def test_idempotent_verification_detects_stale_final_alias(data_root):
    organizer.organize(data_root, apply=True)
    final = data_root / "Final" / organizer.PANEL
    final.unlink()
    final.symlink_to("../Archive/pre_metadata_repair/Panels/v2/clean.parquet")
    with pytest.raises(ValueError, match="Final link is not"):
        organizer.organize(data_root, apply=True)


def test_concurrent_artifact_change_cannot_complete_migration(data_root, monkeypatch):
    original_write = organizer.write_guides

    def change_artifact(staging, *args):
        original_write(staging, *args)
        (staging / "Releases/2023-sfa-v1/Exports/affected_2023.csv").write_bytes(b"concurrent edit")

    monkeypatch.setattr(organizer, "write_guides", change_artifact)
    with pytest.raises(ValueError, match="changed during reorganization"):
        organizer.organize(data_root, apply=True)
    assert not (data_root / "layout.json").exists()
    assert not (data_root / "Final").exists()
    assert (data_root / "Metadata_repairs/2023-sfa-v1").is_dir()
