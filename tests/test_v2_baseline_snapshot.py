from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import shutil
import tarfile

import pytest

from helpers import REPO_ROOT, load_script_module


@pytest.fixture
def preparer():
    return load_script_module("prepare_v2_snapshot_test", "Scripts/QA_QC/25_prepare_v2_repair_pipeline.py")


@pytest.fixture
def source_tree(tmp_path, preparer):
    """The required source files, without a checkout or historical Git objects."""
    repository = tmp_path / "source-download"
    for relative in [preparer.BASE_ARCHIVE, Path("Scripts/source_metadata_corrections.py")]:
        target = repository / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(REPO_ROOT / relative, target)
    shutil.copytree(REPO_ROOT / "contracts/source_metadata_corrections",
                    repository / "contracts/source_metadata_corrections")
    return repository


def test_prepare_without_git_history_preserves_original_policy(tmp_path, preparer, source_tree):
    assert not (source_tree / ".git").exists()
    output = tmp_path / "prepared"
    receipt = preparer.prepare(output, source_tree)
    assert receipt["base_commit"] == "799a63930f97df9a3ec35be857f340ea3743afac"
    assert receipt["base_archive"] == {
        "path": str(preparer.BASE_ARCHIVE), "sha256": preparer.BASE_ARCHIVE_SHA256,
    }
    assert preparer.digest(output / "contracts/prch_policy.csv") == (
        "67fca57085ec759944152f6e6e38374fac75991ef2dbd0489d241641d310e4f3"
    )
    assert all(preparer.digest(output / row["path"]) == row["sha256"]
               for row in receipt["pipeline_scripts"])
    assert "apply_source_metadata_corrections(lake, root=layout.root)" in (
        output / "Scripts/03_dictionary_ingest.py"
    ).read_text()
    assert "physical_columns=pd.read_csv" in (output / "Scripts/04_harmonize.py").read_text()
    assert not (output / "manuscript").exists()
    assert not (output / ".git").exists()


def test_snapshot_matches_manifest_and_contains_only_reviewed_source(preparer):
    manifest = json.loads((REPO_ROOT / preparer.BASE_ARCHIVE.with_suffix("").with_suffix(".json")).read_text())
    archive = REPO_ROOT / preparer.BASE_ARCHIVE
    assert preparer.digest(archive) == preparer.BASE_ARCHIVE_SHA256 == manifest["archive"]["sha256"]
    assert archive.stat().st_size == manifest["archive"]["bytes"]
    assert manifest["source_commit"] == preparer.BASE_COMMIT
    approved_roots = {
        "Scripts", "contracts", "tools", "Queries", "Artifacts", "LICENSE", "manual_commands.sh",
        "requirements.txt", "requirements-lock.txt", "requirements-linux-arm64-hashes.txt",
    }
    assert set(manifest["included_paths"]) == approved_roots
    expected = {row["path"]: row for row in manifest["files"]}
    with tarfile.open(archive, "r:gz") as bundle:
        files = [member for member in bundle if member.isfile()]
        assert len(files) == len(expected)
        assert {member.name for member in files} == set(expected)
        for member in files:
            assert Path(member.name).parts[0] in approved_roots
            contents = bundle.extractfile(member).read()
            assert len(contents) == expected[member.name]["bytes"]
            assert hashlib.sha256(contents).hexdigest() == expected[member.name]["sha256"]
            header = f"blob {len(contents)}\0".encode()
            assert hashlib.sha1(header + contents).hexdigest() == expected[member.name]["git_blob"]


def test_tampered_archive_rejected_before_output_creation(tmp_path, preparer, source_tree):
    archive = source_tree / preparer.BASE_ARCHIVE
    archive.write_bytes(archive.read_bytes() + b"tampered")
    output = tmp_path / "absent-parent" / "prepared"
    with pytest.raises(ValueError, match="archive checksum mismatch"):
        preparer.prepare(output, source_tree)
    assert not output.parent.exists()


@pytest.mark.parametrize("name,link", [("../escape", False), ("/absolute", False), ("Scripts/link", True)])
def test_unsafe_member_rejected_before_output_creation(tmp_path, preparer, source_tree, monkeypatch, name, link):
    archive = source_tree / preparer.BASE_ARCHIVE
    with tarfile.open(archive, "w:gz") as bundle:
        member = tarfile.TarInfo(name)
        if link:
            member.type = tarfile.SYMTYPE
            member.linkname = "../../escape"
        bundle.addfile(member, io.BytesIO(b""))
    monkeypatch.setattr(preparer, "BASE_ARCHIVE_SHA256", preparer.digest(archive))
    output = tmp_path / "absent-parent" / "prepared"
    with pytest.raises(ValueError, match="Unsafe preserved v2 archive member"):
        preparer.prepare(output, source_tree)
    assert not output.parent.exists()
