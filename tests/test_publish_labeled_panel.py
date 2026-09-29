"""A labeled publication preserves the original release and rolls back failed promotions."""
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from helpers import load_script_module
from panel_export import annotated_schema

publisher = load_script_module("labeled_panel_publisher", "Scripts/publish_labeled_panel.py")


def snapshot(root):
    return {str(path.relative_to(root)): ("link", str(path.readlink())) if path.is_symlink()
            else ("file", path.read_bytes()) for path in root.rglob("*") if path.is_file() or path.is_symlink()}


@pytest.fixture
def package(tmp_path):
    root = tmp_path / "data"
    base = root / "Metadata_repairs" / publisher.RELEASE
    table = pa.table({"year": [2023] * 3, "UNITID": [1, 2, 3], "VALUE": [1.5, None, 2.0]})
    original = base / f"Panels/v2/{publisher.PANEL}"
    original.parent.mkdir(parents=True)
    pq.write_table(table, original, row_group_size=2)
    names = ["Checks/v2/wide_qc/qc_value_lineage.parquet", "Exports/affected_2023.csv"]
    names += [f"Dictionary/v2/{name}.{ext}" for name in ("dictionary_lake", "dictionary_codes") for ext in ("csv", "parquet")]
    for name in names:
        path = base / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"Preserved base fixture")
    artifacts = [{"path": str(path.relative_to(base)), "bytes": path.stat().st_size, "sha256": publisher.sha256(path)}
                 for path in base.rglob("*") if path.is_file()]
    (base / "manifest.json").write_text(json.dumps({"status": "verified", "correction_version": publisher.RELEASE, "artifacts": artifacts}))
    (root / "Raw_Access_Databases").mkdir(parents=True)
    publisher.organize(root, apply=True)
    staging = root / "Work/full-panel-labels-v1"
    staging.mkdir()
    variables = [{"name": name, "label": name, "description": "Fixture definition", "metadata_status": "complete"}
                 for name in table.column_names]
    metadata = {"variables": variables, "metadata_status": "complete", "readiness_status": "complete",
                "format_readiness_status": "complete", "row_count": 3, "column_count": 3, "issues": [],
                "source_panel_sha256": publisher.sha256(root / "Releases" / publisher.RELEASE / f"Panels/v2/{publisher.PANEL}")}
    pq.write_table(table.cast(annotated_schema(table.schema, metadata)), staging / publisher.PANEL, row_group_size=3)
    (staging / publisher.DTA).write_bytes(b"Native file mocked in publication unit tests")
    for name in (publisher.PANEL, publisher.DTA):
        for suffix in publisher.PACKAGE_SUFFIXES:
            (staging / (name + suffix)).write_text("Fixture companion\n")
        (staging / (name + ".metadata.json")).write_text(json.dumps({**metadata, "data_sha256": publisher.sha256(staging / name)}))
    proof = staging / "Checks/native_stata"
    chunk = proof / "chunk_01"
    chunk.mkdir(parents=True)
    (chunk / "native_verify.log").write_text("Mock native pass for unit test only\n")
    (chunk / "native_verify.do").write_text("Mock native commands for unit test only\n")
    receipt = {"status": "pass", "exact_observations_in_export_representation": True, "rows": 3, "columns": 3,
               "variable_labels_checked": 3, "value_labels_checked": 0, "inputs": {
                   "data": {"sha256": publisher.sha256(staging / publisher.DTA)},
                   "metadata": {"sha256": publisher.sha256(staging / (publisher.DTA + ".metadata.json"))},
                   "source": {"sha256": metadata["source_panel_sha256"]}},
               "chunks": [{"status": "pass", "rows": 3, "columns": table.column_names,
                           "log_sha256": publisher.sha256(chunk / "native_verify.log"),
                           "do_file_sha256": publisher.sha256(chunk / "native_verify.do")}]}
    (proof / "validation.json").write_text(json.dumps(receipt))
    (staging / "Evidence").mkdir()
    (staging / "Evidence/official.zip").write_bytes(b"Fixture official evidence")
    return root, staging


def test_preview_is_read_only(package):
    root, staging = package
    before = snapshot(root)
    assert publisher.publish(root, staging)["status"] == "preview"
    assert snapshot(root) == before


def test_published_layer_preserves_original_and_organizer_verifies_both(package):
    root, staging = package
    base = root / "Releases" / publisher.RELEASE
    before = snapshot(base)
    result = publisher.publish(root, staging, apply=True)
    assert result["status"] == "published"
    assert snapshot(base) == before
    assert not staging.exists()
    layout = json.loads((root / "layout.json").read_text())
    assert layout["current_release"] == publisher.RELEASE
    assert layout["labeled_panel_release"] == "full-panel-labels-v1"
    final = root / "Final"
    assert (final / publisher.PANEL).resolve() == root / "Releases/full-panel-labels-v1" / publisher.PANEL
    assert (final / "Metadata").resolve() == base / "Dictionary/v2"
    manifest = json.loads((root / "Releases/full-panel-labels-v1/manifest.json").read_text())
    assert "Evidence/official.zip" in {entry["path"] for entry in manifest["artifacts"]}
    assert publisher.organize(root, apply=True)["status"] == "already_organized"


def test_failed_final_update_rolls_back_package_links_and_manifests(package, monkeypatch):
    root, staging = package
    before = snapshot(root)
    real_replace = publisher.os.replace
    failed = False
    def fail_marker_once(source, target):
        nonlocal failed
        if Path(target) == root / "layout.json" and not failed:
            failed = True
            raise OSError("simulated marker failure")
        return real_replace(source, target)
    monkeypatch.setattr(publisher.os, "replace", fail_marker_once)
    with pytest.raises(OSError, match="simulated"):
        publisher.publish(root, staging, apply=True)
    assert snapshot(root) == before
    assert not list(root.glob(".publish-labels-*"))


def test_changed_values_cannot_be_published_even_when_companion_checksum_is_refreshed(package):
    root, staging = package
    panel = staging / publisher.PANEL
    table = pq.read_table(panel)
    pq.write_table(table.set_column(2, table.schema.field(2), pa.array([999.0, None, 2.0])), panel)
    companion = staging / (publisher.PANEL + ".metadata.json")
    metadata = json.loads(companion.read_text())
    metadata["data_sha256"] = publisher.sha256(panel)
    companion.write_text(json.dumps(metadata))
    with pytest.raises(ValueError, match="values or missingness changed"):
        publisher.publish(root, staging, apply=True)
    assert staging.exists()


def test_stale_native_receipt_cannot_publish(package):
    root, staging = package
    receipt = staging / "Checks/native_stata/validation.json"
    value = json.loads(receipt.read_text())
    value["inputs"]["data"]["sha256"] = "outdated"
    receipt.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="not bound"):
        publisher.publish(root, staging, apply=True)


def test_native_receipt_requires_every_declared_value_label(package):
    root, staging = package
    receipt = staging / "Checks/native_stata/validation.json"
    value = json.loads(receipt.read_text())
    value.pop("value_labels_checked")
    receipt.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="full-panel verification did not pass"):
        publisher.publish(root, staging, apply=True)


def test_observation_gap_is_disclosed_even_if_status_flags_are_incorrect(package):
    root, staging = package
    companion = staging / (publisher.PANEL + ".metadata.json")
    value = json.loads(companion.read_text())
    value["observation_validation"] = {"status": "incomplete", "issues": [
        {"code": "unknown_observed_code", "variable": "VALUE", "message": "1 unknown code", "count": 1}]}
    companion.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="allow-source-gaps"):
        publisher.publish(root, staging)
    assert publisher.publish(root, staging, allow_source_gaps=True)["metadata_gaps"]


@pytest.mark.parametrize("change", ["label", "source"])
def test_companion_must_match_embedded_labels_and_original_source(package, change):
    root, staging = package
    companion = staging / (publisher.PANEL + ".metadata.json")
    value = json.loads(companion.read_text())
    if change == "label":
        value["variables"][2]["label"] = "Incorrect replacement label"
        expected = "Embedded Parquet metadata differs"
    else:
        value["source_panel_sha256"] = "wrong original source"
        expected = "source fingerprint is not the original"
    companion.write_text(json.dumps(value))
    with pytest.raises(ValueError, match=expected):
        publisher.publish(root, staging, apply=True)


def test_source_gaps_require_explicit_option(package):
    root, staging = package
    companion = staging / (publisher.PANEL + ".metadata.json")
    value = json.loads(companion.read_text())
    value.update(metadata_status="incomplete", readiness_status="incomplete",
                 issues=[{"code": "variable_description_missing", "variable": "VALUE", "message": "Missing source description"}])
    companion.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="allow-source-gaps"):
        publisher.publish(root, staging)
    assert publisher.publish(root, staging, allow_source_gaps=True)["metadata_gaps"]


@pytest.mark.parametrize("code", ["invalid_panel_key", "duplicate_panel_key", "codes_schema_invalid", "value_label_identity_ambiguous"])
def test_source_gap_option_cannot_waive_integrity_or_identity_failures(package, code):
    root, staging = package
    companion = staging / (publisher.PANEL + ".metadata.json")
    value = json.loads(companion.read_text())
    value.update(metadata_status="incomplete", readiness_status="incomplete",
                 issues=[{"code": code, "variable": "VALUE", "message": "Unwaivable validation failure"}])
    companion.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="cannot be waived"):
        publisher.publish(root, staging, allow_source_gaps=True)


def test_source_gap_option_cannot_waive_format_readiness(package):
    root, staging = package
    companion = staging / (publisher.PANEL + ".metadata.json")
    value = json.loads(companion.read_text())
    value["format_readiness_status"] = "incomplete"
    companion.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="Format readiness failed"):
        publisher.publish(root, staging, allow_source_gaps=True)
    value.pop("format_readiness_status")
    companion.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="Format readiness failed"):
        publisher.publish(root, staging, allow_source_gaps=True)


def test_only_missing_yearly_descriptions_can_explain_an_unrendered_description_conflict():
    issue = {"code": "variable_description_conflict", "variable": "VALUE"}
    records = [{"year": 2021, "longDescription": "Earlier meaning"},
               {"year": 2022, "longDescription": "Later meaning"},
               {"year": 2023, "longDescription": None}]
    variables = {"VALUE": {"source_metadata": records}}
    assert publisher.source_gap(issue, variables)
    records.append({"year": 2022, "longDescription": "Same-year conflicting meaning"})
    assert not publisher.source_gap(issue, variables)
    records.pop()
    records[-1]["longDescription"] = "Known meaning"
    assert not publisher.source_gap(issue, variables)
