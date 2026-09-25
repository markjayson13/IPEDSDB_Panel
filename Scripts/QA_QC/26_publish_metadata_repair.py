#!/usr/bin/env python3
"""Publish a verified repair as one immutable directory with content hashes.

Existing canonical artifacts are retained as the before-repair baseline. No
downstream dataset is touched. A same-filesystem rename is the publication
boundary; incomplete copies remain unpublished under a unique staging name.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import uuid


def sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def add_tree(members: dict[str, Path], source: Path, target: str) -> None:
    source = source.resolve(strict=True)
    for item in sorted(source.rglob("*")):
        if item.is_file() and "__pycache__" not in item.parts and item.suffix != ".pyc":
            name = str(Path(target) / item.relative_to(source))
            if name in members:
                raise ValueError(f"Duplicate package member: {name}")
            members[name] = item


def expected_member_hashes(work: Path, receipt: dict, review: dict, exports: dict) -> dict[str, str]:
    """Bind publication to already validated bytes, not merely a success flag."""
    expected = {}
    required_panels = {"panel_wide_analysis_2004_2023.parquet", "panel_clean_prch_2004_2023.parquet"}
    if not required_panels.issubset(receipt["verification"]["panels"]):
        raise ValueError("Both wide and cleaned panel equality proofs are required")
    for name, record in receipt["verification"]["panels"].items():
        if not record.get("all_values_equal"):
            raise ValueError(f"Panel equality is incomplete: {name}")
        expected[f"Panels/v2/{name}"] = record["sha256"]
    for relative, digest in receipt["verification"]["artifact_sha256"].items():
        if relative in expected and expected[relative] != digest:
            raise ValueError(f"Validation receipts disagree: {relative}")
        expected[relative] = digest
    for name in ("dictionary_lake", "dictionary_codes"):
        expected[f"Dictionary/v2/{name}.parquet"] = review[name]["corrected_sha256"]
    stage03 = json.loads((work / "dictionary-stage/stage03_reproduction_receipt.json").read_text())
    if stage03["correction_registry_sha256"] != review["registry_sha256"]:
        raise ValueError("Validation receipts disagree: correction registry")
    expected["Evidence/source_metadata_corrections/2023-sfa-v1.json"] = review["registry_sha256"]
    for item in stage03["outputs"]:
        key = f"Dictionary/v2/{Path(item['path']).name}"
        if key in expected and expected[key] != item["sha256"]:
            raise ValueError(f"Validation receipts disagree: {key}")
        expected[key] = item["sha256"]
    export_root = (work / "final_exports").resolve()
    for item in exports["artifacts"]:
        key = str(Path("Exports") / Path(item["path"]).resolve().relative_to(export_root))
        expected[key] = item["sha256"]
    required = {"Panels/v2/panel_long_scalar_2004_2023.parquet",
                "Panels/v2/panel_wide_analysis_2004_2023.parquet",
                "Panels/v2/panel_clean_prch_2004_2023.parquet",
                "Checks/v2/wide_qc/qc_value_lineage.parquet",
                "Checks/v2/prch_qc/prch_cell_actions.parquet", "Checks/v2/prch_qc/prch_flag_policy.csv"}
    if not required.issubset(expected):
        raise ValueError(f"Missing validated artifact hashes: {sorted(required - expected.keys())}")
    for name, digest in expected.items():
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError(f"Invalid validation checksum: {name}")
    return expected


def verify_validated_member(path: Path, relative: str, expected: dict[str, str]) -> str:
    digest = sha256(path)
    if relative in expected and digest != expected[relative]:
        raise ValueError(f"Artifact changed after validation: {relative}")
    return digest


def publish(work: Path, repository: Path, destination: Path) -> dict:
    rebuild = work / "rebuild-stage"
    receipt = json.loads((rebuild / "metadata_repair_receipt.json").read_text())
    review = json.loads((work / "correction_review/dictionary_diff_summary.json").read_text())
    if receipt.get("status") != "complete" or review.get("result") != "pass":
        raise ValueError("Full rebuild and dictionary verification must pass before publication")
    if not receipt.get("verification", {}).get("prch_actions_equal"):
        raise ValueError("PRCH action equality has not passed")
    if not receipt.get("verification", {}).get("full_value_lineage_equal"):
        raise ValueError("Full value-lineage equality has not passed")
    export_receipt = json.loads((work / "final_exports/validation.json").read_text())
    if export_receipt.get("status") != "pass":
        raise ValueError("Actual corrected-output export validation must pass")
    if destination.exists():
        raise ValueError("A versioned publication already exists; refusing replacement")
    # This publisher is intentionally confined to the requested IPEDS output root.
    intended = Path("/Volumes/CIRAGO/IPEDSDB_PANEL/Metadata_repairs/2023-sfa-v1").resolve()
    if destination.resolve() != intended:
        raise ValueError("Publication must use the versioned IPEDS metadata-repair destination")
    expected = expected_member_hashes(work, receipt, review, export_receipt)
    members: dict[str, Path] = {}
    add_tree(members, work / "dictionary-stage/Dictionary/v2", "Dictionary/v2")
    for name in ("panel_long_scalar_2004_2023.parquet", "panel_wide_analysis_2004_2023.parquet", "panel_clean_prch_2004_2023.parquet"):
        members[f"Panels/v2/{name}"] = (rebuild / "Panels/v2" / name).resolve(strict=True)
    for name in ("wide_qc", "disc_qc", "prch_qc", "harmonize_qc"):
        add_tree(members, rebuild / "Checks/v2" / name, f"Checks/v2/{name}")
    add_tree(members, rebuild / "Checks/metadata_repair", "Checks/metadata_repair")
    add_tree(members, work / "final_exports", "Exports")
    add_tree(members, work / "value_audit", "Evidence/original_value_audit")
    add_tree(members, work / "correction_review", "Evidence/dictionary_review")
    add_tree(members, repository / "contracts/source_metadata_corrections", "Evidence/source_metadata_corrections")
    add_tree(members, work / "pipeline-v2", "Reproduction/pipeline-v2")
    add_tree(members, repository / "Scripts", "Reproduction/current_scripts")
    add_tree(members, rebuild / "build/repair_logs", "Reproduction/logs")
    for name in ("source_file_sha256.json", "source_file_sha256_details.json"):
        members[f"Reproduction/{name}"] = work / name
    for name in ("schema.sql", "SFAV2223.csv", "sfa2223_p1.csv", "sfa2223_p2.csv", "varTable23.csv",
                 "valueSets23.csv", "Tables23.csv", "ReadMe2023-24.docx", "IPEDS202324Tablesdoc.xlsx",
                 "release_readme_paragraphs.txt"):
        members[f"Evidence/fresh_source/{name}"] = work / "verified_source" / name
    for relative in ("metadata_repair_receipt.json", "contracts/prch_policy.csv"):
        members[f"Reproduction/{relative}"] = rebuild / relative
    for relative in ("stage03_reproduction_receipt.json", "metadata_repair_overlay.json"):
        members[f"Reproduction/{relative}"] = work / "dictionary-stage" / relative
    members["README.md"] = repository / "docs/2023_SFA_METADATA_REPAIR.md"
    members["Evidence/software_validation.json"] = work / "software_validation.json"
    members["Evidence/affected_variable_list.csv"] = repository / "contracts/source_metadata_corrections/2023-sfa-v1.audit.csv"
    missing = expected.keys() - members.keys()
    if missing:
        raise ValueError(f"Validated artifacts omitted from package: {sorted(missing)}")
    for relative in members:
        if relative.startswith("Exports/") and relative != "Exports/validation.json" and relative not in expected:
            raise ValueError(f"Export has no validation checksum: {relative}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = destination.parent / f".{destination.name}.staging-{uuid.uuid4().hex}"
    staging.mkdir()
    entries = []
    for relative, source in sorted(members.items()):
        target = staging / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        before = source.stat()
        source_hash = verify_validated_member(source, relative, expected)
        shutil.copyfile(source, target)
        if sha256(target) != source_hash or source.stat().st_mtime_ns != before.st_mtime_ns or source.stat().st_size != before.st_size:
            raise ValueError(f"Source changed or package copy failed: {source}")
        entries.append({"path": relative, "published_path": str(destination / relative),
                        "bytes": target.stat().st_size, "sha256": source_hash,
                        "build_source_path": str(source)})
    manifest = {"schema_version": 1, "correction_version": "2023-sfa-v1",
                "created_utc": datetime.now(timezone.utc).isoformat(), "status": "verified",
                "publication_root": str(destination), "original_baseline_retained": True,
                "downstream_refresh": "separate; FSA-IPEDS_DS not modified",
                "build": receipt, "dictionary_validation": review, "export_validation": export_receipt,
                "artifacts": entries}
    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (staging / "SHA256SUMS").write_text("".join(f"{row['sha256']}  {row['path']}\n" for row in entries)
                                       + f"{sha256(staging / 'manifest.json')}  manifest.json\n")
    os.rename(staging, destination)
    return {"path": str(destination), "artifact_count": len(entries),
            "manifest_sha256": sha256(destination / "manifest.json")}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    print(json.dumps(publish(args.work_dir, args.repository, args.destination), indent=2))
