#!/usr/bin/env python3
"""Verify and publish a labeled copy of the corrected panel without changing its base release."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import uuid

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from export_integrity import PACKAGE_SUFFIXES
from export_metadata import decode_embedded_variable_metadata
from organize_data_root import PANEL, RELEASE, final_manifest, identity, organize, read_manifest, sha256

DTA = str(Path(PANEL).with_suffix(".dta"))
PACKAGE_FILES = [name + suffix for name in (PANEL, DTA) for suffix in ("", *PACKAGE_SUFFIXES)]
SOURCE_GAP_ISSUES = {
    "variable_metadata_missing", "variable_label_missing", "variable_description_missing",
    "variable_label_coverage_incomplete", "variable_description_coverage_incomplete",
    "variable_year_coverage_incomplete", "value_labels_missing", "value_label_coverage_incomplete",
    "unknown_observed_code",
}
FORMAT_NOTICES = {"stata_name_changed", "stata_label_shortened", "stata_string_null"}


def source_gap(issue: dict, variables: dict[str, dict]) -> bool:
    if issue.get("code") in SOURCE_GAP_ISSUES:
        return True
    if issue.get("code") != "variable_description_conflict":
        return False
    # Missing descriptions can prevent the scoped renderer from displaying
    # otherwise unambiguous historical definitions. A same-year disagreement
    # is an ambiguity, not a source gap, and must still block publication.
    records = variables.get(issue.get("variable"), {}).get("source_metadata", [])
    meanings, blank = {}, False
    for record in records:
        try:
            year = int(record["year"])
        except (KeyError, TypeError, ValueError, OverflowError):
            return False
        text = re.sub(r"\s+", " ", str(record.get("longDescription") or "")).strip()
        if text:
            meanings.setdefault(year, set()).add(text)
        else:
            blank = True
    return blank and bool(meanings) and all(len(values) == 1 for values in meanings.values())


def parquet_parity(original: Path, annotated: Path) -> dict:
    before, after = pq.ParquetFile(original), pq.ParquetFile(annotated)
    if not before.schema_arrow.equals(after.schema_arrow, check_metadata=False) or before.metadata.num_rows != after.metadata.num_rows:
        raise ValueError("Labeled Parquet schema or row count differs from the corrected source")
    original_batches = iter(before.iter_batches(batch_size=4096))
    labeled_batches = iter(after.iter_batches(batch_size=4096))
    left, right = next(original_batches, None), next(labeled_batches, None)
    rows = 0
    while left is not None and right is not None:
        length = min(left.num_rows, right.num_rows)
        for field, a, b in zip(before.schema_arrow, left.slice(0, length).columns, right.slice(0, length).columns):
            if a.equals(b):
                continue
            equal = pc.or_(pc.fill_null(pc.equal(a, b), False), pc.and_(pc.is_null(a), pc.is_null(b)))
            if pa.types.is_floating(a.type):
                equal = pc.or_(equal, pc.fill_null(pc.and_(pc.is_nan(a), pc.is_nan(b)), False))
            if not pc.all(equal).as_py():
                raise ValueError(f"Labeled Parquet values or missingness changed: {field.name}, row batch beginning {rows}")
        rows += length
        left = next(original_batches, None) if length == left.num_rows else left.slice(length)
        right = next(labeled_batches, None) if length == right.num_rows else right.slice(length)
    if left is not None or right is not None:
        raise ValueError("Parquet observations ended at different rows")
    return {"status": "pass", "rows": rows, "columns": len(before.schema_arrow),
            "all_values_equal": True, "all_missingness_equal": True,
            "source_names": before.schema_arrow.names}


def labeled_final_manifest(root: Path, base: dict, base_digest: str, labeled: dict, labeled_digest: str) -> dict:
    result = final_manifest(root, base, base_digest)
    result["artifacts"] = [entry for entry in result["artifacts"] if entry["path"] != f"Final/{PANEL}"]
    release_name = labeled["release_name"]
    for entry in labeled["artifacts"]:
        if entry["path"] in PACKAGE_FILES:
            name = entry["path"]
            result["artifacts"].append({"path": f"Final/{name}", "absolute_path": str(root / "Final" / name),
                                        "release_path": f"Releases/{release_name}/{name}",
                                        "bytes": entry["bytes"], "sha256": entry["sha256"]})
    result.update(labeled_panel_release=release_name,
                  labeled_panel_manifest=f"Releases/{release_name}/manifest.json",
                  labeled_panel_manifest_sha256=labeled_digest,
                  metadata_gaps=labeled["metadata_gaps"],
                  export_scope="The full panel is available as labeled Parquet and native Stata; SFA_2023_exports remains the corrected 2023 SFA subset.")
    return result


def verify_labeled_layer(root: Path, layout: dict, base: dict, base_digest: str, *, verify: bool) -> dict:
    name = layout["labeled_panel_release"]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", name) or name == RELEASE:
        raise ValueError("Invalid labeled panel release name")
    release = root / "Releases" / name
    digest = sha256(release / "manifest.json")
    if digest != layout.get("labeled_panel_manifest_sha256"):
        raise ValueError("Labeled panel release manifest has changed")
    manifest = json.loads((release / "manifest.json").read_text())
    if manifest.get("status") != "verified" or manifest.get("release_name") != name or manifest.get("base_release_manifest_sha256") != base_digest:
        raise ValueError("Labeled panel is not bound to the verified base release")
    seen = set()
    for entry in manifest["artifacts"]:
        relative = Path(entry["path"])
        if relative.is_absolute() or ".." in relative.parts or str(relative) in seen:
            raise ValueError("Unsafe or duplicate labeled release artifact")
        seen.add(str(relative))
        path = release / relative
        if path.is_symlink() or not path.resolve(strict=True).is_relative_to(release.resolve()) or path.stat().st_size != entry["bytes"]:
            raise ValueError(f"Labeled release artifact changed: {relative}")
        if verify and sha256(path) != entry["sha256"]:
            raise ValueError(f"Labeled release checksum mismatch: {relative}")
    if not set(PACKAGE_FILES).issubset(seen):
        raise ValueError("Labeled release lacks required data and companions")
    for name in PACKAGE_FILES:
        link = root / "Final" / name
        if not link.is_symlink() or link.resolve(strict=True) != release / name:
            raise ValueError(f"Final link is not the labeled release: {name}")
    expected = labeled_final_manifest(root, base, base_digest, manifest, digest)
    if json.loads((root / "Final/manifest.json").read_text()) != expected:
        raise ValueError("Final manifest does not match the labeled panel release")
    return manifest


def verify_package(package: Path, base_panel: Path, *, allow_source_gaps: bool,
                   panel_name: str = PANEL) -> tuple[dict, dict]:
    # Candidate extensions use the same full-data and native-label gates.
    if Path(panel_name).name != panel_name or Path(panel_name).suffix != ".parquet":
        raise ValueError("Expected a plain Parquet panel filename")
    stata_name = str(Path(panel_name).with_suffix(".dta"))
    package_files = [name + suffix for name in (panel_name, stata_name) for suffix in ("", *PACKAGE_SUFFIXES)]
    files = sorted(path for path in package.rglob("*") if path.is_file())
    if any(path.is_symlink() for path in package.rglob("*")):
        raise ValueError("A labeled release package cannot contain symlinks")
    if (package / "manifest.json").exists():
        raise ValueError("The work package already contains a release manifest")
    required = package_files + ["Checks/native_stata/validation.json"]
    if any(not (package / name).is_file() for name in required):
        raise ValueError("Labeled package lacks data, companions, or the full native Stata receipt")
    fingerprints, snapshots = {}, {}
    for path in files:
        relative = str(path.relative_to(package))
        before = identity(path)
        fingerprints[relative] = {"path": relative, "bytes": before[2], "sha256": sha256(path)}
        if identity(path) != before:
            raise ValueError(f"Package changed during hashing: {relative}")
        snapshots[relative] = before
    metadata = {name: json.loads((package / (name + ".metadata.json")).read_text()) for name in (panel_name, stata_name)}
    gap_diagnostics = {}
    for name, record in metadata.items():
        if record.get("data_sha256") != fingerprints[name]["sha256"]:
            raise ValueError(f"Companion checksum does not match {name}")
        if record.get("format_readiness_status") != "complete":
            raise ValueError(f"Format readiness failed and cannot be waived: {name}")
        issues = record.get("issues", []) + record.get("observation_validation", {}).get("issues", [])
        variables = {variable["name"]: variable for variable in record["variables"]}
        blocked = sorted({issue.get("code", "unknown") for issue in issues
                          if issue.get("code") not in FORMAT_NOTICES and not source_gap(issue, variables)})
        if blocked:
            raise ValueError(f"Validation failures cannot be waived as source gaps: {name}: {', '.join(blocked)}")
        gap_diagnostics[name] = []
        for issue in issues:
            if source_gap(issue, variables) and issue not in gap_diagnostics[name]:
                gap_diagnostics[name].append(issue)
    gaps = any(record.get("metadata_status") != "complete" or record.get("readiness_status") != "complete"
               for record in metadata.values()) or any(gap_diagnostics.values())
    if gaps and not allow_source_gaps:
        raise ValueError("Source metadata gaps remain; review them and use --allow-source-gaps to publish with an explicit disclosure")
    print("Comparing every Parquet value and missing value with the immutable corrected source", flush=True)
    parity = parquet_parity(base_panel, package / panel_name)
    names = parity["source_names"]
    for name, record in metadata.items():
        if (record.get("row_count") != parity["rows"] or record.get("column_count") != parity["columns"]
                or [variable["name"] for variable in record["variables"]] != names):
            raise ValueError(f"Metadata shape or named columns do not match the full panel: {name}")
    parquet_variables = {variable["name"]: variable for variable in metadata[panel_name]["variables"]}
    for field in pq.read_schema(package / panel_name):
        embedded = decode_embedded_variable_metadata(field)
        if embedded is None or embedded != parquet_variables[field.name]:
            raise ValueError(f"Embedded Parquet metadata differs from its companion: {field.name}")
    receipt = json.loads((package / "Checks/native_stata/validation.json").read_text())
    if (receipt.get("status") != "pass" or not receipt.get("exact_observations_in_export_representation")
            or receipt.get("rows") != parity["rows"] or receipt.get("columns") != parity["columns"]
            or receipt.get("variable_labels_checked") != parity["columns"]
            or receipt.get("value_labels_checked") != sum(len(variable.get("stata_value_labels", {}))
                                                          for variable in metadata[stata_name]["variables"])):
        raise ValueError("Native Stata full-panel verification did not pass")
    bound = receipt.get("inputs", {})
    base_hash = sha256(base_panel)
    if metadata[panel_name].get("source_panel_sha256") != base_hash:
        raise ValueError("Labeled Parquet source fingerprint is not the original corrected panel")
    if (bound.get("data", {}).get("sha256") != fingerprints[stata_name]["sha256"]
            or bound.get("metadata", {}).get("sha256") != fingerprints[stata_name + ".metadata.json"]["sha256"]
            or bound.get("source", {}).get("sha256") not in {base_hash, fingerprints[panel_name]["sha256"]}):
        raise ValueError("Native verification receipt is not bound to this data, metadata, and source")
    checked = []
    for index, chunk in enumerate(receipt.get("chunks", []), 1):
        if chunk.get("status") != "pass" or chunk.get("rows") != parity["rows"]:
            raise ValueError("Native verification has a missing or failed chunk")
        checked.extend(chunk["columns"])
        for filename, key in (("native_verify.log", "log_sha256"), ("native_verify.do", "do_file_sha256")):
            path = f"Checks/native_stata/chunk_{index:02d}/{filename}"
            if path not in fingerprints or fingerprints[path]["sha256"] != chunk.get(key):
                raise ValueError("Native verification commands or log differ from the receipt")
    if set(checked) != set(names):
        raise ValueError("Native verification does not cover every source column")
    for relative, before in snapshots.items():
        if identity(package / relative) != before:
            raise ValueError(f"Package changed during verification: {relative}")
    return {"metadata_gaps": gaps, "parquet_parity": parity, "native_stata_receipt_sha256": fingerprints["Checks/native_stata/validation.json"]["sha256"],
            "original_panel_sha256": base_hash, "artifacts": list(fingerprints.values()),
            "source_gap_diagnostics": gap_diagnostics,
            "export_provenance": {name: {key: value for key, value in record.items() if key in {
                "source_panel", "source_panel_sha256", "source_fingerprint", "metadata_sources", "metadata_source_sha256",
                "exporter_provenance", "upstream_export_provenance", "metadata_scope_policy", "metadata_status", "readiness_status"}}
                                  for name, record in metadata.items()}}, snapshots


def publish(root: Path, package: Path, *, apply: bool = False, allow_source_gaps: bool = False) -> dict:
    root, package = root.resolve(strict=True), package.resolve(strict=True)
    if package.parent != root / "Work" or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", package.name):
        raise ValueError("Package must be a named directory directly inside this data root's Work folder")
    if not package.is_dir() or package.stat().st_dev != root.stat().st_dev:
        raise ValueError("Publication requires a package on the same filesystem")
    destination = root / "Releases" / package.name
    if os.path.lexists(destination):
        raise ValueError("Refusing to overwrite an existing release")
    organize(root)
    layout = json.loads((root / "layout.json").read_text())
    base = root / "Releases" / RELEASE
    base_manifest = read_manifest(base, verify=False)
    base_digest = sha256(base / "manifest.json")
    base_panel = base / f"Panels/v2/{PANEL}"
    expected_hash = next(entry["sha256"] for entry in base_manifest["artifacts"] if entry["path"] == f"Panels/v2/{PANEL}")
    verified, snapshots = verify_package(package, base_panel, allow_source_gaps=allow_source_gaps)
    if verified["original_panel_sha256"] != expected_hash:
        raise ValueError("Original corrected source checksum differs from its immutable release manifest")
    manifest = {"schema_version": 1, "status": "verified", "release_name": package.name,
                "created_utc": datetime.now(timezone.utc).isoformat(), "base_release": RELEASE,
                "base_release_manifest_sha256": base_digest, **verified}
    result = {"status": "preview", "release": str(destination), "metadata_gaps": verified["metadata_gaps"],
              "rows": verified["parquet_parity"]["rows"], "columns": verified["parquet_parity"]["columns"]}
    if not apply:
        return result
    for name in PACKAGE_FILES:
        target = root / "Final" / name
        if os.path.lexists(target) and not target.is_symlink():
            raise ValueError(f"Refusing to replace a non-link Final artifact: {target}")
    staging = root / (".publish-labels-" + uuid.uuid4().hex)
    staging.mkdir()
    moved = []
    def move(source: Path, target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(source, target)
        moved.append((source, target))
    try:
        package_manifest = staging / "package_manifest.json"
        package_manifest.write_text(json.dumps(manifest, indent=2) + "\n")
        digest = sha256(package_manifest)
        layout.update(labeled_panel_release=package.name, labeled_panel_manifest_sha256=digest)
        (staging / "layout.json").write_text(json.dumps(layout, indent=2) + "\n")
        (staging / "Final").mkdir()
        for name in PACKAGE_FILES:
            (staging / "Final" / name).symlink_to(f"../Releases/{package.name}/{name}")
        (staging / "Final/manifest.json").write_text(json.dumps(labeled_final_manifest(root, base_manifest, base_digest, manifest, digest), indent=2) + "\n")
        gaps = ("**Source metadata gaps remain.** The companion JSON records unresolved definitions and code labels; this package does not claim complete metadata readiness."
                if verified["metadata_gaps"] else "Metadata and format readiness checks passed.")
        (staging / "Final/README.md").write_text(f"""# Start here

- [{PANEL}]({PANEL}): full corrected panel with embedded variable and source metadata.
- [{DTA}]({DTA}): full panel with native Stata variable and value labels.

Both contain {result['rows']:,} rows and {result['columns']:,} columns. Every Parquet value and missing value matches the immutable corrected source; all Stata columns and rows passed native validation.

{gaps}

Keep each dataset with its `.metadata.json`, `.dictionary.csv`, `.value_labels.csv`, and `.README.txt` companions. Changing historical meanings are labeled with explicit year ranges. Stata string categories use documented reversible numeric codes; original null strings become Stata empty strings. Stata/BE users must select fewer than 2,049 variables with `use varlist using`; Stata/SE or MP can load the full file.

[Metadata](Metadata) retains the original corrected dictionary, [value_lineage.parquet](value_lineage.parquet) traces source observations, and [SFA_2023_exports](SFA_2023_exports) contains the earlier 2023-only subset. The new labeled package is in `../Releases/{package.name}`; the original `../Releases/{RELEASE}` is unchanged. See [manifest.json](manifest.json) for checksums. Write new extracts to `../Work`.
""")
        move(package_manifest, package / "manifest.json")
        move(package, destination)
        for name in [*(f"Final/{name}" for name in PACKAGE_FILES), "Final/README.md", "Final/manifest.json", "layout.json"]:
            target = root / name
            if os.path.lexists(target):
                move(target, staging / "backup" / name)
            move(staging / name, target)
        for relative, before in snapshots.items():
            if identity(destination / relative) != before:
                raise ValueError(f"Package changed during publication: {relative}")
        verify_labeled_layer(root, layout, base_manifest, base_digest, verify=False)
    except BaseException:
        for source, target in reversed(moved):
            source.parent.mkdir(parents=True, exist_ok=True)
            os.replace(target, source)
        shutil.rmtree(staging)
        raise
    shutil.rmtree(staging)
    result.update(status="published", manifest_sha256=digest)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--package", required=True, type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--allow-source-gaps", action="store_true")
    args = parser.parse_args()
    print(json.dumps(publish(args.root, args.package, apply=args.apply, allow_source_gaps=args.allow_source_gaps), indent=2))


if __name__ == "__main__":
    main()
