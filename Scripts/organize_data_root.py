#!/usr/bin/env python3
"""Preview or apply the folder-only reorganization of a verified IPEDS release.

No data files are rewritten or deleted. Renames stay on the same filesystem;
ordinary failures roll back completed renames. The layout marker is installed last.
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

RELEASE = "2023-sfa-v1"
PANEL = "panel_clean_prch_2004_2023.parquet"
BASELINE_DIRS = ("Panels", "Dictionary", "Checks", "Cross_sections", "build", "Stata")
FOLDERS = ("Final", "Sources", "Work", "Releases", "Archive")
LINKS = {
    PANEL: f"Panels/v2/{PANEL}",
    "Metadata": "Dictionary/v2",
    "value_lineage.parquet": "Checks/v2/wide_qc/qc_value_lineage.parquet",
    "SFA_2023_exports": "Exports",
}


def sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def identity(path: Path) -> tuple[int, int, int, int]:
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns


def read_manifest(release: Path, *, verify: bool, identities: dict | None = None) -> dict:
    manifest = json.loads((release / "manifest.json").read_text())
    if manifest.get("status") != "verified" or manifest.get("correction_version") != RELEASE:
        raise ValueError("The expected verified 2023-sfa-v1 release is required")
    required = {LINKS[PANEL], LINKS["value_lineage.parquet"]}
    required.update(f"Dictionary/v2/{name}.{extension}"
                    for name in ("dictionary_lake", "dictionary_codes")
                    for extension in ("csv", "parquet"))
    seen = set()
    for entry in manifest["artifacts"]:
        relative = Path(entry["path"])
        if relative.is_absolute() or ".." in relative.parts or str(relative) in seen:
            raise ValueError(f"Unsafe or duplicate release member: {relative}")
        seen.add(str(relative))
        path = release / relative
        if not path.resolve(strict=True).is_relative_to(release.resolve()):
            raise ValueError(f"Release member escapes its package: {relative}")
        before = identity(path)
        if path.stat().st_size != entry["bytes"]:
            raise ValueError(f"Release size mismatch: {relative}")
        if verify and sha256(path) != entry["sha256"]:
            raise ValueError(f"Release checksum mismatch: {relative}")
        if identity(path) != before:
            raise ValueError(f"Release member changed during verification: {relative}")
        if identities is not None:
            identities[str(relative)] = before
    if not required.issubset(seen) or not any(name.startswith("Exports/") for name in seen):
        raise ValueError("Verified release is missing required final artifacts")
    for target in LINKS.values():
        if not (release / target).exists():
            raise ValueError(f"Missing final link target: {target}")
    return manifest


def moves_for(root: Path) -> list[tuple[Path, Path]]:
    return [(root / "Raw_Access_Databases", Path("Sources/Raw_Access_Databases")),
            (root / "Metadata_repairs", Path("Releases"))] + [
        (root / name, Path("Archive/pre_metadata_repair") / name)
        for name in BASELINE_DIRS if (root / name).exists()
    ]


def final_manifest(root: Path, manifest: dict, digest: str) -> dict:
    entries = []
    for entry in manifest["artifacts"]:
        relative = entry["path"]
        alias = next((name + relative[len(target):] for name, target in LINKS.items()
                      if relative == target or relative.startswith(target + "/")), None)
        if alias:
            entries.append({"path": f"Final/{alias}", "absolute_path": str(root / "Final" / alias),
                            "release_path": f"Releases/{RELEASE}/{relative}",
                            "bytes": entry["bytes"], "sha256": entry["sha256"]})
    return {"schema_version": 1, "current_release": RELEASE,
            "release_manifest": f"Releases/{RELEASE}/manifest.json",
            "release_manifest_sha256": digest,
            "main_panel": f"Final/{PANEL}",
            "export_scope": "SFA_2023_exports contains the corrected 2023 SFA subset, not the full panel",
            "artifacts": entries}


def write_guides(staging: Path, root: Path, manifest: dict, digest: str, moves: list) -> None:
    (staging / "README.md").write_text(f"""# IPEDS datasets

Start with **[Final/{PANEL}](Final/{PANEL})**.
This is the verified corrected 2004–2023 panel: 141,711 rows and 2,721 columns.
See [Final/README.md](Final/README.md) for its dictionary, lineage and exports.

| Folder | Purpose |
| --- | --- |
| Final | Current analysis panel and matching metadata; links use no extra data space. |
| Sources | Original Access releases and extracted source tables. |
| Work | New pipeline builds, custom exports and logs. These are drafts. |
| Releases | Verified versioned packages, including evidence and reproduction receipts. |
| Archive | Earlier panels and supporting files retained for comparison and reproduction. |

Scripts default to Final for analysis and Work for new builds. A build does not
automatically replace the verified Final release. The repository is at
`{Path(__file__).resolve().parents[1]}`.

This was a folder-only reorganization. Original release manifests and audit
receipts retain their historical paths and hashes. `layout.json` records the
path moves; `Final/manifest.json` lists current paths and checksums.
""")
    (staging / "Final/README.md").write_text(f"""# Start here

**[{PANEL}]({PANEL})** is the complete corrected 2004–2023 panel
(141,711 institution-year rows × 2,721 columns).

- [Metadata/dictionary_lake.parquet](Metadata/dictionary_lake.parquet): variable
  definitions, years, units and source metadata; CSV is available beside it.
- [Metadata/dictionary_codes.parquet](Metadata/dictionary_codes.parquet): category
  codes and labels; CSV is available beside it.
- [value_lineage.parquet](value_lineage.parquet): source lineage for panel values.
- [SFA_2023_exports](SFA_2023_exports): ready-made Stata, Excel, CSV and Parquet
  exports of the corrected **2023 SFA subset only** (6,163 rows × 345 columns).
  Keep each export's metadata, dictionary, labels and README sidecars together.
- [manifest.json](manifest.json): current paths and SHA-256 checksums.

The full panel and its metadata are linked to `../Releases/{RELEASE}`.
Avoid editing these files in place. Use `Scripts/08_build_custom_panel.py --root
"{root}" --vars ... --output ... --strict --require-ready` to create an analysis export
with labels and sidecars. New outputs belong in `../Work`.
""")
    (staging / "Final/manifest.json").write_text(json.dumps(final_manifest(root, manifest, digest), indent=2) + "\n")
    layout = {"schema_version": 1, "current_release": RELEASE,
              "organized_utc": datetime.now(timezone.utc).isoformat(),
              "release_manifest_sha256": digest,
              "relocations": [{"from": str(source.relative_to(root)), "to": str(target)}
                              for source, target in moves],
              "historical_paths": "Original release receipts are preserved unchanged; apply relocations above."}
    (staging / "layout.json").write_text(json.dumps(layout, indent=2) + "\n")


def organize(root: Path, *, apply: bool = False, expected_manifest_sha256: str | None = None) -> dict:
    root = root.resolve(strict=True)
    marker = root / "layout.json"
    if marker.exists():
        layout = json.loads(marker.read_text())
        if layout.get("schema_version") != 1 or layout.get("current_release") != RELEASE:
            raise ValueError("Unrecognized existing layout")
        release = root / "Releases" / RELEASE
        manifest = read_manifest(release, verify=apply)
        digest = sha256(release / "manifest.json")
        if digest != layout["release_manifest_sha256"]:
            raise ValueError("Original release manifest has changed")
        if expected_manifest_sha256 and digest != expected_manifest_sha256:
            raise ValueError("Unexpected release manifest checksum")
        for name, target in LINKS.items():
            if name == PANEL and layout.get("labeled_panel_release"):
                continue
            if not (root / "Final" / name).is_symlink() or (root / "Final" / name).resolve(strict=True) != release / target:
                raise ValueError(f"Final link is not the verified release: {name}")
        if layout.get("labeled_panel_release"):
            from publish_labeled_panel import verify_labeled_layer
            verify_labeled_layer(root, layout, manifest, digest, verify=apply)
        elif json.loads((root / "Final/manifest.json").read_text()) != final_manifest(root, manifest, digest):
            raise ValueError("Final manifest does not match the verified release")
        return {"status": "already_organized", "verified_checksums": apply,
                "artifacts": len(manifest["artifacts"]), "manifest_sha256": digest}
    for name in (*FOLDERS, "README.md", "layout.json"):
        if os.path.lexists(root / name):
            raise ValueError(f"Destination already exists: {root / name}")
    moves = moves_for(root)
    for source, _ in moves:
        if not source.is_dir() or source.is_symlink():
            raise ValueError(f"Expected a real source directory: {source}")
        if source.stat().st_dev != root.stat().st_dev:
            raise ValueError("Reorganization requires same-filesystem renames")
    release = root / "Metadata_repairs" / RELEASE
    identities = {"manifest.json": identity(release / "manifest.json")}
    manifest = read_manifest(release, verify=apply, identities=identities)
    digest = sha256(release / "manifest.json")
    if identity(release / "manifest.json") != identities["manifest.json"]:
        raise ValueError("Release manifest changed during verification")
    if expected_manifest_sha256 and digest != expected_manifest_sha256:
        raise ValueError("Unexpected release manifest checksum")
    result = {"status": "preview", "manifest_sha256": digest,
              "artifacts": len(manifest["artifacts"]),
              "moves": [{"from": str(source), "to": str(root / target)} for source, target in moves],
              "final_panel": str(root / "Final" / PANEL)}
    if not apply:
        return result
    # Record existing baseline symlink targets by inode. Moving the baseline
    # directories together must preserve every internal reference.
    links = []
    for source, target in moves:
        for path in source.rglob("*"):
            if path.is_symlink():
                stat = path.stat()  # A broken pre-existing link requires review.
                links.append((target / path.relative_to(source), (stat.st_dev, stat.st_ino)))
    staging = root / f".organize-staging-{uuid.uuid4().hex}"
    staging.mkdir()
    completed = []
    try:
        (staging / "Final").mkdir()
        (staging / "Archive/pre_metadata_repair").mkdir(parents=True)
        for name in ("Dictionary", "Panels", "Checks", "Cross_sections", "build"):
            (staging / "Work" / name).mkdir(parents=True)
        for source, target in moves:
            destination = staging / target
            destination.parent.mkdir(parents=True, exist_ok=True)
            os.rename(source, destination)
            completed.append((source, destination))
        for name, target in LINKS.items():
            (staging / "Final" / name).symlink_to(f"../Releases/{RELEASE}/{target}")
        write_guides(staging, root, manifest, digest, moves)
        for name in (*FOLDERS, "README.md"):
            os.rename(staging / name, root / name)
            completed.append((staging / name, root / name))
        for relative, link_identity in links:
            stat = (root / relative).stat()
            if (stat.st_dev, stat.st_ino) != link_identity:
                raise ValueError(f"Existing symlink target changed: {relative}")
        for name, target in LINKS.items():
            if (root / "Final" / name).resolve(strict=True) != root / "Releases" / RELEASE / target:
                raise ValueError(f"Final link verification failed: {name}")
        for relative, before in identities.items():
            if identity(root / "Releases" / RELEASE / relative) != before:
                raise ValueError(f"Release member changed during reorganization: {relative}")
        os.rename(staging / "layout.json", marker)
        completed.append((staging / "layout.json", marker))
        staging.rmdir()
    except BaseException:
        # Rollback errors intentionally leave staging in place for recovery.
        for source, destination in reversed(completed):
            os.rename(destination, source)
        shutil.rmtree(staging)
        raise
    result.update(status="organized", verified_checksums=True, preserved_symlinks=len(links))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("/Volumes/CIRAGO/IPEDSDB_PANEL"))
    parser.add_argument("--apply", action="store_true", help="Verify all release hashes and apply; default is read-only preview")
    parser.add_argument("--expected-manifest-sha256", help="Require the reviewed release manifest checksum")
    args = parser.parse_args()
    print(json.dumps(organize(args.root, apply=args.apply,
                              expected_manifest_sha256=args.expected_manifest_sha256), indent=2))


if __name__ == "__main__":
    main()
