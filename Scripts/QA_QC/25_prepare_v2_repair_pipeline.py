#!/usr/bin/env python3
"""Recreate the preserved v2 pipeline with the narrowly versioned metadata repair."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile

BASE_COMMIT = "799a63930f97df9a3ec35be857f340ea3743afac"
POLICY_SHA256 = "67fca57085ec759944152f6e6e38374fac75991ef2dbd0489d241641d310e4f3"


def digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def prepare(output: Path, repository: Path) -> dict:
    if output.exists():
        raise ValueError("Pipeline output must be a new directory")
    patch = repository / "contracts/source_metadata_corrections/v2-pipeline.patch"
    output.mkdir(parents=True)
    with tempfile.TemporaryFile() as archive:
        subprocess.run(["git", "archive", BASE_COMMIT], cwd=repository, stdout=archive, check=True)
        archive.seek(0)
        with tarfile.open(fileobj=archive) as bundle:
            bundle.extractall(output, filter="data")
    subprocess.run(["git", "apply", str(patch)], cwd=output, check=True)
    copied = [repository / "Scripts/source_metadata_corrections.py",
              *sorted((repository / "contracts/source_metadata_corrections").glob("2023-sfa-v1.*"))]
    for source in copied:
        target = output / source.relative_to(repository)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    if digest(output / "contracts/prch_policy.csv") != POLICY_SHA256:
        raise ValueError("Preserved PRCH policy differs from the original verified policy")
    receipt = {"base_commit": BASE_COMMIT, "patch_sha256": digest(patch), "prch_policy_sha256": POLICY_SHA256,
               "overlay": [{"path": str(path.relative_to(repository)), "sha256": digest(path)} for path in copied],
               "pipeline_scripts": [{"path": str(path.relative_to(output)), "sha256": digest(path)}
                                    for path in sorted((output / "Scripts").rglob("*.py"))]}
    (output / "metadata_repair_overlay.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    result = prepare(args.output, args.repository)
    print(json.dumps({"base_commit": result["base_commit"], "patch_sha256": result["patch_sha256"]}))
