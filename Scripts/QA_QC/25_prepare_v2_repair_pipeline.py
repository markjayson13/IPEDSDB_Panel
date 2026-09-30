#!/usr/bin/env python3
"""Recreate the preserved v2 pipeline with the narrowly versioned metadata repair."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tarfile

BASE_COMMIT = "799a63930f97df9a3ec35be857f340ea3743afac"
BASE_ARCHIVE = Path("contracts/reproduction/v2-baseline-799a639.tar.gz")
BASE_ARCHIVE_SHA256 = "53ba61017aef0985edc3743475842f801a2ec2758338e15677b67553ef7169fc"
POLICY_SHA256 = "67fca57085ec759944152f6e6e38374fac75991ef2dbd0489d241641d310e4f3"


def digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def prepare(output: Path, repository: Path) -> dict:
    if output.exists():
        raise ValueError("Pipeline output must be a new directory")
    repository = repository.resolve()
    patch = repository / "contracts/source_metadata_corrections/v2-pipeline.patch"
    # The original commit is not in the public Git history. Keep the exact
    # reviewed source bytes available to shallow clones and source ZIP users.
    with (repository / BASE_ARCHIVE).open("rb") as archive:
        if hashlib.file_digest(archive, "sha256").hexdigest() != BASE_ARCHIVE_SHA256:
            raise ValueError("Preserved v2 source archive checksum mismatch")
        archive.seek(0)
        with tarfile.open(fileobj=archive, mode="r:gz") as bundle:
            for member in bundle.getmembers():
                path = PurePosixPath(member.name)
                if path.is_absolute() or ".." in path.parts or not (member.isfile() or member.isdir()):
                    raise ValueError(f"Unsafe preserved v2 archive member: {member.name}")
            output.mkdir(parents=True)
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
    receipt = {"base_commit": BASE_COMMIT,
               "base_archive": {"path": str(BASE_ARCHIVE), "sha256": BASE_ARCHIVE_SHA256},
               "patch_sha256": digest(patch), "prch_policy_sha256": POLICY_SHA256,
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
