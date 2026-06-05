#!/usr/bin/env python3
"""
QA 17: write release build provenance.

Reads:
- `Checks/release_manifest/release_manifest.csv`

Writes:
- `Checks/release_metadata/build_provenance.json`
"""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from access_build_utils import DEFAULT_IPEDSDB_ROOT, data_layout, repo_root


def parse_args() -> argparse.Namespace:
    root = Path(os.environ.get("IPEDSDB_ROOT", str(DEFAULT_IPEDSDB_ROOT)))
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", default=str(root), help="External IPEDSDB_ROOT")
    p.add_argument("--manifest", default=None)
    p.add_argument("--out", default=None)
    p.add_argument("--repo-root", default=str(repo_root()))
    p.add_argument("--build-type", default="https://github.com/markjayson13/IPEDSDB_Panel/release-build/v1")
    return p.parse_args()


def git_value(args: list[str], cwd: Path) -> str:
    try:
        return subprocess.check_output(["git", *args], cwd=cwd, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return ""


def package_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return ""


def tool_version(command: list[str]) -> str:
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip().splitlines()[0]
    except Exception:
        return ""


def subject_rows(manifest: pd.DataFrame) -> list[dict[str, object]]:
    rows = []
    for _, row in manifest.iterrows():
        if str(row.get("present", "")).strip().lower() not in {"true", "1", "yes", "y"}:
            continue
        sha = str(row.get("sha256", "")).strip()
        if not sha:
            continue
        rows.append(
            {
                "name": str(row.get("relative_path", "")).strip(),
                "digest": {"sha256": sha},
                "role": str(row.get("role", "")).strip(),
                "pathBase": str(row.get("path_base", "")).strip(),
            }
        )
    return rows


def main() -> int:
    args = parse_args()
    root = Path(args.root).expanduser()
    repo = Path(args.repo_root).expanduser()
    layout = data_layout(root)
    manifest_path = Path(args.manifest).expanduser() if args.manifest else layout.checks / "release_manifest" / "release_manifest.csv"
    out_path = Path(args.out).expanduser() if args.out else layout.checks / "release_metadata" / "build_provenance.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    manifest = pd.read_csv(manifest_path, dtype=str).fillna("")
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    provenance = {
        "_type": "https://in-toto.io/Statement/v1",
        "predicateType": "https://slsa.dev/provenance/v1",
        "subject": subject_rows(manifest),
        "predicate": {
            "buildDefinition": {
                "buildType": args.build_type,
                "externalParameters": {
                    "root": str(root),
                    "manifest": str(manifest_path),
                    "repository": "https://github.com/markjayson13/IPEDSDB_Panel",
                    "gitCommit": git_value(["rev-parse", "HEAD"], repo),
                    "gitBranch": git_value(["branch", "--show-current"], repo),
                },
                "internalParameters": {
                    "python": sys.version.split()[0],
                    "platform": platform.platform(),
                    "packages": {
                        "pandas": package_version("pandas"),
                        "pyarrow": package_version("pyarrow"),
                        "duckdb": package_version("duckdb"),
                    },
                    "tools": {
                        "mdb-tables": tool_version(["mdb-tables", "--version"]),
                    },
                },
                "resolvedDependencies": [
                    {
                        "uri": "git+https://github.com/markjayson13/IPEDSDB_Panel",
                        "digest": {"gitCommit": git_value(["rev-parse", "HEAD"], repo)},
                    }
                ],
            },
            "runDetails": {
                "builder": {
                    "id": "local:Scripts/QA_QC/17_build_provenance.py",
                    "version": {"script": "1"},
                },
                "metadata": {
                    "invocationId": now,
                    "startedOn": now,
                    "finishedOn": now,
                },
                "byproducts": [
                    {"name": "release_manifest.csv", "uri": str(manifest_path)},
                ],
            },
        },
    }
    out_path.write_text(json.dumps(provenance, indent=2), encoding="utf-8")
    print(f"Wrote build provenance to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
