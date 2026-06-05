#!/usr/bin/env python3
"""
QA 20: write runtime and dependency metadata for a release.

Reads:
- Python runtime
- installed Python package versions
- command-line tool versions
- requirements-lock.txt

Writes:
- Checks/release_metadata/environment_report.json
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from access_build_utils import DEFAULT_IPEDSDB_ROOT, data_layout, repo_root

DIRECT_PACKAGES = ["pandas", "pyarrow", "openpyxl", "requests", "beautifulsoup4", "duckdb", "pytest"]


def parse_args() -> argparse.Namespace:
    root = Path(os.environ.get("IPEDSDB_ROOT", str(DEFAULT_IPEDSDB_ROOT)))
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", default=str(root), help="External IPEDSDB_ROOT")
    p.add_argument("--repo-root", default=str(repo_root()))
    p.add_argument("--out", default=None)
    return p.parse_args()


def file_sha256(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def package_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return ""


def command_version(command: list[str]) -> str:
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip().splitlines()[0]
    except Exception:
        return ""


def pip_freeze() -> list[str]:
    try:
        out = subprocess.check_output([sys.executable, "-m", "pip", "freeze"], text=True, stderr=subprocess.DEVNULL)
    except Exception:
        return []
    return sorted(line.strip() for line in out.splitlines() if line.strip())


def main() -> int:
    args = parse_args()
    root = Path(args.root).expanduser()
    repo = Path(args.repo_root).expanduser()
    layout = data_layout(root)
    out_path = Path(args.out).expanduser() if args.out else layout.checks / "release_metadata" / "environment_report.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = repo / "requirements-lock.txt"
    report = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "repo_root": str(repo),
        "data_root": str(root),
        "python": {
            "executable": sys.executable,
            "version": sys.version.split()[0],
            "implementation": platform.python_implementation(),
        },
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "platform": platform.platform(),
        },
        "direct_packages": {name: package_version(name) for name in DIRECT_PACKAGES},
        "tools": {
            "mdb-tables": command_version(["mdb-tables", "--version"]),
            "mdb-export": command_version(["mdb-export", "--version"]),
            "duckdb_python": package_version("duckdb"),
        },
        "requirements_lock": {
            "path": "requirements-lock.txt",
            "sha256": file_sha256(lock_path),
            "lines": lock_path.read_text(encoding="utf-8").splitlines() if lock_path.exists() else [],
        },
        "pip_freeze": pip_freeze(),
    }
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote environment report to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
