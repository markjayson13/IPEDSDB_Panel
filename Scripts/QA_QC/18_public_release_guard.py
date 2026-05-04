#!/usr/bin/env python3
"""
QA 18: check public-release repository files.

Reads:
- release policy files in the repository
- citation and archive metadata
- GitHub ownership and intake files

Writes:
- no files
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]

REQUIRED_FILES = [
    "LICENSE",
    "DATA_LICENSE.md",
    "CONTACT.md",
    "ACKNOWLEDGMENTS.md",
    "CITATION.cff",
    "codemeta.json",
    ".zenodo.json",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "GOVERNANCE.md",
    "CHANGELOG.md",
    ".github/CODEOWNERS",
    ".github/PULL_REQUEST_TEMPLATE.md",
    ".github/dependabot.yml",
    ".github/workflows/ci.yml",
    ".github/workflows/repo-size-guard.yml",
    ".github/workflows/scorecard.yml",
    ".github/ISSUE_TEMPLATE/config.yml",
    ".github/ISSUE_TEMPLATE/data_bug.yml",
    ".github/ISSUE_TEMPLATE/variable_mapping.yml",
    ".github/ISSUE_TEMPLATE/reproducibility_failure.yml",
    ".github/ISSUE_TEMPLATE/documentation.yml",
]

NO_PLACEHOLDER = re.compile(r"\b(TBD|TODO|FIXME|INSERT|CHANGEME)\b", re.IGNORECASE)
LOCAL_PATH = re.compile(
    "|".join(
        [
            r"/Users/" + "markjaysonfarol13",
            "Documents/" + "GitHub",
            "Projects/" + "IPEDSDB" + "_Paneling",
            "IPEDSDB" + "_Paneling",
        ]
    )
)
TEXT_SUFFIXES = {".md", ".txt", ".rst", ".html", ".svg", ".py", ".sh", ".json", ".toml", ".yml", ".yaml", ".cff", ".csv"}
SKIP_PARTS = {".git", ".venv", "__pycache__", ".pytest_cache"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(REPO_ROOT), help="Repository root")
    return p.parse_args()


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def load_json(path: Path, failures: list[str]) -> dict[str, Any]:
    try:
        return json.loads(read_text(path))
    except Exception as exc:
        failures.append(f"{path.name} is not valid JSON: {exc}")
        return {}


def load_toml(path: Path, failures: list[str]) -> dict[str, Any]:
    try:
        with path.open("rb") as fh:
            return tomllib.load(fh)
    except Exception as exc:
        failures.append(f"{path.name} is not valid TOML: {exc}")
        return {}


def has_line(text: str, expected: str) -> bool:
    return any(line.strip() == expected for line in text.splitlines())


def check_required_files(repo: Path, failures: list[str]) -> None:
    for rel in REQUIRED_FILES:
        path = repo / rel
        if not path.exists() or not path.is_file():
            failures.append(f"missing required file: {rel}")
            continue
        text = read_text(path)
        match = NO_PLACEHOLDER.search(text)
        if match:
            failures.append(f"{rel} contains placeholder marker: {match.group(0)}")


def check_local_path_leaks(repo: Path, failures: list[str]) -> None:
    for path in sorted(repo.rglob("*")):
        if any(part in SKIP_PARTS for part in path.parts):
            continue
        if not path.is_file():
            continue
        if path.suffix.lower() not in TEXT_SUFFIXES and path.name not in {"LICENSE"}:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        match = LOCAL_PATH.search(text)
        if match:
            rel = path.relative_to(repo)
            line = text.count("\n", 0, match.start()) + 1
            failures.append(f"{rel}:{line} contains local path text: {match.group(0)}")


def check_citation(repo: Path, failures: list[str]) -> None:
    text = read_text(repo / "CITATION.cff")
    required = [
        'title: "IPEDSDB_Panel"',
        'family-names: "Farol"',
        'given-names: "Mark Jayson"',
        'website: "https://markjayson.com"',
        'url: "https://github.com/markjayson13/IPEDSDB_Panel"',
        'license: "CC-BY-4.0"',
    ]
    for item in required:
        if item not in text:
            failures.append(f"CITATION.cff missing {item}")


def check_json_metadata(repo: Path, failures: list[str]) -> None:
    codemeta = load_json(repo / "codemeta.json", failures)
    if codemeta.get("name") != "IPEDSDB_Panel":
        failures.append("codemeta.json name is not IPEDSDB_Panel")
    if "license" not in codemeta:
        failures.append("codemeta.json missing license")
    author = (codemeta.get("author") or [{}])[0]
    if author.get("url") != "https://markjayson.com":
        failures.append("codemeta.json missing author url")
    maintainer = codemeta.get("maintainer", {})
    if maintainer.get("url") != "https://markjayson.com":
        failures.append("codemeta.json missing maintainer url")
    contributors = codemeta.get("contributor", [])
    mentor = contributors[0] if contributors else {}
    if mentor.get("familyName") != "Assane" or mentor.get("roleName") != "Research mentor":
        failures.append("codemeta.json missing Djeto Assane research mentor contributor")
    if mentor.get("url") != "https://www.unlv.edu/people/djeto-assane":
        failures.append("codemeta.json missing Djeto Assane UNLV profile")
    same_as = mentor.get("sameAs", [])
    if "https://scholar.google.com/citations?user=VJYYWTIAAAAJ" not in same_as:
        failures.append("codemeta.json missing Djeto Assane Google Scholar profile")
    if codemeta.get("codeRepository") != "https://github.com/markjayson13/IPEDSDB_Panel":
        failures.append("codemeta.json missing codeRepository")

    zenodo = load_json(repo / ".zenodo.json", failures)
    if zenodo.get("title") != "IPEDSDB_Panel":
        failures.append(".zenodo.json title is not IPEDSDB_Panel")
    if zenodo.get("license") != "mit":
        failures.append(".zenodo.json license should be mit for the repository archive")
    creators = zenodo.get("creators", [])
    if not creators or creators[0].get("name") != "Farol, Mark Jayson":
        failures.append(".zenodo.json missing creator Farol, Mark Jayson")
    contributors = zenodo.get("contributors", [])
    if not any(row.get("name") == "Assane, Djeto" and row.get("type") == "Supervisor" for row in contributors):
        failures.append(".zenodo.json missing Djeto Assane supervisor contributor")
    related = zenodo.get("related_identifiers", [])
    if not any(row.get("identifier") == "https://markjayson.com" for row in related):
        failures.append(".zenodo.json missing maintainer website related identifier")
    if not any(row.get("identifier") == "https://www.unlv.edu/people/djeto-assane" for row in related):
        failures.append(".zenodo.json missing Djeto Assane UNLV profile related identifier")


def check_contract(repo: Path, failures: list[str]) -> None:
    contract = load_toml(repo / "contracts" / "panel_spec.toml", failures)
    status = str(contract.get("status", ""))
    if status not in {"release-candidate", "active"}:
        failures.append("contracts/panel_spec.toml status should be release-candidate or active before public release")
    release = contract.get("release", {})
    if release.get("final_only") is not True or release.get("include_provisional") is not False:
        failures.append("contracts/panel_spec.toml must declare final-only release inputs")


def check_ownership(repo: Path, failures: list[str]) -> None:
    owners = read_text(repo / ".github" / "CODEOWNERS")
    if not has_line(owners, "* @markjayson13"):
        failures.append(".github/CODEOWNERS must assign all paths to @markjayson13")

    governance = read_text(repo / "GOVERNANCE.md")
    if "sole researcher, maintainer, and code owner" not in governance:
        failures.append("GOVERNANCE.md must state sole ownership")
    contact = read_text(repo / "CONTACT.md")
    if "https://markjayson.com" not in contact:
        failures.append("CONTACT.md must include https://markjayson.com")
    acknowledgments = read_text(repo / "ACKNOWLEDGMENTS.md")
    for phrase in [
        "Djeto Assane",
        "research mentor",
        "https://www.unlv.edu/people/djeto-assane",
        "https://scholar.google.com/citations?user=VJYYWTIAAAAJ",
    ]:
        if phrase not in acknowledgments:
            failures.append(f"ACKNOWLEDGMENTS.md missing {phrase}")


def check_readme(repo: Path, failures: list[str]) -> None:
    text = read_text(repo / "README.md")
    for phrase in [
        "## Public release checklist",
        "## License and reuse",
        "## Acknowledgments",
        "bash Scripts/QA_QC/release_gate.sh",
    ]:
        if phrase not in text:
            failures.append(f"README.md missing public-release phrase: {phrase}")


def main() -> int:
    args = parse_args()
    repo = Path(args.repo_root).resolve()
    failures: list[str] = []
    check_required_files(repo, failures)
    check_local_path_leaks(repo, failures)
    if not failures:
        check_citation(repo, failures)
        check_json_metadata(repo, failures)
        check_contract(repo, failures)
        check_ownership(repo, failures)
        check_readme(repo, failures)

    if failures:
        print("Public release guard failed:")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print("Public release guard passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
