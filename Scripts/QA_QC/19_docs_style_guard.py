#!/usr/bin/env python3
"""
QA 19: scan release-facing prose for common generated-text patterns.

Reads:
- markdown, text, reStructuredText, and HTML files in the repository

Writes:
- no files
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PROSE_SUFFIXES = {".md", ".txt", ".rst", ".html"}
SKIP_PARTS = {".git", ".venv", "__pycache__", ".pytest_cache"}

PATTERNS = [
    (re.compile(r"\b(in plain terms|black box|opening this repo cold|if you only remember)\b", re.I), "template phrase"),
    (
        re.compile(
            r"\b(delve|leverage|utilize|robust|seamless|streamline|pivotal|landscape|realm|unlock|harness|foster|elevate|empower)\b",
            re.I,
        ),
        "stock verb or noun",
    ),
    (
        re.compile(
            r"\b(cutting-edge|state-of-the-art|game-chang\w*|transformative|holistic|meticulous|sophisticated|best-in-class)\b",
            re.I,
        ),
        "inflated descriptor",
    ),
    (re.compile(r"\bnot only\b.{0,120}\bbut also\b", re.I | re.S), "not only/but also frame"),
    (re.compile(r"\b(moreover|furthermore|additionally|in conclusion)\b", re.I), "stock transition"),
    (re.compile(r"\b(serves as|testament|journey|navigate|tapestry)\b", re.I), "stock explanatory phrase"),
    (re.compile(r"\b(low-friction|no-brainer|magical|easy to trust)\b", re.I), "promotional phrase"),
    (re.compile(r"[—–]"), "dash style"),
    (re.compile(r"[💡✅❌🚀🎯]"), "emoji marker"),
    (re.compile(r"\*\*[^*\n]+\*\*"), "bold inline label"),
    (
        re.compile(r"^#{1,4} .*[A-Z][a-z]+ (And|The|To|In|Of|A|Is|Not|Does|What|How|When|Where|Why)\b", re.M),
        "title-case heading",
    ),
]

SCRIPT_JSON = re.compile(
    r'<script id="variable-browser-data" type="application/json">.*?</script>',
    re.S,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(REPO_ROOT), help="Repository root")
    return p.parse_args()


def prose_files(repo: Path) -> list[Path]:
    out: list[Path] = []
    for path in repo.rglob("*"):
        if any(part in SKIP_PARTS for part in path.parts):
            continue
        if path.is_file() and path.suffix.lower() in PROSE_SUFFIXES:
            out.append(path)
    return sorted(out)


def cleaned_text(path: Path) -> str:
    text = path.read_text(encoding="utf-8", errors="ignore")
    return SCRIPT_JSON.sub("", text)


def main() -> int:
    args = parse_args()
    repo = Path(args.repo_root).resolve()
    failures: list[str] = []

    for path in prose_files(repo):
        rel = path.relative_to(repo)
        text = cleaned_text(path)
        for pattern, label in PATTERNS:
            for match in pattern.finditer(text):
                line = text.count("\n", 0, match.start()) + 1
                excerpt = " ".join(match.group(0).split())
                failures.append(f"{rel}:{line}: {label}: {excerpt}")

    if failures:
        print("Docs style guard failed:")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print("Docs style guard passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
