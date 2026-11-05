"""
The committed schema preview stays a literal excerpt of the public codebook.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PREVIEW_FIELDS = [
    "name",
    "label",
    "storage_type",
    "stata_name",
    "observed_years",
    "sources",
    "metadata_status",
    "null_count",
]
PREVIEW_ORDER = [
    "UNITID",
    "year",
    "INSTNM",
    "STABBR",
    "SECTOR",
    "CONTROL",
    "ICLEVEL",
    "HLOFFER",
    "LOCALE",
    "INSTSIZE",
    "OPENADMP",
    "PSET4FLG",
    "TUITION2",
    "FEE1",
    "UPGRNTN",
    "UPGRNTT",
]


def test_schema_preview_matches_codebook_rows() -> None:
    with (ROOT / "Artifacts" / "panel_schema_preview.csv").open(encoding="utf-8", newline="") as handle:
        preview = list(csv.DictReader(handle))
    with (ROOT / "docs" / "codebook" / "codebook.csv").open(encoding="utf-8-sig", newline="") as handle:
        codebook = {row["name"]: row for row in csv.DictReader(handle)}

    assert [row["name"] for row in preview] == PREVIEW_ORDER
    for row in preview:
        source = codebook[row["name"]]
        for field in PREVIEW_FIELDS:
            assert row[field] == source[field]


def test_readme_uses_codebook_manifest_counts() -> None:
    manifest = json.loads((ROOT / "docs" / "codebook" / "manifest.json").read_text(encoding="utf-8"))
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert f"{manifest['row_count']:,}" in readme
    assert f"{manifest['column_count']:,}" in readme
    assert "39/39" not in readme
    citation = (ROOT / "CITATION.cff").read_text(encoding="utf-8")
    assert f"{manifest['row_count']:,}" in citation
    assert f"{manifest['column_count']:,}" in citation
