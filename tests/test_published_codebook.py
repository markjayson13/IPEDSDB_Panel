"""Verify the shipped documentation without needing the external data drive."""
import csv
import gzip
import hashlib
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1] / "docs" / "codebook"


def test_published_assets_match_manifest_and_variable_inventory():
    manifest = json.loads((ROOT / "manifest.json").read_text())
    index = json.loads((ROOT / "index.json").read_text())
    assert manifest["generated_from"] == index["generated_from"]
    for artifact in manifest["artifacts"]:
        data = (ROOT / artifact["path"]).read_bytes()
        assert len(data) == artifact["size_bytes"], artifact["path"]
        assert hashlib.sha256(data).hexdigest() == artifact["sha256"], artifact["path"]
    summaries = {v["name"]: v for v in index["variables"]}
    details = {}
    for name in {v["detail_file"] for v in index["variables"]}:
        shard = json.loads((ROOT / name).read_text())
        assert not (set(details) & set(shard))
        details.update(shard)
    assert set(summaries) == set(details)
    assert len(details) == index["column_count"] == manifest["column_count"]
    for name, detail in details.items():
        assert detail["name"] == name
        for source in detail["source_records"]:
            definition = detail["definitions"][source["definition_index"]]
            assert set(source["years"]) <= set(definition["years"])
    filename = index["downloads"]["dictionary"]
    opener = gzip.open if filename.endswith(".gz") else open
    with opener(ROOT / filename, "rt", encoding="utf-8-sig") as file:
        rows = list(csv.DictReader(file))
    assert {row["name"] for row in rows} == set(details)


def test_pdf_has_a_working_destination_for_every_variable():
    pypdf = pytest.importorskip("pypdf")
    index = json.loads((ROOT / "index.json").read_text())
    pdf = pypdf.PdfReader(ROOT / index["downloads"]["pdf"])
    destinations = {d.title: d for d in pdf.outline}
    for variable in index["variables"]:
        assert variable["name"] in destinations
        assert 0 <= pdf.get_destination_page_number(destinations[variable["name"]]) < len(pdf.pages)
    # This regression was the user's original source-conflict concern.
    page = pdf.get_destination_page_number(destinations["UPGRNTN"])
    text = "\n".join(pdf.pages[i].extract_text() for i in range(page, min(page+4, len(pdf.pages))))
    assert "SFA2223_P1" in text
    assert "SFA2223_P2" in text
    assert "2023-sfa-v1" in text
