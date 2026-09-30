"""Verify shipped documentation without needing the external data drive."""
import csv
import gzip
import hashlib
import json
from pathlib import Path

import pytest

DOCS = Path(__file__).parents[1] / "docs"


@pytest.fixture(params=["final", "provisional"])
def published(request):
    root = DOCS / ("codebook" if request.param == "final" else "provisional/codebook")
    return root, request.param


def variable_detail(root, index, name):
    variable = next(row for row in index["variables"] if row["name"] == name)
    return json.loads((root / variable["detail_file"]).read_text())[name]


def test_published_assets_match_manifest_and_variable_inventory(published):
    root, _ = published
    manifest = json.loads((root / "manifest.json").read_text())
    index = json.loads((root / "index.json").read_text())
    assert manifest["generated_from"] == index["generated_from"]
    for artifact in manifest["artifacts"]:
        data = (root / artifact["path"]).read_bytes()
        assert len(data) == artifact["size_bytes"], artifact["path"]
        assert hashlib.sha256(data).hexdigest() == artifact["sha256"], artifact["path"]
    summaries = {v["name"]: v for v in index["variables"]}
    details = {}
    for name in {v["detail_file"] for v in index["variables"]}:
        shard = json.loads((root / name).read_text())
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
    with opener(root / filename, "rt", encoding="utf-8-sig") as file:
        rows = list(csv.DictReader(file))
    assert {row["name"] for row in rows} == set(details)


def test_release_scope_and_pell_source_period_are_explicit(published):
    root, release = published
    index = json.loads((root / "index.json").read_text())
    end_year = 2023 if release == "final" else 2024
    assert index["years"] == list(range(2004, end_year + 1))
    expected = "full-panel-labels-v1" if release == "final" else "2024-extension-v1"
    assert index["release"] == expected
    if release == "final":
        assert index.get("release_status", "Final") == "Final"
        assert index["row_count"] == 141711
    else:
        assert "mixed" in index["release_status"].lower()
        assert "provisional" in index["release_status"].lower()
        assert index["row_count"] == 147782
        assert index["column_count"] == 2676
        quarantine = [note for note in index["notes"] if "quarantines two" in note]
        assert len(quarantine) == 1
        assert all(token in quarantine[0] for token in ("111111", "2019", "2024", "unchanged"))
        assert index["codebook_url"].endswith("/provisional/")
    for name in ("UPGRNTN", "UPGRNTT"):
        detail = variable_detail(root, index, name)
        old = [r for r in detail["source_records"] if 2023 in r["years"]]
        assert old and all(r["table"] == "SFA2223_P1" for r in old)
        assert all(r["original_table"] == "SFA2223_P2" for r in old)
        if release == "provisional":
            current = [r for r in detail["source_records"] if 2024 in r["years"]]
            assert current and all(r["table"] == "SFA2324" for r in current)
            for record in current:
                assert record["release_type"] == "Provisional"
                assert record["source_release_date"] == "2025-12-09"
                period = record["source_table_reference_period"]
                assert "2023" in period and "2024" in period
                assert "fall 2023" in record["reporting_population_note"]
                assert "2023-24" in record["reporting_population_note"]
                assert record["source_archive_sha256"] and record["source_member_sha256"]


def test_pdf_has_a_working_destination_for_every_variable(published):
    pypdf = pytest.importorskip("pypdf")
    root, release = published
    index = json.loads((root / "index.json").read_text())
    pdf = pypdf.PdfReader(root / index["downloads"]["pdf"])
    destinations = {d.title: d for d in pdf.outline}
    for variable in index["variables"]:
        assert variable["name"] in destinations
        assert 0 <= pdf.get_destination_page_number(destinations[variable["name"]]) < len(pdf.pages)
    # The extended entry can span more pages; inspect its whole bookmark range.
    page = pdf.get_destination_page_number(destinations["UPGRNTN"])
    later_pages = [pdf.get_destination_page_number(value) for value in destinations.values()
                   if pdf.get_destination_page_number(value) > page]
    end = min(later_pages, default=len(pdf.pages))
    text = "\n".join(pdf.pages[i].extract_text() for i in range(page, end))
    assert "SFA2223_P1" in text
    assert "SFA2223_P2" in text
    assert "2023-sfa-v1" in text
    if release == "provisional":
        assert "SFA2324" in text
        assert "Provisional" in text
        introduction = " ".join("\n".join(page.extract_text() for page in pdf.pages[:8]).split())
        assert "111111 in 2019 and 2024" in introduction
        assert "IPEDSDB_Panel/provisional/" in introduction


def test_release_interfaces_use_separate_data_and_shared_assets():
    final = (DOCS / "index.html").read_text()
    extension = (DOCS / "provisional/index.html").read_text()
    assert 'href="./provisional/"' in final
    assert 'href="../assets/codebook.css"' in extension
    assert 'src="../assets/codebook.js"' in extension
    assert 'href="./codebook/ipeds-panel-codebook.pdf"' in extension
    assert 'href="../"' in extension
    assert "2004-2024 · Mixed final/provisional" in extension
    assert "unchanged 2004-2023 final codebook" in extension


def test_consolidated_release_exposes_each_original_column_and_its_years():
    root = DOCS / 'provisional/codebook'
    index = json.loads((root / 'index.json').read_text())
    policy = json.loads((DOCS.parent / 'contracts/harmonization/source-family-consolidation-v1.json').read_text())
    with (root / index['downloads']['column_crosswalk']).open(encoding='utf-8-sig', newline='') as stream:
        rows = list(csv.DictReader(stream))
    expected = {(g['canonical_name'], m['column']) for g in policy['groups'] for m in g['members']}
    assert len(rows) == 204
    assert {(row['canonical_name'], row['original_column']) for row in rows} == expected
    names = {v['name'] for v in index['variables']}
    for group in policy['groups']:
        assert group['canonical_name'] in names
        assert all(m['column'] not in names for m in group['members'])
        detail = variable_detail(root, index, group['canonical_name'])
        assert detail['column_consolidation']['members'] == group['members']
        assert detail['column_consolidation']['caveats'] == group['caveats']
        assert {y for r in detail['source_records'] for y in r['years']} == {
            y for member in group['members'] for y in member['years']}
