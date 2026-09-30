import importlib.util
import json
from pathlib import Path

import pytest

pytest.importorskip("reportlab")
pypdf = pytest.importorskip("pypdf")
spec = importlib.util.spec_from_file_location("codebook_pdf", Path(__file__).parents[1] / "Scripts/codebook_pdf.py")
pdf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pdf)


@pytest.fixture
def codebook(tmp_path):
    variable = {
        "name": "PELL", "label": "Pell recipients", "storage_type": "string", "stata_name": "PELL",
        "observed_years": [2021, 2023], "null_count": 2,
        "definitions": [{"years": [2021], "label": "Pell recipients", "description": "Original 2021 definition."},
                        {"years": [2023], "label": "Pell recipients", "description": "Revised 2023 definition < 100 & valid."}],
        "codes": [{"code": "A", "label": "Reported", "years": [2021, 2023], "sources": ["SFA"]}],
        "source_records": [{"years": [2023], "table": "SFA2223_P1", "original_table": "SFA2223_P2",
                            "varname": "PELL", "varnumber": "70306", "source_file": "SFA", "correction_id": "2023-sfa-v1",
                            "correction_reason": "Verified physical columns", "reference_period": "2022-23", "imputationvar": "XPELL"}],
        "semantic_metadata": {"units": {"status": "unknown", "value": None}},
        "issues": [{"message": "One unknown category meaning remains."}],
        "stata": {"label": "Pell recipients", "storage_conversion": "categorical_string_encoding",
                  "source_code_map": [{"source_code": "A", "export_code": 1, "label": "Reported"}]},
    }
    index = {"schema_version": 1, "row_count": 8, "column_count": 1, "release": "test-v1", "years": [2021, 2023],
             "codebook_url": "https://markjayson13.github.io/IPEDSDB_Panel/provisional/",
             "release_notes": ["Quarantined UNITID 111111 in 2019 and 2024; complete rows retained separately."],
             "issues": variable["issues"], "generated_from": {"parquet_sha256": "a" * 64},
             "variables": [{"name": "PELL", "detail_file": "variables-001.json"}]}
    (tmp_path / "index.json").write_text(json.dumps(index))
    (tmp_path / "variables-001.json").write_text(json.dumps({"PELL": variable}))
    return tmp_path


def test_pdf_retains_year_scopes_corrections_and_gaps(codebook):
    result = pdf.render_pdf(codebook)
    reader = pypdf.PdfReader(result["path"])
    text = "\n".join(page.extract_text() for page in reader.pages)
    for expected in ["Original 2021 definition", "Revised 2023 definition < 100 & valid", "SFA2223_P1", "SFA2223_P2",
                     "2023-sfa-v1", "One unknown category meaning", "Reversible Stata category mapping", "Reported"]:
        assert expected in text
    assert "2021, 2023" in text
    assert "2021-2023" not in text
    assert "Quarantined UNITID 111111 in 2019 and 2024" in text
    assert "IPEDSDB_Panel/provisional/" in text
    assert len(reader.outline) >= 4
    assert any(p.get("/Annots") for p in reader.pages)
    # Identical inputs produce byte-identical PDFs (no wall-clock timestamps).
    second = pdf.render_pdf(codebook)
    assert result["sha256"] == second["sha256"]


def test_pdf_refuses_incomplete_variable_inventory(codebook):
    path = codebook / "index.json"
    index = json.loads(path.read_text())
    index["column_count"] = 2
    path.write_text(json.dumps(index))
    with pytest.raises(ValueError, match="Variable count"):
        pdf.render_pdf(codebook)


def test_pdf_mapping_uses_exact_native_label_not_source_meaning(codebook):
    path = codebook / "variables-001.json"
    details = json.loads(path.read_text())
    details["PELL"]["stata"].update({
        "source_code_map": [
            {"source_code": "012003", "export_code": 3, "label": "Source meaning only"},
            {"source_code": "UNLABELED", "export_code": 4, "label": "Unverified fallback meaning"},
        ],
        "value_labels": {"3": "012003: 2004: January 2003"},
    })
    path.write_text(json.dumps(details))
    result = pdf.render_pdf(codebook)
    text = " ".join("\n".join(page.extract_text() for page in pypdf.PdfReader(result["path"]).pages).split())
    assert "012003: 2004: January 2003" in text
    assert "No native label assigned" in text
    assert "Source meaning only" not in text
    assert "Unverified fallback meaning" not in text
    assert pdf.native_stata_label({"value_labels": {"3": ""}}, 3) == ""


@pytest.mark.parametrize("has_source_records", [True, False])
@pytest.mark.parametrize("caveats", [["Keep the later definition < 100 & its reference period."],
                                   "Keep the later definition < 100 & its reference period."])
def test_pdf_prints_consolidation_provenance_even_when_source_records_are_missing(
        codebook, has_source_records, caveats):
    path = codebook / "variables-001.json"
    details = json.loads(path.read_text())
    variable = details["PELL"]
    variable["observed_years"] = [2004, 2006, 2021, 2023]
    variable["column_consolidation"] = {
        "canonical_name": "PELL", "rationale": "Verified table relocation; original values remain unchanged.",
        "members": [{"column": "PELL__SFA__00070326__OLD", "years": [2004, 2006]},
                    {"column": "PELL__SFA_P__00070326__NEW", "years": [2021, 2023]}],
        "caveats": caveats,
    }
    if not has_source_records:
        variable["source_records"] = []
    path.write_text(json.dumps(details))
    result = pdf.render_pdf(codebook)
    reader = pypdf.PdfReader(result["path"])
    text = " ".join("\n".join(page.extract_text() for page in reader.pages).split())
    assert "Consolidated source columns" in text
    assert "Verified table relocation; original values remain unchanged." in text
    assert "PELL__SFA__00070326__OLD: 2004, 2006" in text
    assert "PELL__SFA_P__00070326__NEW: 2021, 2023" in text
    assert "2004-2006" not in text and "2021-2023" not in text
    assert "Keep the later definition < 100 & its reference period." in text


def test_pdf_refuses_detail_path_escape(codebook):
    path = codebook / "index.json"
    index = json.loads(path.read_text())
    index["variables"][0]["detail_file"] = "../index.json"
    path.write_text(json.dumps(index))
    with pytest.raises(ValueError, match="Unsafe detail"):
        pdf.render_pdf(codebook)


def test_pdf_symbol_fallback_preserves_right_arrows_in_source_text(codebook):
    path = codebook / "variables-001.json"
    details = json.loads(path.read_text())
    variable = details["PELL"]
    variable["definitions"][1]["description"] = "Original→later definition < 100 & unchanged."
    variable["codes"][0]["label"] = "Received→awarded"
    variable["column_consolidation"] = {
        "canonical_name": "PELL", "rationale": "SFA→SFA_P keeps the original observations.",
        "members": [{"column": "PELL_OLD", "years": [2021]}, {"column": "PELL_NEW", "years": [2023]}],
        "caveats": ["Table movement→year-specific definitions; do not recode."],
    }
    path.write_text(json.dumps(details, ensure_ascii=False))
    source_bytes = path.read_bytes()
    result = pdf.render_pdf(codebook)
    reader = pypdf.PdfReader(result["path"])
    text = " ".join("\n".join(page.extract_text() for page in reader.pages).split())
    for expected in ["Original→later definition < 100 & unchanged.", "Received→awarded",
                     "SFA→SFA_P keeps the original observations.",
                     "Table movement→year-specific definitions; do not recode."]:
        assert expected in text
    assert any(font.get_object().get("/BaseFont") == "/Symbol"
               for page in reader.pages for font in page["/Resources"]["/Font"].values())
    assert path.read_bytes() == source_bytes
    assert pdf.render_pdf(codebook)["sha256"] == result["sha256"]


def test_pdf_right_arrow_fallback_does_not_allow_other_missing_glyphs(codebook):
    path = codebook / "variables-001.json"
    details = json.loads(path.read_text())
    details["PELL"]["definitions"][0]["description"] = "SFA→SFA_P; undocumented glyph 漢"
    path.write_text(json.dumps(details, ensure_ascii=False))
    with pytest.raises(ValueError, match="PDF font lacks source characters") as exc:
        pdf.render_pdf(codebook)
    assert "漢" in str(exc.value) and "→" not in str(exc.value)
    assert not (codebook / "ipeds-panel-codebook.pdf").exists()


def test_nonconsecutive_years_never_become_continuous_range():
    assert pdf.year_ranges([2023, 2004, 2006, 2007, 2008, 2004]) == "2004, 2006-2008, 2023"
