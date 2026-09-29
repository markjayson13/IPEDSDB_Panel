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


def test_pdf_refuses_detail_path_escape(codebook):
    path = codebook / "index.json"
    index = json.loads(path.read_text())
    index["variables"][0]["detail_file"] = "../index.json"
    path.write_text(json.dumps(index))
    with pytest.raises(ValueError, match="Unsafe detail"):
        pdf.render_pdf(codebook)


def test_nonconsecutive_years_never_become_continuous_range():
    assert pdf.year_ranges([2023, 2004, 2006, 2007, 2008, 2004]) == "2004, 2006-2008, 2023"
