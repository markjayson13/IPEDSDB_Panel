"""
Tests for shared Access-pipeline normalization and classification helpers.
"""
from __future__ import annotations

from access_build_utils import (
    academic_year_to_start_year,
    can_serve_metadata_role,
    canonical_source_file,
    classify_table_role,
    source_file_qaqc_category,
    start_year_to_academic_label,
)


def test_academic_year_round_trip() -> None:
    assert academic_year_to_start_year("2023-24") == 2023
    assert academic_year_to_start_year("2004/05") == 2004
    assert start_year_to_academic_label(2023) == "2023-24"


def test_canonical_source_file_normalization() -> None:
    assert canonical_source_file("F2324_F1A") == "F_FA"
    assert canonical_source_file("GR200_2023") == "GR200"
    assert canonical_source_file("IC2023_AY") == "IC_AY"
    assert canonical_source_file("S2023_ABD") == "S_ABD"
    assert canonical_source_file("SAL2010_A_LT9") == "SAL_A_LT"


def test_noncanonical_source_file_categories() -> None:
    assert source_file_qaqc_category("FLAGS") == "auxiliary_expected"
    assert source_file_qaqc_category("CUSTOMCGIDS") == "derived_or_custom"
    assert source_file_qaqc_category("SFA_P") == "canonical"
    assert source_file_qaqc_category("SAL_NIS") == "auxiliary_expected"
    assert source_file_qaqc_category("EFD") == "derived_or_custom"
    assert source_file_qaqc_category("MYSTERY_SOURCE") == "needs_review"


def test_table_role_classification() -> None:
    assert classify_table_role("HD2023", ["UNITID", "INSTNM", "STABBR"]) == "data"
    assert classify_table_role("HD_VARLIST", ["varNumber", "varName", "varTitle"]) == "metadata_varlist"
    assert classify_table_role("HD_DESCRIPTION", ["varNumber", "longDescription"]) == "metadata_description"
    assert classify_table_role("HD_FREQUENCIES", ["varNumber", "codeValue", "valueLabel"]) == "metadata_codes"
    assert classify_table_role("HD_IMPUTATION", ["codeValue", "valueLabel"]) == "metadata_imputation"


def test_early_year_combined_metadata_tables() -> None:
    vartable_cols = [
        "SurveyOrder",
        "TableName",
        "varNumber",
        "varName",
        "imputationvar",
        "varTitle",
        "longDescription",
    ]
    valueset_cols = [
        "TableName",
        "varNumber",
        "varName",
        "Codevalue",
        "valueLabel",
        "varTitle",
    ]
    assert classify_table_role("vartable04", vartable_cols) == "metadata_varlist"
    assert can_serve_metadata_role("metadata_description", "vartable04", vartable_cols) is True
    assert classify_table_role("valuesets04", valueset_cols) == "metadata_codes"
