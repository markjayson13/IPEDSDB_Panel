"""
Tests for harmonize QA helper outputs.
"""
from __future__ import annotations

from io import StringIO

import pandas as pd

from helpers import load_script_module


harmonize_mod = load_script_module("harmonize_stage", "Scripts/04_harmonize.py")


def test_empty_missing_unitid_frame_writes_readable_csv() -> None:
    df = harmonize_mod.empty_missing_unitid_frame()
    buf = StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    loaded = pd.read_csv(buf)
    assert loaded.empty
    assert loaded.columns.tolist() == harmonize_mod.MISSING_UNITID_COLUMNS


def test_classify_exclude_reason_distinguishes_missing_source_rows() -> None:
    empty = pd.DataFrame()
    assert (
        harmonize_mod.classify_exclude_reason(empty, source_match_rows=0, access_match_rows=0, source_row_count=0)
        == "source_table_has_zero_rows"
    )
    assert (
        harmonize_mod.classify_exclude_reason(empty, source_match_rows=0, access_match_rows=0, source_row_count=10)
        == "missing_dictionary_rows_for_source_file"
    )
    assert (
        harmonize_mod.classify_exclude_reason(empty, source_match_rows=4, access_match_rows=0, source_row_count=10)
        == "dictionary_rows_found_for_source_file_but_access_table_missing"
    )
