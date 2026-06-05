"""
Tests for harmonize QA helper outputs.
"""
from __future__ import annotations

from io import StringIO

import pandas as pd
import pytest

from helpers import load_script_module, write_parquet


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


def ambiguous_dictionary_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "year": 2023,
                "source_file": "HD",
                "access_table_name": "HD2023",
                "varname": "INSTNM",
                "varnumber": "00000001",
                "metadata_source": "source_a",
                "metadata_table_name": "meta_a",
            },
            {
                "year": 2023,
                "source_file": "HD",
                "access_table_name": "HD2023",
                "varname": "INSTNM",
                "varnumber": "00000002",
                "metadata_source": "source_b",
                "metadata_table_name": "meta_b",
            },
        ]
    )


def test_select_dict_source_fails_on_ambiguous_varname_without_override() -> None:
    overrides = harmonize_mod.empty_ambiguity_overrides()

    with pytest.raises(SystemExit, match="ambiguous dictionary mapping"):
        harmonize_mod.select_dict_source(ambiguous_dictionary_frame(), "HD", "HD2023", overrides)


def test_select_dict_source_uses_documented_ambiguity_override() -> None:
    overrides = pd.DataFrame(
        [
            {
                "year": "2023",
                "source_file": "HD",
                "access_table_name": "HD2023",
                "varname": "INSTNM",
                "selected_varnumber": "00000002",
                "selected_metadata_source": "",
                "selected_metadata_table_name": "",
                "justification": "Synthetic test override chooses the revised metadata row.",
            }
        ],
        columns=harmonize_mod.AMBIGUITY_OVERRIDE_COLUMNS,
    )

    selected = harmonize_mod.select_dict_source(ambiguous_dictionary_frame(), "HD", "HD2023", overrides)

    assert len(selected) == 1
    assert selected.iloc[0]["varnumber"] == "00000002"
    assert selected.iloc[0]["metadata_source"] == "source_b"


def test_dedupe_long_panel_drops_all_blank_placeholder_rows(tmp_path) -> None:
    out_path = tmp_path / "panel_long_varnum_2010.parquet"
    valid_row = {col: None for col in harmonize_mod.LONG_PANEL_COLUMNS}
    valid_row.update(
        {
            "year": 2010,
            "UNITID": 100654,
            "varname": "INSTNM",
            "varnumber": "00000001",
            "value": "Example University",
            "source_file": "HD",
            "access_table_name": "HD2010",
        }
    )
    blank_row = {col: None for col in harmonize_mod.LONG_PANEL_COLUMNS}
    write_parquet(out_path, [valid_row, blank_row])

    harmonize_mod.dedupe_long_panel(out_path, ["HD"], temp_dir=tmp_path / "dedupe_tmp")

    deduped = pd.read_parquet(out_path)
    assert len(deduped) == 1
    assert deduped.loc[0, "UNITID"] == 100654
    assert deduped[["year", "UNITID", "varnumber", "source_file"]].notna().all().all()
