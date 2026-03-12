"""
Tests for discrete-conflict triage helpers.
"""
from __future__ import annotations

from helpers import load_script_module


wide_common_mod = load_script_module("wide_build_common_stage", "Scripts/wide_build_common.py")


def test_expected_disc_conflicts_are_whitelisted() -> None:
    assert wide_common_mod.is_expected_disc_conflict("IC", "LEVEL") is True
    assert wide_common_mod.is_expected_disc_conflict("IC_PY", "CIPCODE") is True
    assert wide_common_mod.is_expected_disc_conflict("IC", "MYSTERY") is False
