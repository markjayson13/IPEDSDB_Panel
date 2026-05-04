"""
Tests for the versioned panel contract.

Focus:
- the declared contract stays aligned with code defaults and shared PRCH policy
"""
from __future__ import annotations

from helpers import run_script


def test_panel_contract_matches_current_code_policy() -> None:
    result = run_script("Scripts/QA_QC/11_validate_panel_contract.py")

    assert result.returncode == 0, result.stdout
    assert "Panel contract validation passed" in result.stdout
