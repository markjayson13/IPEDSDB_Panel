"""
Tests for shared PRCH cleaning policy and QA defaults.
"""
from __future__ import annotations

from helpers import load_script_module


policy_mod = load_script_module("prch_policy", "Scripts/prch_policy.py")
panel_qa_mod = load_script_module("panel_qa", "Scripts/QA_QC/01_panel_qa.py")


def test_finance_prch_policy_marks_code_4_as_child_and_6_as_review() -> None:
    assert policy_mod.cleaned_child_codes("PRCH_F") == {2, 3, 4, 5}
    assert policy_mod.review_only_codes("PRCH_F") == {6}
    assert policy_mod.classify_flag_code("PRCH_F", 4) == "child_apply"
    assert policy_mod.classify_flag_code("PRCH_F", 6) == "review_only"


def test_non_finance_prch_defaults_to_code_2() -> None:
    assert policy_mod.cleaned_child_codes("PRCH_AL") == {2}
    assert policy_mod.review_only_codes("PRCH_AL") == set()


def test_panel_qa_auto_child_codes_follow_shared_policy() -> None:
    assert panel_qa_mod.resolve_child_codes("PRCH_F", "AUTO") == {2, 3, 4, 5}


def test_prch_c_targets_include_derived_completions_family() -> None:
    assert policy_mod.targets_source_file("PRCH_C", "DRVC") is True
    assert policy_mod.targets_source_file("PRCH_F", "DRVC") is False


def test_student_aid_prch_policy_includes_private_aid_family() -> None:
    assert policy_mod.targets_source_file("PRCH_SFA", "SFA_P") is True
    assert policy_mod.targets_source_file("PRCH_SA", "SFA_P") is True


def test_prchtp_f_is_documented_as_coverage_only() -> None:
    policy = policy_mod.get_policy("PRCHTP_F")
    assert policy.cleaned_child_codes == (2,)
    assert policy.target_source_files == ()


def test_panel_qa_status_logic_distinguishes_cleaned_review_only_and_no_targets() -> None:
    assert panel_qa_mod.determine_flag_status(
        child_rows_raw=10,
        review_rows_raw=0,
        target_columns=4,
        clean_target_nonnull=0,
    ) == ("cleaned", "Observed child rows were cleaned across the targeted column family.")
    assert panel_qa_mod.determine_flag_status(
        child_rows_raw=0,
        review_rows_raw=5,
        target_columns=4,
        clean_target_nonnull=0,
    ) == ("review_only", "Only review-only codes were observed for this flag.")
    assert panel_qa_mod.determine_flag_status(
        child_rows_raw=3,
        review_rows_raw=0,
        target_columns=0,
        clean_target_nonnull=0,
    ) == ("no_targets", "Child rows were observed but no target columns were configured for this flag.")
    assert panel_qa_mod.determine_flag_status(
        child_rows_raw=2,
        review_rows_raw=0,
        target_columns=4,
        clean_target_nonnull=1,
    ) == ("suspicious", "Targeted child-row cells remained non-null after cleaning.")
