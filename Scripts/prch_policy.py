"""
Shared PRCH parent/child cleaning policy used by cleaning and QA scripts.

Reads:
- local IPEDS metadata labels when callers compare policy to dictionary artifacts

Writes:
- no files directly; this module centralizes policy for other scripts

Focus:
- keep PRCH child-code handling explicit and consistent
- distinguish fully cleaned child rows from review-only partial cases
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class PrchFlagPolicy:
    flag: str
    cleaned_child_codes: tuple[int, ...]
    review_only_codes: tuple[int, ...] = ()
    target_source_files: tuple[str, ...] = ()
    target_source_prefixes: tuple[str, ...] = ()
    rationale: str = ""


DEFAULT_POLICY = PrchFlagPolicy(
    flag="*",
    cleaned_child_codes=(2,),
    review_only_codes=(),
    target_source_files=(),
    target_source_prefixes=(),
    rationale="Most IPEDS PRCH flags use code 2 for child rows whose data are reported with the parent institution.",
)


POLICY_BY_FLAG: dict[str, PrchFlagPolicy] = {
    "PRCHTP_F": PrchFlagPolicy(
        flag="PRCHTP_F",
        cleaned_child_codes=(2,),
        target_source_files=(),
        rationale=(
            "Finance system-type flag from FLAGS. Track its codes in QA coverage, but do not null panel values "
            "directly from this flag because it describes system structure rather than a component-family payload."
        ),
    ),
    "PRCH_ADM": PrchFlagPolicy(flag="PRCH_ADM", cleaned_child_codes=(2,), target_source_files=("ADM",)),
    "PRCH_AL": PrchFlagPolicy(flag="PRCH_AL", cleaned_child_codes=(2,), target_source_files=("AL",)),
    "PRCH_C": PrchFlagPolicy(
        flag="PRCH_C",
        cleaned_child_codes=(2,),
        target_source_files=("C_A", "C_B", "C_C", "CDEP", "DRVC"),
        rationale=(
            "Completions child rows should not retain either direct completions tables or derived completions totals. "
            "This includes DRVC variables because they are derived from the same completions reporting relationship."
        ),
    ),
    "PRCH_COS": PrchFlagPolicy(flag="PRCH_COS", cleaned_child_codes=(2,), target_source_files=("COST",)),
    "PRCH_E12": PrchFlagPolicy(flag="PRCH_E12", cleaned_child_codes=(2,), target_source_files=("E12",)),
    "PRCH_EAP": PrchFlagPolicy(flag="PRCH_EAP", cleaned_child_codes=(2,), target_source_files=("EAP",)),
    "PRCH_EF": PrchFlagPolicy(
        flag="PRCH_EF",
        cleaned_child_codes=(2,),
        target_source_files=("EFA", "EFA_DIST", "EFB", "EFC", "EFCP", "EFFY", "EFFY_DIST", "EFIA"),
        target_source_prefixes=("EF", "EFFY"),
    ),
    "PRCH_F": PrchFlagPolicy(
        flag="PRCH_F",
        cleaned_child_codes=(2, 3, 4, 5),
        review_only_codes=(6,),
        target_source_files=("F_F", "F_FA", "F_FA_F", "F_FA_G"),
        rationale=(
            "Finance uses more than one child code. Codes 2, 3, 4, and 5 indicate child or partial-child rows "
            "whose finance values are reported elsewhere and should therefore be nulled in the child row. "
            "Code 6 is a partial parent/child case: the child row reports some revenues/expenses while "
            "assets/liabilities remain with the parent, so blanket nulling would remove valid reported data."
        ),
    ),
    "PRCH_GR": PrchFlagPolicy(flag="PRCH_GR", cleaned_child_codes=(2,), target_source_files=("GR", "GR_PELL_SSL")),
    "PRCH_GR2": PrchFlagPolicy(flag="PRCH_GR2", cleaned_child_codes=(2,), target_source_files=("GR200",)),
    "PRCH_HR": PrchFlagPolicy(
        flag="PRCH_HR",
        cleaned_child_codes=(2,),
        target_source_files=("EAP", "SAL_A", "SAL_A_LT", "SAL_B", "SAL_FACULTY", "SAL_IS"),
    ),
    "PRCH_OM": PrchFlagPolicy(flag="PRCH_OM", cleaned_child_codes=(2,), target_source_files=("OM",)),
    "PRCH_S": PrchFlagPolicy(
        flag="PRCH_S",
        cleaned_child_codes=(2,),
        target_source_files=("S_ABD", "S_CN", "S_F", "S_G", "S_IS", "S_NH", "S_OC", "S_SIS"),
        target_source_prefixes=("S_",),
    ),
    "PRCH_SA": PrchFlagPolicy(flag="PRCH_SA", cleaned_child_codes=(2,), target_source_files=("SFA", "SFA_P", "SFAV")),
    "PRCH_SFA": PrchFlagPolicy(flag="PRCH_SFA", cleaned_child_codes=(2,), target_source_files=("SFA", "SFA_P", "SFAV")),
}


def normalize_flag(flag: str) -> str:
    return (flag or "").strip().upper()


def get_policy(flag: str) -> PrchFlagPolicy:
    return POLICY_BY_FLAG.get(normalize_flag(flag), DEFAULT_POLICY)


def cleaned_child_codes(flag: str) -> set[int]:
    return set(get_policy(flag).cleaned_child_codes)


def review_only_codes(flag: str) -> set[int]:
    return set(get_policy(flag).review_only_codes)


def target_source_files(flag: str) -> tuple[str, ...]:
    return tuple(get_policy(flag).target_source_files)


def target_source_prefixes(flag: str) -> tuple[str, ...]:
    return tuple(get_policy(flag).target_source_prefixes)


def targets_source_file(flag: str, source_file: str) -> bool:
    source_norm = (source_file or "").strip().upper()
    if not source_norm:
        return False
    policy = get_policy(flag)
    if source_norm in policy.target_source_files:
        return True
    return any(source_norm.startswith(prefix) for prefix in policy.target_source_prefixes)


def classify_flag_code(flag: str, code: int | float | str | None) -> str:
    if code is None:
        return "missing"
    try:
        code_int = int(code)
    except (TypeError, ValueError):
        return "other"
    policy = get_policy(flag)
    if code_int in policy.cleaned_child_codes:
        return "child_apply"
    if code_int in policy.review_only_codes:
        return "review_only"
    return "other"


def policy_rows(flags: Iterable[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for flag in sorted({normalize_flag(flag) for flag in flags if str(flag).strip()}):
        policy = get_policy(flag)
        rows.append(
            {
                "flag": flag,
                "child_codes_applied": ",".join(str(x) for x in policy.cleaned_child_codes),
                "review_only_codes": ",".join(str(x) for x in policy.review_only_codes),
                "target_source_files": ",".join(policy.target_source_files),
                "target_source_prefixes": ",".join(policy.target_source_prefixes),
                "rationale": policy.rationale,
            }
        )
    return rows
