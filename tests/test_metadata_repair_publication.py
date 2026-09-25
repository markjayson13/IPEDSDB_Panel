"""Publication must retain the exact bytes accepted by the independent audits."""
import hashlib
import json
from pathlib import Path

import pytest

from helpers import load_script_module


publisher = load_script_module("metadata_repair_publication", "Scripts/QA_QC/26_publish_metadata_repair.py")
WIDE = "Panels/v2/panel_wide_analysis_2004_2023.parquet"
CORE = (
    "Panels/v2/panel_long_scalar_2004_2023.parquet",
    WIDE,
    "Panels/v2/panel_clean_prch_2004_2023.parquet",
    "Checks/v2/wide_qc/qc_value_lineage.parquet",
    "Checks/v2/prch_qc/prch_cell_actions.parquet",
    "Checks/v2/prch_qc/prch_flag_policy.csv",
)


@pytest.fixture
def validation_receipts(tmp_path: Path):
    digest = hashlib.sha256(b"validated data").hexdigest()
    panels = {
        Path(relative).name: {"all_values_equal": True, "sha256": digest}
        for relative in CORE[1:3]
    }
    receipt = {"verification": {"panels": panels,
                                 "artifact_sha256": {relative: digest for relative in CORE}}}
    review = {name: {"corrected_sha256": digest}
              for name in ("dictionary_lake", "dictionary_codes")}
    review["registry_sha256"] = digest
    dictionary = tmp_path / "dictionary-stage"
    dictionary.mkdir()
    stage03 = {"correction_registry_sha256": digest,
               "outputs": [{"path": str(dictionary / "Dictionary/v2" / f"{name}.parquet"),
                              "sha256": digest}
                             for name in ("dictionary_lake", "dictionary_codes")]}
    (dictionary / "stage03_reproduction_receipt.json").write_text(json.dumps(stage03))
    export_root = tmp_path / "final_exports"
    export_root.mkdir()
    exported = export_root / "sfa.dta"
    exported.write_bytes(b"validated data")
    exports = {"artifacts": [{"path": str(exported), "sha256": digest}]}
    return tmp_path, receipt, review, exports


def test_validated_hashes_map_to_package_members(validation_receipts) -> None:
    work, receipt, review, exports = validation_receipts
    expected = publisher.expected_member_hashes(work, receipt, review, exports)
    assert set(expected) == set(CORE) | {
        "Dictionary/v2/dictionary_lake.parquet",
        "Dictionary/v2/dictionary_codes.parquet",
        "Exports/sfa.dta",
        "Evidence/source_metadata_corrections/2023-sfa-v1.json",
    }
    assert publisher.verify_validated_member(work / "final_exports/sfa.dta", "Exports/sfa.dta", expected) == exports["artifacts"][0]["sha256"]


def test_changed_data_cannot_reuse_a_successful_validation(validation_receipts) -> None:
    work, receipt, review, exports = validation_receipts
    expected = publisher.expected_member_hashes(work, receipt, review, exports)
    path = work / "final_exports/sfa.dta"
    # Same length still invalidates the original validation.
    path.write_bytes(b"corrupted data")
    with pytest.raises(ValueError, match="changed after validation"):
        publisher.verify_validated_member(path, "Exports/sfa.dta", expected)


@pytest.mark.parametrize("missing", [CORE[0], CORE[3], CORE[4], CORE[5]])
def test_missing_core_hash_is_rejected(validation_receipts, missing: str) -> None:
    work, receipt, review, exports = validation_receipts
    del receipt["verification"]["artifact_sha256"][missing]
    with pytest.raises(ValueError, match="Missing validated artifact hashes"):
        publisher.expected_member_hashes(work, receipt, review, exports)


def test_dictionary_validation_receipts_must_agree(validation_receipts) -> None:
    work, receipt, review, exports = validation_receipts
    review["dictionary_lake"]["corrected_sha256"] = "f" * 64
    with pytest.raises(ValueError, match="receipts disagree"):
        publisher.expected_member_hashes(work, receipt, review, exports)


def test_export_receipt_cannot_escape_the_validated_export_directory(validation_receipts) -> None:
    work, receipt, review, exports = validation_receipts
    outside = work / "unvalidated.dta"
    outside.write_bytes(b"validated data")
    # Resolve symlinks before testing containment, not only lexical ../ paths.
    inside_alias = work / "final_exports/escape.dta"
    inside_alias.symlink_to(outside)
    exports["artifacts"][0]["path"] = str(inside_alias)
    with pytest.raises(ValueError):
        publisher.expected_member_hashes(work, receipt, review, exports)


def test_conflicting_panel_equality_and_artifact_hashes_are_rejected(validation_receipts) -> None:
    work, receipt, review, exports = validation_receipts
    receipt["verification"]["artifact_sha256"][WIDE] = "f" * 64
    with pytest.raises(ValueError, match="disagree"):
        publisher.expected_member_hashes(work, receipt, review, exports)


def test_checksums_without_panel_equality_proof_are_rejected(validation_receipts) -> None:
    work, receipt, review, exports = validation_receipts
    receipt["verification"]["panels"] = {}
    with pytest.raises(ValueError, match="equality"):
        publisher.expected_member_hashes(work, receipt, review, exports)


def test_correction_registry_proofs_must_agree(validation_receipts) -> None:
    work, receipt, review, exports = validation_receipts
    review["registry_sha256"] = "f" * 64
    with pytest.raises(ValueError, match="registry|Registry"):
        publisher.expected_member_hashes(work, receipt, review, exports)
