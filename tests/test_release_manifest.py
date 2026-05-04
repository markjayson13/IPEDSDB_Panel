"""
Tests for release manifest construction and checksum verification.

Focus:
- generated data-release manifests include expected public artifacts
- verification catches post-manifest file drift
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from helpers import run_script, write_parquet


def write_release_fixture(root: Path) -> None:
    for year in [2022, 2023]:
        year_dir = root / "Raw_Access_Databases" / str(year)
        downloads = year_dir / "downloads"
        downloads.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([{"year": year, "academic_year_label": f"{year}-{(year + 1) % 100:02d}", "release_type": "Final"}]).to_csv(
            year_dir / "manifest.csv", index=False
        )
        (downloads / f"ipeds_{year}.zip").write_bytes(f"synthetic download {year}\n".encode("utf-8"))
        write_parquet(
            root / "Cross_sections" / f"panel_long_varnum_{year}.parquet",
            [
                {
                    "UNITID": 100000 + year,
                    "year": year,
                    "varname": "FINVAL",
                    "value": float(year),
                    "source_file": "F_F",
                    "varnumber": "00000001",
                }
            ],
        )

    write_parquet(
        root / "Dictionary" / "dictionary_lake.parquet",
        [
            {
                "year": 2022,
                "varnumber": "00000001",
                "varname": "FINVAL",
                "varTitle": "Finance value",
                "DataType": "cont",
                "source_file": "F_F",
            }
        ],
    )
    write_parquet(
        root / "Dictionary" / "dictionary_codes.parquet",
        [{"year": 2022, "varname": "FINVAL", "codeValue": "1", "valueLabel": "Synthetic"}],
    )
    panel_rows = [
        {"UNITID": 100654, "year": 2022, "FINVAL": 100.0},
        {"UNITID": 100663, "year": 2023, "FINVAL": 200.0},
    ]
    write_parquet(root / "Panels" / "2022-2023" / "panel_long_varnum_2022_2023.parquet", panel_rows)
    write_parquet(root / "Panels" / "panel_long_scalar_unique.parquet", panel_rows)
    write_parquet(root / "Panels" / "panel_long_dim.parquet", panel_rows)
    write_parquet(root / "Panels" / "panel_wide_analysis_2022_2023.parquet", panel_rows)
    write_parquet(root / "Panels" / "panel_clean_analysis_2022_2023.parquet", panel_rows)
    qc_dir = root / "Checks" / "acceptance_qc"
    qc_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"check": "synthetic", "status": "pass"}]).to_csv(qc_dir / "acceptance_summary.csv", index=False)
    prch_dir = root / "Checks" / "prch_qc"
    prch_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "flag": "PRCH_F",
                "child_codes_applied": "2,3,4,5",
                "review_only_codes": "6",
                "target_source_files": "F_F",
                "target_source_prefixes": "",
                "rationale": "Synthetic test policy",
            }
        ]
    ).to_csv(prch_dir / "prch_flag_policy.csv", index=False)
    pd.DataFrame(
        [
            {
                "year": 2022,
                "flag": "PRCH_F",
                "child_codes_applied": "2,3,4,5",
                "review_only_codes": "6",
                "target_columns": 1,
                "has_target_columns": True,
                "lineage_source": "column_lineage",
                "child_rows_cleaned": 1,
                "review_only_rows": 0,
            }
        ]
    ).to_csv(prch_dir / "prch_clean_summary.csv", index=False)
    pd.DataFrame([{"flag": "PRCH_F", "column": "FINVAL", "source_files": "F_F", "lineage_source": "column_lineage"}]).to_csv(
        prch_dir / "prch_clean_columns.csv", index=False
    )
    pd.DataFrame([{"year": 2022, "flag": "PRCH_F", "code": 2, "policy_bucket": "child_apply", "target_columns": 1, "rows": 1}]).to_csv(
        prch_dir / "prch_flag_code_counts.csv", index=False
    )


def test_release_manifest_builds_and_verifies_public_artifacts(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    write_release_fixture(root)
    out_dir = root / "Checks" / "release_manifest"
    env = {"IPEDSDB_ROOT": str(root)}

    build = run_script(
        "Scripts/QA_QC/12_build_release_manifest.py",
        "--root",
        root,
        "--years",
        "2022:2023",
        "--out-dir",
        out_dir,
        env=env,
        timeout=60,
    )
    assert build.returncode == 0, build.stdout

    manifest_path = out_dir / "release_manifest.csv"
    manifest = pd.read_csv(manifest_path)
    clean = manifest[manifest["relative_path"] == "Panels/panel_clean_analysis_2022_2023.parquet"].iloc[0]
    assert bool(clean["present"]) is True
    assert int(clean["rows"]) == 2
    assert int(clean["columns"]) == 3
    assert isinstance(clean["sha256"], str) and len(clean["sha256"]) == 64
    assert "repo_contract" in set(manifest["role"])

    verify = run_script(
        "Scripts/QA_QC/13_verify_release_manifest.py",
        "--manifest",
        manifest_path,
        "--root",
        root,
        env=env,
        timeout=60,
    )
    assert verify.returncode == 0, verify.stdout
    assert "Release manifest verification passed" in verify.stdout

    datapackage = run_script(
        "Scripts/QA_QC/16_build_datapackage.py",
        "--root",
        root,
        "--years",
        "2022:2023",
        "--manifest",
        manifest_path,
        env=env,
        timeout=60,
    )
    assert datapackage.returncode == 0, datapackage.stdout
    provenance = run_script(
        "Scripts/QA_QC/17_build_provenance.py",
        "--root",
        root,
        "--manifest",
        manifest_path,
        env=env,
        timeout=60,
    )
    assert provenance.returncode == 0, provenance.stdout
    assert (root / "Checks" / "release_metadata" / "datapackage.json").exists()
    assert (root / "Checks" / "release_metadata" / "build_provenance.json").exists()

    bundle_dir = root / "Releases" / "ipedsdb_panel_2022_2023"
    bundle = run_script(
        "Scripts/QA_QC/14_build_public_release_bundle.py",
        "--root",
        root,
        "--years",
        "2022:2023",
        "--out-dir",
        bundle_dir,
        "--version",
        "test-release",
        "--doi",
        "10.0000/ipedsdb-panel-test",
        "--license",
        "CC-BY-4.0",
        env=env,
        timeout=60,
    )
    assert bundle.returncode == 0, bundle.stdout

    copied_clean_panel = bundle_dir / "IPEDSDB_ROOT" / "Panels" / "panel_clean_analysis_2022_2023.parquet"
    assert copied_clean_panel.exists()
    assert (bundle_dir / "metadata" / "release_manifest.csv").exists()
    assert (bundle_dir / "metadata" / "release_manifest_verification.csv").exists()
    assert (bundle_dir / "bundle_manifest.csv").exists()
    assert (bundle_dir / "SHA256SUMS.txt").exists()
    assert (bundle_dir / "CITATION.cff").exists()
    assert (bundle_dir / "datacite.json").exists()
    assert (bundle_dir / "ro-crate-metadata.json").exists()
    assert (bundle_dir / "metadata" / "datapackage.json").exists()
    assert (bundle_dir / "metadata" / "build_provenance.json").exists()

    bundle_manifest = pd.read_csv(bundle_dir / "bundle_manifest.csv")
    assert "IPEDSDB_ROOT/Panels/panel_clean_analysis_2022_2023.parquet" in set(bundle_manifest["bundle_relative_path"])
    citation = (bundle_dir / "CITATION.cff").read_text(encoding="utf-8")
    assert "test-release" in citation
    assert "10.0000/ipedsdb-panel-test" in citation
    datacite = json.loads((bundle_dir / "datacite.json").read_text(encoding="utf-8"))
    assert datacite["contributors"][0]["name"] == "Djeto Assane"
    assert datacite["contributors"][0]["contributorType"] == "Supervisor"
    ro_crate = json.loads((bundle_dir / "ro-crate-metadata.json").read_text(encoding="utf-8"))
    dataset_node = next(row for row in ro_crate["@graph"] if row.get("@id") == "./")
    assert dataset_node["contributor"][0]["roleName"] == "Research mentor"
    assert dataset_node["contributor"][0]["url"] == "https://www.unlv.edu/people/djeto-assane"
    package = json.loads((root / "Checks" / "release_metadata" / "datapackage.json").read_text(encoding="utf-8"))
    assert package["resources"]

    write_parquet(
        root / "Panels" / "panel_clean_analysis_2022_2023.parquet",
        [
            {"UNITID": 100654, "year": 2022, "FINVAL": 999.0},
            {"UNITID": 100663, "year": 2023, "FINVAL": 200.0},
        ],
    )
    verify_tampered = run_script(
        "Scripts/QA_QC/13_verify_release_manifest.py",
        "--manifest",
        manifest_path,
        "--root",
        root,
        env=env,
        timeout=60,
    )
    assert verify_tampered.returncode == 1, verify_tampered.stdout

    results = pd.read_csv(out_dir / "release_manifest_verification.csv")
    clean_result = results[results["relative_path"] == "Panels/panel_clean_analysis_2022_2023.parquet"].iloc[0]
    assert "sha256_mismatch" in clean_result["failure_reasons"]


def test_release_compare_fails_on_schema_dictionary_and_prch_drift(tmp_path: Path) -> None:
    baseline_root = tmp_path / "baseline_root"
    current_root = tmp_path / "current_root"
    write_release_fixture(baseline_root)
    write_release_fixture(current_root)

    write_parquet(
        current_root / "Panels" / "panel_clean_analysis_2022_2023.parquet",
        [
            {"UNITID": 100654, "year": 2022, "FINVAL": 100.0, "NEWCOL": "x"},
            {"UNITID": 100663, "year": 2023, "FINVAL": 200.0, "NEWCOL": "y"},
        ],
    )
    write_parquet(
        current_root / "Dictionary" / "dictionary_lake.parquet",
        [
            {
                "year": 2022,
                "varnumber": "00000002",
                "varname": "FINVAL",
                "varTitle": "Finance value",
                "DataType": "char",
                "source_file": "F_F",
            }
        ],
    )
    pd.DataFrame(
        [
            {
                "flag": "PRCH_F",
                "child_codes_applied": "2",
                "review_only_codes": "6",
                "target_source_files": "F_F",
                "target_source_prefixes": "",
                "rationale": "Changed synthetic test policy",
            }
        ]
    ).to_csv(current_root / "Checks" / "prch_qc" / "prch_flag_policy.csv", index=False)

    baseline_manifest_dir = baseline_root / "Checks" / "release_manifest"
    current_manifest_dir = current_root / "Checks" / "release_manifest"
    baseline_build = run_script(
        "Scripts/QA_QC/12_build_release_manifest.py",
        "--root",
        baseline_root,
        "--years",
        "2022:2023",
        "--out-dir",
        baseline_manifest_dir,
        env={"IPEDSDB_ROOT": str(baseline_root)},
        timeout=60,
    )
    assert baseline_build.returncode == 0, baseline_build.stdout
    current_build = run_script(
        "Scripts/QA_QC/12_build_release_manifest.py",
        "--root",
        current_root,
        "--years",
        "2022:2023",
        "--out-dir",
        current_manifest_dir,
        env={"IPEDSDB_ROOT": str(current_root)},
        timeout=60,
    )
    assert current_build.returncode == 0, current_build.stdout

    compare = run_script(
        "Scripts/QA_QC/15_compare_release_to_baseline.py",
        "--baseline-manifest",
        baseline_manifest_dir / "release_manifest.csv",
        "--current-manifest",
        current_manifest_dir / "release_manifest.csv",
        "--baseline-root",
        baseline_root,
        "--current-root",
        current_root,
        "--out-dir",
        current_root / "Checks" / "release_compare",
        env={"IPEDSDB_ROOT": str(current_root)},
        timeout=60,
    )
    assert compare.returncode == 1, compare.stdout

    findings = pd.read_csv(current_root / "Checks" / "release_compare" / "release_comparison.csv")
    categories = set(findings["category"])
    assert "parquet_schema_column_added" in categories
    assert "dictionary_mapping_changed" in categories
    assert "prch_changed" in categories

    overrides = current_root / "overrides.csv"
    pd.DataFrame(
        [
            {
                "category": "parquet_columns_changed",
                "artifact_key": "",
                "relative_path": "Panels/panel_clean_analysis_2022_2023.parquet",
                "field": "columns",
                "baseline": "",
                "current": "",
                "justification": "Synthetic schema drift accepted for test.",
            },
            {
                "category": "parquet_schema_column_added",
                "artifact_key": "",
                "relative_path": "Panels/panel_clean_analysis_2022_2023.parquet",
                "field": "NEWCOL",
                "baseline": "",
                "current": "",
                "justification": "Synthetic schema drift accepted for test.",
            },
            {
                "category": "dictionary_mapping_changed",
                "artifact_key": "",
                "relative_path": "Dictionary/dictionary_lake.parquet",
                "field": "2022|F_F|FINVAL",
                "baseline": "",
                "current": "",
                "justification": "Synthetic dictionary drift accepted for test.",
            },
            {
                "category": "prch_changed",
                "artifact_key": "",
                "relative_path": "Checks/prch_qc/prch_flag_policy.csv",
                "field": "PRCH_F",
                "baseline": "",
                "current": "",
                "justification": "Synthetic PRCH drift accepted for test.",
            },
        ]
    ).to_csv(overrides, index=False)
    compare_overridden = run_script(
        "Scripts/QA_QC/15_compare_release_to_baseline.py",
        "--baseline-manifest",
        baseline_manifest_dir / "release_manifest.csv",
        "--current-manifest",
        current_manifest_dir / "release_manifest.csv",
        "--baseline-root",
        baseline_root,
        "--current-root",
        current_root,
        "--out-dir",
        current_root / "Checks" / "release_compare_overridden",
        "--overrides",
        overrides,
        env={"IPEDSDB_ROOT": str(current_root)},
        timeout=60,
    )
    assert compare_overridden.returncode == 0, compare_overridden.stdout
