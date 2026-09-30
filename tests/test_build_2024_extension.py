"""Publication must bind its manifest and roll back incomplete promotion."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from helpers import load_script_module

runner = load_script_module("build_2024_extension_test", "Scripts/build_2024_extension.py")


@pytest.fixture
def candidate(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "verify_2024_evidence", lambda package: None, raising=False)
    monkeypatch.setattr(runner, "verify_append_evidence", lambda package, root: None)
    monkeypatch.setattr(runner, "verify_stata_history", lambda package, root: {"status": "pass"})
    monkeypatch.setattr(runner, "analysis_input", lambda package, **kwargs:
                        package / f"Supporting/{runner.PANEL}.analysis.parquet")
    monkeypatch.setattr(runner, "consolidated_input", lambda package, **kwargs:
                        runner.analysis_input(package, **kwargs))
    root = tmp_path / "release-root"
    (root / "Releases").mkdir(parents=True)
    names = [f"Final/{runner.BASE}.{ext}{suffix}" for ext in ("parquet", "dta")
             for suffix in ("", ".metadata.json")]
    names += ["Final/Metadata/dictionary_lake.parquet", "Final/Metadata/dictionary_codes.parquet",
              "Final/value_lineage.parquet"]
    rows = []
    for name in names:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"Immutable historical fixture: " + name.encode())
        rows.append({"path": name, "sha256": runner.sha256(path), "bytes": path.stat().st_size})
    runner.write_json(root / "Final/manifest.json", {"artifacts": rows})
    package = tmp_path / "work/package"
    package.mkdir(parents=True)
    (package / "payload.txt").write_text("Verified candidate payload\n")
    runner.write_json(package / f"Supporting/{runner.PANEL}.analysis.parquet.quarantine-validation.json",
                      {"status": "pass", "dedicated_quarantine_tests_cover_this_fixture": True})
    runner.write_json(package / f"Supporting/{runner.PANEL}.analysis.parquet.consolidation-validation.json",
                      {"status": "pass", "dedicated_consolidation_tests_cover_this_fixture": True})
    runner.write_json(package / "Checks/stata_history_validation.json", {"status": "pass"})
    manifest = {"status": "verified", "release_name": runner.RELEASE,
                "historical_release": runner.baseline(root), "artifacts": runner.artifacts(package)}
    runner.write_json(package / "manifest.json", manifest)
    return root, package


def assert_unpublished(root):
    assert not (root / "Provisional").exists()
    assert not (root / "Provisional").is_symlink()
    assert not (root / "Releases" / runner.RELEASE).exists()
    assert not list((root / "Work").glob(".2024-publish-*"))


def test_publish_keeps_final_unchanged_and_pointer_resolves(candidate):
    root, package = candidate
    before = runner.baseline(root)
    result = runner.publish(package, root)
    assert result["status"] == "published"
    assert (root / "Provisional").resolve() == root / "Releases" / runner.RELEASE
    assert (root / "Provisional/payload.txt").read_bytes() == (package / "payload.txt").read_bytes()
    assert runner.baseline(root) == before
    manifest = json.loads((root / "Provisional/manifest.json").read_text())
    runner.verify_artifacts(root / "Provisional", manifest)


def test_changed_candidate_artifact_is_rejected_before_copy(candidate):
    root, package = candidate
    (package / "payload.txt").write_text("Unverified replacement\n")
    with pytest.raises(ValueError, match="artifacts"):
        runner.publish(package, root)
    assert_unpublished(root)


def test_manifest_mutation_during_copy_cannot_publish(candidate, monkeypatch):
    root, package = candidate
    original_copy = runner.shutil.copytree

    def mutate_manifest(source, destination, *args, **kwargs):
        result = original_copy(source, destination, *args, **kwargs)
        if Path(source) == package:
            changed = json.loads((Path(destination) / "manifest.json").read_text())
            changed["status"] = "not verified"
            runner.write_json(Path(destination) / "manifest.json", changed)
        return result

    monkeypatch.setattr(runner.shutil, "copytree", mutate_manifest)
    with pytest.raises(ValueError, match="[Mm]anifest"):
        runner.publish(package, root)
    assert_unpublished(root)


def test_data_mutation_during_copy_cannot_publish(candidate, monkeypatch):
    root, package = candidate
    original_copy = runner.shutil.copytree

    def mutate_payload(source, destination, *args, **kwargs):
        result = original_copy(source, destination, *args, **kwargs)
        (Path(destination) / "payload.txt").write_text("Corrupted during transfer\n")
        return result

    monkeypatch.setattr(runner.shutil, "copytree", mutate_payload)
    with pytest.raises(ValueError, match="artifacts"):
        runner.publish(package, root)
    assert_unpublished(root)


def test_failed_pointer_promotion_rolls_back_new_release(candidate, monkeypatch):
    root, package = candidate
    original_replace = runner.os.replace

    def fail_pointer(source, destination, *args, **kwargs):
        if Path(destination) == root / "Provisional":
            raise OSError("Simulated pointer promotion failure")
        return original_replace(source, destination, *args, **kwargs)

    monkeypatch.setattr(runner.os, "replace", fail_pointer)
    with pytest.raises(OSError, match="Simulated"):
        runner.publish(package, root)
    assert_unpublished(root)
    assert not (root / ".Provisional-pending").is_symlink()
    # A failed first attempt must not make the verified candidate unretryable.
    monkeypatch.setattr(runner.os, "replace", original_replace)
    assert runner.publish(package, root)["status"] == "published"


def test_preexisting_pending_pointer_is_checked_before_release_move(candidate):
    root, package = candidate
    pending = root / ".Provisional-pending"
    pending.write_text("A prior operation needs inspection\n")
    with pytest.raises(ValueError, match="pointer"):
        runner.publish(package, root)
    assert_unpublished(root)
    assert pending.read_text() == "A prior operation needs inspection\n"


def test_existing_broken_provisional_pointer_is_never_overwritten(candidate):
    root, package = candidate
    pointer = root / "Provisional"
    pointer.symlink_to("Releases/prior-release")
    with pytest.raises(ValueError, match="already exists"):
        runner.publish(package, root)
    assert pointer.readlink() == Path("Releases/prior-release")
    assert not (root / "Releases" / runner.RELEASE).exists()


def test_source_release_mutation_during_copy_blocks_publication(candidate, monkeypatch):
    root, package = candidate
    original_copy = runner.shutil.copytree

    def mutate_baseline(source, destination, *args, **kwargs):
        result = original_copy(source, destination, *args, **kwargs)
        (root / f"Final/{runner.BASE}.parquet").write_bytes(b"Historical file changed")
        return result

    monkeypatch.setattr(runner.shutil, "copytree", mutate_baseline)
    with pytest.raises(ValueError, match="Historical release artifact changed"):
        runner.publish(package, root)
    assert_unpublished(root)


def test_symlink_in_candidate_is_rejected(candidate):
    root, package = candidate
    (package / "unbound-link").symlink_to(root / f"Final/{runner.BASE}.parquet")
    with pytest.raises(ValueError, match="symlinks"):
        runner.publish(package, root)
    assert_unpublished(root)


@pytest.fixture
def assembly_inputs(candidate, monkeypatch):
    root, package = candidate
    runner.shutil.rmtree(package)
    work = package.parent
    sources = work / "source-snapshot"
    sources.mkdir()
    (sources / "source.csv").write_text("UNITID,VALUE\n1,8\n")
    runner.write_json(sources / "source_manifest.json", {
        "artifacts": [{"path": "source.csv", "sha256": runner.sha256(sources / "source.csv")}]
    })
    preflight = {"tables": [{"table": "fixture", "sha256": "verified source"}]}
    runner.write_json(work / "pipeline/extension_2024_overlay.json", {"source_preflight": preflight})
    monkeypatch.setattr(runner, "validate_sources", lambda *args: preflight)
    for name in ("panel_clean_prch", "panel_wide_analysis", "panel_long_scalar", "panel_long_dimensioned"):
        path = work / f"build/Panels/v2/{name}_2024_2024.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"Already verified single-year panel")
    for name in ("dictionary_lake", "dictionary_codes"):
        path = work / f"build/Dictionary/v2/{name}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"Verified dictionary fixture")
    lineage = work / "build/Checks/v2/wide_qc/qc_value_lineage.parquet"
    lineage.parent.mkdir(parents=True)
    lineage.write_bytes(b"Verified lineage fixture")

    def create_output(path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"Transformation already tested in dedicated extension tests")
        return {"status": "pass"}

    monkeypatch.setattr(runner, "append_year", lambda base, new, output, **kwargs: create_output(output))
    monkeypatch.setattr(runner, "quarantine_mission_records", lambda source, output, quarantine, **kwargs:
                        (create_output(output), create_output(quarantine))[0])
    monkeypatch.setattr(runner, "combine_metadata", lambda base, new, output: create_output(output))
    monkeypatch.setattr(runner, "compact_lineage", lambda inputs, output: create_output(output))
    monkeypatch.setattr(runner, "run", lambda *args, **kwargs: None)
    return root, work, sources


def test_mutated_copied_source_snapshot_is_rejected(assembly_inputs, monkeypatch):
    root, work, sources = assembly_inputs
    original_copy = runner.shutil.copytree

    def corrupt_source(source, destination, *args, **kwargs):
        result = original_copy(source, destination, *args, **kwargs)
        if Path(source) == sources:
            (Path(destination) / "source.csv").write_text("UNITID,VALUE\n1,999\n")
        return result

    monkeypatch.setattr(runner.shutil, "copytree", corrupt_source)
    with pytest.raises(ValueError, match="changed or missing"):
        runner.assemble(work, sources, root)
    assert not (work / "package/manifest.json").exists()
    assert not (root / "Provisional").exists()


def test_different_source_snapshot_cannot_be_attached_to_existing_build(assembly_inputs, monkeypatch):
    root, work, sources = assembly_inputs
    monkeypatch.setattr(runner, "validate_sources", lambda *args: {"tables": [{"sha256": "different source"}]})
    with pytest.raises(ValueError, match="Requested sources differ"):
        runner.assemble(work, sources, root)
    assert not (work / "package").exists()


def test_strict_new_year_export_failure_stops_assembly(assembly_inputs, monkeypatch):
    root, work, sources = assembly_inputs
    append_calls = []

    def command(argv, receipt, **kwargs):
        if "--require-metadata" in argv:
            raise RuntimeError("New-year metadata is incomplete")

    monkeypatch.setattr(runner, "run", command)
    monkeypatch.setattr(runner, "append_year", lambda *args, **kwargs: append_calls.append(args))
    with pytest.raises(RuntimeError, match="metadata is incomplete"):
        runner.assemble(work, sources, root)
    assert append_calls == []
    assert not (work / "package/manifest.json").exists()


def test_fresh_assembly_copies_hash_bound_audits_without_manual_build_checks(assembly_inputs):
    root, work, sources = assembly_inputs
    rules = json.loads(runner.DEFAULT_POLICY.read_text())
    for row in rules["records"]:
        assert not (work / "build/Checks" / row["evidence"]["audit_receipt_name"]).exists()
    runner.assemble(work, sources, root)
    for row in rules["records"]:
        evidence = row["evidence"]
        actual = work / "package/Checks/2024" / evidence["audit_receipt_name"]
        assert runner.sha256(actual) == evidence["audit_receipt_sha256"]
        versioned = work / "package/Reproduction/extension-code/contracts/source_quality/evidence" / evidence["audit_receipt_name"]
        assert actual.read_bytes() == versioned.read_bytes()


def test_reviewed_source_audit_copy_is_idempotent_and_refuses_conflicting_evidence(tmp_path):
    package = tmp_path / "package"
    runner.copy_quarantine_evidence(package)
    before = runner.artifacts(package)
    runner.copy_quarantine_evidence(package)
    assert runner.artifacts(package) == before
    rules = json.loads(runner.DEFAULT_POLICY.read_text())
    target = package / "Checks/2024" / rules["records"][0]["evidence"]["audit_receipt_name"]
    target.write_text("Conflicting audit that requires inspection\n")
    with pytest.raises(ValueError, match="Existing mission source audit"):
        runner.copy_quarantine_evidence(package)
    assert target.read_text() == "Conflicting audit that requires inspection\n"


def test_versioned_audit_must_match_the_reviewed_policy_before_copy(tmp_path, monkeypatch):
    policy_root = tmp_path / "policy"
    runner.shutil.copytree(runner.DEFAULT_POLICY.parent, policy_root)
    policy = policy_root / runner.DEFAULT_POLICY.name
    rules = json.loads(policy.read_text())
    source = policy_root / "evidence" / rules["records"][0]["evidence"]["audit_receipt_name"]
    source.write_text("Changed versioned evidence\n")
    monkeypatch.setattr(runner, "DEFAULT_POLICY", policy)
    with pytest.raises(ValueError, match="Versioned mission source audit"):
        runner.copy_quarantine_evidence(tmp_path / "package")


@pytest.mark.parametrize("failure", ["codebook", "pdf", "asset_manifest"])
def test_failed_codebook_cannot_leave_a_publishable_manifest(candidate, monkeypatch, failure):
    import build_codebook
    import codebook_pdf
    import publish_labeled_panel

    root, package = candidate
    (package / "manifest.json").unlink()
    runner.write_json(package / "Checks/historical_release.json", runner.baseline(root))
    monkeypatch.setattr(runner, "run", lambda *args, **kwargs: None)
    monkeypatch.setattr(publish_labeled_panel, "verify_package", lambda *args, **kwargs: (
        {"parquet_parity": {"status": "pass"}, "artifacts": [], "metadata_gaps": []}, None))

    def codebook(*args, **kwargs):
        pending = json.loads(kwargs["manifest_path"].read_text())
        assert pending["status"] != "verified"
        if failure == "codebook":
            raise RuntimeError("Simulated codebook failure")
        (package / "Codebook").mkdir()
        (package / "Codebook/index.json").write_text("{}")

    def pdf(*args):
        if failure == "pdf":
            raise RuntimeError("Simulated PDF failure")

    def asset_manifest(*args):
        raise RuntimeError("Simulated asset manifest failure")

    monkeypatch.setattr(build_codebook, "build", codebook)
    monkeypatch.setattr(codebook_pdf, "render_pdf", pdf)
    monkeypatch.setattr(codebook_pdf, "write_manifest", asset_manifest)
    with pytest.raises(RuntimeError, match="Simulated"):
        runner.validate(package.parent, root)
    assert not (package / "manifest.json").exists()
    with pytest.raises(FileNotFoundError):
        runner.publish(package, root)
    assert not (root / "Provisional").exists()


@pytest.fixture
def native_checkpoint_package(tmp_path, monkeypatch):
    import build_codebook
    import codebook_pdf

    publisher_test = load_script_module("native_checkpoint_fixture", "tests/test_publish_labeled_panel.py")
    root, original_package = publisher_test.package.__wrapped__(tmp_path)
    package = original_package.with_name("package")
    original_package.rename(package)
    old_stem = Path(publisher_test.publisher.PANEL).stem
    for path in package.glob(old_stem + ".*"):
        path.rename(path.with_name(path.name.replace(old_stem, runner.PANEL, 1)))
    raw = root / "Releases" / publisher_test.publisher.RELEASE / f"Panels/v2/{publisher_test.publisher.PANEL}"
    runner.write_json(raw.with_name(raw.name + ".quarantine-validation.json"), {"status": "pass"})
    runner.write_json(raw.with_name(raw.name + ".consolidation-validation.json"), {"status": "pass"})
    runner.write_json(package / f"Supporting/{runner.PANEL}.analysis.parquet.quarantine-validation.json", {"status": "pass"})
    runner.write_json(package / "Checks/historical_release.json", {})
    monkeypatch.setattr(runner, "baseline", lambda root: {})
    monkeypatch.setattr(runner, "verify_2024_evidence", lambda package: None)
    monkeypatch.setattr(runner, "verify_append_evidence", lambda package, root: None)
    monkeypatch.setattr(runner, "verify_stata_history", lambda package, root: {"status": "pass"})
    monkeypatch.setattr(runner, "analysis_input", lambda package, **kwargs: raw)
    monkeypatch.setattr(runner, "consolidated_input", lambda package, **kwargs: raw)
    def codebook(*args, **kwargs):
        directory = package / "Codebook"
        runner.write_json(directory / "index.json", {
            "release": runner.RELEASE, "row_count": 3, "column_count": 3,
            "generated_from": {"parquet_sha256": runner.sha256(package / f"{runner.PANEL}.parquet")},
            "codebook_url": "https://markjayson13.github.io/IPEDSDB_Panel/provisional/",
            "variables": [{"name": name, "detail_file": "variables-001.json"} for name in ("year", "UNITID", "VALUE")],
        })
        runner.write_json(directory / "variables-001.json", {name: {"name": name} for name in ("year", "UNITID", "VALUE")})

    def pdf(directory):
        (directory / "ipeds-panel-codebook.pdf").write_bytes(b"PDF rendering is exercised in dedicated codebook tests")

    monkeypatch.setattr(build_codebook, "build", codebook)
    monkeypatch.setattr(codebook_pdf, "render_pdf", pdf)
    monkeypatch.setattr(runner, "run", lambda *args, **kwargs: pytest.fail("Must reuse the native checkpoint"))
    return root, package


def test_validate_reuses_native_checkpoint_but_runs_full_package_verification(native_checkpoint_package, monkeypatch):
    import publish_labeled_panel

    root, package = native_checkpoint_package
    path = package / "Checks/native_stata/validation.json"
    checkpoint = path.read_bytes()
    verified = []
    original_verify = publish_labeled_panel.verify_package

    def verify(*args, **kwargs):
        result = original_verify(*args, **kwargs)
        verified.append(result[0]["native_stata_receipt_sha256"])
        return result

    monkeypatch.setattr(publish_labeled_panel, "verify_package", verify)
    runner.validate(package.parent, root)
    assert verified == [runner.sha256(path)]
    assert path.read_bytes() == checkpoint
    assert json.loads((package / "manifest.json").read_text())["status"] == "verified"
    assert json.loads((package / "Checks/export_validation.json").read_text())["parquet_parity"]["status"] == "pass"


def test_validation_includes_hash_bound_codebook_manifest_and_correct_website_readme(native_checkpoint_package):
    root, package = native_checkpoint_package
    runner.validate(package.parent, root)
    directory = package / "Codebook"
    index = json.loads((directory / "index.json").read_text())
    assets = json.loads((directory / "manifest.json").read_text())
    assert assets["generated_from"] == index["generated_from"]
    assert assets["row_count"] == index["row_count"]
    assert assets["column_count"] == index["column_count"]
    assert {row["path"] for row in assets["artifacts"]} == {
        "README.txt", "index.json", "ipeds-panel-codebook.pdf", "variables-001.json"}
    for row in assets["artifacts"]:
        path = directory / row["path"]
        assert runner.sha256(path) == row["sha256"]
        assert path.stat().st_size == row["size_bytes"]
    assert index["codebook_url"] in (directory / "README.txt").read_text()
    manifest = json.loads((package / "manifest.json").read_text())
    bound = {row["path"]: row["sha256"] for row in manifest["artifacts"]}
    assert bound["Codebook/manifest.json"] == runner.sha256(directory / "manifest.json")
    assert bound["Codebook/README.txt"] == runner.sha256(directory / "README.txt")


@pytest.mark.parametrize("failure,match", [("status", "full-panel verification did not pass"),
                                          ("hash", "not bound"), ("log", "commands or log differ")])
def test_saved_native_checkpoint_cannot_bypass_receipt_verification(native_checkpoint_package, failure, match):
    root, package = native_checkpoint_package
    path = package / "Checks/native_stata/validation.json"
    receipt = json.loads(path.read_text())
    if failure == "status":
        receipt["status"] = "failed"
    elif failure == "hash":
        receipt["inputs"]["data"]["sha256"] = "0" * 64
    else:
        (package / "Checks/native_stata/chunk_01/native_verify.log").write_text("A different native log\n")
    runner.write_json(path, receipt)
    checkpoint = path.read_bytes()
    with pytest.raises(ValueError, match=match):
        runner.validate(package.parent, root)
    assert path.read_bytes() == checkpoint
    assert not (package / "manifest.json").exists()


def test_missing_native_receipt_does_not_treat_an_occupied_directory_as_a_checkpoint(native_checkpoint_package, monkeypatch):
    root, package = native_checkpoint_package
    (package / "Checks/native_stata/validation.json").unlink()
    log = package / "Checks/native_stata/chunk_01/native_verify.log"
    before = log.read_bytes()
    commands = []

    def native_command(*args, **kwargs):
        commands.append(args)
        raise RuntimeError("Native validator refuses occupied output directory")

    monkeypatch.setattr(runner, "run", native_command)
    with pytest.raises(RuntimeError, match="occupied output directory"):
        runner.validate(package.parent, root)
    assert len(commands) == 1
    assert log.read_bytes() == before
    assert not (package / "manifest.json").exists()


@pytest.fixture
def bound_qa_package(tmp_path):
    qa_test = load_script_module("qa29_binding_fixture", "tests/test_validate_2024_extension.py")
    original = qa_test.fixture(tmp_path / "qa-inputs")
    receipt = qa_test.MODULE.validate(*original)
    package = tmp_path / "package"
    package.mkdir()
    paths = {
        "wide": package / "Supporting/panel_wide_analysis_2024_2024.parquet",
        "clean": package / "Supporting/panel_clean_prch_2024_2024.parquet",
        "mapping": package / "Reproduction/pipeline-2024/contracts/extension_2024/variable_mapping.csv",
        "policy": package / "Reproduction/pipeline-2024/contracts/extension_2024/prch_policy.csv",
        "actions": package / "Checks/2024/v2/prch_qc/prch_cell_actions.parquet",
    }
    for name, source in zip(paths, original[1:]):
        target = paths[name]
        target.parent.mkdir(parents=True, exist_ok=True)
        runner.shutil.copy2(source, target)
    runner.shutil.copytree(original[0], package / "Sources/2024")
    runner.write_json(package / "Checks/raw_source_validation.json", receipt)
    sources = {}
    for name, relative in {
        "dictionary": "Metadata/2024/dictionary_lake.parquet",
        "codes": "Metadata/2024/dictionary_codes.parquet",
        "lineage": "Checks/2024/v2/wide_qc/qc_value_lineage.parquet",
    }.items():
        target = package / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"Metadata binding fixture: " + name.encode())
        sources[name] = runner.sha256(target)
    strict = package / "Checks/2024_metadata/panel.parquet"
    strict.parent.mkdir(parents=True)
    runner.shutil.copy2(paths["clean"], strict)
    metadata = {"metadata_status": "complete", "readiness_status": "complete",
                "format_readiness_status": "complete", "issues": [],
                "data_sha256": runner.sha256(strict), "source_panel_sha256": runner.sha256(paths["clean"]),
                "metadata_source_sha256": sources, "row_count": 2, "column_count": 6,
                "requested_years": [2024], "years": [2024],
                "variables": [{"name": name} for name in ["UNITID", "PRCH_F", "FORM_F", "F3EQUITR", "F3SALRPC", "year"]]}
    runner.write_json(strict.with_name(strict.name + ".metadata.json"), metadata)
    return package, paths


def test_actual_qa29_receipt_binds_packaged_sources_and_strict_export(bound_qa_package):
    package, _ = bound_qa_package
    runner.verify_2024_evidence(package)


@pytest.mark.parametrize("name", ["wide", "clean", "mapping", "policy", "actions"])
def test_qa29_receipt_rejects_changed_bound_inputs(bound_qa_package, name):
    package, paths = bound_qa_package
    paths[name].write_bytes(paths[name].read_bytes() + b"corrupted after validation")
    with pytest.raises(ValueError):
        runner.verify_2024_evidence(package)


def test_qa29_receipt_rejects_changed_physical_source(bound_qa_package):
    package, _ = bound_qa_package
    source = package / "Sources/2024/Raw_Access_Databases/2024/tables_csv/FIN2024.csv"
    source.write_text(source.read_text().replace(",48,", ",49,"))
    with pytest.raises(ValueError):
        runner.verify_2024_evidence(package)


def test_qa29_receipt_cannot_omit_a_mapped_source_table(bound_qa_package):
    package, _ = bound_qa_package
    path = package / "Checks/raw_source_validation.json"
    receipt = json.loads(path.read_text())
    receipt["scalar_source_tables"] = []
    runner.write_json(path, receipt)
    with pytest.raises(ValueError):
        runner.verify_2024_evidence(package)


@pytest.mark.parametrize("field,value", [("status", "failed"), ("year", 2023),
                                         ("raw_to_wide_discrepancies", 1),
                                         ("unexpected_cleaning_changes", 1)])
def test_qa29_receipt_must_attest_the_correct_year_and_zero_discrepancies(bound_qa_package, field, value):
    package, _ = bound_qa_package
    path = package / "Checks/raw_source_validation.json"
    receipt = json.loads(path.read_text())
    receipt[field] = value
    runner.write_json(path, receipt)
    with pytest.raises(ValueError):
        runner.verify_2024_evidence(package)


@pytest.mark.parametrize("field,value", [("readiness_status", "incomplete"),
                                         ("data_sha256", "0" * 64),
                                         ("source_panel_sha256", "0" * 64),
                                         ("metadata_source_sha256", {})])
def test_strict_new_year_receipt_must_bind_the_validated_data_and_metadata(bound_qa_package, field, value):
    package, _ = bound_qa_package
    path = package / "Checks/2024_metadata/panel.parquet.metadata.json"
    receipt = json.loads(path.read_text())
    receipt[field] = value
    runner.write_json(path, receipt)
    with pytest.raises(ValueError):
        runner.verify_2024_evidence(package)


@pytest.mark.parametrize("scope", ["rows", "columns"])
def test_strict_gate_cannot_cover_only_part_of_the_new_year(bound_qa_package, scope):
    import pyarrow.parquet as pq

    package, _ = bound_qa_package
    path = package / "Checks/2024_metadata/panel.parquet"
    metadata_path = path.with_name(path.name + ".metadata.json")
    table = pq.read_table(path)
    table = table.slice(0, 1) if scope == "rows" else table.select(table.column_names[:-1])
    pq.write_table(table, path)
    metadata = json.loads(metadata_path.read_text())
    metadata.update(data_sha256=runner.sha256(path), row_count=table.num_rows, column_count=table.num_columns,
                    variables=[{"name": name} for name in table.column_names])
    runner.write_json(metadata_path, metadata)
    with pytest.raises(ValueError):
        runner.verify_2024_evidence(package)


@pytest.fixture
def bound_append_package(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    root = tmp_path / "release-root"
    package = tmp_path / "work/package"
    paths = {
        "historical": root / f"Final/{runner.BASE}.parquet",
        "new_year": package / "Supporting/panel_clean_prch_2024_2024.parquet",
        "combined": package / f"Supporting/{runner.PANEL}.unannotated.parquet",
    }
    historical = pa.table({"UNITID": [1, 2], "year": [2023, 2023],
                           "VALUE": [8.0, None], "OLD_ONLY": [5, 6]})
    incoming = pa.table({"UNITID": [1, 2, 3], "year": [2024, 2024, 2024],
                         "VALUE": [9.0, None, 12.0], "NEW_ONLY": ["a", None, "c"]})
    for key, table in (("historical", historical), ("new_year", incoming)):
        paths[key].parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, paths[key])
    # Generate the receipt from an actual append and independent cell comparison.
    runner.append_year(paths["historical"], paths["new_year"], paths["combined"], base_years=[2023])
    paths["receipt"] = paths["combined"].with_name(paths["combined"].name + ".append-validation.json")
    for name in ("dictionary_lake", "dictionary_codes"):
        metadata_paths = {
            "historical": root / f"Final/Metadata/{name}.parquet",
            "new_year": package / f"Metadata/2024/{name}.parquet",
            "combined": package / f"Metadata/{name}.parquet",
            "receipt": package / f"Checks/{name}_append.json",
        }
        for key, years in (("historical", [2023, 2023]), ("new_year", [2024])):
            metadata_paths[key].parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.table({"year": years, "varname": ["VALUE"] * len(years),
                                     "label": [f"{name} label {year}" for year in years]}), metadata_paths[key])
        metadata_receipt = runner.combine_metadata(metadata_paths["historical"], metadata_paths["new_year"],
                                                   metadata_paths["combined"])
        runner.write_json(metadata_paths["receipt"], metadata_receipt)
        paths[name] = metadata_paths
    return root, package, paths


def test_actual_append_receipt_binds_historical_new_year_and_combined_data(bound_append_package):
    root, package, _ = bound_append_package
    runner.verify_append_evidence(package, root)


def test_append_receipt_remains_valid_after_package_relocation(bound_append_package, tmp_path):
    root, package, _ = bound_append_package
    copied = tmp_path / "relocated-package"
    runner.shutil.copytree(package, copied)
    runner.verify_append_evidence(copied, root)


@pytest.mark.parametrize("name", ["historical", "new_year", "combined"])
def test_append_receipt_rejects_values_changed_after_append(bound_append_package, name):
    import pyarrow as pa
    import pyarrow.parquet as pq

    root, package, paths = bound_append_package
    table = pq.read_table(paths[name])
    index = table.schema.get_field_index("VALUE")
    values = table.column(index).to_pylist()
    values[0] = 999.0
    pq.write_table(table.set_column(index, "VALUE", pa.array(values, type=pa.float64())), paths[name])
    with pytest.raises(ValueError, match="append receipt"):
        runner.verify_append_evidence(package, root)


@pytest.mark.parametrize("name", ["historical", "new_year"])
def test_append_receipt_rejects_wrong_source_checksum(bound_append_package, name):
    root, package, paths = bound_append_package
    receipt = json.loads(paths["receipt"].read_text())
    receipt["source_sha256"][str(paths[name])] = "0" * 64
    runner.write_json(paths["receipt"], receipt)
    with pytest.raises(ValueError, match="append receipt"):
        runner.verify_append_evidence(package, root)


@pytest.mark.parametrize("change", ["omit", "add"])
def test_append_receipt_requires_exactly_the_two_source_checksums(bound_append_package, change):
    root, package, paths = bound_append_package
    receipt = json.loads(paths["receipt"].read_text())
    if change == "omit":
        receipt["source_sha256"].pop(str(paths["historical"]))
    else:
        receipt["source_sha256"]["unrelated-source.parquet"] = runner.sha256(paths["historical"])
    runner.write_json(paths["receipt"], receipt)
    with pytest.raises(ValueError, match="append receipt"):
        runner.verify_append_evidence(package, root)


def test_append_source_checksums_must_bind_the_corresponding_parity_blocks(bound_append_package):
    root, package, paths = bound_append_package
    receipt = json.loads(paths["receipt"].read_text())
    old, new = str(paths["historical"]), str(paths["new_year"])
    hashes = receipt["source_sha256"]
    hashes[old], hashes[new] = hashes[new], hashes[old]
    runner.write_json(paths["receipt"], receipt)
    with pytest.raises(ValueError, match="source-block parity"):
        runner.verify_append_evidence(package, root)


@pytest.mark.parametrize("block", [0, 1])
@pytest.mark.parametrize("flag", ["all_values_equal", "all_missingness_equal", "out_of_scope_columns_all_null"])
@pytest.mark.parametrize("invalid", [None, False, 1])
def test_append_parity_requires_every_boolean_attestation(bound_append_package, block, flag, invalid):
    root, package, paths = bound_append_package
    receipt = json.loads(paths["receipt"].read_text())
    evidence = receipt["parity"]["blocks"][block]
    if invalid is None:
        evidence.pop(flag)
    else:
        evidence[flag] = invalid
    runner.write_json(paths["receipt"], receipt)
    with pytest.raises(ValueError, match="source-block parity"):
        runner.verify_append_evidence(package, root)


@pytest.mark.parametrize("scope", ["status", "year", "parity_status", "missing_block", "extra_block"])
def test_append_receipt_requires_passing_complete_parity(bound_append_package, scope):
    root, package, paths = bound_append_package
    receipt = json.loads(paths["receipt"].read_text())
    if scope == "status":
        receipt["status"] = "failed"
    elif scope == "year":
        receipt["year"] = 2023
    elif scope == "parity_status":
        receipt["parity"]["status"] = "failed"
    elif scope == "missing_block":
        receipt["parity"]["blocks"].pop()
    else:
        receipt["parity"]["blocks"].append(receipt["parity"]["blocks"][0])
    runner.write_json(paths["receipt"], receipt)
    with pytest.raises(ValueError, match="append"):
        runner.verify_append_evidence(package, root)


@pytest.mark.parametrize("block", [0, 1])
@pytest.mark.parametrize("field", ["rows", "columns"])
def test_append_parity_must_cover_each_complete_source(bound_append_package, block, field):
    root, package, paths = bound_append_package
    receipt = json.loads(paths["receipt"].read_text())
    receipt["parity"]["blocks"][block][field] -= 1
    runner.write_json(paths["receipt"], receipt)
    with pytest.raises(ValueError, match="source-block parity"):
        runner.verify_append_evidence(package, root)


@pytest.mark.parametrize("scope", ["rows", "columns", "column_type"])
def test_append_rejects_incomplete_shape_even_with_refreshed_data_checksum(bound_append_package, scope):
    import pyarrow as pa
    import pyarrow.parquet as pq

    root, package, paths = bound_append_package
    table = pq.read_table(paths["combined"])
    if scope == "rows":
        table = table.slice(0, table.num_rows - 1)
    elif scope == "columns":
        table = table.drop(["OLD_ONLY"])
    else:
        index = table.schema.get_field_index("VALUE")
        table = table.set_column(index, "VALUE", table.column(index).cast(pa.float32()))
    pq.write_table(table, paths["combined"])
    receipt = json.loads(paths["receipt"].read_text())
    receipt["data_sha256"] = runner.sha256(paths["combined"])
    runner.write_json(paths["receipt"], receipt)
    with pytest.raises(ValueError, match="shape or schema"):
        runner.verify_append_evidence(package, root)


@pytest.mark.parametrize("phase", ["validate", "publish"])
def test_append_evidence_is_required_at_validation_and_publication(bound_append_package, monkeypatch, phase):
    root, package, paths = bound_append_package
    receipt = json.loads(paths["receipt"].read_text())
    receipt["parity"]["blocks"][0].pop("all_missingness_equal")
    runner.write_json(paths["receipt"], receipt)
    monkeypatch.setattr(runner, "verify_2024_evidence", lambda package: None)
    if phase == "publish":
        # Re-signing the package inventory cannot legitimize incomplete append evidence.
        runner.write_json(package / "manifest.json", {
            "status": "verified", "release_name": runner.RELEASE,
            "historical_release": {}, "artifacts": runner.artifacts(package),
        })
    with pytest.raises(ValueError, match="source-block parity"):
        if phase == "validate":
            runner.validate(package.parent, root)
        else:
            runner.publish(package, root)
    assert not (root / "Provisional").exists()
    if phase == "validate":
        assert not (package / "manifest.json").exists()


@pytest.mark.parametrize("name", ["dictionary_lake", "dictionary_codes"])
@pytest.mark.parametrize("scope", ["historical", "new_year", "combined"])
def test_metadata_append_rejects_changed_bound_dictionary(bound_append_package, name, scope):
    import pyarrow as pa
    import pyarrow.parquet as pq

    root, package, paths = bound_append_package
    path = paths[name][scope]
    table = pq.read_table(path)
    index = table.schema.get_field_index("label")
    changed = pa.array(["Unverified definition"] * table.num_rows)
    pq.write_table(table.set_column(index, "label", changed), path)
    with pytest.raises(ValueError, match="Metadata append evidence changed"):
        runner.verify_append_evidence(package, root)


@pytest.mark.parametrize("name", ["dictionary_lake", "dictionary_codes"])
@pytest.mark.parametrize("field", ["historical_records_unchanged", "new_records_unchanged", "source_sha256"])
def test_metadata_append_requires_complete_evidence(bound_append_package, name, field):
    root, package, paths = bound_append_package
    path = paths[name]["receipt"]
    receipt = json.loads(path.read_text())
    receipt.pop(field)
    runner.write_json(path, receipt)
    with pytest.raises(ValueError, match="Metadata append evidence changed"):
        runner.verify_append_evidence(package, root)


@pytest.fixture
def bound_analysis_package(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq

    package = tmp_path / "work/package"
    raw = package / f"Supporting/{runner.PANEL}.unannotated.parquet"
    selected = package / f"Supporting/{runner.PANEL}.analysis.parquet"
    quarantine = package / "Supporting/quarantined_mission_records.parquet"
    policy = package / "Reproduction/extension-code/contracts/source_quality/mission-orphans-v1.json"
    rules = json.loads(runner.DEFAULT_POLICY.read_text())
    # Synthetic source-audit documents stand in for independently reviewed audit files;
    # the quarantine's passing data receipt is always generated by the real helper.
    audits = []
    for row in rules["records"]:
        audit = package / "Checks/2024" / row["evidence"]["audit_receipt_name"]
        runner.write_json(audit, {"year": row["year"], "fixture": "Reviewed synthetic source evidence"})
        row["evidence"]["audit_receipt_sha256"] = runner.sha256(audit)
        audits.append(audit)
    runner.write_json(policy, rules)
    reviewed_policy = tmp_path / "reviewed-policy.json"
    reviewed_policy.write_bytes(policy.read_bytes())
    monkeypatch.setattr(runner, "DEFAULT_POLICY", reviewed_policy)
    raw.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"UNITID": [7, 111111, 7, 111111], "year": [2019, 2019, 2024, 2024],
                             "MISSIONURL": [None, rules["records"][0]["MISSIONURL"],
                                            None, rules["records"][1]["MISSIONURL"]],
                             "VALUE": [8.0, None, 9.0, None]}), raw)
    proof = runner.quarantine_mission_records(raw, selected, quarantine, policy=policy)
    for fmt in ("parquet", "dta"):
        runner.write_json(package / f"{runner.PANEL}.{fmt}.metadata.json",
                          {"analysis_provenance": proof["analysis_provenance"]})
    return package, {"raw": raw, "selected": selected, "quarantine": quarantine,
                     "receipt": selected.with_name(selected.name + ".quarantine-validation.json"),
                     "policy": policy, "audits": audits}


def test_analysis_input_requires_real_quarantine_proof_and_export_provenance(bound_analysis_package):
    package, paths = bound_analysis_package
    assert runner.analysis_input(package) == paths["selected"]
    assert runner.analysis_input(package, verify_exports=True) == paths["selected"]


def test_analysis_input_rejects_a_tampered_quarantine_receipt(bound_analysis_package):
    package, paths = bound_analysis_package
    receipt = json.loads(paths["receipt"].read_text())
    receipt["source_sha256"] = "0" * 64
    runner.write_json(paths["receipt"], receipt)
    with pytest.raises(ValueError, match="not bound"):
        runner.analysis_input(package)


def test_analysis_input_rejects_missing_embedded_provenance_even_if_inventory_is_resigned(bound_analysis_package):
    import pyarrow.parquet as pq
    from entity_quarantine import PROVENANCE_KEY

    package, paths = bound_analysis_package
    table = pq.read_table(paths["selected"])
    metadata = dict(table.schema.metadata)
    metadata.pop(PROVENANCE_KEY)
    pq.write_table(table.replace_schema_metadata(metadata), paths["selected"])
    receipt = json.loads(paths["receipt"].read_text())
    receipt["data_sha256"] = runner.sha256(paths["selected"])
    runner.write_json(paths["receipt"], receipt)
    with pytest.raises(ValueError, match="analysis provenance"):
        runner.analysis_input(package)


def test_analysis_input_rejects_an_unreviewed_policy(bound_analysis_package):
    package, paths = bound_analysis_package
    policy = json.loads(paths["policy"].read_text())
    policy["reason"] = "Unreviewed replacement policy"
    runner.write_json(paths["policy"], policy)
    with pytest.raises(ValueError, match="reviewed policy"):
        runner.analysis_input(package)


@pytest.mark.parametrize("index", [0, 1])
def test_analysis_input_binds_both_original_source_audits(bound_analysis_package, index):
    package, paths = bound_analysis_package
    paths["audits"][index].write_text("Altered source evidence\n")
    with pytest.raises(ValueError, match="source audit changed"):
        runner.analysis_input(package)


@pytest.mark.parametrize("fmt", ["parquet", "dta"])
@pytest.mark.parametrize("change", ["omit", "alter"])
def test_analysis_input_requires_each_export_to_disclose_the_exact_quarantine(bound_analysis_package, fmt, change):
    package, _ = bound_analysis_package
    path = package / f"{runner.PANEL}.{fmt}.metadata.json"
    metadata = json.loads(path.read_text())
    if change == "omit":
        metadata.pop("analysis_provenance")
    else:
        metadata["analysis_provenance"]["excluded_keys"].pop()
    runner.write_json(path, metadata)
    with pytest.raises(ValueError, match="Export omitted or changed"):
        runner.analysis_input(package, verify_exports=True)


@pytest.mark.parametrize("phase", ["export", "validate", "publish"])
def test_missing_quarantine_receipt_stops_every_release_phase(bound_analysis_package, monkeypatch, phase):
    package, paths = bound_analysis_package
    root = package.parent.parent / "release-root"
    paths["receipt"].unlink()
    commands = []
    monkeypatch.setattr(runner, "run", lambda *args, **kwargs: commands.append(args))
    monkeypatch.setattr(runner, "verify_2024_evidence", lambda package: None)
    monkeypatch.setattr(runner, "verify_append_evidence", lambda package, root: None)
    monkeypatch.setattr(runner, "baseline", lambda root: {})
    runner.write_json(package / "Checks/historical_release.json", {})
    if phase == "publish":
        runner.write_json(package / "manifest.json", {
            "status": "verified", "release_name": runner.RELEASE,
            "historical_release": {}, "artifacts": runner.artifacts(package),
        })
    with pytest.raises(FileNotFoundError, match="quarantine-validation"):
        getattr(runner, phase)(package if phase == "publish" else package.parent, root)
    assert commands == []
    assert not (root / "Provisional").exists()
    if phase != "publish":
        assert not (package / "manifest.json").exists()


@pytest.fixture
def bound_consolidation_package(tmp_path, monkeypatch):
    import gzip
    import pyarrow as pa
    import pyarrow.parquet as pq
    from panel_consolidation import consolidate, consolidate_identities

    package = tmp_path / "work/package"
    source = package / f"Supporting/{runner.PANEL}.analysis.parquet"
    output = package / f"Supporting/{runner.PANEL}.consolidated.parquet"
    policy = package / "Reproduction/extension-code/contracts/harmonization/source-family-consolidation-v1.json"
    evidence = policy.with_name("source-family-consolidation-v1.evidence.json.gz")
    evidence.parent.mkdir(parents=True)
    evidence.write_bytes(gzip.compress(b'{"review":"Synthetic source-family evidence"}', mtime=0))
    rule = {"schema_version": 1, "policy_id": "source-family-consolidation-v1",
            "evidence_file": evidence.name, "evidence_sha256": runner.sha256(evidence),
            "groups": [{"canonical_name": "MEASURE", "rationale": "Reviewed disjoint source years",
                        "caveats": ["Keep year-specific definitions"], "evidence": {"file": evidence.name},
                        "members": [{"column": "OLD_MEASURE", "years": [2019]},
                                    {"column": "NEW_MEASURE", "years": [2024]}]}]}
    runner.write_json(policy, rule)
    reviewed = tmp_path / "reviewed-policy.json"
    reviewed.write_bytes(policy.read_bytes())
    monkeypatch.setattr(runner, "CONSOLIDATION_POLICY", reviewed)
    source.parent.mkdir(parents=True)
    pq.write_table(pa.table({"UNITID": [1, 1], "year": [2019, 2024],
                            "OLD_MEASURE": pa.array([8.0, None]), "NEW_MEASURE": pa.array([None, 9.0]),
                            "KEEP": ["", None]}).replace_schema_metadata({b"original": b"quarantine retained"}), source)
    monkeypatch.setattr(runner, "analysis_input", lambda package, **kwargs: source)
    proof = consolidate(source, output, policy)
    lineage = package / "Metadata/column_identities.parquet"
    lineage.parent.mkdir()
    pq.write_table(pa.table({"analysis_column": ["OLD_MEASURE", "NEW_MEASURE", "KEEP"],
                            "year": [2019, 2024, 2019], "varname": ["SOURCE_A", "SOURCE_B", "KEEP"],
                            "source_file": ["SOURCE_2019", "SOURCE_2024", "SOURCE_2019"],
                            "access_table_name": ["A", "B", "A"], "source_varnumber": ["1", "2", None]})
                   .replace_schema_metadata({b"scope": b"all compact source identities"}), lineage)
    remapped = package / "Metadata/consolidated_column_identities.parquet"
    lineage_proof = consolidate_identities(lineage, remapped, policy)
    checks = package / "Checks/consolidated_column_identities.json"
    runner.write_json(checks, lineage_proof)
    names = pq.read_schema(output).names
    group = proof["column_consolidation"]["groups"][0]
    for fmt in ("parquet", "dta"):
        runner.write_json(package / f"{runner.PANEL}.{fmt}.metadata.json", {
            "column_consolidation": proof["column_consolidation"],
            "variables": [{"name": name, **({"column_consolidation": group} if name == "MEASURE" else {})}
                          for name in names]})
    (package / f"{runner.PANEL}.parquet").write_bytes(output.read_bytes())
    return package, {"source": source, "output": output, "policy": policy, "evidence": evidence,
                     "reviewed": reviewed, "receipt": output.with_name(output.name + ".consolidation-validation.json"),
                     "lineage": lineage, "remapped": remapped, "checks": checks,
                     "lineage_receipt": remapped.with_name(remapped.name + ".consolidation-identities.json")}


def test_consolidated_input_requires_real_inverse_lineage_and_export_proofs(bound_consolidation_package):
    package, paths = bound_consolidation_package
    assert runner.consolidated_input(package) == paths["output"]
    assert runner.consolidated_input(package, verify_exports=True) == paths["output"]


@pytest.mark.parametrize("artifact", ["policy", "evidence"])
def test_consolidation_requires_reviewed_policy_and_compressed_evidence(bound_consolidation_package, artifact):
    package, paths = bound_consolidation_package
    if artifact == "evidence":
        paths[artifact].write_bytes(b"Replaced compressed evidence")
    else:
        rule = json.loads(paths[artifact].read_text())
        rule["groups"][0]["rationale"] = "Not the reviewed registry"
        runner.write_json(paths[artifact], rule)
    with pytest.raises(ValueError, match="reviewed"):
        runner.consolidated_input(package)


def test_consolidation_rejects_an_escaping_evidence_reference(bound_consolidation_package):
    package, paths = bound_consolidation_package
    rule = json.loads(paths["policy"].read_text())
    rule["evidence_file"] = "../outside.json.gz"
    runner.write_json(paths["policy"], rule)
    paths["reviewed"].write_bytes(paths["policy"].read_bytes())
    with pytest.raises(ValueError, match="unsafe"):
        runner.consolidated_input(package)


@pytest.mark.parametrize("mutation", ["remap", "source_panel_column", "definition_identity", "missingness", "row_order", "row_count", "schema"])
def test_consolidation_independently_rechecks_lineage_even_with_refreshed_receipts(bound_consolidation_package, mutation):
    import pyarrow as pa
    import pyarrow.parquet as pq

    package, paths = bound_consolidation_package
    table = pq.read_table(paths["remapped"])
    if mutation in {"remap", "source_panel_column", "definition_identity", "missingness"}:
        name = {"remap": "analysis_column", "source_panel_column": "source_panel_column",
                "definition_identity": "varname", "missingness": "source_varnumber"}[mutation]
        values = table[name].to_pylist()
        values[0] = None if mutation == "missingness" else "Changed identity"
        field = table.schema.field(name)
        table = table.set_column(table.schema.get_field_index(name), field, pa.array(values, type=field.type))
    elif mutation == "row_order":
        table = table.take(pa.array([1, 0, 2]))
    elif mutation == "row_count":
        table = table.slice(1)
    else:
        table = table.select(list(reversed(table.column_names)))
    pq.write_table(table, paths["remapped"])
    receipt = json.loads(paths["lineage_receipt"].read_text())
    receipt["data_sha256"] = runner.sha256(paths["remapped"])
    for name in ("lineage_receipt", "checks"):
        runner.write_json(paths[name], receipt)
    with pytest.raises(ValueError, match="Consolidated lineage"):
        runner.consolidated_input(package)


@pytest.mark.parametrize("mutation", ["source_hash", "policy_hash", "missing_proof", "false_proof", "mirror"])
def test_consolidation_requires_complete_matching_lineage_receipts(bound_consolidation_package, mutation):
    package, paths = bound_consolidation_package
    receipt = json.loads(paths["lineage_receipt"].read_text())
    if mutation in {"source_hash", "policy_hash"}:
        receipt[mutation.replace("_hash", "_sha256")] = "0" * 64
    elif mutation == "missing_proof":
        receipt.pop("all_original_missingness_equal")
    else:
        receipt["all_original_fields_equal"] = False
    runner.write_json(paths["lineage_receipt"], receipt)
    if mutation != "mirror":
        runner.write_json(paths["checks"], receipt)
    with pytest.raises(ValueError, match="lineage receipt"):
        runner.consolidated_input(package)


def test_this_release_rejects_out_of_scope_compact_lineage_even_when_utility_retains_it(bound_consolidation_package):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from panel_consolidation import consolidate_identities

    package, paths = bound_consolidation_package
    table = pq.read_table(paths["lineage"])
    field = table.schema.field("year")
    table = table.set_column(table.schema.get_field_index("year"), field, pa.array([2024, 2024, 2019], type=field.type))
    pq.write_table(table, paths["lineage"])
    paths["remapped"].unlink()
    paths["lineage_receipt"].unlink()
    receipt = consolidate_identities(paths["lineage"], paths["remapped"], paths["policy"])
    assert receipt["out_of_scope_registered_identities"]
    runner.write_json(paths["checks"], receipt)
    with pytest.raises(ValueError, match="out-of-scope"):
        runner.consolidated_input(package)


@pytest.mark.parametrize("fmt", ["parquet", "dta"])
@pytest.mark.parametrize("mutation", ["omit", "change", "variable_crosswalk", "missing_variable"])
def test_consolidation_requires_exact_export_and_variable_provenance(bound_consolidation_package, fmt, mutation):
    package, _ = bound_consolidation_package
    path = package / f"{runner.PANEL}.{fmt}.metadata.json"
    metadata = json.loads(path.read_text())
    if mutation == "omit":
        metadata.pop("column_consolidation")
    elif mutation == "change":
        metadata["column_consolidation"]["source_sha256"] = "0" * 64
    elif mutation == "missing_variable":
        metadata["variables"] = [row for row in metadata["variables"] if row["name"] != "MEASURE"]
    else:
        next(row for row in metadata["variables"] if row["name"] == "MEASURE")["column_consolidation"]["members"].pop()
    runner.write_json(path, metadata)
    with pytest.raises(ValueError, match="Export"):
        runner.consolidated_input(package, verify_exports=True)


def test_consolidation_requires_embedded_parquet_provenance(bound_consolidation_package):
    import pyarrow.parquet as pq

    package, _ = bound_consolidation_package
    path = package / f"{runner.PANEL}.parquet"
    table = pq.read_table(path)
    metadata = dict(table.schema.metadata)
    metadata.pop(b"ipeds:column_consolidation")
    pq.write_table(table.replace_schema_metadata(metadata), path)
    with pytest.raises(ValueError, match="embedded column consolidation provenance"):
        runner.consolidated_input(package, verify_exports=True)


@pytest.mark.parametrize("phase", ["export", "validate", "publish"])
def test_corrupt_consolidation_proof_stops_every_release_phase(bound_consolidation_package, monkeypatch, phase):
    package, paths = bound_consolidation_package
    record = json.loads(paths["receipt"].read_text())
    record["parity"].pop("all_source_missingness_equal")
    runner.write_json(paths["receipt"], record)
    root = package.parent.parent / "release-root"
    commands = []
    monkeypatch.setattr(runner, "run", lambda *args, **kwargs: commands.append(args))
    monkeypatch.setattr(runner, "verify_2024_evidence", lambda package: None)
    monkeypatch.setattr(runner, "verify_append_evidence", lambda package, root: None)
    monkeypatch.setattr(runner, "baseline", lambda root: {})
    runner.write_json(package / "Checks/historical_release.json", {})
    if phase == "publish":
        runner.write_json(package / "manifest.json", {
            "status": "verified", "release_name": runner.RELEASE,
            "historical_release": {}, "artifacts": runner.artifacts(package),
        })
    with pytest.raises(ValueError, match="inverse parity"):
        getattr(runner, phase)(package if phase == "publish" else package.parent, root)
    assert commands == []
    assert not (root / "Provisional").exists()


@pytest.mark.parametrize("corrupt_evidence", [False, True])
def test_consolidate_phase_checks_evidence_and_revalidates_lineage(bound_consolidation_package, monkeypatch, corrupt_evidence):
    package, paths = bound_consolidation_package
    repository = package.parent.parent / "reviewed-code"
    for directory in ("Scripts", "tests", "contracts/harmonization", "docs"):
        (repository / directory).mkdir(parents=True)
    (repository / "docs/2024_EXTENSION.md").write_text("Current reviewed consolidation workflow\n")
    reviewed = repository / "contracts/harmonization/source-family-consolidation-v1.json"
    reviewed.write_bytes(paths["policy"].read_bytes())
    (reviewed.parent / paths["evidence"].name).write_bytes(
        b"Changed evidence" if corrupt_evidence else paths["evidence"].read_bytes())
    monkeypatch.setattr(runner, "REPOSITORY", repository)
    monkeypatch.setattr(runner, "CONSOLIDATION_POLICY", reviewed)
    for name in ("output", "receipt", "remapped", "lineage_receipt", "checks"):
        paths[name].unlink()
    if corrupt_evidence:
        with pytest.raises(ValueError, match="source evidence"):
            runner.consolidate(package.parent)
        assert not paths["output"].exists()
        assert not paths["remapped"].exists()
    else:
        runner.consolidate(package.parent)
        assert runner.consolidated_input(package) == paths["output"]
        assert (package / "Reproduction/extension-code/docs/2024_EXTENSION.md").read_bytes() == (
            repository / "docs/2024_EXTENSION.md").read_bytes()
