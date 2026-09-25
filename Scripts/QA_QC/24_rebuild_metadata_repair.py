#!/usr/bin/env python3
"""Stage a checked incremental v2 metadata repair through normal Stage 04-07 code.

This is not a new full-source release build. It reuses the unchanged historical
lanes, regenerates a declared physical-table scope from source CSVs, stitches
those complete records with the unaffected scalar partition, then runs the
normal archived v2 wide builder and unchanged PRCH cleaner. All writes must be
inside a fresh local staging root; source/release artifacts are read-only.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


POLICY_SHA256 = "67fca57085ec759944152f6e6e38374fac75991ef2dbd0489d241641d310e4f3"
TOKEN = "2004_2023"
SCOPE_YEAR = 2023
SCOPE_TABLES = ("SFA2223_P1", "SFA2223_P2")
METADATA_CELL_COLUMNS = {"varTitle", "longDescription", "DataType", "format", "Fieldwidth"}


def quote(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def identifier(value) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def file_identity(path: Path) -> dict:
    stat = path.stat()
    return {"path": str(path.resolve()), "bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns,
            "inode": stat.st_ino, "device": stat.st_dev}


def atomic_json(path: Path, value) -> None:
    pending = path.with_suffix(path.suffix + ".pending")
    pending.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    pending.replace(path)


def link_readonly(source: Path, destination: Path) -> None:
    if not source.exists():
        raise ValueError(f"Missing reuse input: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink() and destination.resolve() == source.resolve():
        return
    if destination.exists() or destination.is_symlink():
        raise ValueError(f"Refusing to replace an existing staged input: {destination}")
    destination.symlink_to(source.resolve(), target_is_directory=source.is_dir())


def copy_verified(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if sha256(source) != sha256(destination):
            raise ValueError(f"Existing staged copy differs: {destination}")
    else:
        shutil.copy2(source, destination)
    if sha256(source) != sha256(destination):
        raise ValueError(f"Copy checksum mismatch: {destination}")


def scope_sql() -> str:
    return f"year = {SCOPE_YEAR} AND upper(access_table_name) IN ({', '.join(quote(name) for name in SCOPE_TABLES)})"


def read_csv(path: Path) -> tuple[list[str], list[dict]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return reader.fieldnames or [], list(reader)


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def prepare_inputs(source: Path, output: Path, fresh_source: Path | None) -> dict:
    """Use normal full raw inputs for Stage 03 and a separate exact Stage 04 scope."""
    raw = output / "Raw_Access_Databases"
    raw.mkdir(parents=True, exist_ok=True)
    for year in range(2004, 2024):
        link_readonly(source / "Raw_Access_Databases" / str(year), raw / str(year))
    coverage = output / "Checks/v2/harmonize_qc"
    coverage.mkdir(parents=True, exist_ok=True)
    for path in sorted((source / "Checks/v2/harmonize_qc").glob("source_column_coverage_*.csv")):
        copy_verified(path, coverage / path.name)
    scoped = output / "scoped_stage04"
    year_source = source / "Raw_Access_Databases" / str(SCOPE_YEAR)
    year_target = scoped / "Raw_Access_Databases" / str(SCOPE_YEAR)
    copy_verified(year_source / "manifest.csv", year_target / "manifest.csv")
    columns, records = read_csv(year_source / "metadata/table_inventory.csv")
    records = [row for row in records if row["table_name"].upper() in SCOPE_TABLES]
    if {row["table_name"].upper() for row in records} != set(SCOPE_TABLES):
        raise ValueError("Source inventory must contain each declared SFA table exactly once.")
    if len(records) != len(SCOPE_TABLES):
        raise ValueError("Duplicate physical tables in source inventory.")
    bindings = []
    for row in records:
        original = year_source / row["csv_path"]
        original_hash = sha256(original)
        if original_hash != row["csv_sha256"]:
            raise ValueError(f"Source export no longer matches original inventory: {original}")
        selected = fresh_source / original.name if fresh_source else original
        if sha256(selected) != original_hash:
            raise ValueError(f"Fresh export bytes differ from historical source; this metadata-only repair cannot reuse source-row identity: {selected}")
        link_readonly(selected, year_target / row["csv_path"])
        bindings.append({"table": row["table_name"], "original": str(original), "selected": str(selected),
                         "sha256": original_hash, "rows": int(row["row_count_csv"])})
    write_csv(year_target / "metadata/table_inventory.csv", columns, records)
    policy = source / "Checks/v2/prch_qc/prch_flag_policy.csv"
    if sha256(policy) != POLICY_SHA256:
        raise ValueError("Prior PRCH policy checksum differs from the approved baseline.")
    copy_verified(policy, output / "contracts/prch_policy.csv")
    return {"scope": {"year": SCOPE_YEAR, "physical_tables": list(SCOPE_TABLES)},
            "source_exports": bindings, "policy_sha256": POLICY_SHA256,
            "build_claim": "incremental metadata repair using normal scoped Stage04/05 and full Stage06/07; historical unaffected inputs reused"}


def open_connection(output: Path, memory: str, threads: int):
    spill = output / "build/repair_spill"
    spill.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit={quote(memory)}")
    con.execute(f"SET threads={int(threads)}")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory={quote(spill)}")
    return con


def assert_multiset_equal(con, left: str, right: str, label: str) -> None:
    for first, second in ((left, right), (right, left)):
        mismatch = con.execute(f"SELECT * FROM (({first}) EXCEPT ALL ({second})) LIMIT 1").fetchone()
        if mismatch is not None:
            raise ValueError(f"{label} differs; first differing row begins {mismatch[:4]!r}")


def run_stage(command: list[str], output: Path, receipt: dict, label: str) -> None:
    logs = output / "build/repair_logs"
    logs.mkdir(parents=True, exist_ok=True)
    bound_inputs = {}
    for index, token in enumerate(command[:-1]):
        if token in {"--dictionary", "--prch-policy", "--analysis-schema-contract", "--discrete-family-contract",
                     "--table-grain-contract", "--source-column-contract", "--dictionary-ambiguity-overrides", "--source-metadata-aliases"}:
            path = Path(command[index + 1])
            bound_inputs[str(path)] = sha256(path)
    attempt = 1 + sum(stage["stage"] == label for stage in receipt.get("stages", []))
    log_path = logs / f"{label}-{attempt:02}.log"
    record = {"stage": label, "command": command, "started_unix": time.time(), "status": "running", "log": str(log_path),
              "script_sha256": sha256(Path(command[1])), "bound_input_sha256": bound_inputs}
    script_root = Path(command[1]).parent
    record["code_sha256"] = {path.name: sha256(path) for path in sorted(script_root.glob("*.py"))}
    receipt.setdefault("stages", []).append(record)
    atomic_json(output / "metadata_repair_receipt.json", receipt)
    print(f"[{label}] starting; log={log_path}", flush=True)
    with log_path.open("w") as log:
        env = dict(os.environ, IPEDSDB_ROOT=str(output), PYTHONDONTWRITEBYTECODE="1")
        result = subprocess.run(command, cwd=output, env=env, stdout=log, stderr=subprocess.STDOUT, check=False)
    record.update({"ended_unix": time.time(), "returncode": result.returncode,
                   "status": "complete" if result.returncode == 0 else "failed"})
    atomic_json(output / "metadata_repair_receipt.json", receipt)
    if result.returncode:
        raise RuntimeError(f"Stage {label} failed; inspect {log_path}")
    if any(sha256(Path(path)) != digest for path, digest in bound_inputs.items()):
        record["status"] = "input_changed"
        atomic_json(output / "metadata_repair_receipt.json", receipt)
        raise ValueError(f"A metadata/contract input changed while running {label}; regenerate this stage.")


def load_stage05(pipeline: Path):
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(pipeline / "Scripts"))
    spec = importlib.util.spec_from_file_location("repair_stage05", pipeline / "Scripts/05_stitch_long.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def staged_dictionary(args, output: Path) -> Path:
    source = Path(args.dictionary) if args.dictionary else output / "Dictionary/v2/dictionary_lake.parquet"
    if not source.is_file():
        raise ValueError("Run corrected normal Stage03 before regenerating records.")
    target = output / "Dictionary/v2/dictionary_lake.parquet"
    if source.resolve() != target.resolve():
        copy_verified(source, target)
        codes = source.parent / "dictionary_codes.parquet"
        if not codes.is_file():
            raise ValueError("Corrected Stage03 category codebook is missing.")
        copy_verified(codes, target.parent / codes.name)
    return target


def regenerate_scalar(args, output: Path, receipt: dict) -> None:
    source, pipeline = Path(args.source_root), Path(args.pipeline_repo)
    dictionary = staged_dictionary(args, output)
    scoped = output / "scoped_stage04"
    annual = scoped / "Cross_sections/v2"
    contracts = pipeline / "contracts"
    run_stage([sys.executable, str(pipeline / "Scripts/04_harmonize.py"), "--root", str(scoped), "--years", "2023:2023",
               "--dictionary", str(dictionary), "--output-dir", str(annual), "--parts-dir-base", str(scoped / "build/parts"),
               "--table-grain-contract", str(contracts / "table_grain.csv"), "--source-column-contract", str(contracts / "source_columns.csv"),
               "--dictionary-ambiguity-overrides", str(contracts / "dictionary_ambiguity_overrides.csv"),
               "--source-metadata-aliases", str(contracts / "source_metadata_aliases.csv"), "--chunksize", "2000",
               "--value-cols-per-chunk", "25", "--dedupe-temp-dir", str(scoped / "build/duckdb"),
               "--dedupe-threads", str(args.threads), "--duckdb-memory-limit", args.memory_limit, "--release-strict"], output, receipt, "04_scoped")
    stitched = scoped / "Panels/v2"
    scoped_scalar = stitched / "panel_long_scalar_2023_2023.parquet"
    scoped_rows = stitched / "panel_source_rows_2023_2023.parquet"
    scoped_dimensioned = stitched / "panel_long_dimensioned_2023_2023.parquet"
    run_stage([sys.executable, str(pipeline / "Scripts/05_stitch_long.py"), "--root", str(scoped), "--years", "2023:2023",
               "--cross-sections-dir", str(annual), "--output", str(scoped_scalar), "--dimensioned-output", str(scoped_dimensioned),
               "--source-rows-output", str(scoped_rows), "--duckdb-memory-limit", args.memory_limit, "--duckdb-threads", str(args.threads),
               "--duckdb-temp-dir", str(scoped / "build/stitch_spill"), "--batch-rows", "10000"], output, receipt, "05_scoped")
    if pq.ParquetFile(scoped_dimensioned).metadata.num_rows:
        raise ValueError("Repair scope unexpectedly includes dimensioned cells; full-lane reuse is not valid.")
    old_scalar = source / f"Panels/v2/panel_long_scalar_{TOKEN}.parquet"
    old_rows = source / f"Panels/v2/panel_source_rows_{TOKEN}.parquet"
    old_dimensioned = source / f"Panels/v2/panel_long_dimensioned_{TOKEN}.parquet"
    con = open_connection(output, args.memory_limit, args.threads)
    try:
        original_scope = output / "build/original_scalar_scope.parquet"
        print("[proof] extracting historical scalar scope once", flush=True)
        con.execute(f"COPY (SELECT * FROM read_parquet({quote(old_scalar)}) WHERE {scope_sql()}) TO {quote(original_scope)} (FORMAT PARQUET, COMPRESSION SNAPPY)")
        old_scope = f"SELECT * FROM read_parquet({quote(original_scope)})"
        new_scope = f"SELECT * FROM read_parquet({quote(scoped_scalar)})"
        if not pq.read_schema(old_scalar).equals(pq.read_schema(scoped_scalar), check_metadata=False):
            raise ValueError("Regenerated scalar schema changed.")
        protected = [name for name in pq.read_schema(old_scalar).names if name not in METADATA_CELL_COLUMNS]
        selected = ", ".join(map(identifier, protected))
        print("[proof] comparing scoped scientific values and canonical identities", flush=True)
        assert_multiset_equal(con, f"SELECT {selected} FROM ({old_scope})", f"SELECT {selected} FROM ({new_scope})", "Scoped scientific values and canonical identities")
        print("[proof] comparing source rows and proving dimensioned scope empty", flush=True)
        assert_multiset_equal(con, f"SELECT * FROM read_parquet({quote(old_rows)}) WHERE {scope_sql()}",
                              f"SELECT * FROM read_parquet({quote(scoped_rows)})", "Scoped source-row lineage")
        if con.execute(f"SELECT COUNT(*) FROM read_parquet({quote(old_dimensioned)}) WHERE {scope_sql()}").fetchone()[0]:
            raise ValueError("Historical dimensioned scope is nonempty; cannot reuse dimensioned lane unchanged.")
        unaffected = output / "build/scalar_unaffected.parquet"
        print("[stitch] extracting unchanged full scalar complement", flush=True)
        con.execute(f"COPY (SELECT * FROM read_parquet({quote(old_scalar)}) WHERE NOT ({scope_sql()})) TO {quote(unaffected)} (FORMAT PARQUET, COMPRESSION SNAPPY)")
    finally:
        con.close()
    panels = output / "Panels/v2"
    panels.mkdir(parents=True, exist_ok=True)
    scalar_target = panels / old_scalar.name
    stage05 = load_stage05(pipeline)
    pending = scalar_target.with_suffix(".pending.parquet")
    print("[stitch] normal Stage05.stitch_parts across unchanged and regenerated partitions", flush=True)
    rows, _ = stage05.stitch_parts([unaffected, scoped_scalar], pending, batch_rows=65536)
    if rows != pq.ParquetFile(old_scalar).metadata.num_rows:
        raise ValueError("Full scalar row count changed after scoped replacement.")
    pending.replace(scalar_target)
    unaffected.unlink()
    link_readonly(old_rows, panels / old_rows.name)
    link_readonly(old_dimensioned, panels / old_dimensioned.name)
    baseline_coverage = output / "Checks/v2/harmonize_qc/source_column_coverage_2023.csv"
    columns, before = read_csv(baseline_coverage)
    _, regenerated = read_csv(scoped / "Checks/v2/harmonize_qc/source_column_coverage_2023.csv")
    if any(row["access_table_name"].upper() not in SCOPE_TABLES for row in regenerated):
        raise ValueError("Scoped Stage04 coverage escaped declared physical tables.")
    after = [row for row in before if row["access_table_name"].upper() not in SCOPE_TABLES] + regenerated
    write_csv(baseline_coverage, columns, after)
    receipt["incremental_stitch"] = {"status": "complete", "function": "archived Stage05.stitch_parts",
                                      "row_count": rows, "replaced_scope": scope_sql(),
                                      "protected_cell_columns": protected, "source_rows_exact_match": True,
                                      "dimensioned_scope_empty": True, "reused_lanes": [str(old_rows), str(old_dimensioned)]}
    atomic_json(output / "metadata_repair_receipt.json", receipt)


def wide_and_clean(args, output: Path, receipt: dict, *, clean_only: bool = False, wide_only: bool = False) -> None:
    pipeline = Path(args.pipeline_repo)
    dictionary = staged_dictionary(args, output)
    if receipt.get("incremental_stitch", {}).get("status") != "complete":
        raise ValueError("Complete and validate the incremental scalar stitch before running Stage06/07.")
    prior_scoped = [stage for stage in receipt.get("stages", []) if stage["stage"] == "04_scoped" and stage["status"] == "complete"]
    if prior_scoped:
        record = prior_scoped[-1]
        source_path = record["command"][record["command"].index("--dictionary") + 1]
        if record.get("bound_input_sha256", {}).get(source_path) != sha256(dictionary):
            raise ValueError("The wide-build dictionary differs from the dictionary bound to the final Stage04 run.")
    panels, checks, build = output / "Panels/v2", output / "Checks/v2", output / "build/v2"
    for name, cached in receipt.get("reused_lane_cache", {}).items():
        if (panels / name).resolve() != Path(cached["local_path"]) or file_identity(Path(cached["local_path"])) != cached["local_identity"]:
            raise ValueError("A cached reused lane changed after its checksum validation.")
    wide = panels / f"panel_wide_analysis_{TOKEN}.parquet"
    lineage = checks / "wide_qc/qc_value_lineage.parquet"
    if not clean_only:
        run_stage([sys.executable, str(pipeline / "Scripts/06_build_wide_panel.py"), "--root", str(output),
                   "--input", str(panels / f"panel_long_scalar_{TOKEN}.parquet"),
                   "--dimensioned-input", str(panels / f"panel_long_dimensioned_{TOKEN}.parquet"),
                   "--source-column-coverage", str(checks / "harmonize_qc"), "--out_dir", str(build / "wide_parts"),
                   "--years", "2004:2023", "--dictionary", str(dictionary), "--build-mode", "release",
                   "--analysis-schema-contract", str(pipeline / "contracts/analysis_schema.csv"),
                   "--discrete-family-contract", str(pipeline / "contracts/discrete_families.csv"),
                   "--wide-analysis-out", str(wide), "--raw-wide-out", str(panels / f"panel_wide_raw_{TOKEN}.parquet"),
                   "--typed-output", "--collapse-disc", "--qc-dir", str(checks / "wide_qc"), "--column-lineage-out", str(lineage),
                   "--disc-qc-dir", str(checks / "disc_qc"), "--release-bundle-root", str(panels / "wide_release"),
                   "--duckdb-path", str(build / "repair.duckdb"), "--duckdb-temp-dir", str(build / "duckdb_spill"),
                   "--duckdb-memory-limit", args.wide_memory_limit, "--duckdb-threads", str(args.threads),
                   "--scan-batch-rows", "25000", "--wide-parquet-row-group-size", "8192",
                   "--log-file", str(build / "06_build_wide_panel.log"), "--no-persist-duckdb"], output, receipt, "06_full_wide")
    if wide_only:
        return
    policy = output / "contracts/prch_policy.csv"
    if sha256(policy) != POLICY_SHA256:
        raise ValueError("Staged PRCH policy changed.")
    run_stage([sys.executable, str(pipeline / "Scripts/07_clean_panel.py"), "--input", str(wide),
               "--output", str(panels / f"panel_clean_prch_{TOKEN}.parquet"), "--dictionary", str(dictionary),
               "--column-lineage", str(lineage), "--qc-dir", str(checks / "prch_qc"), "--prch-policy", str(policy),
               "--policy-mode", "release", "--batch-rows", "250", "--output-row-group-rows", "4096",
               "--log-file", str(build / "07_clean_panel.log")], output, receipt, "07_full_clean")


def cache_reused_lanes(args, output: Path, receipt: dict) -> None:
    """Cache the unchanged dimensioned lane before (never during) normal Stage06."""
    if receipt.get("incremental_stitch", {}).get("status") != "complete":
        raise ValueError("Complete the checked incremental stitch before caching its reused lane.")
    if any(stage.get("status") == "running" for stage in receipt.get("stages", [])):
        raise ValueError("Do not change staged input links while a recorded stage is running.")
    if not args.source_hashes:
        raise ValueError("Caching requires --source-hashes with an independently verified source checksum.")
    name = f"panel_long_dimensioned_{TOKEN}.parquet"
    source = Path(args.source_root).resolve() / "Panels/v2" / name
    before = file_identity(source)
    if before != receipt.get("reused_inputs", {}).get(name):
        raise ValueError("The dimensioned source changed since staging preparation.")
    hashes = json.loads(Path(args.source_hashes).read_text())
    expected = hashes.get(str(source))
    if not isinstance(expected, str) or len(expected) != 64 or any(c not in "0123456789abcdef" for c in expected):
        raise ValueError("Independent dimensioned source checksum is missing or malformed.")
    cached = output / "build/local_cache" / name
    cached.parent.mkdir(parents=True, exist_ok=True)
    staged = output / "Panels/v2" / name
    if not staged.is_symlink() or staged.resolve() not in {source, cached.resolve()}:
        raise ValueError("The staged dimensioned input must point to its original source or validated cache.")
    started = time.time()
    stream_hash = None
    if not cached.exists():
        pending = cached.with_suffix(".pending.parquet")
        digest = hashlib.sha256()
        copied, next_report = 0, 1024 ** 3
        print(f"[cache] sequential source copy to {cached}", flush=True)
        with source.open("rb") as reader, pending.open("wb") as writer:
            for block in iter(lambda: reader.read(8 * 1024 * 1024), b""):
                writer.write(block)
                digest.update(block)
                copied += len(block)
                if copied >= next_report:
                    print(f"[cache] copied {copied / 1024 ** 3:.1f} / {before['bytes'] / 1024 ** 3:.1f} GiB", flush=True)
                    next_report += 1024 ** 3
            writer.flush()
            os.fsync(writer.fileno())
        stream_hash = digest.hexdigest()
        if before != file_identity(source) or copied != before["bytes"] or stream_hash != expected:
            raise ValueError("Source identity, size, or checksum changed during local caching.")
        print("[cache] verifying completed local copy checksum", flush=True)
        if sha256(pending) != expected:
            raise ValueError("Local dimensioned cache checksum differs from the source.")
        pending.replace(cached)
    elif cached.stat().st_size != before["bytes"] or sha256(cached) != expected:
        raise ValueError("Existing dimensioned cache is not identical to its verified source.")
    if before != file_identity(source):
        raise ValueError("Source identity changed while validating its cache.")
    pending_link = staged.with_suffix(".pending-link")
    if pending_link.exists() or pending_link.is_symlink():
        raise ValueError("An unfinished staged input link exists; inspect it before resuming.")
    pending_link.symlink_to(cached)
    pending_link.replace(staged)
    receipt.setdefault("reused_lane_cache", {})[name] = {
        "source_identity": before, "source_sha256": expected,
        "source_hash_receipt": str(Path(args.source_hashes).resolve()),
        "source_hash_receipt_sha256": sha256(Path(args.source_hashes)),
        "local_path": str(cached), "local_identity": file_identity(cached), "local_sha256": expected,
        "copy_stream_sha256": stream_hash, "started_unix": started, "finished_unix": time.time(),
        "source_unchanged": True, "local_copy_rehashed": True,
    }
    atomic_json(output / "metadata_repair_receipt.json", receipt)


def compare_panel_values(left: Path, right: Path, *, batch_rows: int = 250) -> int:
    """Exact bounded comparison; require the normal pipeline's identical key order."""
    identities = (file_identity(left), file_identity(right))
    a, b = pq.ParquetFile(left), pq.ParquetFile(right)
    if a.metadata.num_rows != b.metadata.num_rows or not a.schema_arrow.equals(b.schema_arrow, check_metadata=False):
        raise ValueError(f"Panel shape/schema changed: {right}")
    rows = 0
    for first, second in zip(a.iter_batches(batch_size=batch_rows, use_threads=False), b.iter_batches(batch_size=batch_rows, use_threads=False), strict=True):
        if not first.equals(second, check_metadata=False):
            raise ValueError(f"Panel values or row order changed at offset {rows}: {right}")
        rows += first.num_rows
    if rows != a.metadata.num_rows:
        raise ValueError("Panel equality scan was incomplete.")
    if identities != (file_identity(left), file_identity(right)):
        raise ValueError("A compared artifact changed during semantic verification.")
    return rows


def verify_outputs(args, output: Path, receipt: dict) -> None:
    source = Path(args.source_root)
    for cached in receipt.get("reused_lane_cache", {}).values():
        if file_identity(Path(cached["local_path"])) != cached["local_identity"] or sha256(Path(cached["local_path"])) != cached["source_sha256"]:
            raise ValueError("A cached reused lane changed during the build.")
    result = {}
    for name in (f"panel_wide_analysis_{TOKEN}.parquet", f"panel_clean_prch_{TOKEN}.parquet"):
        print(f"[verify] exact full values: {name}", flush=True)
        result[name] = {"rows": compare_panel_values(source / "Panels/v2" / name, output / "Panels/v2" / name),
                        "all_values_equal": True, "sha256": sha256(output / "Panels/v2" / name)}
    lineage_relative = Path("Checks/v2/wide_qc/qc_value_lineage.parquet")
    print("[verify] exact full value-lineage stream (normal Stage06 total-key order)", flush=True)
    lineage_rows = compare_panel_values(source / lineage_relative, output / lineage_relative, batch_rows=65536)
    con = open_connection(output, args.memory_limit, args.threads)
    try:
        original = source / "Checks/v2/prch_qc/prch_cell_actions.parquet"
        rebuilt = output / "Checks/v2/prch_qc/prch_cell_actions.parquet"
        assert_multiset_equal(con, f"SELECT * FROM read_parquet({quote(original)})", f"SELECT * FROM read_parquet({quote(rebuilt)})", "PRCH cell actions")
    finally:
        con.close()
    policy = output / "Checks/v2/prch_qc/prch_flag_policy.csv"
    if sha256(policy) != POLICY_SHA256:
        raise ValueError("Replayed PRCH policy differs from the original exact policy.")
    external_hashes = json.loads(Path(args.source_hashes).read_text()) if args.source_hashes else {}
    for label, original_identity in receipt.get("reused_inputs", {}).items():
        path = Path(original_identity["path"])
        if file_identity(path) != original_identity:
            raise ValueError(f"A reused source artifact changed during rebuild: {path}")
        digest = external_hashes.get(str(path)) if args.source_hashes else sha256(path)
        if not isinstance(digest, str) or len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise ValueError(f"Source hash receipt is incomplete for {path}")
        receipt.setdefault("reused_input_sha256", {})[label] = digest
    if args.source_hashes:
        original = Path(args.source_hashes)
        destination = output / "Checks/metadata_repair/source_file_sha256.json"
        copy_verified(original, destination)
        details = original.with_name(original.stem + "_details.json")
        if details.is_file():
            copy_verified(details, destination.with_name(details.name))
        receipt["source_hash_receipt"] = {"path": str(destination.relative_to(output)), "sha256": sha256(destination)}
    artifacts = {output / "Panels/v2" / name for name in
                 (f"panel_long_scalar_{TOKEN}.parquet", f"panel_wide_analysis_{TOKEN}.parquet", f"panel_clean_prch_{TOKEN}.parquet")}
    for folder in ("Dictionary/v2", "Checks/v2/wide_qc", "Checks/v2/prch_qc", "Checks/metadata_repair"):
        artifacts.update(path for path in (output / folder).rglob("*") if path.is_file())
    print("[verify] binding validated artifact hashes", flush=True)
    artifact_hashes = {str(path.relative_to(output)): sha256(path) for path in sorted(artifacts)}
    receipt["verification"] = {"panels": result, "prch_actions_equal": True, "full_value_lineage_equal": True,
                               "policy_sha256": POLICY_SHA256,
                               "value_lineage": {"rows": lineage_rows, "all_values_equal": True,
                                                 "comparison": "exact Arrow batch equality in normal Stage06 total-key order"},
                               "artifact_sha256": artifact_hashes}
    receipt["status"] = "complete"
    atomic_json(output / "metadata_repair_receipt.json", receipt)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--pipeline-repo", required=True, help="Recovered and repaired v2 repository; never current legacy main Stage06")
    parser.add_argument("--output-root", required=True, help="Fresh local staging root, never /Volumes or inside the source root")
    parser.add_argument("--fresh-source", help="Fresh original MDB CSV export folder; bytes must match existing source bindings")
    parser.add_argument("--dictionary", help="Corrected v2 dictionary; defaults to normal Stage03 output in staging root")
    parser.add_argument("--phase", choices=["prepare", "regenerate", "cache-reused-lanes", "wide", "wide-clean", "clean", "verify", "all"], default="prepare")
    parser.add_argument("--source-hashes", help="Independent stable-file SHA256 JSON {absolute source path: digest}, to avoid rereading reused lanes")
    parser.add_argument("--memory-limit", default="512MB")
    parser.add_argument("--wide-memory-limit", default="1GB")
    parser.add_argument("--threads", type=int, default=2)
    args = parser.parse_args()
    source, output, pipeline = Path(args.source_root).resolve(), Path(args.output_root).resolve(), Path(args.pipeline_repo).resolve()
    if output == source or source in output.parents or "/Volumes" in str(output) or output == pipeline or pipeline in output.parents:
        raise ValueError("Rebuild writes require an independent local staging root.")
    if args.threads < 1:
        raise ValueError("--threads must be positive.")
    output.mkdir(parents=True, exist_ok=True)
    receipt_path = output / "metadata_repair_receipt.json"
    receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else {"status": "staging", "source_root": str(source), "pipeline_repo": str(pipeline)}
    if receipt["source_root"] != str(source) or receipt["pipeline_repo"] != str(pipeline):
        raise ValueError("Existing staging receipt belongs to different inputs.")
    if args.phase not in {"prepare", "all"} and not receipt_path.exists():
        raise ValueError("Run --phase prepare first.")
    receipt.setdefault("helper_executions", []).append({"phase": args.phase, "started_unix": time.time(),
                                                        "sha256": sha256(Path(__file__))})
    if args.phase != "prepare":
        receipt["status"] = f"{args.phase}_running"
        receipt.pop("verification", None)
    atomic_json(receipt_path, receipt)
    if args.phase in {"prepare", "all"}:
        receipt.update(prepare_inputs(source, output, Path(args.fresh_source) if args.fresh_source else None))
        receipt["reused_inputs"] = {name: file_identity(source / "Panels/v2" / name)
                                    for name in (f"panel_long_scalar_{TOKEN}.parquet", f"panel_long_dimensioned_{TOKEN}.parquet", f"panel_source_rows_{TOKEN}.parquet")}
        receipt["code_sha256"] = {str(path.relative_to(pipeline)): sha256(path)
                                  for path in sorted((pipeline / "Scripts").glob("*.py"))}
        receipt["helper_sha256"] = sha256(Path(__file__))
        atomic_json(receipt_path, receipt)
    if args.phase in {"regenerate", "all"}:
        regenerate_scalar(args, output, receipt)
    if args.phase == "cache-reused-lanes":
        cache_reused_lanes(args, output, receipt)
    if args.phase in {"wide", "wide-clean", "clean", "all"}:
        wide_and_clean(args, output, receipt, clean_only=args.phase == "clean", wide_only=args.phase == "wide")
    if args.phase in {"verify", "all"}:
        verify_outputs(args, output, receipt)
    print(f"[{args.phase}] complete; receipt={receipt_path}", flush=True)


if __name__ == "__main__":
    main()
