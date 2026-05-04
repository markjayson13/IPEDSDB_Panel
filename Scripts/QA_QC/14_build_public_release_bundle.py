#!/usr/bin/env python3
"""
QA 14: assemble a verified public release bundle.

Reads:
- `Checks/release_manifest/release_manifest.csv`
- `Checks/release_manifest/release_manifest_verification.csv`
- files listed as present in the release manifest

Writes:
- release bundle directory
- `bundle_manifest.csv`
- `SHA256SUMS.txt`
- `README.md`
- `CITATION.cff`
- `datacite.json`
- `ro-crate-metadata.json`

Run `12_build_release_manifest.py` and `13_verify_release_manifest.py` first,
or pass `--refresh-manifest`.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from access_build_utils import DEFAULT_IPEDSDB_ROOT, compute_file_metadata, ensure_data_layout, parse_years, repo_root


def parse_args() -> argparse.Namespace:
    data_root = Path(os.environ.get("IPEDSDB_ROOT", str(DEFAULT_IPEDSDB_ROOT)))
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", default=str(data_root), help="External IPEDSDB_ROOT")
    p.add_argument("--years", default="2004:2023", help='Release year span, e.g. "2004:2023"')
    p.add_argument("--manifest", default=None, help="release_manifest.csv. Defaults under IPEDSDB_ROOT.")
    p.add_argument("--verification", default=None, help="release_manifest_verification.csv. Defaults under IPEDSDB_ROOT.")
    p.add_argument("--out-dir", default=None, help="Bundle directory. Defaults to Releases/ipedsdb_panel_<start>_<end>.")
    p.add_argument("--repo-root", default=str(repo_root()), help="Repository root")
    p.add_argument("--refresh-manifest", action=argparse.BooleanOptionalAction, default=False)
    p.add_argument("--require-clean-verification", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--title", default="IPEDSDB_Panel")
    p.add_argument("--version", default="", help="Release version. Defaults to contract_id when available.")
    p.add_argument("--doi", default="", help="DOI or DOI placeholder for citation metadata")
    p.add_argument("--url", default="https://github.com/markjayson13/IPEDSDB_Panel")
    p.add_argument("--publisher", default="IPEDSDB_Panel")
    p.add_argument("--license", default="Creative Commons Attribution 4.0 International (CC BY 4.0)")
    p.add_argument("--license-url", default="https://creativecommons.org/licenses/by/4.0/")
    p.add_argument("--author", action="append", default=["Mark Jayson Farol"], help="Repeatable author name")
    p.add_argument("--author-website", default="https://markjayson.com", help="Maintainer contact website")
    p.add_argument("--mentor-name", default="Djeto Assane")
    p.add_argument("--mentor-affiliation", default="University of Nevada, Las Vegas")
    p.add_argument("--mentor-url", default="https://www.unlv.edu/people/djeto-assane")
    return p.parse_args()


def boolish(value: object) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def read_contract(repo: Path) -> dict[str, Any]:
    path = repo / "contracts" / "panel_spec.toml"
    if not path.exists():
        return {}
    with path.open("rb") as fh:
        return tomllib.load(fh)


def run_script(script: str, *args: object, cwd: Path) -> None:
    cmd = [sys.executable, str(cwd / script), *(str(arg) for arg in args)]
    result = subprocess.run(cmd, cwd=str(cwd), text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    if result.returncode != 0:
        raise SystemExit(result.stdout)


def refresh_manifest(repo: Path, root: Path, years: str, manifest_path: Path) -> None:
    manifest_dir = manifest_path.parent
    run_script(
        "Scripts/QA_QC/12_build_release_manifest.py",
        "--root",
        root,
        "--years",
        years,
        "--out-dir",
        manifest_dir,
        cwd=repo,
    )
    run_script(
        "Scripts/QA_QC/13_verify_release_manifest.py",
        "--manifest",
        manifest_path,
        "--root",
        root,
        cwd=repo,
    )


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"Required file does not exist: {path}")
    return pd.read_csv(path, dtype=str).fillna("")


def require_clean_verification(verification: pd.DataFrame) -> None:
    if "passed" not in verification.columns:
        raise SystemExit("Verification file missing required column: passed")
    failed = verification.loc[~verification["passed"].map(boolish)]
    if not failed.empty:
        sample = ", ".join(failed["relative_path"].head(5).astype(str))
        raise SystemExit(f"Release manifest verification has failing rows: {len(failed)}. Sample: {sample}")


def resolve_source(row: pd.Series, root: Path, repo: Path) -> Path:
    path_base = str(row.get("path_base", "")).strip()
    rel = str(row.get("relative_path", "")).strip()
    if path_base == "IPEDSDB_ROOT":
        return root / rel
    if path_base == "repo":
        return repo / rel
    absolute = str(row.get("absolute_path", "")).strip()
    return Path(absolute) if absolute else Path(rel)


def bundle_relative(row: pd.Series) -> Path:
    path_base = str(row.get("path_base", "")).strip() or "external"
    rel = str(row.get("relative_path", "")).strip()
    base = "repo" if path_base == "repo" else "IPEDSDB_ROOT" if path_base == "IPEDSDB_ROOT" else "external"
    return Path(base) / rel


def copy_file(src: Path, dst: Path) -> tuple[str, str]:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return compute_file_metadata(dst)


def author_objects(names: list[str], website: str = "") -> list[dict[str, str]]:
    out = []
    for name in names:
        clean = str(name).strip()
        if clean:
            row = {"name": clean}
            if website:
                row["url"] = website
            out.append(row)
    return out


def cff_authors(names: list[str], website: str = "") -> str:
    rows = []
    for name in names:
        clean = str(name).strip()
        if not clean:
            continue
        parts = clean.split()
        family = parts[-1] if len(parts) > 1 else clean
        given = " ".join(parts[:-1]) if len(parts) > 1 else ""
        rows.append("  - family-names: " + json.dumps(family))
        if given:
            rows.append("    given-names: " + json.dumps(given))
        if website:
            rows.append("    website: " + json.dumps(website))
    return "\n".join(rows) if rows else "  - name: \"Unknown\""


def write_citation(
    path: Path,
    *,
    title: str,
    version: str,
    doi: str,
    url: str,
    authors: list[str],
    date: str,
    license_id: str,
    website: str,
) -> None:
    lines = [
        "cff-version: 1.2.0",
        "message: \"Cite this dataset using the metadata below.\"",
        f"title: {json.dumps(title)}",
        f"version: {json.dumps(version)}",
        f"date-released: {date}",
        "type: dataset",
        "authors:",
        cff_authors(authors, website),
        f"url: {json.dumps(url)}",
        f"license: {json.dumps(license_id)}",
    ]
    if doi:
        lines.append(f"doi: {json.dumps(doi)}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_datacite(
    path: Path,
    *,
    title: str,
    version: str,
    doi: str,
    url: str,
    publisher: str,
    license_name: str,
    license_url: str,
    authors: list[str],
    website: str,
    mentor_name: str,
    mentor_affiliation: str,
    mentor_url: str,
    year: int,
    date: str,
    description: str,
) -> None:
    metadata = {
        "identifiers": [{"identifier": doi, "identifierType": "DOI"}] if doi else [],
        "creators": author_objects(authors, website),
        "titles": [{"title": title}],
        "publisher": publisher,
        "publicationYear": year,
        "resourceType": {"resourceTypeGeneral": "Dataset", "resourceType": "Panel dataset"},
        "version": version,
        "dates": [{"date": date, "dateType": "Issued"}],
        "contributors": [
            {
                "name": mentor_name,
                "contributorType": "Supervisor",
                "affiliation": [{"name": mentor_affiliation}],
                "nameIdentifiers": [{"nameIdentifier": mentor_url, "nameIdentifierScheme": "URL"}],
            }
        ]
        if mentor_name
        else [],
        "descriptions": [{"description": description, "descriptionType": "Abstract"}],
        "rightsList": [{"rights": license_name, "rightsUri": license_url}],
        "url": url,
    }
    path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")


def write_ro_crate(
    path: Path,
    *,
    title: str,
    date: str,
    license_name: str,
    license_url: str,
    authors: list[str],
    website: str,
    mentor_name: str,
    mentor_affiliation: str,
    mentor_url: str,
    bundle_rows: list[dict[str, object]],
) -> None:
    parts = [{"@id": str(row["bundle_relative_path"])} for row in bundle_rows]
    graph: list[dict[str, Any]] = [
        {
            "@id": "ro-crate-metadata.json",
            "@type": "CreativeWork",
            "about": {"@id": "./"},
            "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"},
        },
        {
            "@id": "./",
            "@type": "Dataset",
            "name": title,
            "datePublished": date,
            "license": license_url or license_name,
            "creator": author_objects(authors, website),
            "contributor": [
                {
                    "@type": "Person",
                    "name": mentor_name,
                    "affiliation": mentor_affiliation,
                    "url": mentor_url,
                    "roleName": "Research mentor",
                }
            ]
            if mentor_name
            else [],
            "hasPart": parts,
        },
    ]
    for row in bundle_rows:
        graph.append(
            {
                "@id": str(row["bundle_relative_path"]),
                "@type": "File",
                "name": str(row["bundle_relative_path"]),
                "contentSize": str(row["bundle_size_bytes"]),
                "sha256": str(row["bundle_sha256"]),
                "encodingFormat": str(row.get("file_format", "")),
            }
        )
    path.write_text(json.dumps({"@context": "https://w3id.org/ro/crate/1.1/context", "@graph": graph}, indent=2), encoding="utf-8")


def write_readme(
    path: Path,
    *,
    title: str,
    version: str,
    years_spec: str,
    generated_at: str,
    manifest_rel: str,
    verification_rel: str,
    copied_files: int,
    license_name: str,
    mentor_name: str,
    mentor_url: str,
) -> None:
    text = f"""# {title} release bundle

Generated: `{generated_at}`

Release years: `{years_spec}`

Version: `{version}`

License: `{license_name}`

Research mentor: [{mentor_name}]({mentor_url})

This directory contains the files listed in `bundle_manifest.csv`.

Main metadata files:

- `{manifest_rel}`
- `{verification_rel}`
- `bundle_manifest.csv`
- `SHA256SUMS.txt`
- `CITATION.cff`
- `datacite.json`
- `ro-crate-metadata.json`
- `metadata/datapackage.json` when generated
- `metadata/build_provenance.json` when generated

Copied files: `{copied_files}`

Verify the bundle checksums with:

```bash
shasum -a 256 -c SHA256SUMS.txt
```
"""
    path.write_text(text, encoding="utf-8")


def write_sha256sums(path: Path, rows: list[dict[str, object]]) -> None:
    lines = [f"{row['bundle_sha256']}  {row['bundle_relative_path']}" for row in sorted(rows, key=lambda row: str(row["bundle_relative_path"]))]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    layout = ensure_data_layout(args.root)
    repo = Path(args.repo_root).expanduser()
    years = parse_years(args.years)
    years_spec = f"{years[0]}:{years[-1]}"
    manifest_path = Path(args.manifest).expanduser() if args.manifest else layout.checks / "release_manifest" / "release_manifest.csv"
    verification_path = (
        Path(args.verification).expanduser()
        if args.verification
        else layout.checks / "release_manifest" / "release_manifest_verification.csv"
    )
    out_dir = (
        Path(args.out_dir).expanduser()
        if args.out_dir
        else layout.root / "Releases" / f"ipedsdb_panel_{years[0]}_{years[-1]}"
    )

    if args.refresh_manifest:
        refresh_manifest(repo, layout.root, args.years, manifest_path)

    manifest = read_csv(manifest_path)
    verification = read_csv(verification_path)
    if args.require_clean_verification:
        require_clean_verification(verification)

    contract = read_contract(repo)
    version = args.version or str(contract.get("contract_id", "") or f"ipedsdb-panel-{years[0]}-{years[-1]}")
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    release_date = generated_at[:10]
    out_dir.mkdir(parents=True, exist_ok=True)

    bundle_rows: list[dict[str, object]] = []
    for _, row in manifest.iterrows():
        if not boolish(row.get("present", "")):
            continue
        src = resolve_source(row, layout.root, repo)
        if not src.exists() or not src.is_file():
            raise SystemExit(f"Manifest row is present but file is missing: {src}")
        dst_rel = bundle_relative(row)
        dst = out_dir / dst_rel
        size_bytes, sha256 = copy_file(src, dst)
        bundle_rows.append(
            {
                "artifact_id": row.get("artifact_id", ""),
                "role": row.get("role", ""),
                "source_path_base": row.get("path_base", ""),
                "source_relative_path": row.get("relative_path", ""),
                "bundle_relative_path": str(dst_rel),
                "bundle_size_bytes": size_bytes,
                "bundle_sha256": sha256,
                "rows": row.get("rows", ""),
                "columns": row.get("columns", ""),
                "file_format": row.get("file_format", ""),
                "copied_at_utc": generated_at,
            }
        )

    metadata_dir = out_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    metadata_sources = [
        (manifest_path, metadata_dir / manifest_path.name),
        (verification_path, metadata_dir / verification_path.name),
    ]
    for extra_name in ["release_manifest.json", "release_manifest_summary.csv", "release_manifest_summary.md", "release_manifest_verification.md"]:
        extra = manifest_path.parent / extra_name
        if extra.exists():
            metadata_sources.append((extra, metadata_dir / extra.name))
    release_metadata_dir = layout.checks / "release_metadata"
    for extra_name in ["datapackage.json", "build_provenance.json"]:
        extra = release_metadata_dir / extra_name
        if extra.exists():
            metadata_sources.append((extra, metadata_dir / extra.name))
    release_compare_dir = layout.checks / "release_compare"
    for extra_name in ["release_comparison.csv", "release_comparison_summary.csv", "release_comparison.md"]:
        extra = release_compare_dir / extra_name
        if extra.exists():
            metadata_sources.append((extra, metadata_dir / extra.name))
    for src, dst in metadata_sources:
        size_bytes, sha256 = copy_file(src, dst)
        bundle_rows.append(
            {
                "artifact_id": f"bundle_metadata:{dst.name}",
                "role": "bundle_metadata",
                "source_path_base": "generated",
                "source_relative_path": str(src),
                "bundle_relative_path": str(dst.relative_to(out_dir)),
                "bundle_size_bytes": size_bytes,
                "bundle_sha256": sha256,
                "rows": "",
                "columns": "",
                "file_format": dst.suffix.lower().lstrip("."),
                "copied_at_utc": generated_at,
            }
        )

    bundle_manifest = pd.DataFrame(bundle_rows).sort_values(["role", "bundle_relative_path"], kind="mergesort")
    bundle_manifest.to_csv(out_dir / "bundle_manifest.csv", index=False)
    write_sha256sums(out_dir / "SHA256SUMS.txt", bundle_rows)

    description = f"IPEDSDB_Panel release bundle for {years_spec}. The bundle includes panel artifacts, dictionary artifacts, QA evidence, release manifest files, and repository release documents."
    write_readme(
        out_dir / "README.md",
        title=args.title,
        version=version,
        years_spec=years_spec,
        generated_at=generated_at,
        manifest_rel="metadata/release_manifest.csv",
        verification_rel="metadata/release_manifest_verification.csv",
        copied_files=len(bundle_rows),
        license_name=args.license,
        mentor_name=args.mentor_name,
        mentor_url=args.mentor_url,
    )
    write_citation(
        out_dir / "CITATION.cff",
        title=args.title,
        version=version,
        doi=args.doi,
        url=args.url,
        authors=args.author,
        date=release_date,
        license_id="CC-BY-4.0",
        website=args.author_website,
    )
    write_datacite(
        out_dir / "datacite.json",
        title=args.title,
        version=version,
        doi=args.doi,
        url=args.url,
        publisher=args.publisher,
        license_name=args.license,
        license_url=args.license_url,
        authors=args.author,
        website=args.author_website,
        mentor_name=args.mentor_name,
        mentor_affiliation=args.mentor_affiliation,
        mentor_url=args.mentor_url,
        year=int(release_date[:4]),
        date=release_date,
        description=description,
    )
    write_ro_crate(
        out_dir / "ro-crate-metadata.json",
        title=args.title,
        date=release_date,
        license_name=args.license,
        license_url=args.license_url,
        authors=args.author,
        website=args.author_website,
        mentor_name=args.mentor_name,
        mentor_affiliation=args.mentor_affiliation,
        mentor_url=args.mentor_url,
        bundle_rows=bundle_rows,
    )

    print(f"Wrote release bundle to {out_dir}")
    print(f"Copied files: {len(bundle_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
