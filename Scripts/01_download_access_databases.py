#!/usr/bin/env python3
"""
Stage 01: download annual IPEDS Access database packages from NCES.

Reads:
- the NCES Access-database release page

Writes:
- `Raw_Access_Databases/<year>/downloads/*`
- `Raw_Access_Databases/<year>/manifest.csv`
- `Checks/download_qc/*`

Open this file when you want to understand how the repo decides which NCES Access releases count as in-scope and how yearly manifests get written.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from access_build_utils import (
    academic_year_to_start_year,
    compute_file_metadata,
    ensure_data_layout,
    parse_years,
)


BASE_URL = "https://nces.ed.gov"
ACCESS_PAGE_URL = "https://nces.ed.gov/ipeds/use-the-data/download-access-database"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=None, help="External IPEDSDB_ROOT")
    ap.add_argument("--years", default=None, help='Year span, default is discovered "2004:latest_final"')
    ap.add_argument("--skip-existing", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--timeout", type=int, default=120)
    ap.add_argument("--final-only", action=argparse.BooleanOptionalAction, default=True)
    return ap.parse_args()


def fetch_page(session: requests.Session, timeout: int) -> str:
    response = session.get(ACCESS_PAGE_URL, timeout=timeout)
    response.raise_for_status()
    return response.text


def basename_from_url(url: str) -> str:
    return Path(urlparse(url).path).name


def parse_access_release_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict] = []
    for row in soup.select("table tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        access_link = cells[0].find("a", href=True)
        doc_link = cells[1].find("a", href=True)
        if access_link is None or doc_link is None:
            continue
        academic_year_label = access_link.get_text(" ", strip=True).replace(" Access", "").strip()
        release_type = cells[2].get_text(" ", strip=True)
        release_date_text = cells[3].get_text(" ", strip=True)
        access_url = urljoin(BASE_URL, access_link["href"])
        doc_url = urljoin(BASE_URL, doc_link["href"])
        out.append(
            {
                "year": academic_year_to_start_year(academic_year_label),
                "academic_year_label": academic_year_label,
                "release_type": release_type,
                "release_date_text": release_date_text,
                "access_url": access_url,
                "access_filename": basename_from_url(access_url),
                "doc_url": doc_url,
                "doc_filename": basename_from_url(doc_url),
            }
        )
    return sorted(out, key=lambda row: (int(row["year"]), row["release_type"], row["academic_year_label"]))


def write_rows(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def download_file(session: requests.Session, url: str, destination: Path, timeout: int) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with session.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        with destination.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=65536):
                if chunk:
                    fh.write(chunk)


def main() -> None:
    args = parse_args()
    if not args.final_only:
        raise SystemExit("v1 only supports --final-only")

    layout = ensure_data_layout(args.root)
    qc_dir = layout.checks / "download_qc"
    qc_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    html = fetch_page(session, args.timeout)
    all_entries = parse_access_release_table(html)
    if not all_entries:
        raise SystemExit(f"No Access releases parsed from {ACCESS_PAGE_URL}")

    inventory_fields = [
        "year",
        "academic_year_label",
        "release_type",
        "release_date_text",
        "access_url",
        "access_filename",
        "doc_url",
        "doc_filename",
    ]
    write_rows(qc_dir / "release_inventory.csv", all_entries, inventory_fields)

    final_entries = [row for row in all_entries if str(row["release_type"]).strip().lower() == "final"]
    latest_final = max(int(row["year"]) for row in final_entries)
    requested_years = parse_years(args.years) if args.years else list(range(2004, latest_final + 1))

    final_by_year: dict[int, list[dict]] = {}
    for row in final_entries:
        final_by_year.setdefault(int(row["year"]), []).append(row)

    duplicate_rows = []
    for year, rows in sorted(final_by_year.items()):
        if len(rows) > 1:
            duplicate_rows.extend(rows)
    if duplicate_rows:
        write_rows(qc_dir / "duplicate_final_entries.csv", duplicate_rows, inventory_fields)
        raise SystemExit("Duplicate final Access entries detected on the NCES page.")

    missing_years = [year for year in requested_years if year not in final_by_year]
    write_rows(qc_dir / "missing_years.csv", [{"year": year} for year in missing_years], ["year"])
    if missing_years:
        raise SystemExit(f"Missing requested final Access releases for years: {missing_years}")

    manifest_fields = [
        "year",
        "academic_year_label",
        "release_type",
        "release_date_text",
        "access_url",
        "access_filename",
        "access_zip_filesize_bytes",
        "access_zip_sha256",
        "doc_url",
        "doc_filename",
        "doc_filesize_bytes",
        "doc_sha256",
        "download_status",
        "downloaded_at",
    ]

    failures: list[dict] = []
    for year in requested_years:
        row = dict(final_by_year[year][0])
        year_dir = layout.raw_access / str(year)
        downloads_dir = year_dir / "downloads"
        downloads_dir.mkdir(parents=True, exist_ok=True)

        access_path = downloads_dir / row["access_filename"]
        doc_path = downloads_dir / row["doc_filename"]
        status = "existing"
        try:
            if not (args.skip_existing and access_path.exists()):
                download_file(session, row["access_url"], access_path, args.timeout)
                status = "downloaded"
            if not (args.skip_existing and doc_path.exists()):
                download_file(session, row["doc_url"], doc_path, args.timeout)
                status = "downloaded" if status != "failed" else status
        except Exception as exc:  # noqa: BLE001
            status = "failed"
            failures.append({"year": year, "error": str(exc)})

        access_size, access_sha = compute_file_metadata(access_path)
        doc_size, doc_sha = compute_file_metadata(doc_path)
        manifest_row = {
            "year": year,
            "academic_year_label": row["academic_year_label"],
            "release_type": row["release_type"],
            "release_date_text": row["release_date_text"],
            "access_url": row["access_url"],
            "access_filename": row["access_filename"],
            "access_zip_filesize_bytes": access_size,
            "access_zip_sha256": access_sha,
            "doc_url": row["doc_url"],
            "doc_filename": row["doc_filename"],
            "doc_filesize_bytes": doc_size,
            "doc_sha256": doc_sha,
            "download_status": status,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        }
        write_rows(year_dir / "manifest.csv", [manifest_row], manifest_fields)
        print(f"[year {year}] {status}: {access_path.name}, {doc_path.name}")

    write_rows(qc_dir / "download_failures.csv", failures, ["year", "error"])
    if failures:
        raise SystemExit(f"Download failures encountered for years: {[row['year'] for row in failures]}")

    print(f"Wrote release inventory to {qc_dir / 'release_inventory.csv'}")
    print(f"Resolved latest final year: {latest_final}")


if __name__ == "__main__":
    main()
