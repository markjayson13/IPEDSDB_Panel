#!/usr/bin/env python3
"""
Stage 05: stitch yearly long parquet files into one cross-year long panel.

Reads:
- `Cross_sections/panel_long_varnum_<year>.parquet`

Writes:
- `Panels/<span>/panel_long_varnum_<start>_<end>.parquet`
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow as pa
import pyarrow.parquet as pq

from access_build_utils import ensure_data_layout, parse_years


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=None, help="External IPEDSDB_ROOT")
    ap.add_argument("--years", default="2004:2023")
    ap.add_argument("--cross-sections-dir", default=None)
    ap.add_argument("--output", required=True)
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    layout = ensure_data_layout(args.root)
    years = parse_years(args.years)
    cross_sections = Path(args.cross_sections_dir) if args.cross_sections_dir else layout.cross_sections
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    tmp_output = output.with_suffix(output.suffix + ".tmp")
    if tmp_output.exists():
        tmp_output.unlink()
    writer = None
    for year in years:
        part = cross_sections / f"panel_long_varnum_{year}.parquet"
        if not part.exists():
            raise SystemExit(f"Missing per-year long parquet: {part}")
        pf = pq.ParquetFile(part)
        for batch in pf.iter_batches():
            if writer is None:
                writer = pq.ParquetWriter(tmp_output, batch.schema, compression="snappy")
            writer.write_batch(batch)
    if writer is None:
        raise SystemExit("No per-year long parquet files were stitched.")
    writer.close()
    tmp_output.replace(output)

    dataset = ds.dataset(str(output), format="parquet")
    years_seen = sorted(int(v) for v in pc.unique(dataset.to_table(columns=["year"]).column(0)).to_pylist() if v is not None)
    if years_seen != years:
        raise SystemExit(f"Stitched panel year coverage mismatch. expected={years} actual={years_seen}")
    null_counts = {"year": 0, "UNITID": 0, "varnumber": 0, "source_file": 0}
    for batch in dataset.to_batches(columns=list(null_counts), batch_size=200_000):
        schema_names = list(batch.schema.names)
        for col in null_counts:
            arr = batch.column(schema_names.index(col))
            null_counts[col] += arr.null_count
            if pa.types.is_string(arr.type) or pa.types.is_large_string(arr.type):
                null_counts[col] += int(pc.sum(pc.equal(pc.utf8_trim_whitespace(arr), "")).as_py() or 0)
    if any(null_counts[col] > 0 for col in null_counts):
        raise SystemExit(f"Stitched panel contains null/blank key fields: {null_counts}")

    summary_path = layout.checks / "harmonize_qc" / "stitch_summary.csv"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        "years_min,years_max,years_count,output\n"
        f"{years_seen[0]},{years_seen[-1]},{len(years_seen)},{output}\n",
        encoding="utf-8",
    )
    print(f"Wrote stitched long panel: {output}")


if __name__ == "__main__":
    main()
