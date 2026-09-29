# User codebook

The [public codebook](https://markjayson13.github.io/IPEDSDB_Panel/) replaces
the former Pages landing page and variable-browser snapshot. It describes
`full-panel-labels-v1`, including all 2,721 variables, their exact observed years,
definitions, category codes, Stata names and encodings, source table references,
documented corrections, and unresolved source gaps.

The searchable interface and full downloadable PDF use the same generated
metadata. The PDF has an alphabetical linked index and variable bookmarks.
Dictionary and value-label CSV downloads are provided; large CSVs use gzip
compression and should be decompressed before opening in a spreadsheet.
These are codebook downloads, not institution-level dataset downloads.
The complete PDF is about 12 MB. Repository guards allow up to 16 MiB for
that specific documentation file; the usual 5 MiB limit remains for other files.

## Rebuild

With the published external data root attached, run from the repository:

```sh
.venv/bin/python -m pip install -r requirements-codebook.txt
.venv/bin/python Scripts/build_codebook.py \
  --root "$IPEDSDB_ROOT" --output-dir docs/codebook
.venv/bin/python Scripts/codebook_pdf.py --input-dir docs/codebook
.venv/bin/python -m http.server 8765 --bind 127.0.0.1 --directory docs
```

Open `http://127.0.0.1:8765/` to review the page. A local HTTP server is needed
because the interface fetches JSON; opening `index.html` as a file is not
supported. Commit the reviewed `docs/` assets with their generator changes;
GitHub Pages serves the repository's `docs/` directory on `main`.

The builder verifies SHA-256 hashes of both published data files and their
metadata against `Final/manifest.json`. It does not modify or reconstruct
panel values. All grouped definitions preserve exact year sets; a gap between
years is not expanded into continuous coverage. Physical table corrections
retain the original dictionary table alongside the resolved table.

`docs/codebook/manifest.json` records checksums for the codebook artifacts and
the data/metadata hashes they describe. The external copy belongs in
`$IPEDSDB_ROOT/Final/Codebook/`; it is separate from the immutable data releases.
Regenerate and replace that documentation copy when a new panel release is
published. Do not overwrite the immutable release directories.

## Meaning and limitations

Observed year coverage means at least one nonmissing panel value exists in
that year. The panel's reporting year is not automatically the measurement's
academic or fiscal reference period. Year filtering restricts definitions and
category meanings to their recorded years. It does not establish comparability
over time or resolve an unknown unit, currency, price basis, or reference period.

Five variables have source-description gaps. Another five variables include
6,256 observations with unresolved same-year category meanings. These issues
remain visible in the interface, PDF and downloads. Source gaps are neither
filled by inference nor hidden by the codebook's formatting.

The original corrected metadata and all panel data remain unchanged. See
[full-panel export validation](FULL_PANEL_LABELS.md) for the evidence and
[2023 SFA corrections](2023_SFA_METADATA_REPAIR.md) for physical source repairs.
