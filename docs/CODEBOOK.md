# User codebook

The [public codebook](https://markjayson13.github.io/IPEDSDB_Panel/) replaces
the former Pages landing page and variable-browser snapshot. It describes
`full-panel-labels-v1`, including all 2,721 variables, their exact observed years,
definitions, category codes, Stata names and encodings, source table references,
documented corrections, and unresolved source gaps.

The [2004-2024 codebook](https://markjayson13.github.io/IPEDSDB_Panel/provisional/)
describes the separately published extension: 2,676 variables, including 95
consolidated variables. Its release tab identifies the provisional 2024 sources.
Both interfaces use the same compact search and reference layout.

## Using the reference

- Search by variable name, label or source. The extension also recognizes the
  204 original column names in its downloadable crosswalk.
- `Values in year` requires a nonmissing value in that year. All-null variables
  remain searchable under `All years`. Source, category-label and metadata-gap
  filters cover the complete release.
- Variable links preserve the selected year and reference section. On phones,
  a year selector remains available inside the variable view.
- Source records lead with reporting period, population and release status.
  Original table references, corrections and verification evidence remain
  accessible below the summary.
- `Save list as CSV` downloads summaries of the matching variables, not panel
  observations. Missing-row counts describe the full release. Reference-table
  and PDF downloads use release-specific filenames.
- Keyboard users can press `/` to search and use arrow keys between reference
  tabs. Printing includes all four sections and the release identity.

The skip link also opens search from the phone's variable view. Interrupted
reference-file requests have a time limit and can be retried; a failed detail
file does not prevent searches for other variables.

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

## Documentation corrections

The public CSV summaries and PDFs were corrected after the dataset releases.
The Stata mapping tables now display the exact native value labels, including
source-code prefixes. This affects `ACT`, `CIPCODE1` through `CIPCODE6`,
`FYBEG`, `FYEND`, and `STABBR` in both codebooks. The labels in the data exports
were already correct.

For `LINE_55`, `PCF_F_RV`, `REV_IC`, and `SFTETOTL`, the dictionary CSV now
retains every distinct definition recorded for the latest year, including
missing descriptions. The full definitions download and source JSON retain
the original records.

These documentation corrections change the public PDF, dictionary CSV and
codebook checksums. They do not change the dataset release identity, panel
values, native Stata labels, or original source metadata. Codebooks archived
inside sealed CIRAGO releases retain their original bytes; use the public
downloads for the corrected presentation. Their recorded data fingerprints
still identify the same datasets.
The [documentation audit](../Artifacts/codebook_documentation_audit.json)
records the before/after file hashes, unchanged dataset fingerprints, source-link
checks and observed browser recovery behavior.

Interface regression checks use Node 24 and no npm packages:

```sh
node --test tests/test_codebook_browser.cjs
.venv/bin/python -m pytest -q tests/test_build_codebook.py \
  tests/test_codebook_pdf.py tests/test_published_codebook.py \
  tests/test_published_codebook_content.py
```

CI runs these interaction checks alongside the complete Python suite. Browser
checks separately cover responsive layout, keyboard access and request recovery.

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
