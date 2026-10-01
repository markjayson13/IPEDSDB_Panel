# Local data storage

The repository contains code, contracts and published documentation. Large
IPEDS source files and working datasets live under
`/Volumes/CIRAGO/IPEDSDB_PANEL`.

| Location under the data root | Contents |
| --- | --- |
| `Final/` | Final-only 2004-2023 analysis dataset and matching labels |
| `Provisional/` | Verified 2004-2024 analysis extension and matching labels |
| `Sources/Raw_Access_Databases/` | Original 2004-2023 archives and extracted tables |
| `Sources/2024/` | Shortcut to the frozen source snapshot in the verified 2024 extension |
| `Sources/2024_source_audit_2026-09-29/` | Original 2024 Access download, metadata and initial audit evidence |
| `Sources/2024_download_cache/` | Official component archives and dictionaries used in source preparation |
| `Sources/Post2023_exploration/` | Retained exploratory downloads and extracts, including a 2025 enrollment archive that is not part of the published panel |
| `Work/2024-extension-v1/local-build-retained/` | Relocated intermediate cross-sections, panels, prepared sources, pipeline copies and development receipts |
| `Archive/full_panel_label_audit/` | Earlier label-audit downloads and intermediate metadata |

Use `Final` or `Provisional` for analysis. Files in `Work` and exploratory
source folders are supporting material, not alternative final datasets.

## September 2026 relocation

The large IPEDS files previously stored in `/private/tmp` were copied to
CIRAGO and checked individually with SHA-256 before their local copies were
removed. The duplicate 2004 archive in Downloads was matched against the
existing external archive. Small symbolic links preserve the old local paths.
They require CIRAGO to be connected and may disappear when macOS clears its
temporary folder; use the documented external paths in new commands.

The CIRAGO `Work/2024-extension-v1/build` and `pipeline` shortcuts now resolve
directly within CIRAGO. Links inside the relocated workspace use relative paths.
Historical receipts preserve their original recorded locations.

The full per-file verification manifest is stored at
`Work/Storage_moves/2026-09-30/manifest.json`. The repository keeps a compact
[relocation receipt](../Artifacts/storage_relocation_2026-09-30.json).
The original dataset release manifests, values and labels are unchanged.

## Future source preparation

`Scripts/prepare_2024_sources.py` follows `--root` or `IPEDSDB_ROOT`, defaulting
to CIRAGO. Its source download cache stays under `Sources`; a newly prepared
snapshot goes to `Work/2024-source-preparation/sources`. It refuses writes to
an unmounted external volume, including paths reached through local links.

To verify the already frozen source snapshot without rebuilding:

```sh
.venv/bin/python Scripts/prepare_2024_sources.py \
  --output-root /Volumes/CIRAGO/IPEDSDB_PANEL/Sources/2024
```

Explicit `--audit-root`, `--cache-root` and `--output-root` overrides remain
available. Choose a new `Work` directory for a new build; never overwrite a
sealed release under `Releases`.
