# Preserved v2 pipeline source

`v2-baseline-799a639.tar.gz` contains the original pipeline source required by
the 2023 metadata repair and the 2024 extension. The preparation script verifies
its pinned SHA-256 before extraction. A fresh checkout does not need an old
local Git branch or network access to prepare the pipeline.

The source is commit `799a63930f97df9a3ec35be857f340ea3743afac`.
The matching JSON manifest records the original Git tree and blob identities,
each file's SHA-256, and the archive hash. Every archived file was compared
byte for byte with that commit. The preserved PRCH policy still hashes to
`67fca57085ec759944152f6e6e38374fac75991ef2dbd0489d241641d310e4f3`.

The archive contains `Scripts/`, `contracts/`, `tools/`, `Queries/`, the small
runtime evidence and templates under `Artifacts/`, the three
requirements files, `manual_commands.sh`, and `LICENSE`. It contains no data
release, manuscript, or Git history. Existing versioned repair and extension
patches are applied after extraction, exactly as before.

This snapshot supports the metadata-repair and extension commands, which run
Stages 03-07. The published release also retains the complete prepared pipeline
under `Provisional/Reproduction/pipeline-2024/`; use the
[release reproduction instructions](../../docs/2024_EXTENSION.md) for that replay.

To regenerate from a checkout that retains the original commit, archive the
manifest's `included_paths` with `git archive --format=tar`, then compress those
bytes with Python's `gzip.compress(..., mtime=0)`. Verify the resulting files
against the manifest before updating any pinned hash.
