# Governance

Mark Jayson Farol is the sole researcher, maintainer, and code owner for IPEDSDB_Panel.

Current contact information is available at `https://markjayson.com`.

Release decisions are made through the repository contract and QA evidence, not by informal edits to generated files. A public release should have:

- passing repository tests
- passing panel contract validation
- passing public-release guard
- passing documentation style guard
- passing release manifest verification
- a bundle with checksums, citation metadata, DataCite metadata, and RO-Crate metadata

Generated data should stay outside git. Release bundles may be deposited in an external archive after checksums and citation files are frozen.

Breaking changes require a new contract identifier and a changelog entry. Small documentation fixes may stay on the current release line.
