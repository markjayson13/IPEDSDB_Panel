IPEDSDB Panel user codebook

Release: full-panel-labels-v1
Dataset: 141,711 institution-year rows, 2,721 variables.

Start with ipeds-panel-codebook.pdf, or use the searchable interface:
https://markjayson13.github.io/IPEDSDB_Panel/

codebook.csv gives one row per variable. Each description applies only
to its description_years. definitions.csv.gz contains the full history;
decompress it before opening the CSV. value-labels.csv gives source
category meanings with their exact years. Stata mappings and physical
source corrections are in the PDF and interactive reference.

Missing definitions and undocumented category meanings remain flagged.
Do not assume comparability across years, or interpret nulls as zero.
These files contain documentation, not institution-level data.

manifest.json records codebook checksums and the data/metadata hashes
this codebook describes. index.json and variables-*.json power the site.
