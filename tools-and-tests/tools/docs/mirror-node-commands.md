## Mirror Node Subcommands

Top-level `mirror` command contains utilities for downloading Mirror Node CSV exports and producing the `block_times.bin` file used by the `record2block` pipeline.

Available subcommands include:
- `fetchRecordsCsv` - Download Mirror Node record table CSV dump from GCP bucket
- `extractBlockTimes` - Extract block times binary file from the Mirror Node record CSV
- `validateBlockTimes` - Validate a `block_times.bin` file against the Mirror Node CSV
- `addNewerBlockTimes` - Extend an existing `block_times.bin` with newer block times by listing GCP
- `extractDayBlocks` - (utility) Extract per-day block info from CSV/other sources

> Important: many mirror commands download from public GCP buckets that are configured as Requester Pays. To access these you must have Google Cloud authentication configured locally. Typical steps:
>
> ```bash
> gcloud auth application-default login
> # or
> gcloud auth login
> ```
>
> Some commands also require a project id to be set that will be used for requester pays billing. See the command help or code for details.

---

### `fetchRecordsCsv`

Download Mirror Node record table CSV dump from the `mirrornode-db-export` GCP bucket. This bucket contains large gzipped CSVs (many GB) under a versioned directory.

Usage:

```
mirror fetchRecordsCsv [--record-dir=<dir>]
```

Options:
- `--record-dir <dir>` — Destination directory for downloaded gzipped CSV files (default: `data/mirror_node_record_files`).

Notes:
- Requires Google Cloud credentials (application default) and a project id for requester pays access.

---

### `extractBlockTimes`

Parses the Mirror Node `record_file` CSV gzipped files and writes a binary `block_times.bin` file. The resulting file is a binary array of 64-bit longs where each long is the number of nanoseconds since the first block; the array index is the block number. This file can be memory-mapped for fast random access.

Usage:

```
mirror extractBlockTimes [--record-dir=<dir>] [--block-times=<blockTimesFile>]
```

Options:
- `--record-dir <dir>` — Directory containing downloaded Mirror Node CSV gz files (default: `data/mirror_node_record_files`).
- `--block-times <file>` — Output path for `block_times.bin` (default: `data/block_times.bin`).

---

### `validateBlockTimes`

Validate an existing `block_times.bin` by comparing block-time prefixes against the Mirror Node CSV file names.

Usage:

```
mirror validateBlockTimes [--record-csv=<recordCsv.gz>] [--block-times=<blockTimesFile>]
```

Options:
- `--record-csv <file>` — Path to Mirror Node record CSV gz (default: `data/record_file.csv.gz`).
- `--block-times <file>` — Path to block times binary file (default: `data/block_times.bin`).

---

### `addNewerBlockTimes`

Appends block times for blocks newer than those recorded in an existing `block_times.bin` file by listing per-day files from the GCP buckets and writing additional longs to the binary file.

Usage:

```
mirror addNewerBlockTimes [-c] [--min-node-account-id=<n>] [--max-node-account-id=<n>] [-d <dataDir>] [--block-times=<blockTimesFile>]
```

Options:
- `-c`, `--cache-enabled` — Use local cache for downloaded content (default: true).
- `--min-node-account-id` / `--max-node-account-id` — Node account id range used for bucket access (defaults: 3 and 34).
- `-d`, `--data-dir <dataDir>` — Base data directory (default: `data`).
- `--block-times <file>` — Path to block times binary file (default: `data/block_times.bin`).

Notes:
- The command verifies the last file seen for each day against the Mirror Node REST-derived record file name for the calculated last block of the day.

---

### `extractDayBlocks`

Utility for extracting per-day block information (used by other tooling). See command help for parameters and usage.
