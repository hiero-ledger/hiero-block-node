## Records Subcommands

Top-level `records` command contains subcommands for working with raw record stream files (.rcd / .rcd.gz).

- `ls` - List record file info contained in provided `.rcd` / `.rcd.gz` files or directories
- `record2block` - Convert historical record stream files into block stream blocks

---

### The `ls` (records) Subcommand

Lists parsed metadata for each record file, including record format version, HAPI version and short hashes.

Usage:

```
records ls <files-or-dirs>...
```

Options:
- `<files-or-dirs>...` — Files or directories to process. Directories are walked and files ending with `.rcd` or `.rcd.gz` are included.

Output columns include file name, record format version, HAPI version, previous block hash, block hash and human readable size.

---

### The `record2block` Subcommand
> **Important:**
> This is old command is in the process of being replaced by a new [days wrap](days-commands.md#wrap) command.

Converts historical record stream files into blocks. This command downloads supporting files (record files, signature files, sidecars)
from the public GCP buckets and constructs Block protobufs for a range of block numbers.

Usage:

```
records record2block [-s <start>] [-e <end>] [-j] [-c] [--min-node-account-id=<n>] [--max-node-account-id=<n>] [-d <dataDir>] [--block-times=<blockTimesFile>]
```

Options:
- `-s`, `--start-block <blockNumber>`
    - The first block number to process (default: 0).
- `-e`, `--end-block <blockNumber>`
    - The last block number to process (default: 3001).
- `-j`, `--json`
    - Also output blocks as JSON (for debugging).
- `-c`, `--cache-enabled`
    - Use a local GCP cache for downloads (saves bandwidth/costs).
- `--min-node-account-id` / `--max-node-account-id`
    - Configure range of node account ids used when listing/downloading from GCP buckets (defaults: 3..34).
- `-d`, `--data-dir <dataDir>`
    - Base directory for output and temporary files (default: `data`).
- `--block-times <blockTimesFile>`
    - Path to the `block_times.bin` file used to map block number → record file time (default: `data/block_times.bin`).

Notes & prerequisites:
- The command expects a `block_times.bin` file that maps block numbers to record file times. See the mirror subcommands below to produce/validate that file.
- This command downloads public data from Google Cloud Storage (requester pays). You must authenticate with Google Cloud SDK (for example `gcloud auth application-default login` or `gcloud auth login`) and have project billing set up for requester pays access.
