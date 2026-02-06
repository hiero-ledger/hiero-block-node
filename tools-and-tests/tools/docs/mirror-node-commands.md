# Mirror Node Subcommands

Top-level `mirror` command contains utilities for downloading Mirror Node CSV exports and producing the `block_times.bin` file used by the `record2block` pipeline.

Available subcommands include:
- `fetchRecordsCsv` - Download Mirror Node record table CSV dump from GCP bucket
- `extractBlockTimes` - Extract block times binary file from the Mirror Node record CSV
- `validateBlockTimes` - Validate a `block_times.bin` file against the Mirror Node CSV
- `addNewerBlockTimes` - Extend an existing `block_times.bin` with newer block times by listing GCP
- `fixBlockTime` - Repair incorrect entries in `block_times.bin` by querying the Mirror Node
- `fetchMissingTransactions` - Download and compile mainnet errata for missing transactions
- `update` - Update `block_times.bin` and `day_blocks.json` with newer blocks from mirror node REST API
- `extractDayBlocks` - (utility) Extract per-day block info from CSV/other sources
- `generateAddressBook` - Generate address book history JSON from Mirror Node CSV export
- `compareAddressBooks` - Compare two address book history JSON files

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

### `update`

Update `block_times.bin` and `day_blocks.json` with newer blocks from the Mirror Node REST API. This is the simplest way to create or extend the metadata files required by the `blocks wrap` command. It fetches block data in batches from the public Mirror Node API (no GCP credentials required).

Usage:

```
mirror update [--block-times=<file>] [--day-blocks=<file>] [--end-date=<date>]
```

Options:
- `--block-times <file>` — Path to the block times `.bin` file (default: `metadata/block_times.bin`).
- `--day-blocks <file>` — Path to the day blocks `.json` file (default: `metadata/day_blocks.json`).
- `--end-date <date>` — Stop after fetching all blocks through this date inclusive (format: `YYYY-MM-DD`). If not specified, fetches all available blocks up to the latest.

Features:
- Creates files from scratch if they don't exist (including parent directories).
- Resumes from the highest block already present in the files.
- Fetches blocks in batches of 100 from the mainnet Mirror Node REST API.
- Excludes the current UTC day from `day_blocks.json` since it is incomplete.

Example:

```bash
# Update metadata files with all available blocks
mirror update

# Fetch only the first two days of mainnet (for testing)
mirror update --end-date 2019-09-14

# Use custom file paths
mirror update --block-times /path/to/block_times.bin --day-blocks /path/to/day_blocks.json
```

Notes:
- Does not require GCP credentials — uses the public Mirror Node REST API.
- Progress is printed every 1000 blocks and for the first few blocks.
- The `block_times.bin` file is flushed to disk after each batch.

---

### `extractDayBlocks`

Utility for extracting per-day block information (used by other tooling). See command help for parameters and usage.

---

### `generateAddressBook`

Generate an address book history JSON file from the Mirror Node `address_book` CSV export. This command downloads the latest CSV from the `mirrornode-db-export` GCP bucket, parses it, filters duplicates, and produces a JSON file compatible with `AddressBookHistory`.

Usage:

```
mirror generateAddressBook [--output=<file>] [--temp-dir=<dir>] [options]
```

Options:
- `-o`, `--output <file>` — Output path for address book history JSON (default: `data/addressBookHistory.json`).
- `--temp-dir <dir>` — Temporary directory for downloading CSV file (default: `data/temp`).
- `--keep-duplicates` — Keep all entries even if same timestamp appears multiple times (default: false, keeps latest).
- `--filter-duplicates` — Filter out duplicate entries with identical content (default: true).
- `--filter-description-only` — Filter out entries where only node descriptions changed (default: false).
- `--show-changes` — Show detailed changes between consecutive address book entries.

Features:
- **Automatic Genesis Handling**: Converts epoch 0 timestamps to the proper genesis timestamp (2019-09-13).
- **Content Deduplication**: Removes consecutive entries with identical content (same nodes, IPs, ports, keys, descriptions).
- **Node Account ID Extraction**: Correctly identifies nodes using either `nodeAccountId` field or `memo` field.
- **Transaction Result Filtering**: Infrastructure to filter by transaction success (when column is available in CSV).

Example:

```bash
# Basic usage
java -jar tools/build/libs/tools-all.jar mirror generateAddressBook \
  --output addressbook.json

# With change analysis
java -jar tools/build/libs/tools-all.jar mirror generateAddressBook \
  --output addressbook.json \
  --show-changes

# Disable all filtering (debug mode)
java -jar tools/build/libs/tools-all.jar mirror generateAddressBook \
  --output addressbook.json \
  --filter-duplicates=false
```

Notes:
- Requires Google Cloud credentials (application default) for accessing the GCP bucket.
- Downloads approximately 1.2 MB compressed CSV data.
- By default, filters ~20 content-duplicate entries from typical datasets.
- See [ADDRESS_BOOK_ANALYSIS.md](address-book-analysis.md) for detailed technical documentation.

---

### `compareAddressBooks`

Compare two address book history JSON files and report discrepancies. This is useful for validating address books generated from different sources (e.g., Mirror Node CSV vs. Block Stream processing).

Usage:

```
mirror compareAddressBooks --old=<file> --new=<file> [options]
```

Options:
- `--old <file>` — Path to first (old) address book history JSON file (required).
- `--new <file>` — Path to second (new) address book history JSON file (required).
- `--verbose` — Show detailed node information for mismatches (default: false).
- `--print-all-dates` — Print all dates even if they match (default: false).

Output:
- ✓ Matching dates
- ❌ Dates with discrepancies (shows node details if `--verbose`)
- Summary statistics (total dates, matches, missing in each file)

Example:

```bash
# Basic comparison
java -jar tools/build/libs/tools-all.jar mirror compareAddressBooks \
  --old oldAddressBook.json \
  --new newAddressBook.json

# Detailed comparison with all dates
java -jar tools/build/libs/tools-all.jar mirror compareAddressBooks \
  --old oldAddressBook.json \
  --new newAddressBook.json \
  --verbose \
  --print-all-dates
```

Exit Codes:
- `0` — Address books match
- `1` — Address books differ or error occurred

Use Cases:
- Validate Mirror Node CSV export against Block Stream data
- Verify address book generation after code changes
- Debug discrepancies between data sources
- Track address book evolution over time

See [ADDRESS_BOOK_ANALYSIS.md](address-book-analysis.md) for analysis of expected discrepancies between different data sources.

---

### `fixBlockTime`

Repair incorrect entries in `block_times.bin` by querying the Mirror Node for correct timestamps. This command is useful when the block times file has stale or incorrect data that causes download failures.

Usage:

```
mirror fixBlockTime [--dry-run] <from-block> [to-block]
```

Options:
- `--dry-run` — Preview changes without writing to the file.
- `<from-block>` — Starting block number to fix (required).
- `[to-block]` — Ending block number to fix (optional, defaults to `from-block` for single block).

Features:
- Fix single block or range of blocks
- Dry-run mode to preview changes before applying
- Built-in verification of writes
- Detailed output showing current vs correct values

Example:

```bash
# Fix a single block
java -jar tools-all.jar mirror fixBlockTime 90725361

# Fix a range of blocks
java -jar tools-all.jar mirror fixBlockTime 90725361 90726000

# Preview changes without writing
java -jar tools-all.jar mirror fixBlockTime --dry-run 90725361 90726000
```

Notes:
- Queries the Mirror Node `/api/v1/blocks/{blockNumber}` endpoint for each block.
- Updates the `block_times.bin` file in place.
- Useful for fixing stale block times that cause `IndexOutOfBoundsException` during downloads.

---

### `fetchMissingTransactions`

Download and compile mainnet errata for missing transactions. This command fetches information about transactions that are known to be missing from the blockchain record.

Usage:

```
mirror fetchMissingTransactions
```

Features:
- Downloads missing transaction data from known sources
- Compiles errata into a format usable by block processing tools
- Stores results in `missing_transactions.gz` resource file

Notes:
- This is primarily used for mainnet historical data correction.
- The output is used by block validation and conversion tools to handle known gaps in the transaction record.
