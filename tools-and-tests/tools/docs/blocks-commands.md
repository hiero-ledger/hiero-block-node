# Blocks Subcommands

The `blocks` command contains utilities for working with Block Stream files (.blk, .blk.gz, .blk.zstd).

### Available Subcommands

|          Command          |                                     Description                                     |
|---------------------------|-------------------------------------------------------------------------------------|
| `json`                    | Converts a binary Block Stream to JSON                                              |
| `ls`                      | Prints info for block files (supports .blk, .blk.gz, .blk.zstd, and zip archives)   |
| `validate`                | Validates a wrapped Block Stream (hash chain and signatures)                        |
| `validate-wrapped`        | Validates wrapped blocks produced by `wrap` (chain, merkle tree, structure, supply) |
| `wrap`                    | Convert record file blocks in day files to wrapped Block Stream blocks              |
| `fetchBalanceCheckpoints` | Fetch balance checkpoint files from GCP and compile into a resource file            |

---

### The `json` Subcommand

Converts binary Block Stream files (.blk or .blk.gz) to JSON files placed next to the input files.

#### Usage

```
blocks json [-t] [-ms=<minSizeMb>] [<files>...]
```

#### Options

|                   Option                    |                                                               Description                                                               |
|---------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `-t`, `--transactions`                      | Expand transactions (replaces applicationTransaction base64 fields with parsed TransactionBody JSON). Useful for human-readable output. |
| `-ms <minSizeMb>`, `--min-size=<minSizeMb>` | Filter to only files bigger than this minimum size in megabytes. Default is no limit.                                                   |
| `<files>...`                                | Files or directories to convert. Directories are walked and files with `.blk` or `.blk.gz` are processed.                               |

#### Notes

- The command reads block protobufs and writes a `.json` file next to each source file.
- Processing is parallelized for multiple files.

---

### The `ls` Subcommand

Prints info for Block Stream files. Supports standalone block files, compressed files, and zip archives.

#### Supported Formats

- Standalone block files: `<BLOCK_NUMBER>.blk` (uncompressed)
- Compressed standalone: `.blk.gz` or `.blk.zstd`
- Zip archives containing blocks (each internally as `.blk` or `.blk.zstd`)

Input can be files or directories. Directories are recursively scanned for blocks. Results are sorted by block number.

#### Usage

```
blocks ls [-c] [-ms=<minSizeMb>] [-o=<outputFile>] [<files>...]
```

#### Options

|                     Option                      |                              Description                              |
|-------------------------------------------------|-----------------------------------------------------------------------|
| `-c`, `--csv`                                   | Enable CSV output mode (default: false).                              |
| `-ms <minSizeMb>`, `--min-size=<minSizeMb>`     | Filter to only files bigger than this minimum file size in megabytes. |
| `-o <outputFile>`, `--output-file=<outputFile>` | Write output to the specified file instead of stdout.                 |
| `<files>...`                                    | Block files, directories, or zip archives to process.                 |

---

### The `validate` Subcommand

Validates a wrapped Block Stream by checking:

- **Hash chain continuity** - each block's previousBlockRootHash matches computed hash of previous block
- **Genesis block** - first block has 48 zero bytes for previous hash
- **Signature validation** - at least 1/3 + 1 of address book nodes must sign

#### Supported Inputs

- Individual block files (`*.blk`, `*.blk.gz`, `*.blk.zstd`)
- Hierarchical directory structures produced by `wrap` command and `BlockWriter`
- Zip archives containing multiple blocks

#### Usage

```
blocks validate [-v] [--skip-signatures] [-a=<addressBookFile>] [<files>...]
```

#### Options

|            Option             |                                                      Description                                                       |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------|
| `-a`, `--address-book <file>` | Path to address book history JSON file. If not specified, auto-detects `addressBookHistory.json` in input directories. |
| `--skip-signatures`           | Skip signature validation (only check hash chain).                                                                     |
| `-v`, `--verbose`             | Print details for each block.                                                                                          |
| `<files>...`                  | Block files or directories to validate.                                                                                |

#### Notes

- When validating output from the `wrap` command, you can simply pass the output directory as the only parameter. The command will automatically find the `addressBookHistory.json` file in that directory if not explicitly specified.

#### Example

```bash
# Validate blocks in a directory (auto-detect address book)
blocks validate /path/to/wrappedBlocks

# Validate with explicit address book
blocks validate -a /path/to/addressBookHistory.json /path/to/blocks/

# Hash chain validation only (skip signatures)
blocks validate --skip-signatures /path/to/blocks/
```

---

### The `validate-wrapped` Subcommand

Validates wrapped block stream files produced by the `wrap` command. Walks all blocks in the input directory in order and performs the following checks:

- **Blockchain chain validation** — each block's `previousBlockRootHash` in the footer matches the computed hash of the preceding block.
- **Historical block tree root** — the `rootHashOfAllBlockHashesTree` in the footer matches the expected merkle tree root computed from all preceding block hashes (only when starting from block 0).
- **Required items** — every block contains at least one `BlockHeader`, `RecordFile`, `BlockFooter`, and `BlockProof`.
- **Item ordering** — items appear in the correct order: `BlockHeader`, optional `StateChanges`, `RecordFile`, `BlockFooter`, one or more `BlockProof` items, with no duplicates or misplaced items.
- **50 billion HBAR supply** — tracks account balances across all blocks (from `StateChanges` and `RecordFile` transfer lists) and verifies the total equals exactly 50 billion HBAR after each block (only when starting from block 0).

#### Usage

```
blocks validate-wrapped [-n=<network>] [--[no-]validate-balances] [--balance-checkpoints=<file>]
                        [--custom-balances-dir=<dir>] [--balance-check-interval-days=<days>] [<files>...]
```

#### Options

|                      Option                      |                                           Description                                            |
|--------------------------------------------------|--------------------------------------------------------------------------------------------------|
| `-n`, `--network <name>`                         | Network name for network-specific validation (`mainnet`, `testnet`, `none`). Default: `mainnet`. |
| `--validate-balances` / `--no-validate-balances` | Enable or disable balance checkpoint validation. Default: enabled.                               |
| `--balance-checkpoints <file>`                   | Path to pre-fetched balance checkpoints file (`balance_checkpoints.zstd`).                       |
| `--custom-balances-dir <dir>`                    | Directory containing custom balance files (`accountBalances_{blockNumber}.pb.gz`).               |
| `--balance-check-interval-days <days>`           | Only validate balance checkpoints every N days (default: 30 = monthly).                          |
| `<files>...`                                     | Block files, directories, or zip archives to process.                                            |

#### Balance Validation

When balance validation is enabled (default), the command validates computed account balances against pre-fetched balance checkpoints. This ensures the 50 billion HBAR supply is correctly tracked through all transactions.

Balance checkpoints can be loaded from:
- A compiled checkpoint file created by `fetchBalanceCheckpoints` (recommended)
- `balance_checkpoints_monthly.zstd` - 32 checkpoints, ~14MB (default, faster)
- `balance_checkpoints_weekly.zstd` - 136 checkpoints, ~20MB (more thorough)
- A directory of custom balance files extracted from saved states

The `--balance-check-interval-days` option controls how often checkpoints are validated. The default of 30 days (monthly) provides a good balance between validation coverage and performance. Use smaller intervals for more thorough validation or larger intervals for faster runs.

**Important:** The validation interval can only be as granular as the checkpoints that were fetched.
For example, if checkpoints were fetched with `--interval-days 30` (monthly), you cannot validate
weekly since weekly checkpoints don't exist in the file. To validate at a smaller interval, you
must first re-fetch checkpoints using `fetchBalanceCheckpoints` with a matching `--interval-days`
value.

#### Notes

- When starting from block 0, a `StreamingHasher` is created to validate the historical block hash merkle tree and a balance map is maintained for 50 billion HBAR supply validation. When starting from a later block, both are skipped because the prior state is unavailable.
- Supports both individual block files (nested directories of `.blk.zstd`) and zip archives produced by the `wrap` command.
- Progress is printed every 1000 blocks with an ETA.
- If no balance checkpoints are loaded, balance validation is automatically skipped with a warning.

#### Example

```bash
# Validate wrapped blocks in a directory (balance validation enabled by default)
blocks validate-wrapped /path/to/wrappedBlocks

# Validate with explicit balance checkpoint file
blocks validate-wrapped --balance-checkpoints data/balance_checkpoints.zstd /path/to/wrappedBlocks

# Validate balances weekly instead of monthly
blocks validate-wrapped --balance-check-interval-days 7 /path/to/wrappedBlocks

# Skip balance validation for faster runs
blocks validate-wrapped --no-validate-balances /path/to/wrappedBlocks

# Validate with custom balance files from saved states
blocks validate-wrapped --custom-balances-dir /path/to/balance_files /path/to/wrappedBlocks
```

---

### The `wrap` Subcommand

Converts record file blocks organized in daily tar.zstd files into wrapped Block Stream `Block` protobufs. This is a key command in the record-to-block conversion pipeline.

#### Usage

```
blocks wrap [-u] [-n=<network>] [-b=<blockTimesFile>] [-d=<dayBlocksFile>] [-i=<inputDir>] [-o=<outputDir>]
```

#### Options

|              Option              |                                                  Description                                                  |
|----------------------------------|---------------------------------------------------------------------------------------------------------------|
| `-b`, `--blocktimes-file <file>` | BlockTimes file for mapping record file times to blocks (default: `metadata/block_times.bin`).                |
| `-d`, `--day-blocks <file>`      | Path to the day blocks JSON file (default: `metadata/day_blocks.json`).                                       |
| `-n`, `--network <name>`         | Network name for applying amendments (`mainnet`, `testnet`, `none`). Default: `mainnet`.                      |
| `-u`, `--unzipped`               | Write output files as individual files in nested directories, rather than in uncompressed zip batches of 10k. |
| `-i`, `--input-dir <dir>`        | Directory of record file tar.zstd days to process (default: `compressedDays`).                                |
| `-o`, `--output-dir <dir>`       | Directory to write the output wrapped blocks (default: `wrappedBlocks`).                                      |

#### Prerequisites

The command requires:

1. **Block times file** (`block_times.bin`) - maps block numbers to record file times
2. **Day blocks file** (`day_blocks.json`) - metadata for blocks organized by day

These can be generated using the `mirror` subcommands:

```bash
mirror fetchRecordsCsv
mirror extractBlockTimes
mirror extractDayBlock
```

#### Output Structure

The command writes:

- Wrapped block files (either as individual `.blk` files or in zip batches)
- `addressBookHistory.json` - address book history copied/generated during conversion
- `blockStreamBlockHashes.bin` - registry of computed block hashes
- `streamingMerkleTree.bin` and `completeMerkleTree.bin` - Merkle tree state for resume capability

#### Notes

- The command can resume from where it left off if interrupted
- Progress is saved periodically via shutdown hooks
- A merkle tree of block hashes is computed during conversion

#### Example

```bash
# Basic conversion
blocks wrap -i /path/to/compressedDays -o /path/to/wrappedBlocks

# Output as individual unzipped files
blocks wrap -u -i /path/to/compressedDays -o /path/to/wrappedBlocks
```

---

### The `fetchBalanceCheckpoints` Subcommand

Fetches balance checkpoint files from GCP, optionally verifies signatures, and compiles them into a single zstd-compressed resource file for offline balance validation. This command downloads the `accountBalances` files from the Hedera mainnet GCP bucket and processes them into a compact binary format.

#### Usage

```
blocks fetchBalanceCheckpoints [-o=<outputFile>] [--start-day=<date>] [--end-day=<date>]
                               [--interval-days=<days>] [--interval-hours=<hours>]
                               [--skip-signatures] [--block-times=<file>] [--address-book=<file>]
```

#### Options

|           Option           |                                                 Description                                                  |
|----------------------------|--------------------------------------------------------------------------------------------------------------|
| `-o`, `--output <file>`    | Output zstd-compressed file path (default: `balance_checkpoints.zstd`).                                      |
| `--start-day <date>`       | Start day in format `YYYY-MM-DD` (default: `2019-09-13`).                                                    |
| `--end-day <date>`         | End day in format `YYYY-MM-DD` (default: `2023-10-23`).                                                      |
| `--interval-days <days>`   | Only include one checkpoint every N days (e.g., 7 for weekly, 30 for monthly). Overrides `--interval-hours`. |
| `--interval-hours <hours>` | Only include checkpoints at this hour interval (default: 24 = one per day).                                  |
| `--skip-signatures`        | Skip signature verification (not recommended for production use).                                            |
| `--block-times <file>`     | Path to `block_times.bin` file for timestamp to block mapping (default: `data/block_times.bin`).             |
| `--address-book <file>`    | Path to address book history JSON file for signature verification (default: `data/addressBookHistory.json`). |
| `--gcp-project <project>`  | GCP project for requester-pays bucket access (default: from `GCP_PROJECT_ID` env var).                       |
| `--cache-dir <dir>`        | Directory for caching downloaded files (default: `data/gcp-cache`).                                          |
| `--min-node <id>`          | Minimum node account ID (default: 3).                                                                        |
| `--max-node <id>`          | Maximum node account ID (default: 34).                                                                       |

#### Prerequisites

- **GCP authentication** - Run `gcloud auth application-default login` before using this command
- **block_times.bin** - Required for mapping timestamps to block numbers
- **addressBookHistory.json** - Required for signature verification (unless `--skip-signatures` is used)

#### Output Format

The output file is a zstd-compressed binary file containing checkpoint records:

- Block number (8 bytes, long)
- Account count (4 bytes, int)
- For each account: accountNum (8 bytes, long) + balance (8 bytes, long)

This format avoids protobuf parsing limits and supports files with millions of accounts.

#### Example

```bash
# Fetch monthly checkpoints (recommended for validation)
blocks fetchBalanceCheckpoints --interval-days 30 -o balance_checkpoints.zstd

# Fetch weekly checkpoints for more thorough validation
blocks fetchBalanceCheckpoints --interval-days 7 -o balance_checkpoints_weekly.zstd

# Fetch checkpoints for a specific date range
blocks fetchBalanceCheckpoints --start-day 2022-01-01 --end-day 2022-12-31 -o balance_2022.zstd

# Skip signature verification (faster but less secure)
blocks fetchBalanceCheckpoints --skip-signatures -o balance_checkpoints.zstd
```

#### Notes

- The command handles large balance files (2M+ accounts) that exceed standard protobuf parsing limits by using a custom wire-format parser.
- Downloaded files are cached locally to avoid re-downloading on subsequent runs.
- Signature verification ensures checkpoint integrity but requires an address book history file.
- The compiled output file can be used with `validate-wrapped --balance-checkpoints` for offline validation.
- The `--interval-days` value determines the granularity of validation possible. For example, monthly checkpoints (`--interval-days 30`) only allow monthly validation, not weekly. Choose the fetch interval based on your validation needs.
