# Blocks Subcommands

The `blocks` command contains utilities for working with Block Stream files (.blk, .blk.gz, .blk.zstd).

### Available Subcommands

|          Command          |                                    Description                                    |
|---------------------------|-----------------------------------------------------------------------------------|
| `json`                    | Converts a binary Block Stream to JSON                                            |
| `ls`                      | Prints info for block files (supports .blk, .blk.gz, .blk.zstd, and zip archives) |
| `validate`                | Validates a wrapped Block Stream (hash chain and signatures)                      |
| `wrap`                    | Convert record file blocks in day files to wrapped Block Stream blocks            |
| `fetchBalanceCheckpoints` | Fetch balance checkpoint files from GCP and compile into a resource file          |
| `repair-zips`             | Repair corrupt zip CENs in wrapped-block directories                              |

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

Validates wrapped block stream files produced by the `wrap` command. Walks all blocks in the input directory in order and performs the following checks:

- **Hash chain continuity** — each block's `previousBlockRootHash` in the footer matches the computed hash of the preceding block.
- **Genesis block** — first block has 48 zero bytes for previous hash.
- **Historical block tree root** — the `rootHashOfAllBlockHashesTree` in the footer matches the expected merkle tree root computed from all preceding block hashes (only when starting from block 0).
- **Required items** — every block contains at least one `BlockHeader`, `RecordFile`, `BlockFooter`, and `BlockProof`.
- **Item ordering** — items appear in the correct order: `BlockHeader`, optional `StateChanges`, `RecordFile`, `BlockFooter`, one or more `BlockProof` items, with no duplicates or misplaced items.
- **Signature validation** — at least 1/3 + 1 of address book nodes must sign.
- **50 billion HBAR supply** — tracks account balances across all blocks (from `StateChanges` and `RecordFile` transfer lists) and verifies the total equals exactly 50 billion HBAR after each block (only when starting from block 0).
- **Balance checkpoints** — validates computed account balances against pre-fetched balance checkpoints at configurable intervals.

#### Supported Inputs

- Individual block files (`*.blk`, `*.blk.gz`, `*.blk.zstd`)
- Hierarchical directory structures produced by `wrap` command and `BlockWriter`
- Zip archives containing multiple blocks

#### Usage

```
blocks validate [-v] [--skip-signatures] [--skip-supply] [--no-resume] [--threads=<N>] [--prefetch=<N>]
                [-a=<addressBookFile>] [--network=<network>] [--[no-]validate-balances]
                [--balance-checkpoints=<file>] [--custom-balances-dir=<dir>]
                [--balance-check-interval-days=<days>] [<files>...]
```

#### Options

|                      Option                      |                                                      Description                                                       |
|--------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| `-a`, `--address-book <file>`                    | Path to address book history JSON file. If not specified, auto-detects `addressBookHistory.json` in input directories. |
| `--skip-signatures`                              | Skip signature validation (only check hash chain and state).                                                           |
| `--skip-supply`                                  | Skip HBAR supply validation (useful for networks with known transfer list imbalances).                                 |
| `--no-resume`                                    | Ignore any existing checkpoint and start validation from scratch.                                                      |
| `--threads <N>`                                  | Decompression + parse threads (default: available CPU cores - 1).                                                      |
| `--prefetch <N>`                                 | Number of blocks to buffer ahead for decompression (default: 64).                                                      |
| `-v`, `--verbose`                                | Print details for each block.                                                                                          |
| `--network <name>`                               | Network name for network-specific validation (`mainnet`, `testnet`, `none`). Default: `mainnet`. (Inherited global.)   |
| `--validate-balances` / `--no-validate-balances` | Enable or disable balance checkpoint validation. Default: enabled.                                                     |
| `--balance-checkpoints <file>`                   | Path to pre-fetched balance checkpoints file (`balance_checkpoints.zstd`).                                             |
| `--custom-balances-dir <dir>`                    | Directory containing custom balance files (`accountBalances_{blockNumber}.pb.gz`).                                     |
| `--balance-check-interval-days <days>`           | Only validate balance checkpoints every N days (default: 30 = monthly).                                                |
| `<files>...`                                     | Block files or directories to validate.                                                                                |

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

- When validating output from the `wrap` command, you can simply pass the output directory as the only parameter. The command will automatically find the `addressBookHistory.json` file in that directory if not explicitly specified.
- When starting from block 0, a `StreamingHasher` is created to validate the historical block hash merkle tree and a balance map is maintained for 50 billion HBAR supply validation. When starting from a later block, both are skipped because the prior state is unavailable.
- Supports both individual block files (nested directories of `.blk.zstd`) and zip archives produced by the `wrap` command.
- Progress is printed every 1000 blocks with an ETA and on the last block.
- If no balance checkpoints are loaded, balance validation is automatically skipped with a warning.

#### Example

```bash
# Validate blocks in a directory (auto-detect address book)
blocks validate /path/to/wrappedBlocks

# Validate with explicit address book
blocks validate -a /path/to/addressBookHistory.json /path/to/blocks/

# Hash chain validation only (skip signatures)
blocks validate --skip-signatures /path/to/blocks/

# Validate with explicit balance checkpoint file
blocks validate --balance-checkpoints data/balance_checkpoints.zstd /path/to/wrappedBlocks

# Validate balances weekly instead of monthly
blocks validate --balance-check-interval-days 7 /path/to/wrappedBlocks

# Skip balance validation for faster runs
blocks validate --no-validate-balances /path/to/wrappedBlocks

# Validate with custom balance files from saved states
blocks validate --custom-balances-dir /path/to/balance_files /path/to/wrappedBlocks
```

#### Testnet Example

```bash
# Validate testnet blocks (uses testnet address book automatically)
blocks validate /path/to/testnetWrappedBlocks --network testnet

# Testnet without balance validation (balance checkpoints not yet available for testnet)
blocks validate --no-validate-balances /path/to/testnetWrappedBlocks --network testnet
```

> **Note:** Balance checkpoint files are not yet available for testnet. Use `--no-validate-balances` to skip balance validation when running against testnet data.

---

### The `repair-zips` Subcommand

Repair corrupt zip central directories (CEN) in wrapped-block directories, then optionally fill any blocks still missing from source day archives. This is a two-phase tool:

- **Phase 1** (always runs): Scans all zip files, detects corrupt or missing central directories, and rebuilds them from intact local file entries.
- **Phase 2** (optional, when `-i` is provided): Re-wraps missing blocks from source day archives and appends them to repaired zips.

#### Usage

```
blocks repair-zips [--scan-threads=<N>] [--repair-threads=<N>] [--buffer-size=<MiB>]
                   [--backup=<dir>] [--dry-run] [-i=<inputDir>] [-b=<blockTimesFile>]
                   [-d=<dayBlocksFile>] <wrappedBlockDir>
```

#### Options

|              Option              |                                             Description                                             |
|----------------------------------|-----------------------------------------------------------------------------------------------------|
| `<wrappedBlockDir>`              | Directory containing wrapped block zip files to scan and repair (required).                         |
| `--scan-threads <N>`             | Number of threads for parallel CEN scan phase (default: 4).                                         |
| `--repair-threads <N>`           | Number of threads for parallel CEN repair phase (default: 4).                                       |
| `--buffer-size <MiB>`            | Per-thread buffer size in MiB for in-memory repair (default: auto-computed from heap size).         |
| `--backup <dir>`                 | Directory to copy corrupt zip files before repairing. If not specified, repairs in-place.           |
| `--dry-run`                      | Scan and report missing blocks without modifying files (Phase 2 only).                              |
| `-i`, `--input-dir <dir>`        | Directory containing source record-file `.tar.zstd` day archives for Phase 2 missing-block filling. |
| `-b`, `--blocktimes-file <file>` | Block-times binary file for mapping block numbers to timestamps (default: `block_times.bin`).       |
| `-d`, `--day-blocks <file>`      | Day-blocks JSON file mapping calendar dates to block numbers (default: `day_blocks.json`).          |

#### Example

```bash
# Phase 1 only: scan and repair corrupt zip CENs
blocks repair-zips /path/to/wrappedBlocks

# Phase 1 + Phase 2: repair CENs and fill missing blocks from day archives
blocks repair-zips -i /path/to/compressedDays /path/to/wrappedBlocks

# Dry run to see what's missing without modifying anything
blocks repair-zips --dry-run -i /path/to/compressedDays /path/to/wrappedBlocks

# Backup corrupt zips before repairing
blocks repair-zips --backup /path/to/backup /path/to/wrappedBlocks
```

#### Notes

- Phase 1 rebuilds the central directory from intact local file entries within each zip.
- Phase 2 requires `-i` (source day archives), `-b` (block times), and `-d` (day blocks) to re-wrap and append missing blocks.
- The `--buffer-size` defaults to approximately 75% of max heap divided by `2 × repairThreads`.

---

### The `wrap` Subcommand

Converts record file blocks organized in daily tar.zstd files into wrapped Block Stream `Block` protobufs. This is a key command in the record-to-block conversion pipeline.

#### Usage

```
blocks wrap [-u] [--network=<network>] [-b=<blockTimesFile>] [-d=<dayBlocksFile>] [-i=<inputDir>] [-o=<outputDir>]
            [--parse-threads=<N>] [--serialize-threads=<N>] [--prefetch=<N>]
```

#### Options

|              Option              |                                                    Description                                                     |
|----------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `-b`, `--blocktimes-file <file>` | BlockTimes file for mapping record file times to blocks (default: `metadata/block_times.bin`).                     |
| `-d`, `--day-blocks <file>`      | Path to the day blocks JSON file (default: `metadata/day_blocks.json`).                                            |
| `--network <name>`               | Network name for applying amendments (`mainnet`, `testnet`, `none`). Default: `mainnet`.                           |
| `-u`, `--unzipped`               | Write output files as individual files in nested directories, rather than in uncompressed zip batches of 10k.      |
| `-i`, `--input-dir <dir>`        | Directory of record file tar.zstd days to process (default: `compressedDays`).                                     |
| `-o`, `--output-dir <dir>`       | Directory to write the output wrapped blocks (default: `wrappedBlocks`).                                           |
| `--parse-threads <N>`            | Thread count for the parse + RSA-verify stage (default: CPU count - 1).                                            |
| `--serialize-threads <N>`        | Thread count for the block serialization + compression stage (default: CPU count - 1).                             |
| `--prefetch <N>`                 | Number of parse+verify futures to keep in-flight ahead of the convert thread (default: same as `--parse-threads`). |

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
- `wrap-commit.bin` - durable commit watermark tracking the highest block number fully written to zip
- `jumpstart.bin` - jumpstart data (block number, block hash, consensus timestamp hash, output items tree root hash, streaming hasher state) for Consensus Node WRB catch-up integrity checks

#### Notes

- The command can resume from where it left off if interrupted. Resume uses a durable watermark (`wrap-commit.bin`); on crash, blocks beyond the watermark are re-processed.
- State checkpoints (merkle trees, address book) are saved monthly and on shutdown.
- A 4-stage concurrent pipeline processes blocks: parse+RSA-verify → convert+chain-state → serialize+compress → zip-write.
- Progress is printed once per consensus-minute with processing speed multiplier and ETA.

#### Example

```bash
# Basic conversion (mainnet default)
blocks wrap -i /path/to/compressedDays -o /path/to/wrappedBlocks

# Output as individual unzipped files
blocks wrap -u -i /path/to/compressedDays -o /path/to/wrappedBlocks
```

#### Testnet Example

```bash
# Wrap testnet record files into blocks
blocks wrap -i /path/to/testnetCompressedDays -o /path/to/testnetWrappedBlocks --network testnet
```

> **Testnet prerequisites:** The testnet genesis address book is bundled as a classpath resource and will be used automatically. You still need `block_times.bin` and `day_blocks.json` generated via `mirror update --network testnet`.

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
- The compiled output file can be used with `validate --balance-checkpoints` for offline validation.
- The `--interval-days` value determines the granularity of validation possible. For example, monthly checkpoints (`--interval-days 30`) only allow monthly validation, not weekly. Choose the fetch interval based on your validation needs.

---

## Debug utilities

Standalone helper scripts that live under [`tools-and-tests/tools/scripts/`](../scripts/) and complement the main CLI subcommands above. Not part of the shaded jar; run directly.

### `extractJumpstartData.py` — inspect `jumpstart.bin` contents

Pretty-prints the binary `jumpstart.bin` that `blocks wrap` writes at the end of each run, so an operator can eyeball what `wrap` recorded, cross-check a `jumpstart.bin` handed to the Consensus Node's WRB catch-up path, or diff two runs of `wrap` against the same source archive.

**Location:** [`tools-and-tests/tools/scripts/extractJumpstartData.py`](../scripts/extractJumpstartData.py)

#### Usage

The script has a shebang and is executable — run it directly:

```bash
# Show help
./tools-and-tests/tools/scripts/extractJumpstartData.py --help

# Point at any jumpstart.bin (path can be relative or absolute)
./tools-and-tests/tools/scripts/extractJumpstartData.py /path/to/wrappedBlocks/jumpstart.bin
```

Only dependency is a standard Python 3.

#### What it prints

`jumpstart.bin` is the binary snapshot `blocks wrap` writes at the end of each run and the Consensus Node consumes for WRB catch-up integrity checks. All fields are big-endian:

|           Field           |         Size         |                                What it is                                 |
|---------------------------|----------------------|---------------------------------------------------------------------------|
| `blockNumber`             | 8 bytes (long)       | Highest wrapped block                                                     |
| `blockHash`               | 48 bytes             | SHA-384 chain hash of that block                                          |
| `consensusTimestampHash`  | 48 bytes             | SHA-384 leaf hash of the block's first consensus timestamp                |
| `outputItemsTreeRootHash` | 48 bytes             | Streaming merkle root of all output items                                 |
| `leafCount`               | 8 bytes (long)       | Number of leaves in the streaming hasher                                  |
| `hashCount`               | 4 bytes (int)        | Number of 48-byte hashes that follow (streaming hasher's open-root state) |
| `hashes[]`                | 48 × hashCount bytes | Streaming hasher's internal node hashes                                   |

#### Sample output

Healthy dump from a completed full-mainnet wrap run:

```
$ ./tools-and-tests/tools/scripts/extractJumpstartData.py wrappedBlocks/jumpstart.bin
blockNumber: 98236963
blockHash: 952f122a53d9dafafc6247c3dac7096a8e1943020dbe29862f14ca9130f3af310ca483b45289ca8ffc5b4cabf0a02200
consensusTimestampHash: 04082995241115f9b7e81c29b68fe6f09da9acfd32855bd4ff3a760728382fc3564b27ef4cde26a4c24a76f89aa39f02
outputItemsTreeRootHash: 43ffaec8c07e038f7127c15e8931b7b55baaee8d491491545fda76c90e7539d7cac3e0a51c565395e0baa468394f6518
leafCount: 98236964
hashCount: 15
  hash[0]:  03af2fb881bbadddcf592a9daaecceb32f2e8a687acb9cddeb8f188e0950e6ae221c322be6448f8371350f04dc785da8
  ...
  hash[14]: ddefc67134cc0f7cf807474b8fd82b964cd87595320c3ee21e89b672fecb4228913197fd4c931fb2055512f27e6f2f9a
```

#### What to sanity-check in the output

- **`leafCount == blockNumber + 1`** — blocks are 0-indexed, so a run that finished at block N shows `leafCount = N + 1`. If they don't line up, the wrap watermark and the streaming hasher are out of sync.
- **`hashCount == popcount(leafCount)`** — the streaming merkle tree keeps one open root per set bit in `leafCount`. In the sample above, `leafCount=98236964` has 15 set bits in binary, so `hashCount=15`. A mismatch indicates streaming-hasher state corruption.
- **`blockHash` matches the last entry in `blockStreamBlockHashes.bin`** — if not, either `jumpstart.bin` was written from a different wrap run than the block registry, or one of the two files was manually edited. Cross-check with `blocks validate`'s [`HashRegistryValidation`](#hashregistryvalidation).
- **All hashes are non-zero** — 48 all-zero bytes anywhere in the output means the writer never populated that field, which shouldn't happen for a completed wrap.

#### When to reach for it

- **Debug a wrap run**: compare `jumpstart.bin` between a completed run and an interrupted run to see whether the wrap watermark and streaming hasher are consistent with the last block committed to zip.
- **Verify a jumpstart file handed to CN**: before uploading a `jumpstart.bin` to the Consensus Node for WRB catch-up, dump it and confirm the block number + block hash match what you expect for that mainnet slice.
- **Cross-check re-wraps**: run `wrap` twice on the same source archive with the same fix branch — the resulting `jumpstart.bin` files should be byte-identical. Diffing the pretty-printed outputs is easier than diffing binary.
- **Investigate a validation failure**: if [`JumpstartValidation`](#jumpstartvalidation) (part of `blocks validate`) reports a mismatch, dump the on-disk `jumpstart.bin` to see which field diverged from the freshly-computed values.

#### Related

- Produced by `blocks wrap` (see [The `wrap` Subcommand](#the-wrap-subcommand))
- Consumed by [`JumpstartValidation`](#jumpstartvalidation) as an end-of-run check inside `blocks validate`
- Consumed by Consensus Node for WRB catch-up integrity checks
