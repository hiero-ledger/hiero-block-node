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

Validates wrapped block stream files produced by the `wrap` command. Walks all blocks in the input directory in order and runs a suite of individual validations against each block plus a set of end-of-run state file checks.

At a high level:

- **Hash chain continuity** — each block's `previousBlockRootHash` in the footer matches the computed hash of the preceding block.
- **Genesis block** — first block has 48 zero bytes for previous hash.
- **Historical block tree root** — the `rootHashOfAllBlockHashesTree` in the footer matches the expected merkle tree root computed from all preceding block hashes (only when starting from block 0).
- **Required items** — every block contains at least one `BlockHeader`, `RecordFile`, `BlockFooter`, and `BlockProof`.
- **Item ordering** — items appear in the correct order: `BlockHeader`, optional `StateChanges`, `RecordFile`, `BlockFooter`, one or more `BlockProof` items, with no duplicates or misplaced items.
- **Signature validation** — stake-weighted RSA (or TSS when applicable) signature threshold met.
- **50 billion HBAR supply** — tracks account balances across all blocks (from `StateChanges` and `RecordFile` transfer lists) and verifies the total equals exactly 50 billion HBAR after each block (only when starting from block 0).
- **Balance checkpoints** — validates computed account balances against pre-fetched balance checkpoints at configurable intervals.
- **State file integrity** — end-of-run comparison of the block-hash registry, streaming merkle tree, and jumpstart state files against freshly-computed values.

For a complete catalog of the individual validations that fire under the hood (what each one checks, whether it runs per block or at end, and how to skip it), see [Validations in detail](#validations-in-detail) below.

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

#### Validations in detail

The `validate` command orchestrates a set of individual validation classes under the hood. Some are stateless and run in parallel (one instance per block, no cross-block dependency); others are sequential (need prior blocks or accumulated state and run in strict block order). A final group runs only once, at the end of the run, to check state files against freshly-recomputed values.

##### Summary table

|                     Validation                     |    Type    |   Genesis-only?   |             How to skip             |                                       What it checks                                       |
|----------------------------------------------------|------------|-------------------|-------------------------------------|--------------------------------------------------------------------------------------------|
| [RequiredItemsValidation](#requireditemsvalidation) | Parallel   | No                | `--skip-required-items`             | Every block has ≥1 BlockHeader, RecordFile, BlockFooter, BlockProof                        |
| [BlockStructureValidation](#blockstructurevalidation) | Parallel | No                | `--skip-required-items`             | Item ordering: BlockHeader, StateChanges\*, RecordFile, BlockFooter, BlockProof+           |
| [SignatureValidation](#signaturevalidation)         | Parallel   | No                | `--skip-signatures`                 | RSA (SignedRecordFileProof) or non-empty TSS (SignedBlockProof) signature threshold        |
| [AddressBookUpdateValidation](#addressbookupdatevalidation) | Sequential | No        | Always on                           | Discovers CN address-book updates from block data, keeps registry current                  |
| [NodeStakeUpdateValidation](#nodestakeupdatevalidation) | Sequential | No             | Always on                           | Discovers `NodeStakeUpdate` transactions, keeps stake registry current                     |
| [TssEnablementValidation](#tssenablementvalidation) | Sequential | No                | Always on                           | Discovers `LedgerIdPublication` transactions and writes `tss-enablement.bin`               |
| [BlockChainValidation](#blockchainvalidation)       | Sequential | No                | Always on                           | `previous_block_hash` in footer matches hash of prior block                                |
| [HistoricalBlockTreeValidation](#historicalblocktreevalidation) | Sequential | Yes       | Always on (auto-skipped otherwise)  | `root_hash_of_block_hashes_merkle_tree` in footer matches streaming merkle tree            |
| [HbarSupplyValidation](#hbarsupplyvalidation)       | Sequential | Yes               | `--skip-supply`                     | Total HBAR supply = 50 billion after every block                                           |
| [BalanceCheckpointValidation](#balancecheckpointvalidation) | Sequential | Yes       | `--no-validate-balances`            | Computed balances match pre-fetched checkpoint snapshots at configurable intervals         |
| [HashRegistryValidation](#hashregistryvalidation)   | Sequential | No                | Auto-skipped if no registry file    | Per-block hash matches the `blockStreamBlockHashes.bin` registry                           |
| [StreamingMerkleTreeValidation](#streamingmerkletreevalidation) | End-of-run | Yes     | Auto-skipped when not from genesis  | `streamingMerkleTree.bin` matches freshly-computed streaming hasher                        |
| [JumpstartValidation](#jumpstartvalidation)         | End-of-run | Yes               | Auto-skipped when not from genesis  | `jumpstart.bin` matches freshly-computed streaming hasher + block hashes                   |

"Genesis-only" validations require starting from block 0 because they depend on accumulated state (block hash history, running HBAR balances, streaming merkle tree). They're transparently disabled when validation resumes from a checkpoint or starts mid-stream.

##### RequiredItemsValidation

**Type:** Parallel · **Skip:** `--skip-required-items` (together with `BlockStructureValidation`)

Confirms every wrapped block contains at least one instance of each required item type: `BlockHeader`, `RecordFile`, `BlockFooter`, and `BlockProof`. A missing required item usually indicates a truncated or corrupted block file.

**Example failure:**

```
Block 12345 missing required BlockProof item
```

**When to skip:** validating live-stream blocks that don't yet carry `RecordFile` / `BlockProof` items (e.g., streaming ingest scenarios).

##### BlockStructureValidation

**Type:** Parallel · **Skip:** `--skip-required-items` (together with `RequiredItemsValidation`)

Enforces the canonical item ordering inside a wrapped block:

1. Exactly one `BlockHeader`
2. Zero or more `StateChanges` (genesis amendments for block 0)
3. Exactly one `RecordFile`
4. Exactly one `BlockFooter`
5. One or more `BlockProof` items

Duplicated or out-of-order items fail this check.

**When to skip:** same as `RequiredItemsValidation` above.

##### SignatureValidation

**Type:** Parallel · **Skip:** `--skip-signatures`

Verifies the cryptographic signatures over the block's signed payload:

- For `SignedRecordFileProof` (record-file-era WRBs): RSA signatures from CN nodes. Stake-weighted consensus is used when stake data is available (verified stake must be `>= ceil(totalStake / 3)`). When stake data is unavailable (pre-staking era, before roughly July 2022), it falls back to equal-weight mode where each node counts as 1 and threshold is `(nodeCount / 3) + 1`.
- For `SignedBlockProof` (post-block-stream): non-empty TSS signature check.

Stateless; each block is verified independently.

**When to skip:** operating on data known to have stale signatures, or when only hash chain / state file continuity is being verified.

##### AddressBookUpdateValidation

**Type:** Sequential · **Skip:** always on

Discovers CN address-book updates from `RecordFile` items (file update / append transactions targeting file `0.0.102`) and keeps the in-memory `AddressBookRegistry` current. This lets `validate` proceed without a pre-generated `addressBookHistory.json` covering the full block range.

Failures here typically manifest downstream as `SignatureValidation` failures once the registry falls out of sync with what a block signed.

##### NodeStakeUpdateValidation

**Type:** Sequential · **Skip:** always on

Discovers `NodeStakeUpdate` transactions (issued daily at 00:00 UTC) and updates the `NodeStakeRegistry`. Downstream, `SignatureValidation` reads these stakes for stake-weighted consensus.

##### TssEnablementValidation

**Type:** Sequential · **Skip:** always on

Watches for `LedgerIdPublication` transactions. When one is found, the raw protobuf is written to `tss-enablement.bin` (and a companion `tss-bootstrap-roster.json`) so the block node's `VerificationServicePlugin` can consume them directly.

##### BlockChainValidation

**Type:** Sequential · **Skip:** always on (foundational; other validations depend on it)

Compares each block's footer field `previousBlockRootHash` to the hash of the previously-committed block. On the first block validated (when no prior hash is yet known), the chain check is skipped for that block only.

**Example failure:**

```
Block 12345 previousBlockRootHash mismatch:
  expected: <hex of block 12344's computed hash>
  actual:   <hex from block 12345's footer>
```

Other validations (`HashRegistryValidation`, `HistoricalBlockTreeValidation`) piggyback on the computed hash from this validation to avoid recomputation.

##### HistoricalBlockTreeValidation

**Type:** Sequential · **Skip:** always on (auto-skipped when not starting from block 0)

Maintains a `StreamingHasher` over the sequence of block hashes and verifies the `root_hash_of_block_hashes_merkle_tree` field in each footer matches the running merkle root before the current block's hash is folded in.

Requires starting from block 0 because the full block hash history is needed to reconstruct the correct merkle tree. When validation is resumed from a checkpoint or started mid-stream, this validation is transparently skipped and a message is printed to that effect.

##### HbarSupplyValidation

**Type:** Sequential · **Skip:** `--skip-supply`, auto-skipped when not starting from block 0

Tracks account balances via two sources within each block:

1. `StateChanges` items set absolute HBAR balances or delete accounts.
2. `RecordFile` items apply relative balance changes via transfer lists.

After every block, the sum of all account balances must equal exactly 50,000,000,000 HBAR (in tinybar). Any deviation fails the block.

**When to skip:** networks with known transfer-list imbalances (dev / test networks that started with non-standard supply).

##### BalanceCheckpointValidation

**Type:** Sequential · **Skip:** `--no-validate-balances`, auto-skipped when not starting from block 0 or when no checkpoint file is loaded

At configured checkpoint block numbers, snapshots the computed account state and compares each account's balance to a pre-fetched balance file (from a saved state or the compiled checkpoints file). See the [Balance Validation](#balance-validation) section above for how the checkpoint file, interval, and sources are configured.

Every checkpoint that fails records a per-account mismatch summary.

##### HashRegistryValidation

**Type:** Sequential · **Skip:** auto-skipped when no registry file is available

Compares each block's computed hash (provided by `BlockChainValidation`) against the value stored in `blockStreamBlockHashes.bin` (produced by `wrap`). A mismatch means the registry was written from a different set of bytes than what `validate` is now computing — usually a symptom of an interrupted or forked wrap run.

##### StreamingMerkleTreeValidation

**Type:** End-of-run · **Skip:** auto-skipped when not starting from block 0

Runs once, in `finalize()`, after every block has been validated. Compares the on-disk `streamingMerkleTree.bin` file to the streaming hasher state that `HistoricalBlockTreeValidation` built up during the run.

A mismatch indicates the state file was checkpointed from a wrap run that saw a different sequence of block hashes than this validation.

##### JumpstartValidation

**Type:** End-of-run · **Skip:** auto-skipped when not starting from block 0

Runs once, in `finalize()`, after every block. Reads `jumpstart.bin` and confirms its fields (block number, block hash, consensus timestamp hash, output items tree root hash, streaming hasher state) match what was freshly computed during this validation run.

`jumpstart.bin` is what the Consensus Node consumes for WRB catch-up integrity checks, so any mismatch here would indicate the WRB archive is inconsistent with the state that CN expects.

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
