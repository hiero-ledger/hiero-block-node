# Blocks Subcommands

The `blocks` command contains utilities for working with Block Stream files (.blk, .blk.gz, .blk.zstd).

### Available Subcommands

|  Command   |                                    Description                                    |
|------------|-----------------------------------------------------------------------------------|
| `json`     | Converts a binary Block Stream to JSON                                            |
| `ls`       | Prints info for block files (supports .blk, .blk.gz, .blk.zstd, and zip archives) |
| `validate` | Validates a wrapped Block Stream (hash chain and signatures)                      |
| `wrap`     | Convert record file blocks in day files to wrapped Block Stream blocks            |

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

### The `wrap` Subcommand

Converts record file blocks organized in daily tar.zstd files into wrapped Block Stream `Block` protobufs. This is a key command in the record-to-block conversion pipeline.

#### Usage

```
blocks wrap [-u] [-b=<blockTimesFile>] [-d=<dayBlocksFile>] [-i=<inputDir>] [-o=<outputDir>]
```

#### Options

|              Option              |                                                  Description                                                  |
|----------------------------------|---------------------------------------------------------------------------------------------------------------|
| `-b`, `--blocktimes-file <file>` | BlockTimes file for mapping record file times to blocks (default: `metadata/block_times.bin`).                |
| `-d`, `--day-blocks <file>`      | Path to the day blocks JSON file (default: `metadata/day_blocks.json`).                                       |
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
