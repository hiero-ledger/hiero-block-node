## Blocks Subcommands

The `blocks` command contains utilities for working with block stream files.

- `json` - Converts a binary Block Stream to JSON
- `info` - Prints info for block or record files

---

### The `json` Subcommand

Converts a binary Block Stream (.blk or .blk.gz) to JSON files placed next to the input files.

Usage:

```
blocks json [-t] [-ms=<minSizeMb>] [<files>...]
```

Options:
- `-t`, `--transactions`
- Expand transactions (replaces applicationTransaction base64 fields with parsed TransactionBody JSON). Useful for human-readable output.
- `-ms <minSizeMb>`, `--min-size=<minSizeMb>`
- Filter to only files bigger than this minimum size in megabytes. Default is no limit.
- `<files>...`
- Files or directories to convert. Directories are walked and files with `.blk` or `.blk.gz` are processed.

Notes:
- The command reads block protobufs and writes a `.json` file next to each source file.

---

### The `info` Subcommand

Prints info for block stream or record files. Supports `.blk`, `.blk.gz`, `.rcd` and `.rcd.gz` files.

Usage:

```
blocks info [-c] [-ms=<minSizeMb>] [-o=<outputFile>] [<files>...]
```

Options:
- `-c`, `--csv`
- Enable CSV output mode (default: false).
- `-ms <minSizeMb>`, `--min-size=<minSizeMb>`
- Filter to only files bigger than this minimum file size in megabytes.
- `-o <outputFile>`, `--output-file=<outputFile>`
- Write output to the specified file instead of stdout.
- `<files>...`
- Files or directories to inspect. For record files (`.rcd` / `.rcd.gz`) the tool prints parsed metadata. For block files it delegates to the Blocks info implementation.
