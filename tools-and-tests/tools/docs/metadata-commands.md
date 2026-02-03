# Metadata Subcommands

The `metadata` command works with metadata files in the `metadata` directory. These files are used by the record-to-block conversion pipeline and other tools.

## Available Subcommands

| Command  |                                 Description                                 |
|----------|-----------------------------------------------------------------------------|
| `ls`     | Display summary information about metadata files                            |
| `update` | Update all metadata files (block_times.bin, day_blocks.json, listingsByDay) |

---

### Metadata Files Overview

The metadata directory contains several important files:

|       File        |                               Description                               |
|-------------------|-------------------------------------------------------------------------|
| `block_times.bin` | Binary file mapping block numbers to timestamps (array of 64-bit longs) |
| `day_blocks.json` | JSON file with per-day block info (first/last block numbers per day)    |
| `listingsByDay/`  | Directory containing daily bucket listing files (YYYY/MM/DD.bin format) |

---

### `ls`

Display summary information about metadata files. Shows status, date ranges, and validation results for each metadata file.

#### Usage

```bash
metadata ls [-d=<metadataDir>]
```

#### Options

|       Option        |                       Description                        |
|---------------------|----------------------------------------------------------|
| `-d`, `--dir <dir>` | Base directory for metadata files (default: `metadata`). |

#### Output

The command displays:

**block_times.bin:**
- File status (exists/not found)
- Block range (first and last block numbers with timestamps)
- Total block count

**day_blocks.json:**
- File status (exists/not found)
- Date range (first and last days with block ranges)
- Total day count
- Continuity validation (checks for gaps between days)

**listingsByDay:**
- Directory status (exists/not found)
- Date range (first and last listing files)
- Total day count
- Completeness check (identifies missing days)

#### Example

```bash
# Display metadata summary
metadata ls

# Specify custom directory
metadata ls -d /path/to/metadata
```

#### Example Output

```
════════════════════════════════════════════════════════════
  METADATA FILES SUMMARY
════════════════════════════════════════════════════════════

▶ block_times.bin
──────────────────────────────────────────────────
  ✓ File exists

  Block Range:
    First Block: 0 at 2019-09-13T21:55:18.004846002
    Last Block:  89,000,000 at 2026-01-15T12:34:56.123456789
    Total Blocks: 89,000,001

▶ day_blocks.json
──────────────────────────────────────────────────
  ✓ File exists

  Date Range:
    First Day: 2019-09-13 (blocks 0 - 1,234)
    Last Day:  2026-01-15 (blocks 88,998,765 - 89,000,000)
    Total Days: 2,316

  Validation:
    ✓ All blocks are continuous (no gaps)

▶ listingsByDay
──────────────────────────────────────────────────
  ✓ Directory exists

  Date Range:
    First Day: 2019-09-13
    Last Day:  2026-01-15
    Total Days: 2,316

  Completeness:
    ✓ Complete (no missing days)
```

---

### `update`

Update all metadata files by fetching the latest data from Mirror Node and GCS bucket.

This command updates:
- `block_times.bin` and `day_blocks.json` from Mirror Node
- `listingsByDay` directory from GCS bucket

#### Usage

```
metadata update [options]
```

#### Options

|              Option              |                                  Description                                  |
|----------------------------------|-------------------------------------------------------------------------------|
| `--block-times <file>`           | Path to block times binary file (default: `metadata/block_times.bin`).        |
| `--day-blocks <file>`            | Path to day blocks JSON file (default: `metadata/day_blocks.json`).           |
| `-l`, `--listing-dir <dir>`      | Directory where listing files are stored (default: `metadata/listingsByDay`). |
| `-c`, `--cache-dir <dir>`        | Directory for GCS cache (default: `data/gcp-cache`).                          |
| `--cache`                        | Enable GCS caching (default: false).                                          |
| `--min-node <n>`                 | Minimum node account ID (default: 3).                                         |
| `--max-node <n>`                 | Maximum node account ID (default: 37).                                        |
| `-p`, `--user-project <project>` | GCP project to bill for requester-pays bucket access.                         |
| `--skip-mirror-node`             | Skip updating from Mirror Node (block_times.bin, day_blocks.json).            |
| `--skip-listings`                | Skip updating day listings from GCS.                                          |

#### Process

1. **Step 1: Mirror Node Data** - Updates block_times.bin and day_blocks.json with the latest block information from the Mirror Node.

2. **Step 2: Day Listings** - Updates listingsByDay directory by downloading bucket metadata from GCS.

#### Example

```bash
# Update all metadata
metadata update

# Update with GCP project for requester-pays
metadata update -p my-gcp-project

# Only update Mirror Node data (skip listings)
metadata update --skip-listings

# Only update listings (skip Mirror Node data)
metadata update --skip-mirror-node

# Enable caching for GCS downloads
metadata update --cache -c /path/to/cache
```

#### Notes

- Requires Google Cloud authentication for GCS access
- Mirror Node updates require network connectivity to the public Mirror Node API
- The update process is incremental - only new data since the last update is fetched

---

## Related Commands

Metadata files are used by other commands in the tools:

- [`blocks wrap`](blocks-commands.md#the-wrap-subcommand) - Uses block_times.bin and day_blocks.json for conversion
- [`days download-*`](days-commands.md) - Uses listingsByDay for downloading record files
- [`mirror extractBlockTimes`](mirror-node-commands.md#extractblocktimes) - Creates block_times.bin
- [`mirror extractDayBlock`](mirror-node-commands.md#extractdayblock) - Creates day_blocks.json
