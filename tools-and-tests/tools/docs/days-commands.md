## Days Subcommands

The `days` top-level command works with compressed daily record file archives. Day files are `*.tar.zstd` archives containing directories of record files and related files in chronological order.

Available subcommands:
- `ls` - List record file sets contained in `.tar.zstd` files or directories
- `validate` - Validate blockchain running hash across day archives
- `compress` - Compress day directories into `.tar.zstd` archives
- `download-day` - Download all record files for a specific day (v1 implementation)
- `download-days` - Download many days (v1)
- `download-days-v2` - Download many days (v2, newer implementation)
- `download-live2` - Live block download with inline validation and automatic day rollover
- `print-listing` - Print the listing for a given day from listing files
- `ls-day-listing` - Print all files in the listing for a day
- `split-files-listing` - Split a giant JSON listing (files.json) into per-day binary listing files
- `wrap` - Convert record file blocks in day files into wrapped block stream blocks

Below are brief descriptions and usage for each.

---

### `ls` (days)

List record file sets inside one or more `.tar.zstd` day files.

Usage:

```
days ls <files-or-dirs>...
```

Options:
- `<files-or-dirs>...` — One or more `.tar.zstd` files or directories containing day archives.

Output:
- Prints a `RecordFileBlock` style summary line per block contained in the provided day files.

---

### `validate`

Validate blockchain running hashes across day files. Reads day archives, recomputes/validates running hashes and can persist a resume status so a long run can resume after interruption.

Usage:

```
days validate <compressedDaysDir> [-w <warningsFile>]
```

Options:
- `<compressedDaysDir>` — Directory containing `.tar.zstd` day files.
- `-w`, `--warnings-file <file>` — Write warnings to this file instead of only printing them.

Notes:
- The command will attempt to load prior-day mirror metadata (if available) to initialize carry-over hash for validation. It writes/reads `validateCmdStatus.json` inside the provided `compressedDaysDir` to allow resuming.

---

### `compress`

Compress one or more day directories (like `2019-09-13`) into `YYYY-MM-DD.tar.zstd`, preserving relative paths and ensuring files in archive are in ascending time order.

Usage:

```
days compress -o <outputDir> [-c <level>] <day-dir>...
```

Options:
- `-o`, `--output-dir <outputDir>` — Directory where compressed files are written. Required.
- `-c`, `--compression-level <level>` — zstd compression level (1..22). Default: 6.

---

### `download-day` (v1)

Download all record files for a specific day using listing files and GCP. This is the v1 single-day downloader.

Usage:

```
days download-day [-l <listingDir>] [-d <downloadedDaysDir>] [-t <threads>] <year> <month> <day>
```

Options:
- `-l`, `--listing-dir <listingDir>` — Directory where listing files are stored (default: `listingsByDay`).
- `-d`, `--downloaded-days-dir <downloadedDaysDir>` — Directory where downloaded days are stored (default: `compressedDays`).
- `-t`, `--threads <threads>` — Number of parallel downloads (default: number of available processors).

---

### `download-days` (v1)

Download record files for a date range using the v1 downloader (sequentially iterates days and calls `download-day`).

Usage:

```
days download-days [-l <listingDir>] [-d <downloadedDaysDir>] [-t <threads>] <fromYear> <fromMonth> <fromDay> <toYear> <toMonth> <toDay>
```

Options:
- Same options as `download-day`. Range defaults to/from `2019-09-13` → today if omitted where applicable.

---

### `download-days-v2`

A newer download implementation which uses a concurrent download manager and integrates with block time reader and day block info maps.

Usage:

```
days download-days-v2 [-l <listingDir>] [-d <downloadedDaysDir>] [-t <threads>] <fromYear> <fromMonth> <fromDay> <toYear> <toMonth> <toDay>
```

Options:
- `-t`, `--threads` — Initial concurrency (default: 64 in code). Other options are similar to v1.

Notes:
- Both download commands access public GCP storage (requester pays) and require Google Cloud authentication and a project to be set for requester pays. See mirror-related notes below.

---

### `download-live2`

Live block download with inline validation, signature statistics, and automatic day rollover. Downloads blocks in real-time, validates them, and writes to per-day `.tar.zstd` archives.

Usage:

```
days download-live2 [-l <listingDir>] [-o <outputDir>] [--start-date <YYYY-MM-DD>] [--max-concurrency <n>]
```

Options:
- `-l`, `--listing-dir <listingDir>` — Directory where listing files are stored (default: `listingsByDay`).
- `-o`, `--output-dir <outputDir>` — Directory where compressed day archives are written (default: `compressedDays`).
- `--start-date <YYYY-MM-DD>` — Start date (default: auto-detect from mirror node).
- `--state-json <path>` — Path to state JSON file for resume (default: `outputDir/validateCmdStatus.json`).
- `--stats-csv <path>` — Path to signature statistics CSV file (default: `outputDir/signature_statistics.csv`).
- `--address-book <path>` — Path to address book file for signature validation.
- `--max-concurrency <n>` — Maximum concurrent downloads (default: 64).

Features:
- **Auto-detect start date**: Queries the mirror node to determine the current day if `--start-date` is not specified.
- **HTTP transport**: Uses HTTP (not gRPC) for GCS downloads to avoid deadlock issues with virtual threads.
- **Inline validation**: Validates each block's running hash as it's downloaded.
- **Signature statistics**: Tracks per-day signature counts and writes to CSV (compatible with `validate-with-stats`).
- **Day rollover**: Automatically finalizes day archives at midnight and starts new ones.
- **Resume support**: Saves state periodically to allow resuming after interruption.

Example:

```bash
# Start from a specific date
java -jar tools-all.jar days download-live2 \
  -l /path/to/listingsByDay \
  -o /path/to/compressedDays \
  --start-date 2026-01-09 \
  --max-concurrency 64

# Auto-detect today and run continuously
java -jar tools-all.jar days download-live2 \
  -l /path/to/listingsByDay \
  -o /path/to/compressedDays
```

#### State File Behavior

The command writes its state to `validateCmdStatus.json` in the output directory. This file is shared with the `validate` and `validate-with-stats` commands. Important notes:

|                  Scenario                  |                                          Behavior                                           |
|--------------------------------------------|---------------------------------------------------------------------------------------------|
| `download-live2` writes → `validate` reads | ✅ Works (validate ignores extra `blockNumber` field)                                        |
| `validate` writes → `download-live2` reads | ⚠️ `blockNumber` will be 0, so `download-live2` falls back to `--start-date` or auto-detect |

**If you run `validate` or `validate-with-stats` after `download-live2`**, the state file will be overwritten without the `blockNumber` field. When `download-live2` starts again, it will need `--start-date` to resume from the correct position, or it will auto-detect the current day from the mirror node.

---

### `print-listing`

Prints a curated listing for a single day from listing files created by the download process.

Usage:

```
days print-listing [-l <listingDir>] <year> <month> <day>
```

Options:
- `-l`, `--listing-dir <listingDir>` — Directory where daily listing files are stored (default: `listingsByDay`).

---

### `ls-day-listing`

Print all files in the listing for a day grouped by block timestamp. Useful for inspecting which record and sidecar files were recorded for a given block time.

Usage:

```
days ls-day-listing [-l <listingDir>] <year> <month> <day>
```

Options:
- `-l`, `--listing-dir <listingDir>` — Directory where listing files are stored (default: `listingsByDay`).

---

### `split-files-listing`

Split a giant JSON listing (for example the output of `rclone lsjson`) into per-day binary listing files. The command parses the JSON listing, creates RecordFile objects for each entry, and writes them into day-specific binary files using the `DayListingFileWriter`.

Usage:

```
days split-files-listing [-l <listingDir>] <files.json>
```

Options:
- `-l`, `--listing-dir <listingDir>` — Directory where listing files are stored (default: `listingsByDay`).
- `<files.json>` — The JSON listing file to read (default: `files.json`).

Notes:
- The command expects the JSON to be in the same format produced by `rclone lsjson`.
- It will create binary `.bin` day files under the provided listing directory, one file per UTC day.
- The operation can be very slow for large listings. The implementation's Javadoc notes that generating the JSON with rclone for the whole mirror took approximately two weeks in one environment; plan accordingly.

Example rclone command used to generate the JSON listing (from the command Javadoc):

```
nohup rclone lsjson -R --hash --no-mimetype --no-modtime --gcs-user-project <PROJECT> \
    "gcp:hedera-mainnet-streams/recordstreams" > files.json &
```

---

### `wrap`

Convert record file blocks contained in day archives into wrapped block stream `Block` protobufs and write them out either as unzipped compressed files or grouped zipped batches via the `BlockWriter`.

Usage:

```
days wrap [-w <warningsFile>] [-u] <compressedDaysDir> <outputBlocksDir>
```

Options:
- `-w`, `--warnings-file <file>` — Write warnings to this file.
- `-u`, `--unzipped` — Write output files unzipped (ZSTD per block) instead of as zipped batches.
