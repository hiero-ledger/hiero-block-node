## Days Subcommands

The `days` top-level command works with compressed daily record file archives. Day files are `*.tar.zstd` archives containing directories of record files and related files in chronological order.

Available subcommands:
- `ls` - List record file sets contained in `.tar.zstd` files or directories
- `validate` - Validate blockchain running hash across day archives
- `compress` - Compress day directories into `.tar.zstd` archives
- `download-day` - Download all record files for a specific day (v1 implementation)
- `download-days` - Download many days (v1)
- `download-days-v2` - Download many days (v2, newer implementation)
- `print-listing` - Print the listing for a given day from listing files
- `ls-day-listing` - Print all files in the listing for a day
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

### `wrap`

Convert record file blocks contained in day archives into wrapped block stream `Block` protobufs and write them out either as unzipped compressed files or grouped zipped batches via the `BlockWriter`.

Usage:

```
days wrap [-w <warningsFile>] [-u] <compressedDaysDir> <outputBlocksDir>
```

Options:
- `-w`, `--warnings-file <file>` — Write warnings to this file.
- `-u`, `--unzipped` — Write output files unzipped (ZSTD per block) instead of as zipped batches.

