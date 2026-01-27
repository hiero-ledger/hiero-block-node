# Days Subcommands

The `days` top-level command works with compressed daily record file archives. Day files are `*.tar.zstd` archives containing directories of record files and related files in chronological order.

### Available Subcommands

|         Command          |                              Description                              |
|--------------------------|-----------------------------------------------------------------------|
| `ls`                     | List record file sets contained in `.tar.zstd` files or directories   |
| `validate`               | Validate blockchain running hash across day archives                  |
| `validate-with-stats`    | Validate blockchain with signature statistics per node                |
| `validate-sig-counts`    | Validate that all blocks have signatures from all expected nodes      |
| `compress`               | Compress day directories into `.tar.zstd` archives                    |
| `download-day`           | Download all record files for a specific day (v1)                     |
| `download-days`          | Download many days (v1)                                               |
| `download-days-v2`       | Download many days (v2 implementation)                                |
| `download-days-v3`       | Download many days (v3 with Guava preload fix)                        |
| `download-live`          | Continuously follow Mirror Node for new block files                   |
| `download-live2`         | Live block download with inline validation and automatic day rollover |
| `print-listing`          | Print the listing for a given day                                     |
| `ls-day-listing`         | Print all files in the listing for a day                              |
| `split-files-listing`    | Split a giant JSON listing into per-day binary listing files          |
| `clean`                  | Clean bad record sets from day `.tar.zstd` files                      |
| `updateDayListings`      | Update day listing files by downloading metadata from GCS bucket      |
| `fix-missing-signatures` | Fix missing signatures in downloaded days                             |
| `fix-sig-names`          | Fix signature file names by renaming `.rcs.sig` to `.rcd.sig`         |

---

### `ls`

List record file sets inside one or more `.tar.zstd` day files.

#### Usage

```
days ls <files-or-dirs>...
```

#### Options

|        Option        |                              Description                              |
|----------------------|-----------------------------------------------------------------------|
| `<files-or-dirs>...` | One or more `.tar.zstd` files or directories containing day archives. |

#### Output

Prints a `RecordFileBlock` style summary line per block contained in the provided day files.

---

### `validate`

Validate blockchain running hashes across day files. Reads day archives, recomputes/validates running hashes and can persist a resume status so a long run can resume after interruption.

#### Usage

```
days validate <compressedDaysDir> [-w <warningsFile>]
```

#### Options

|             Option             |                        Description                         |
|--------------------------------|------------------------------------------------------------|
| `<compressedDaysDir>`          | Directory containing `.tar.zstd` day files.                |
| `-w`, `--warnings-file <file>` | Write warnings to this file instead of only printing them. |

#### Notes

- The command will attempt to load prior-day mirror metadata (if available) to initialize carry-over hash for validation.
- It writes/reads `validateCmdStatus.json` inside the provided `compressedDaysDir` to allow resuming.

---

### `validate-with-stats`

Validate blockchain with signature statistics per node. Similar to `validate` but also tracks and reports per-node signature counts.

#### Usage

```
days validate-with-stats [options] <compressedDaysDir>
```

#### Options

|            Option             |                 Description                 |
|-------------------------------|---------------------------------------------|
| `<compressedDaysDir>`         | Directory containing `.tar.zstd` day files. |
| `-a`, `--address-book <file>` | Path to address book history JSON file.     |
| `-o`, `--output <file>`       | Path to output statistics CSV file.         |

---

### `validate-sig-counts`

Validate that all blocks have signatures from all expected nodes.

#### Usage

```
days validate-sig-counts [options] <compressedDaysDir>
```

#### Options

|            Option             |                 Description                 |
|-------------------------------|---------------------------------------------|
| `<compressedDaysDir>`         | Directory containing `.tar.zstd` day files. |
| `-a`, `--address-book <file>` | Path to address book history JSON file.     |

---

### `compress`

Compress one or more day directories (like `2019-09-13`) into `YYYY-MM-DD.tar.zstd`, preserving relative paths and ensuring files in archive are in ascending time order.

#### Usage

```
days compress -o <outputDir> [-c <level>] <day-dir>...
```

#### Options

|               Option                |                       Description                       |
|-------------------------------------|---------------------------------------------------------|
| `-o`, `--output-dir <outputDir>`    | Directory where compressed files are written. Required. |
| `-c`, `--compression-level <level>` | zstd compression level (1..22). Default: 6.             |
| `<day-dir>...`                      | Day directories to compress.                            |

---

### `download-day`

Download all record files for a specific day using listing files and GCP. This is the v1 single-day downloader.

#### Usage

```
days download-day [-l <listingDir>] [-d <downloadedDaysDir>] [-t <threads>] <year> <month> <day>
```

#### Options

|                      Option                       |                               Description                               |
|---------------------------------------------------|-------------------------------------------------------------------------|
| `-l`, `--listing-dir <listingDir>`                | Directory where listing files are stored (default: `listingsByDay`).    |
| `-d`, `--downloaded-days-dir <downloadedDaysDir>` | Directory where downloaded days are stored (default: `compressedDays`). |
| `-t`, `--threads <threads>`                       | Number of parallel downloads (default: number of available processors). |
| `<year> <month> <day>`                            | Date to download (e.g., `2024 1 15`).                                   |

---

### `download-days`

Download record files for a date range using the v1 downloader (sequentially iterates days and calls `download-day`).

#### Usage

```
days download-days [-l <listingDir>] [-d <downloadedDaysDir>] [-t <threads>] <fromYear> <fromMonth> <fromDay> <toYear> <toMonth> <toDay>
```

#### Options

Same options as `download-day`. Range defaults to/from `2019-09-13` to today if omitted where applicable.

---

### `download-days-v2`

A newer download implementation which uses a concurrent download manager and integrates with block time reader and day block info maps.

#### Usage

```
days download-days-v2 [-l <listingDir>] [-d <downloadedDaysDir>] [-t <threads>] <fromYear> <fromMonth> <fromDay> <toYear> <toMonth> <toDay>
```

#### Options

|                      Option                       |                 Description                 |
|---------------------------------------------------|---------------------------------------------|
| `-l`, `--listing-dir <listingDir>`                | Directory where listing files are stored.   |
| `-d`, `--downloaded-days-dir <downloadedDaysDir>` | Directory where downloaded days are stored. |
| `-t`, `--threads`                                 | Initial concurrency (default: 64 in code).  |

#### Notes

Both download commands access public GCP storage (requester pays) and require Google Cloud authentication and a project to be set for requester pays. See [GCP authentication](#gcp-authentication).

---

### `download-days-v3`

Download days using v3 implementation with Guava preload fix.

#### Usage

```
days download-days-v3 [-l <listingDir>] [-d <downloadedDaysDir>] [-t <threads>] <fromYear> <fromMonth> <fromDay> <toYear> <toMonth> <toDay>
```

#### Options

Same as `download-days-v2`.

---

### `download-live`

Continuously follow Mirror Node for new block files; dedupe, validate, and organize into daily folders.

This command:
- Queries the Mirror Node for recent blocks using the `/api/v1/blocks` endpoint
- Filters results to the current day window in the configured rollover timezone
- Downloads, validates and organizes record files into per-day folders
- Appends successfully validated files into a per-day `.tar` archive
- Compresses completed day archives to `.tar.zstd` and cleans up loose files
- Persists state to a JSON file for resumable operation

#### Usage

```
days download-live [options]
```

#### Options

|           Option            |                             Description                              |
|-----------------------------|----------------------------------------------------------------------|
| `-l`, `--listing-dir <dir>` | Directory where listing files are stored (default: `listingsByDay`). |
| `-o`, `--out <dir>`         | Output directory for day archives.                                   |
| `--start-day <YYYY-MM-DD>`  | Start date for ingestion window.                                     |
| `--end-day <YYYY-MM-DD>`    | End date for ingestion window (omit for live mode).                  |
| `--day-rollover-tz <zone>`  | Timezone for day rollover (default: UTC).                            |

#### Modes

1. **Start + end date (finite historical range)**: Specify both `--start-day` and `--end-day` for bounded historical backfill.
2. **Start date only (catch-up then follow live)**: Specify `--start-day` but omit `--end-day` to bootstrap from a date and then stay live.
3. **No start/end date (pure live mode)**: If neither is supplied, the poller starts from "today" and tracks new blocks as they appear.

---

### `download-live2`

Live block download with inline validation, signature statistics, and automatic day rollover. Downloads blocks in real-time, validates them, and writes to per-day `.tar.zstd` archives.

#### Usage

```
days download-live2 [-l <listingDir>] [-o <outputDir>] [--start-date <YYYY-MM-DD>] [--max-concurrency <n>]
```

#### Options

|               Option               |                                      Description                                       |
|------------------------------------|----------------------------------------------------------------------------------------|
| `-l`, `--listing-dir <listingDir>` | Directory where listing files are stored (default: `listingsByDay`).                   |
| `-o`, `--output-dir <outputDir>`   | Directory where compressed day archives are written (default: `compressedDays`).       |
| `--start-date <YYYY-MM-DD>`        | Start date (default: auto-detect from Mirror Node).                                    |
| `--state-json <path>`              | Path to state JSON file for resume (default: `outputDir/validateCmdStatus.json`).      |
| `--stats-csv <path>`               | Path to signature statistics CSV file (default: `outputDir/signature_statistics.csv`). |
| `--address-book <path>`            | Path to address book file for signature validation.                                    |
| `--max-concurrency <n>`            | Maximum concurrent downloads (default: 64).                                            |

#### Features

- **Auto-detect start date**: Queries the Mirror Node to determine the current day if `--start-date` is not specified.
- **HTTP transport**: Uses HTTP (not gRPC) for GCS downloads to avoid deadlock issues with virtual threads.
- **Inline validation**: Validates each block's running hash as it's downloaded.
- **Signature statistics**: Tracks per-day signature counts and writes to CSV.
- **Day rollover**: Automatically finalizes day archives at midnight and starts new ones.
- **Resume support**: Saves state periodically to allow resuming after interruption.

#### Example

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

The command writes its state to `validateCmdStatus.json` in the output directory. This file is shared with the `validate` and `validate-with-stats` commands.

|                  Scenario                   |                                         Behavior                                         |
|---------------------------------------------|------------------------------------------------------------------------------------------|
| `download-live2` writes -> `validate` reads | Works (validate ignores extra `blockNumber` field)                                       |
| `validate` writes -> `download-live2` reads | `blockNumber` will be 0, so `download-live2` falls back to `--start-date` or auto-detect |

---

### `print-listing`

Prints a curated listing for a single day from listing files created by the download process.

#### Usage

```
days print-listing [-l <listingDir>] <year> <month> <day>
```

#### Options

|               Option               |                                Description                                 |
|------------------------------------|----------------------------------------------------------------------------|
| `-l`, `--listing-dir <listingDir>` | Directory where daily listing files are stored (default: `listingsByDay`). |
| `<year> <month> <day>`             | Date to print listing for.                                                 |

---

### `ls-day-listing`

Print all files in the listing for a day grouped by block timestamp. Useful for inspecting which record and sidecar files were recorded for a given block time.

#### Usage

```
days ls-day-listing [-l <listingDir>] <year> <month> <day>
```

#### Options

|               Option               |                             Description                              |
|------------------------------------|----------------------------------------------------------------------|
| `-l`, `--listing-dir <listingDir>` | Directory where listing files are stored (default: `listingsByDay`). |
| `<year> <month> <day>`             | Date to list files for.                                              |

---

### `split-files-listing`

Split a giant JSON listing (for example the output of `rclone lsjson`) into per-day binary listing files. The command parses the JSON listing, creates RecordFile objects for each entry, and writes them into day-specific binary files.

#### Usage

```
days split-files-listing [-l <listingDir>] <files.json>
```

#### Options

|               Option               |                             Description                              |
|------------------------------------|----------------------------------------------------------------------|
| `-l`, `--listing-dir <listingDir>` | Directory where listing files are stored (default: `listingsByDay`). |
| `<files.json>`                     | The JSON listing file to read (default: `files.json`).               |

#### Notes

- The command expects the JSON to be in the same format produced by `rclone lsjson`.
- It will create binary `.bin` day files under the provided listing directory, one file per UTC day.
- The operation can be very slow for large listings.

#### Example rclone command

```bash
nohup rclone lsjson -R --hash --no-mimetype --no-modtime --gcs-user-project <PROJECT> \
    "gcp:hedera-mainnet-streams/recordstreams" > files.json &
```

---

### `clean`

Clean all day `.tar.zstd` files or day files in directories passed in by removing bad record sets.

#### Usage

```
days clean <files-or-dirs>...
```

#### Options

|        Option        |                Description                 |
|----------------------|--------------------------------------------|
| `<files-or-dirs>...` | `.tar.zstd` files or directories to clean. |

---

### `updateDayListings`

Update day listing files by downloading metadata from GCS bucket.

#### Usage

```
days updateDayListings [options]
```

#### Options

|           Option            |                Description                |
|-----------------------------|-------------------------------------------|
| `-l`, `--listing-dir <dir>` | Directory where listing files are stored. |
| `--start-date <YYYY-MM-DD>` | Start date for updating listings.         |
| `--end-date <YYYY-MM-DD>`   | End date for updating listings.           |

---

### `fix-missing-signatures`

Command to fix missing signatures in downloaded days by re-downloading signature files.

#### Usage

```
days fix-missing-signatures [options] <compressedDaysDir>
```

#### Options

|        Option         |                 Description                 |
|-----------------------|---------------------------------------------|
| `<compressedDaysDir>` | Directory containing `.tar.zstd` day files. |

---

### `fix-sig-names`

Command to fix signature file names by renaming `.rcs.sig` to `.rcd.sig`.

#### Usage

```
days fix-sig-names [options] <compressedDaysDir>
```

#### Options

|        Option         |                 Description                 |
|-----------------------|---------------------------------------------|
| `<compressedDaysDir>` | Directory containing `.tar.zstd` day files. |

---

## GCP Authentication

Many download commands access public GCP storage (requester pays) and require Google Cloud authentication:

```bash
# Authenticate with GCP
gcloud auth application-default login
# or
gcloud auth login

# Set your project for requester pays billing
gcloud config set project YOUR_PROJECT_ID
```
