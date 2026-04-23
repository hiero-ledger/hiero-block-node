# blocks-file-historic

Archives blocks into zip files for long-term, space-efficient storage. Acts as the "cold" tier: blocks graduate here once the recent-files plugin evicts them, and are accessible via the standard `HistoricalBlockFacility` interface at lower I/O priority.

Blocks are first written to a staging directory as individual files, then periodically zipped into archive files whose sizes are governed by a configurable power-of-ten blocks-per-zip parameter. The zip tier itself is organised in a nested directory hierarchy to prevent excessive directory entries.

---

## Key Files

|              File               |                                                                                                                                                                                                  Purpose                                                                                                                                                                                                  |
|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `BlockFileHistoricPlugin.java`  | Plugin entry point, `BlockNotificationHandler`, and `BlockProviderPlugin`. Receives `VerificationNotification` events, stages each block, monitors staging thresholds, triggers background zip workers, and exposes the combined (staged + archived) block range to callers.                                                                                                                              |
| `ZipBlockArchive.java`          | Manages the zip-file layer. Creates new zip archives, scans for existing archives to determine the available block range, and provides `ZipBlockAccessor` instances for block reads. Also contains `calculateTotalStoredBytes()` — a full-tree walk with no caching (see notable logic).                                                                                                                  |
| `BlockPath.java`                | Computes the filesystem path and zip entry name for any block number. Also detects the compression type of an existing staged file by inspecting magic bytes, avoiding the need to store metadata separately.                                                                                                                                                                                             |
| `ZipBlockAccessor.java`         | `BlockAccessor` implementation for a block inside a zip archive. Opens the zip, locates the entry, decompresses, and converts to the requested format. For format `ZSTD_PROTOBUF` and a ZSTD-compressed source: returns the compressed bytes directly. For non-ZSTD source: decompresses then re-compresses. Uses temporary hard links to avoid holding the zip file open across the accessor's lifetime. |
| `BlockStagingFileAccessor.java` | `BlockAccessor` implementation for a block in the staging directory (pre-zipped). Reads the raw compressed file and converts to the requested format.                                                                                                                                                                                                                                                     |
| `FilesHistoricConfig.java`      | Configuration record (`@ConfigData("persistence.storage.historic")`). Key properties: `historicRootPath`, `compression`, `blocksPerZip` (must be a power of ten), `zipRetentionThreshold` (max zip count), `maxFilesPerDirectory`.                                                                                                                                                                        |

---

## Notable Logic

### Staging → zip pipeline

Blocks arrive from the verification pipeline and land in the staging directory as individual files (same format as `blocks-file-recent`). A background worker periodically inspects the staging directory; when the number of staged blocks reaches `blocksPerZip`, it creates a new zip archive from that batch and deletes the individual staged files. This two-phase design means reads during the staging period go to `BlockStagingFileAccessor` while reads for archived blocks go to `ZipBlockAccessor` — both implement the same `BlockAccessor` interface.

### `ZipBlockArchive.minStoredBlockNumber()` / `maxStoredBlockNumber()` — NPE risk in scan loops

Both methods walk the archive directory tree iteratively. If a filesystem error sets `lowestPath` or `highestPath` to `null` inside the loop, the next iteration dereferences null when calling `Files.list(path)`. Always validate paths before dereferencing in the scan loop.

### `ZipBlockArchive.calculateTotalStoredBytes()` — expensive on every call

Walks the entire archive tree to sum zip file sizes. The method itself acknowledges the cost with a `// todo(1249)` comment. Any metric or health endpoint that calls this frequently will trigger O(N) filesystem scans at the poll interval. Cache the result and invalidate on write before exposing this to production monitoring.

### `ZipBlockAccessor` — hard link lifecycle

To avoid holding the zip file open during the accessor lifetime, the accessor creates a temporary hard link on construction and deletes it in `close()`. If `close()` is not called (or is called from a non-finally block), hard link files accumulate in the temp directory. Always use `ZipBlockAccessor` in a try-with-resources block.
