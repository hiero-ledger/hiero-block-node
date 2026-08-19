# s3-archive

Archives verified blocks to an S3-compatible object store (AWS S3 or compatible endpoint). Blocks are batched into compressed tar archives and uploaded asynchronously, providing a durable off-node backup tier that survives local disk failure.

On startup the plugin reads the last successfully uploaded block number from a well-known S3 marker file, so uploads resume correctly after a restart without duplicating or skipping blocks.

---

## Key Files

|          File          |                                                                                                                              Purpose                                                                                                                              |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `S3ArchivePlugin.java` | Plugin entry point and `BlockNotificationHandler`. Listens for `PersistedNotification` events, accumulates blocks into upload batches, and dispatches `UploadTask` instances to a thread pool when a batch is complete. Owns the `lastArchivedBlockNumber` state. |
| `S3ArchiveConfig.java` | Configuration record (`@ConfigData("persistence.storage.s3")`). Key properties: `blocksPerFile` (default 100,000), `endpoint`, `bucketName`, `regionName`, `storageClass`, and credential properties.                                                             |

---

## Notable Logic

### Startup — `lastArchivedBlockNumber` restored from S3

`start()` downloads a marker file (`LATEST_ARCHIVED_BLOCK_FILE`) from S3 and sets the in-memory `lastArchivedBlockNumber` `AtomicLong` from its contents. If the file does not exist or is empty, the plugin starts from block 0. This means the durability of `lastArchivedBlockNumber` depends entirely on S3 availability at startup — if S3 is unreachable at startup the plugin falls back to 0 and may re-upload already-archived blocks.

### Parallel upload ordering — TOCTOU gap

`UploadTask` instances run concurrently. Each task calls `lastArchivedBlockNumber.set(nextBatchEndBlockNumber)` after a successful upload. If batch [51–150] completes before batch [1–100], `lastArchivedBlockNumber` jumps to 150 and the [1–100] batch is permanently skipped — its blocks will never appear in S3. Batches should be serialised through a single-writer queue or a compare-and-set ordered by batch start block number.

### `S3ArchiveConfig` — credentials in config

S3 credentials (access key, secret key) are read from the configuration system, which maps to environment variables. Never commit credential values to version-controlled config files. Use the `AUTO` credential provider in production environments where instance role / workload identity is available.
