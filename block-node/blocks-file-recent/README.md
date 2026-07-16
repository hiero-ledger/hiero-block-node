# blocks-file-recent

Stores recently verified blocks on the local filesystem. Acts as the primary "hot" storage tier: blocks are written here immediately after verification and are available for fast local reads before they are eventually migrated to the historic archive.

Files are stored in a nested directory hierarchy (configurable files-per-directory) with optional ZSTD compression. A retention policy deletes the oldest files once the stored block count exceeds a configured threshold.

---

## Key Files

|             File              |                                                                                                                                                    Purpose                                                                                                                                                     |
|-------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `BlockFileRecentPlugin.java`  | Plugin entry point, `BlockNotificationHandler`, and `BlockProviderPlugin`. Receives `VerificationNotification` events, writes each verified block to disk, maintains the `availableBlocks` range set, and runs a background retention loop that evicts the oldest blocks when the count exceeds the threshold. |
| `RecentBlockPath.java`        | Computes the filesystem path for a given block number using the configured directory depth and files-per-directory limit. Encapsulates all path arithmetic so the rest of the plugin treats block numbers as opaque keys.                                                                                      |
| `BlockFileBlockAccessor.java` | `BlockAccessor` implementation for a single on-disk block file. Reads the raw compressed bytes and converts to the requested format (PROTOBUF, JSON, ZSTD_PROTOBUF). JSON conversion re-parses and re-serialises the full block on every call — there is no per-accessor cache.                                |
| `FilesRecentConfig.java`      | Configuration record (`@ConfigData("persistence.storage.live")`). Key properties: `liveRootPath` (storage root), `compression` (compression type, default ZSTD), `filesPerDir` (directory fan-out limit), `blockRetentionThreshold` (max blocks to retain).                                                    |

---

## Notable Logic

### `BlockFileRecentPlugin.handleVerification()` — write path

On every `VerificationNotification` with `success=true`, the plugin serialises the full `BlockUnparsed` to the path computed by `RecentBlockPath` and calls `availableBlocks.add(blockNumber)`. The write is synchronous on the notification handler thread. If the write fails, the block is not added to `availableBlocks` and the error is logged, but no retry mechanism exists — a failed write is a silent gap.

### Retention loop — background thread

A dedicated thread periodically checks whether `availableBlocks.size()` exceeds `blockRetentionThreshold`. When it does, it computes the oldest block numbers to evict, deletes each file via `deleteBlockCleanly()`, and calls `availableBlocks.remove(blockNumber)`. The retention loop and the write path both mutate `availableBlocks`; individual operations on `ConcurrentLongRangeSet` are thread-safe, but check-then-act sequences (e.g., checking range then deleting) are not atomic.

### `BlockFileBlockAccessor.readAllBytes()` — no size cap before read

`readAllBytes()` is called on the raw file without a pre-read size check. `MAX_BLOCK_SIZE_BYTES` is only enforced at the PBJ parser layer. A corrupted or adversarially crafted oversized file can exhaust heap before the size check triggers.
