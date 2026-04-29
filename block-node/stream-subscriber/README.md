# stream-subscriber

Serves the `subscribeBlockStream` gRPC endpoint, allowing downstream consumers (mirror nodes, block explorers, other Block Nodes) to subscribe to a live or historical block feed.

Each incoming subscription spawns a `BlockStreamSubscriberSession` that runs in its own virtual thread, pulling from either a live queue populated by the messaging facility or from the `HistoricalBlockFacility` for past blocks.

---

## Key Files

|                File                 |                                                                                                                                                                                 Purpose                                                                                                                                                                                 |
|-------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SubscriberServicePlugin.java`      | Plugin entry point. Registers the gRPC service, manages session lifecycle (spawning, tracking, and cleanup), and wires the live queue into the messaging facility via `BlockItemHandler` registration.                                                                                                                                                                  |
| `BlockStreamSubscriberSession.java` | Per-subscription session. Implements `Callable` and runs for the lifetime of one subscriber connection. Handles dual-mode operation: historical replay (reads from `HistoricalBlockFacility`) and live streaming (reads from an in-memory queue fed by the ring buffer). Also responsible for chunking large historical blocks to stay within gRPC message size limits. |
| `SubscriberConfig.java`             | Configuration record (`@ConfigData("subscriber")`). Key properties: `liveQueueSize` (ring-buffer capacity per subscriber), `minimumLiveQueueCapacity` (back-pressure threshold), `maxChunkSizeBytes` (gRPC chunk limit), `maxConcurrentSessions`, `sessionRateLimitPerSecond`.                                                                                          |

---

## Notable Logic

### `SubscriberServicePlugin` — session lifecycle and back-pressure registration

When a new subscription arrives, `apply()` creates a `BlockStreamSubscriberSession`, submits it to a `CompletionService`-backed thread pool, and registers a `BlockItemHandler` so the messaging facility delivers live items to that session's queue. On session completion (normal or error), `stop()` drains the `CompletionService` and removes the registered handler, preventing memory and thread leaks.

### `BlockStreamSubscriberSession` — historical / live handoff

On startup, the session determines whether the requested start block is already available historically or is in the live stream. It transitions from historical replay to live streaming at the point where the historical facility no longer has the next needed block. The transition is designed to be seamless: the session holds its position in the live queue while replaying history to avoid missing blocks at the boundary.

### `BlockStreamSubscriberSession.sendBlockItemsChunked()` — O(n²) caution

Historical blocks are split into gRPC-sized chunks using `measureRecord()` per item. Each chunk-build pass re-measures items, giving O(n²) worst-case behaviour for large blocks near the chunk size boundary. Be aware of this when handling blocks with many transactions.
