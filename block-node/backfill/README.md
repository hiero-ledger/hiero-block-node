# backfill

Autonomously detects and fills gaps in the node's block history by fetching missing blocks from peer Block Nodes over gRPC. Runs continuously in the background without operator intervention.

Two schedulers handle different gap types: a **historical scheduler** for blocks older than the current live stream, and a **live-tail scheduler** for very recent blocks that arrived while the node was briefly unavailable.

---

## Key Files

|                       File                       |                                                                                                                                  Purpose                                                                                                                                  |
|--------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `BackfillPlugin.java`                            | Plugin entry point and `BlockNotificationHandler`. Listens for `PersistedNotification` and `NewestBlockKnownToNetworkNotification` events, triggers gap detection, and routes detected gaps to the appropriate scheduler. Owns the metrics registry for the whole plugin. |
| `BackfillRunner.java`                            | Executes a single backfill task: plans block availability across known peers, selects a node, fetches a batch, and awaits persistence confirmation. Retries with backoff on failure.                                                                                      |
| `BackfillTaskScheduler.java`                     | Single-worker bounded FIFO queue. Drops new gap tasks when the queue is full to avoid unbounded backlog growth during prolonged outages.                                                                                                                                  |
| `GapDetector.java`                               | Compares the node's `availableBlocks` range set against the known network range and classifies each gap as `HISTORICAL` or `LIVE_TAIL`.                                                                                                                                   |
| `BackfillFetcher.java`                           | gRPC client that fetches blocks from a specific peer node. Implements per-node health scoring (failure count + latency) and exponential backoff. Maintains one `BlockNodeClient` per known peer address.                                                                  |
| `PriorityHealthBasedStrategy.java`               | Selects the best peer node for a given block range using: earliest available block, configured priority, health score, then random tie-breaking.                                                                                                                          |
| `BackfillPersistenceAwaiter.java`                | `BlockNotificationHandler` that blocks the `BackfillRunner` thread until the just-fetched block is confirmed persisted, providing back-pressure between fetch and storage.                                                                                                |
| `client/BlockStreamSubscribeUnparsedClient.java` | gRPC subscriber used to pull blocks from peers. Uses unparsed responses and per-request pipeline isolation to prevent callback cross-talk between concurrent backfill operations.                                                                                         |
| `BackfillConfiguration.java`                     | Configuration record (`@ConfigData("backfill")`) with 17 properties covering start/end block bounds, retry limits, batch sizes, peer timeouts, TLS settings, and queue capacities.                                                                                        |
| `block_node_source.proto`                        | Defines `BackfillSource`, `BackfillSourceConfig`, and `GrpcWebClientTuning` — the per-peer configuration messages used to declare peer addresses and connection parameters.                                                                                               |

---

## Notable Logic

### `BackfillPlugin` — dual schedulers and gap classification

`GapDetector.detectGaps()` is called on every `PersistedNotification`. It produces a list of `GapRange` objects each tagged `HISTORICAL` or `LIVE_TAIL`. Historical gaps are enqueued on a low-priority background scheduler; live-tail gaps go to a higher-priority scheduler to avoid falling behind the live stream. Both schedulers share a single `BackfillRunner` implementation.

### `BackfillFetcher` — health-scored peer selection

Each peer accumulates a health score from failure count and observed round-trip latency. `PriorityHealthBasedStrategy` selects the healthiest available peer for each fetch attempt. On final failure after all retries, the fetcher logs a `WARNING` (promoted from `TRACE` in the log audit) and marks the peer as temporarily unavailable to prevent repeated hammering of a down node.

### `BackfillPersistenceAwaiter` — back-pressure via blocking

`BackfillRunner` registers `BackfillPersistenceAwaiter` as a `BlockNotificationHandler` before submitting blocks to the messaging facility. It then blocks on a `CountDownLatch` until `handlePersistedNotification` fires. This prevents the backfill pipeline from racing ahead of storage capacity. Ensure the latch is always released (even on error paths) to avoid a hung backfill thread.

### `BackfillConfiguration` — many tunables with wide blast radius

17 config properties control everything from retry counts to TCP buffer sizes. Misconfiguring `backfillBatchSize` too high causes large in-flight memory allocations; setting `publisherUnavailabilityTimeout` too low causes premature peer blacklisting. Review defaults before deployment.
