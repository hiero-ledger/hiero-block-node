# stream-publisher

Receives live block data from one or more Consensus Nodes (CNs) over a bidirectional gRPC stream (`publishBlockStream`) and fans it out to the rest of the Block Node via the internal messaging facility.

A publisher connection goes through three layers: the gRPC gateway (`StreamPublisherPlugin`), a per-connection handler (`PublisherHandler`), and a shared manager (`LiveStreamPublisherManager`) that arbitrates between competing publishers and feeds the ring buffer.

---

## Key Files

|               File                |                                                                                                                       Purpose                                                                                                                       |
|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `StreamPublisherPlugin.java`      | Plugin entry point. Registers the gRPC service, configures the PBJ pipeline, and owns the `LiveStreamPublisherManager` lifecycle.                                                                                                                   |
| `PublisherHandler.java`           | Stateful per-connection handler. Parses `PublishStreamRequestUnparsed` messages, tracks the block-in-progress, sends `ACK / SKIP / RESEND / BEHIND` responses back to the CN, and calls into the manager on block boundaries.                       |
| `LiveStreamPublisherManager.java` | Shared coordinator across all active publisher connections. Decides which publisher "wins" when multiple CNs offer the same block, manages the block queue that feeds `BlockMessagingFacility`, and handles publisher-unavailability timeout logic. |
| `StreamPublisherManager.java`     | Interface contract implemented by `LiveStreamPublisherManager`. Defines the methods `PublisherHandler` calls: `blockIsStarting`, `blockIsEnding`, `getBlockAction`, `registerQueue`, etc.                                                           |
| `PublisherConfig.java`            | Configuration record (`@ConfigData("producer")`). Exposes `batchForwardLimit` (max item batches forwarded before yielding) and `publisherUnavailabilityTimeout` (seconds before a non-responsive publisher is marked unavailable).                  |

---

## Notable Logic

### `StreamPublisherPlugin.open()` — unparsed parsing for performance

The plugin overrides `open()` rather than `publishBlockStream()` to use `PublishStreamRequestUnparsed` instead of the fully-parsed `PublishStreamRequest`. All `BlockItem` fields are kept as raw bytes and never deserialised here. This eliminates ~90 % of publish-to-subscribe latency and substantially reduces GC pressure. The `maxMessageSize` used for the parser is `serverConfig.maxMessageSizeBytes() - 16384`.

### `PublisherHandler` — block lifecycle state machine

Each handler tracks `currentStreamingBlockNumber`, whether a block is mid-flight (`isCurrentlyMidBlock`), and a `blockAction` enum (`SEND`, `SKIP_BLOCK`, `RESEND_BLOCK`). Every incoming `block_items` message is routed through this state machine. On `end_stream`, `handleEndStream` decides whether to call `blockIsEnding` on the manager or simply close the connection.

### `LiveStreamPublisherManager` — block arbitration and queue forwarding

When multiple publishers offer overlapping blocks the manager assigns a `blockAction` to each handler. The inner `MessagingForwarderTask` thread reads from the per-publisher `LinkedTransferQueue` and calls `messagingFacility.sendBlockItems()`. It uses a `waitForDataReady` / notify pattern to avoid busy-spinning when the queue is empty.
