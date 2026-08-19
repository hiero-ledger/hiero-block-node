# spi-plugins

The Service Provider Interface (SPI) layer for the Block Node plugin system. This module defines the contracts that every plugin, facility, and shared infrastructure component must implement or consume. It contains no business logic — only interfaces, records, and utility classes.

All plugin modules depend on `spi-plugins`. No plugin module should depend directly on another plugin module.

---

## Key Files

|              File               |                                                                                                                                                                                      Purpose                                                                                                                                                                                      |
|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `BlockNodePlugin.java`          | The root interface every plugin implements. Defines the lifecycle hooks: `init(BlockNodeContext)`, `start()`, `stop()`, and `onContextUpdate()`.                                                                                                                                                                                                                                  |
| `BlockNodeContext.java`         | The record injected into every plugin at `init()` time. Carries configuration, metrics, `BlockMessagingFacility`, `HistoricalBlockFacility`, `HealthFacility`, `ThreadPoolManager`, version info, TSS data, and a `ServiceBuilder` for registering HTTP/gRPC routes. It is the only channel through which plugins obtain shared resources — no direct injection or static access. |
| `BlockMessagingFacility.java`   | Interface for the internal event bus. Plugins call `sendBlockItems(BlockItems)` to publish block data and `sendNotification(...)` to broadcast lifecycle events. Implements documented back-pressure: if consumers are too slow, `sendBlockItems` blocks the caller.                                                                                                              |
| `BlockItemHandler.java`         | Interface implemented by any plugin that wants to receive block item batches from the ring buffer. Registered via `BlockNodeContext.messagingFacility().registerBlockItemHandler()`.                                                                                                                                                                                              |
| `BlockNotificationHandler.java` | Interface for receiving block lifecycle events: `VerificationNotification`, `PersistedNotification`, `BackfilledBlockNotification`, `NewestBlockKnownToNetworkNotification`, `PublisherStatusUpdateNotification`.                                                                                                                                                                 |
| `BlockProviderPlugin.java`      | Interface implemented by storage plugins. Exposes `block(long blockNumber)` (returns a `BlockAccessor`) and `availableBlocks()` (returns a `BlockRangeSet`). Providers are sorted by priority; the `HistoricalBlockFacility` tries them in order.                                                                                                                                 |
| `BlockAccessor.java`            | Interface for reading a single block. Provides `blockUnparsed()`, `blockBytes(Format)`, and `blockNumber()`. Implementations must be `Closeable` — always use in a try-with-resources block.                                                                                                                                                                                      |
| `HistoricalBlockFacility.java`  | Interface for reading historical blocks. The application wires together all `BlockProviderPlugin` instances behind this interface. Plugins call `blockNodeContext.historicalBlockProvider()` to access it.                                                                                                                                                                        |
| `HealthFacility.java`           | Interface for querying and updating node state (`STARTING`, `RUNNING`, `SHUTTING_DOWN`). The health plugin and the application both use this.                                                                                                                                                                                                                                     |
| `BlockItems.java`               | Record wrapping a `List<BlockItemUnparsed>`, a `blockNumber`, an `isStartOfNewBlock` flag, and an `isEndOfBlock` flag. This is the unit of data flowing through the block-item ring buffer. The list is **not defensively copied** — callers must not retain and mutate the list after constructing a `BlockItems` instance.                                                      |
| `BlockRangeSet.java`            | Interface over a set of contiguous `LongRange` intervals. Used by storage plugins to report which blocks they hold. Key methods: `contains(long)`, `size()`, `min()`, `max()`, `streamRanges()`.                                                                                                                                                                                  |
| `ThreadPoolManager.java`        | Interface for obtaining managed thread pools. Plugins should use this rather than creating raw threads so that thread lifecycle is visible to the application. Provides virtual thread executors, single-thread executors, and scheduled executors.                                                                                                                               |
| `SemanticVersionUtility.java`   | Parses semantic version strings from `Class`, `ModuleDescriptor`, or `String`. Used by `HapiVersionSessionFactory` to route blocks to the correct verification session. Note: the regex excludes hyphens in pre-release identifiers, violating SemVer 2.0.0.                                                                                                                      |
| `ModuleInfoAccessor.java`       | Caches module descriptor metadata (name, version, provided services) per module. `module.getDescriptor()` returns null for unnamed modules; the code does not guard against this — NPE risk in non-modular test harnesses.                                                                                                                                                        |

---

## Key Design Principles

**All cross-plugin communication goes through `BlockNodeContext`.**
Plugins must not import each other or share static state. If a plugin needs a capability another plugin provides, the capability must be exposed through the SPI interfaces in this module.

**Lifecycle order matters.**
`init()` is called on all plugins before `start()` is called on any plugin. Plugins must not use facilities that depend on other plugins' `start()` during their own `init()`.

**`BlockAccessor` is always closeable.**
Implementations may hold file handles or zip entries. Failing to close an accessor leaks resources.
