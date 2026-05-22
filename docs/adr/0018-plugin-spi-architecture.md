# 0020 - Plugin SPI for Block Node Extension Architecture

Date: 2026-05-22

## Status

Accepted

## Context

The Block Node is composed of many independently developed functional components:
block ingestion (stream publisher), verification, storage tiers (recent NVMe,
historic HDD, cloud archive), gRPC service surface (block access, subscriber
streaming, status), TSS bootstrap, backfill, metrics, and so on. Bundling all of
these into a single monolithic application would make the codebase brittle: every
new capability would touch the core wiring, and operators could not configure a
slim BN profile (for example, an RFH that omits the streaming subscriber service).

Three approaches were considered:

1. **Monolithic application with conditional initialisation by configuration
   flag.** Simple to implement but couples capabilities into a single deployment
   unit; testing combinations is combinatorially expensive.
2. **Per-capability microservices over the network.** Maximum isolation but
   imposes network-call latency on the inter-component messaging hot path
   (block hash, block items, verification notifications) which is incompatible
   with the streaming-throughput requirements.
3. **In-process plugin architecture with a stable Service Provider Interface
   (SPI) and JPMS-based discovery.** Each capability is a self-contained module
   that implements a common plugin interface and is wired by a discovery
   mechanism at startup.

## Decision

The Block Node adopts an **in-process plugin architecture**:

- A small `BlockNodePlugin` SPI (`block-node/spi-plugins/src/main/java/org/hiero/block/node/spi/BlockNodePlugin.java`)
  defines the lifecycle methods every plugin participates in: `configDataTypes()`,
  `init(context, serviceBuilder)`, `start()`, `stop()`, optionally `onContextUpdate()`.
- A `BlockNodeContext` record (in the same package) is the shared dependency
  surface that plugins receive in `init()`: configuration, metric registry,
  health facility, block messaging bus, historical block facility, application
  state facility, service loader, thread pool manager, version info, TSS data,
  and node address book.
- Plugins are discovered at startup via `java.util.ServiceLoader<BlockNodePlugin>`.
  Each plugin module declares `provides BlockNodePlugin with <FQCN>` in its
  `module-info.java`; the application aggregates all provided plugins into the
  initialisation order managed by `BlockNodeApp`.
- Facility plugins (block messaging, historical blocks, application state) are
  loaded via their own dedicated `ServiceLoader` calls before general plugins so
  that the `BlockNodeContext` passed to general plugin `init()` is fully wired.
- The gRPC service surface is registered through a `ServiceBuilder` interface
  passed to plugins in `init()`. A plugin that exposes a gRPC service calls
  `serviceBuilder.registerGrpcService(this)` during initialisation, and the
  application binds all registered services to the configured port at startup.

## Consequences

- New capabilities can be added as new modules without touching the core wiring.
  An RFH-specific BN can be built by including only the modules it needs (e.g.
  block messaging, verification, cloud-storage-archive) and omitting the
  streaming subscriber and state-related modules.
- The plugin lifecycle is sequential at present: `init()` runs in the order plugins
  are returned by `ServiceLoader`, then `start()` runs in the same order, then
  `onContextUpdate()` fires when shared state changes (e.g. TSS bootstrap data
  arrives from a peer BN). A future revision could parallelise `init()` for
  faster startup (see the in-progress plugin architecture proposal at
  `docs/design/architecture/new-plugin-architecture.md`).
- JPMS `module-info.java` files become a critical part of every plugin module:
  missing or misordered `provides`, `uses`, or `requires` declarations can cause
  silent runtime failures where a plugin is built but not discovered.
- The `BlockNodeContext` record is a contract shared by every plugin; adding,
  removing, or renaming fields is a cross-cutting change that must be reviewed
  carefully. The current 11 fields are documented in
  `docs/design/architecture/current-plugin-architecture.md`.
- Testing is supported through `PluginTestBase` and related fixtures under
  `block-node/app/src/testFixtures` which build a representative
  `BlockNodeContext` and exercise plugins in isolation or in small combinations.
- The decision intentionally takes the in-process tradeoff: shared JVM means
  shared GC, shared thread pools (mitigated by the `threadPoolManager` facility),
  and shared OOM blast radius. Tier 2 RFH (ADR-0019) deliberately runs in a
  separate process from Tier 1 LFH precisely to escape this shared-failure
  surface at the disaster-recovery boundary.
