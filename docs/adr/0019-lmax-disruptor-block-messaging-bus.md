# 0019 - LMAX Disruptor as Inter-Plugin Block Messaging Bus

Date: 2026-05-22

## Status

Accepted

## Context

The Block Node's plugin architecture (ADR-0018) requires a high-throughput,
low-latency mechanism for plugins to publish and consume block-related events
inside the JVM. The hot path is demanding:

- Block items arrive at 30k–50k items per second at target throughput.
- Multiple consumer plugins must observe each event: verification computes block
  hashes, persistence writes to disk, the cloud archive plugin batches blocks
  for upload, the subscriber service streams to downstream consumers, metrics
  collect counters, and so on.
- Some consumers must keep up with the live producer (no-back-pressure
  handlers like the streaming subscriber, where falling behind means dropping
  client connections); others can tolerate lag (back-pressure handlers like
  cloud archive, which batch and flush).
- The mechanism must not become a serialization bottleneck or impose
  unbounded queueing under load.

Three approaches were considered: standard `BlockingQueue` (simple but lock-heavy
and impedes scaling beyond a handful of consumers), reactive streams with custom
buffering (richer API but adds dependency surface and back-pressure complexity
the team would have to write), and LMAX Disruptor ring buffers (purpose-built for
the producer/multi-consumer hot path with documented performance characteristics
and built-in back-pressure semantics).

## Decision

The Block Node adopts the **LMAX Disruptor** library as the inter-plugin messaging
bus. The bus is exposed to plugins via the `BlockMessagingFacility` accessed
through `BlockNodeContext.blockMessaging()`.

- Block-related events are published on two primary ring buffers:
  `BlockNotificationRingEvent` for block-level notifications (block received,
  block verified, block persisted, etc.) and `BlockItemBatchRingEvent` for the
  high-rate item stream.
- Plugins register as event handlers via `BlockMessagingFacility.register*`
  methods, providing both the handler and a flag indicating whether the handler
  is **back-pressure** (producer waits if handler is too far behind) or
  **no-back-pressure** (events are dropped or producer continues without
  waiting if the handler cannot keep up).
- The no-back-pressure path has an `onTooFarBehindError` callback that fires
  when a handler falls beyond a configured threshold (currently 80% of the
  ring buffer depth). Handlers like the streaming subscriber use this to
  disconnect a slow client rather than blocking the live producer.
- Ring buffer sizes are configurable; production sizing favours large ring
  buffers (powers of two, currently in the 4k–16k range depending on event
  type) so that transient consumer slowdowns absorb without affecting the
  producer.

## Consequences

- Producer throughput scales linearly with the number of consumers — adding a
  new plugin that observes the block stream does not slow down existing
  consumers, which is the central reason the architecture remains scalable as
  the plugin set grows.
- Back-pressure semantics are explicit in plugin registration: a plugin that
  registers as "must keep up" (no back pressure) and falls behind 80% triggers
  the slow-consumer callback rather than silently dropping events or stalling
  the producer. Tier 1 plugins that serve live client streams use this
  semantic; cloud archive and similar batching plugins use back-pressure.
- The Disruptor's ring buffer model means events are never garbage-collected
  individually — the same slot is reused. Event objects must be mutable and
  reused (the Disruptor `EventFactory` pattern). Plugin code that retains
  references to event payloads beyond the handler call must copy the data,
  not the event wrapper.
- Single-JVM coupling: Disruptor-based messaging works only within one JVM. This
  reinforces the in-process plugin architecture (ADR-0018) and is incompatible
  with cross-process Tier 1/Tier 2 communication. RFHs (ADR-0011) communicate
  with Tier 1 LFHs over gRPC, not via the Disruptor bus.
- Adding a new ring buffer or event type is a non-trivial change that touches
  the facility plugin, all consumer plugins that need the new event, and any
  fixtures used in `PluginTestBase`. New event types should be batched into
  intentional design rounds rather than added ad hoc.
- The dependency is a single library (`com.lmax:disruptor`) with a permissive
  licence and stable API. The team does not currently use Disruptor features
  like batch event processors or work pools; if those are adopted later, the
  facility plugin's API must be revised in a follow-up ADR.
