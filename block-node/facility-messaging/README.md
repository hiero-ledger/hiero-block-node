# facility-messaging

The internal event bus for the Block Node. All block data and block lifecycle notifications flow through this facility. It is not a plugin in the user-facing sense — it is a shared infrastructure component instantiated by the application and injected into every plugin via `BlockNodeContext`.

Internally it uses two **LMAX Disruptor** ring buffers: one for block item batches (`BlockItemBatchRingEvent`) and one for block lifecycle notifications (`BlockNotificationRingEvent`). The Disruptor provides lock-free, cache-friendly, high-throughput event routing between the publisher pipeline and all downstream consumers.

---

## Key Files

|               File                |                                                                                                                              Purpose                                                                                                                               |
|-----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `BlockMessagingFacilityImpl.java` | The sole production implementation of `BlockMessagingFacility`. Creates and manages the two Disruptor ring buffers, registers/unregisters handlers at runtime, and dispatches both `sendBlockItems` and `sendNotification` calls into the appropriate ring buffer. |
| `BlockItemBatchRingEvent.java`    | Mutable event container for the block-item ring buffer. Holds a `BlockItems` reference. Mutable because Disruptor ring slots are pre-allocated and reused — the handler must copy data it needs to retain beyond the event callback.                               |
| `BlockNotificationRingEvent.java` | Mutable event container for the notification ring buffer. Holds one of: `VerificationNotification`, `PersistedNotification`, `BackfilledBlockNotification`, `NewestBlockKnownToNetworkNotification`, or `PublisherStatusUpdateNotification`.                       |
| `MessagingConfig.java`            | Configuration record (`@ConfigData("messaging")`). Exposes `blockItemQueueSize` (default 512 slots) and `blockNotificationQueueSize` (default 32 slots). Both must be powers of two (Disruptor constraint).                                                        |

---

## Notable Logic

### LMAX Disruptor ring buffer — pre-allocated, lock-free

The ring buffer is sized at startup and never grows. Slots are objects allocated once and reused. `sendBlockItems(BlockItems)` publishes by writing a reference into the next available slot and advancing the producer cursor. Handlers that fall behind the producer cause back-pressure on the publisher thread — this is intentional. Do not hold event data beyond the `onEvent()` callback without copying it first.

### Handler registration — dynamic at runtime

`registerBlockItemHandler()` and `registerBlockNotificationHandler()` add new consumers to the ring buffer at runtime. Adding a handler adds a new `BatchEventProcessor` with its own sequence tracking. Removing a handler calls `removeGatingSequence()` and `halt()` before `shutdown()`. The correct shutdown order is: `removeGatingSequence` → `halt` → `shutdown` (in that order, per LMAX docs). **Do not call `shutdown()` before `halt()`.**

### `stop()` — interrupts without joining

`stop()` calls `thread.interrupt()` on each handler thread but does not call `thread.join()`. There is no confirmation that handler threads have fully terminated before `stop()` returns. Downstream resources (file handles, network connections) held by handlers may not be released cleanly if the JVM shuts down immediately after `stop()`.

### Exception handling in `onEvent()`

`BLOCK_ITEM_EXCEPTION_HANDLER` is registered as the Disruptor's `ExceptionHandler`. Exceptions thrown inside a handler's `onEvent()` are caught and logged by this handler — they do **not** kill the processor thread. This means a faulty handler will continue to receive events after an error, which may or may not be the right behaviour depending on the exception type.

### `percentageBehindRingHead` metric — uses `barrier.getCursor()`

The backpressure metric passed to handlers uses `barrier.getCursor()` for the ring head position. `barrier.getCursor()` tracks the sequence the consumer can safely read up to (gating sequence), while `ringBuffer.getCursor()` tracks the latest published sequence. These differ under back-pressure conditions. The metric may underreport actual lag.
