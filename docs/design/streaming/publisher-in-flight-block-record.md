# Proposal: Collapse Publisher In-Flight Block State into One Per-Block Record

Status: Draft proposal (not implemented)
Component: `block-node/stream-publisher` — `LiveStreamPublisherManager`

## 1. Summary

`LiveStreamPublisherManager` tracks the lifecycle of a block that is being received from a
publisher in four separate concurrent collections, all keyed by block number:

| Collection | Holds |
|---|---|
| `queueByBlockMap` | `Deque<BlockItemSetUnparsed>` — batches received but not yet forwarded to messaging |
| `blockProofs` | the block proof item, held back so it is forwarded last |
| `endBlocksReceived` | "end of block" marker — the publisher finished sending this block |
| `activeStreamHandlerByBlock` | id of the handler that won ACCEPT for the block (used for stall detection) |

These four describe **one thing**: the in-flight attempt at a single block. Today they are
mutated independently, from multiple threads, with no single point of consistency. Every
transition has to remember to update all four, and the code compensates with a growing set of
guard routines (`clearCollectedBlockEnd`, `discardIfInconsistent`, `clearObsoleteQueueItems`,
"should not actually be needed" removals).

The proposal is to replace the four maps with one `ConcurrentSkipListMap<Long, InFlightBlock>`,
where `InFlightBlock` owns the queue, proof, end marker and owning handler for one attempt, and
exposes the legal state transitions as methods. An invalid combination (a proof with no queue, a
queue belonging to one attempt and a proof from another) becomes unrepresentable instead of
detected after the fact.

This is a refactor of publisher-internal state. It requires no new messages, no SPI change and
no change to the wire protocol.

## 2. Problem

### 2.1 The bug class

The ack stall fixed in #3975 (stale block proof loses a resent block and pins acks) was an
instance of this drift: a `blockProofs` entry from a previous attempt survived while a new
`queueByBlockMap` entry was registered for the same block. The forwarder's condition

```java
if (queueToForward != null && !publisherManager.blockProofs.containsKey(currentBlockNumber))
```

then hid the new queue behind the stale proof, and the proof could be sent without the header
and body it belonged to. The fix added `clearCollectedBlockEnd` and `discardIfInconsistent`.
Those are correct, but they treat the symptom: the data model still allows the inconsistent state,
and each future code path that touches one map must remember the same rule.

### 2.2 Where the state is currently touched

The same four collections are mutated at many sites. Each is a place where a missed update
produces drift:

| Site | Queue | Proof | End marker | Active handler |
|---|---|---|---|---|
| `registerQueueForBlock` | put | clear | clear | put |
| `endOfBlock` | read | remove if no queue | add if queue | remove |
| `blockIsEnding` (mid-block drop) | remove | — | — | remove |
| `checkForStalledHandlers` / `endStalledBlock` | remove | clear | clear | remove (CAS) |
| `clearObsoleteQueueItems` | remove | read and clear | clear | remove |
| `discardIfInconsistent` | read and remove | read and clear | read and clear | remove |
| `endAllHandlersAndClearTracking` | clear | clear | clear | clear |
| `shutdown` | clear | — | — | clear |
| Forwarder task (`MessagingForwarderTask.call`) | get, poll, remove | put, remove | read, remove | — |

Notes:

- `shutdown` clears the queue and active-handler maps but not `blockProofs` or
  `endBlocksReceived`. Whether that is intentional or an omission is not obvious from the code,
  which is itself a symptom.
- `blockIsEnding` removes the queue and active handler but leaves any collected proof and end
  marker behind. `registerQueueForBlock` later cleans that up, so correctness depends on a
  later call happening.
- Stall detection removes entries using `activeStreamHandlerByBlock.remove(key, value)` as a
  compare-and-set. This works, but only because that one map is serving as the authority for the
  other three, an arrangement the code doesn't state.

### 2.3 Derived reads that span the maps

Several decisions are made by combining the maps, and these combinations are where races hide:

- `correctForResendAndStreaming` uses `queueByBlockMap.firstKey()` as "earliest in-flight block"
  when clamping the acknowledgement.
- `clearObsoleteQueueItems` decides whether a queue is safe to drop by checking
  `blockProofs`, the queue's last batch and `isResendingLive`.
- `determineCurrentBlockNumber` treats presence in `queueByBlockMap` as "this block is active"; the
  forwarder removes the queue only when it sends the proof, which is the signal for "no longer
  active".

Because "is this block in flight" is currently defined by the presence of a key in one specific
map, and several others are consulted in combination, the definition is implicit.

## 3. Proposed design

### 3.1 The record

```java
/// One publisher's attempt at one block. Created when a handler wins ACCEPT, discarded when the
/// block completes, is abandoned, stalls, or is superseded by a new attempt.
final class InFlightBlock {
    private final long blockNumber;
    private final long ownerHandlerId;
    private final Deque<BlockItemSetUnparsed> pending = new ConcurrentLinkedDeque<>();
    private volatile BlockItemUnparsed heldProof; // collected by the forwarder, sent last
    private volatile boolean endReceived;         // publisher finished sending this block
    ...
}
```

State owned by the record, replacing the four maps:

| Today | In the record |
|---|---|
| `queueByBlockMap.get(n)` | `block.pending` |
| `blockProofs.get(n)` | `block.heldProof` |
| `endBlocksReceived.contains(n)` | `block.endReceived` |
| `activeStreamHandlerByBlock.get(n)` | `block.ownerHandlerId` |

The manager keeps one map: `ConcurrentSkipListMap<Long, InFlightBlock> inFlight`.

### 3.2 Attempt identity by construction

A new ACCEPT creates a new `InFlightBlock` and replaces the previous one atomically
(`inFlight.put`, or `compute` if the old record needs cleanup). The proof and end marker belong to
the record, so a replacement attempt starts with none of them. The `clearCollectedBlockEnd` call
in `registerQueueForBlock`, and the long comment explaining why it must come before the `put`,
are no longer needed: there is nothing to clear because the old state goes away with the old
record.

The same applies to the forwarder: it works from a record reference it obtained, so a proof it
holds can never be paired with a queue from a different attempt.

### 3.3 Transitions as methods

The state transitions currently spread across the manager become small methods on the record or
on a manager helper that operates on one record:

- `accept(handlerId)` — create the record (replaces `registerQueueForBlock`).
- `append(batch)` — enqueue a batch (replaces queue offer in the handler path).
- `collectProof(item)` — forwarder takes the proof off the final batch (replaces
  `blockProofs.put`).
- `markEnd()` — publisher signalled the end (replaces `endBlocksReceived.add`).
- `isReadyToComplete()` — `endReceived && heldProof != null && pending.isEmpty()`. This replaces
  `discardIfInconsistent`'s detection, because a record with a proof and unforwarded batches
  from a different attempt cannot occur.
- `abandon()` / `discard(reason)` — one removal path used by mid-block drop, stall, obsolete
  cleanup and reset, replacing the four-map removals at each of those sites.

### 3.4 Stall detection

`activeStreamHandlerByBlock.remove(candidate, handlerId)` is used as a compare-and-set so only one
thread acts on a stalled block. With a record this becomes
`inFlight.remove(candidate, record)` against the specific record instance, which is the same
guarantee, and slightly stronger: it also fails if the record was replaced by a new attempt after
the stall was detected, so a late detection cannot discard a newer attempt.

### 3.5 What stays the same

- `blocksToResend` and `activeResendBlocks` stay separate. They describe blocks the publisher
  *wants again*, not an in-flight attempt, and have a different lifecycle.
- `lastPersistedBlockNumber`, `nextUnstreamedBlockNumber`, `lastForwardedBlockNumber` and
  `latestAckedBlock` are unchanged. This proposal does not touch the cursors.
- Handler, flow-control and threading state are unchanged.
- The public `StreamPublisherManager` interface is unchanged; `PublisherHandler` still calls
  `registerQueueForBlock`, `endOfBlock`, `blockIsEnding` and so on. These can keep their
  signatures, with the implementation delegating to the record.

## 4. Benefits

1. **Removes the #3975 bug class.** A proof from one attempt can't coexist with a queue from
   another, because both belong to the record that the new attempt replaces.
2. **One removal path.** Eight or so removal sites, each updating a different subset of four
   maps, become one `discard` call. This also resolves the `shutdown` and `blockIsEnding`
   inconsistencies noted in 2.2.
3. **Explicit definition of "in flight".** `inFlight.firstKey()` replaces
   `queueByBlockMap.firstKey()` in `correctForResendAndStreaming`, and the meaning is documented
   on one type.
4. **Smaller, simpler guards.** `discardIfInconsistent`, `clearCollectedBlockEnd` and parts of
   `clearObsoleteQueueItems` can be deleted or reduced to single-record checks.
5. **Easier testing.** Transitions on `InFlightBlock` can be unit tested without a full manager,
   and a model of "legal sequences of events" can be property tested.

## 5. Risks and open questions

- **Concurrency semantics change subtly.** Today the maps are independently concurrent; a reader
  can observe the queue present and the proof absent. After the change, a reader holding a
  record sees the fields of that record, but they're still separate volatile fields. If a
  transition needs atomicity across fields (for example proof and end marker), it needs either a
  lock on the record or a single immutable state object swapped via `AtomicReference`. This
  needs a decision before implementation: a small per-record lock is simplest, and contention is
  low because each record is touched by one handler plus the forwarder.
- **Forwarder hot path.** The forwarder polls the queue in a tight loop, so the queue type must
  not become a bottleneck. Keep the concrete `Deque` the handlers use today and have the record
  expose it directly, rather than wrapping each operation.
- **Handler-owned queue.** `registerQueueForBlock(handlerId, queue, blockNumber)` currently
  receives a queue created by the handler. Either the record adopts that queue (least
  invasive) or the record creates it and hands it back. The first is recommended for the first
  step.
- **Behaviour to confirm, not assume.** Two current behaviours look accidental and need a
  deliberate decision during implementation rather than being preserved or "fixed" silently:
  `shutdown` not clearing proofs/end markers, and `blockIsEnding` leaving proof/end marker
  behind. The record makes both go away, but a test should pin the intended behaviour.
- **Not verified against all callers.** This proposal is based on a read of
  `LiveStreamPublisherManager` and its call sites for these four maps. `PublisherHandler` (about
  1,400 lines) was not audited for how it uses the queue it registers, and that should happen
  before finalising the interface between handler and record.

## 6. Implementation plan

1. **Characterisation tests first.** Add tests that pin current externally visible behaviour for
   resend, stall, mid-block drop, reset and shutdown, including the #3975 scenario. These should
   pass before and after.
2. **Introduce `InFlightBlock`** and the single `inFlight` map alongside the old maps, with the
   manager updating both (no behaviour change; assert they agree in tests).
3. **Switch readers** (`correctForResendAndStreaming`, `clearObsoleteQueueItems`,
   `determineCurrentBlockNumber`, forwarder) to the new map.
4. **Switch writers** to the record's transition methods, and route all removals through
   `discard`.
5. **Delete the old maps** and the guard routines they made necessary.
6. Run the stream-publisher unit tests and the existing end-to-end or simulator publish scenarios
   with multiple publishers, resends and forced stalls.

Each step is independently mergeable, and steps 2 to 4 can be reverted individually.

## 7. Out of scope

- Deriving `lastPersistedBlockNumber`, `latestAckedBlock` and the resend and gap state from
  notifications (tracked separately; some of it needs new messages).
- Removing `blocksClosedComplete`, which duplicates a metric and can be deleted independently.
- Any change to the publish protocol, messaging SPI or other plugins.
