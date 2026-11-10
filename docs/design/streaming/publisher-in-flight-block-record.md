# Proposal: Collapse Publisher In-Flight Block State into One Per-Block Record

Status: Draft proposal (not implemented)
Component: `block-node/stream-publisher` — `LiveStreamPublisherManager`

## 1. Summary

`LiveStreamPublisherManager` tracks the lifecycle of a block that is being received from a
publisher in four separate concurrent collections, all keyed by block number:

|          Collection          |                                        Holds                                        |
|------------------------------|-------------------------------------------------------------------------------------|
| `queueByBlockMap`            | `Deque<BlockItemSetUnparsed>` — batches received but not yet forwarded to messaging |
| `blockProofs`                | the block proof item, held back so it is forwarded last                             |
| `endBlocksReceived`          | "end of block" marker — the publisher finished sending this block                   |
| `activeStreamHandlerByBlock` | id of the handler that won ACCEPT for the block (used for stall detection)          |

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

|                      Site                      |       Queue       |       Proof        |   End marker   | Active handler |
|------------------------------------------------|-------------------|--------------------|----------------|----------------|
| `registerQueueForBlock`                        | put               | clear              | clear          | put            |
| `endOfBlock`                                   | read              | remove if no queue | add if queue   | remove         |
| `blockIsEnding` (mid-block drop)               | remove            | —                  | —              | remove         |
| `checkForStalledHandlers` / `endStalledBlock`  | remove            | clear              | clear          | remove (CAS)   |
| `clearObsoleteQueueItems`                      | remove            | read and clear     | clear          | remove         |
| `discardIfInconsistent`                        | read and remove   | read and clear     | read and clear | remove         |
| `endAllHandlersAndClearTracking`               | clear             | clear              | clear          | clear          |
| `shutdown`                                     | clear             | —                  | —              | clear          |
| Forwarder task (`MessagingForwarderTask.call`) | get, poll, remove | put, remove        | read, remove   | —              |

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

### 2.2.1 How `PublisherHandler` uses the queue

`PublisherHandler` is the producer, and the manager's forwarder task is the consumer. The handler
works like this:

- On a batch that contains a `BlockHeader` and wins ACCEPT, `handleAccept` creates a
  `ConcurrentLinkedDeque`, stores it in its own `currentBlockQueue` field, and passes the same
  deque to `registerQueueForBlock(handlerId, queue, blockNumber)`.
- For every later batch, it calls `currentBlockQueue.get().offer(batch)` **directly on its own
  reference**, then `signalDataReady()`. The manager is not involved in the write.
- `isCurrentlyMidBlock` is defined as `blockInProgress > UNKNOWN && currentBlockQueue.get() != null`,
  so the handler's idea of "mid-block" is "I hold a queue reference".
- `resetState()` sets `currentBlockQueue` to null. The handler calls the manager's
  `blockIsEnding(blockNumber, handlerId)` from six places (new header mid-block, `AcknowledgeOnly`
  mid-block, `EndStream` mid-block, shutdown mid-block, and so on) and `endOfBlock(blockNumber)` from
  one.
- Before each batch, `getActionForBlock(blockNumber, previousAction, handlerId)` is called. For a
  block already streaming it ends in `getActionForCurrentlyStreaming`, which returns ACCEPT if
  `queueByBlockMap.containsKey(blockNumber)` and SKIP otherwise.

This review found three potential problems:

**(a) Attempt identity is by block number, not by owner.** `getActionForCurrentlyStreaming`
answers "does *any* queue exist for block N", not "is *my* queue the one registered for block N".
`endOfBlock(blockNumber)` and `blockIsEnding(blockNumber, handlerId)` also act on whatever record
is registered for that number. `blockIsEnding` takes a `handlerId` but never compares it with
`activeStreamHandlerByBlock`. A handler whose attempt was discarded and replaced (stall,
obsolete cleanup, `discardIfInconsistent`, or a resend won by another handler) can therefore:

- keep receiving ACCEPT because another handler's queue exists for that number;
- keep offering batches into its own detached queue, so the data is silently dropped;
- call `endOfBlock(N)` and mark the *other* handler's attempt as ended, and clear its stall
  tracking and active-resend entry;
- call `blockIsEnding(N, handlerId)` and remove the *other* handler's queue and schedule a resend.

The window is narrow (a stall ends the old handler's stream with TIMEOUT, so the old handler is
usually already on its way out), but nothing in the code prevents it. This is the same defect as

# 3975 seen from the handler side, and per-record identity fixes it as well. I have not

reproduced it with a test. That should be the first characterisation test (section 6).

**(b) The manager can't tell the handler its queue is dead.** Because the handler writes straight
to the deque, a discard by the manager is invisible until the next `getActionForBlock` call, and
only if no other queue has appeared for that number in the meantime.

**(c) The interface passes raw queues and bare block numbers.** `registerQueueForBlock` accepts a
queue the handler created, so the manager can't know whether a queue was ever the one it
registered, and `endOfBlock` and `blockIsEnding` carry no attempt identity.

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

|                Today                |     In the record      |
|-------------------------------------|------------------------|
| `queueByBlockMap.get(n)`            | `block.pending`        |
| `blockProofs.get(n)`                | `block.heldProof`      |
| `endBlocksReceived.contains(n)`     | `block.endReceived`    |
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

### 3.5 Handler interaction

The handler holds the `InFlightBlock` instead of a raw `Deque`, and every handler-to-manager call
that concerns an attempt takes the record, not a block number.

```java
// StreamPublisherManager
InFlightBlock beginBlock(long handlerId, long blockNumber);   // replaces registerQueueForBlock
ActionForBlock endOfBlock(InFlightBlock block);                // was endOfBlock(long)
void blockIsEnding(InFlightBlock block);                       // was blockIsEnding(long, long)

// InFlightBlock
boolean append(BlockItemSetUnparsed batch);  // false if this attempt was discarded
boolean isCurrent();                          // still the registered attempt for its number
```

Effects:

- **The record creates the queue.** `handleAccept` calls `beginBlock` and keeps the returned record
  in `currentBlockQueue` (renamed `currentBlock`). This resolves the earlier open question of
  whether the record adopts the handler's deque: it should not, because adopting it is what
  allows a queue the manager never registered.
- **Discard is visible to the producer.** `discard()` marks the record closed. The handler's
  `append` then returns false, and the handler can end the block immediately instead of feeding a
  detached deque. `append` must stay a plain `offer` plus a volatile read of the closed flag, so
  the hot path is unchanged.
- **Ownership is checked by identity.** `getActionForCurrentlyStreaming` becomes
  "ACCEPT iff `inFlight.get(n) == block`", not "a queue exists for `n`". `endOfBlock(block)` and
  `blockIsEnding(block)` become no-ops (apart from logging) when the record is no longer the
  registered one, so a displaced handler can't mark the end of, or remove, another handler's
  attempt. Problem (a) in section 2.2.1 goes away.
- **`isCurrentlyMidBlock`** keeps its meaning (the handler holds a record), but
  `block.isCurrent()` is now available if a handler wants to ask whether the attempt is still
  live.
- **Per-block bytes** (`currentBlockBytes`, checked against `maxTotalBlockBytes` in
  `handleAccept`) could move into the record, but they are handler-local and work today, so this
  proposal leaves them where they are.

### 3.6 What stays the same

- `blocksToResend` and `activeResendBlocks` stay separate. They describe blocks the publisher
  *wants again*, not an in-flight attempt, and have a different lifecycle.
- `lastPersistedBlockNumber`, `nextUnstreamedBlockNumber`, `lastForwardedBlockNumber` and
  `latestAckedBlock` are unchanged. This proposal does not touch the cursors.
- Handler, flow-control and threading state are unchanged.
- The rest of the `StreamPublisherManager` interface (`getActionForBlock`, `closeBlock`,
  `getLatestBlockNumber`, `notifyTooFarBehind`, `signalDataReady` and so on) is unchanged. The
  three attempt-related methods change as described in 3.5.

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
- **Handler-owned queue (resolved).** The record creates the queue and the handler holds the
  record. See 3.5.
- **Test churn.** The three changed methods are called from five test files:
  `LiveStreamPublisherManagerTest` (26 references to `registerQueueForBlock` or
  `blockIsEnding`), `PublisherManagerRegressionTest` (8), `LiveStreamPublisherManagerRecoveryTest`
  (3), `PublisherHandlerAcknowledgeOnlyTest` (1) and the `TestStreamPublisherManager` fixture (2).
  `LiveStreamPublisherManagerTest` also calls the manager's `endOfBlock(long)` 11 times
  (`PublishApiUtility` only builds `BlockEnd` requests and is not affected). Expect a mechanical
  but sizeable update. The signature change is the main cost of this proposal, and the identity
  checks are its main benefit.
- **Behaviour change for displaced handlers.** A handler that today keeps getting ACCEPT after
  being displaced will, after the change, learn its attempt is dead. The handler needs a defined
  response (most likely the same path as SKIP: send SKIP, reset state). That response should be
  chosen and tested deliberately, not left as an accident of `append` returning false.
- **Behaviour to confirm, not assume.** Two current behaviours look accidental and need a
  deliberate decision during implementation rather than being preserved or "fixed" silently:
  `shutdown` not clearing proofs/end markers, and `blockIsEnding` leaving proof/end marker
  behind. The record makes both go away, but a test should pin the intended behaviour.
- **Scope of the review.** This proposal is based on `LiveStreamPublisherManager` and
  of every `PublisherHandler` call site for `registerQueueForBlock`, `endOfBlock`,
  `blockIsEnding` and `currentBlockQueue`. Test code and the `StreamPublisherPlugin` entry
  point were only checked for references, not read in full. The race described in 2.2.1(a) is
  derived from reading the code and has not been reproduced.

## 6. Implementation plan

1. **Characterisation tests first.** Add tests that pin current externally visible behaviour for
   resend, stall, mid-block drop, reset and shutdown, including the #3975 scenario. Add one that
   tries to reproduce 2.2.1(a): handler A is displaced by handler B for the same block, then A
   sends `BlockEnd` or ends mid-block, and the test checks what happens to B's attempt. If it
   shows B's attempt being damaged, that is a bug worth fixing on its own, ahead of the refactor.
2. **Introduce `InFlightBlock`** and the single `inFlight` map alongside the old maps, with the
   manager updating both (no behaviour change; assert they agree in tests).
3. **Switch readers** (`correctForResendAndStreaming`, `clearObsoleteQueueItems`,
   `determineCurrentBlockNumber`, forwarder) to the new map.
4. **Change the handler interface** (`beginBlock`, `endOfBlock(block)`, `blockIsEnding(block)`),
   update `PublisherHandler` to hold the record, and update the tests and the
   `TestStreamPublisherManager` fixture.
5. **Switch writers** to the record's transition methods, and route all removals through
   `discard`.
6. **Delete the old maps** and the guard routines they made necessary.
7. Run the stream-publisher unit tests and the existing end-to-end or simulator publish scenarios
   with multiple publishers, resends and forced stalls.

Each step is independently mergeable, and steps 2 to 4 can be reverted individually.

## 7. Out of scope

- Deriving `lastPersistedBlockNumber`, `latestAckedBlock` and the resend and gap state from
  notifications (tracked separately; some of it needs new messages).
- Removing `blocksClosedComplete`, which duplicates a metric and can be deleted independently.
- Any change to the publish protocol, messaging SPI or other plugins.
