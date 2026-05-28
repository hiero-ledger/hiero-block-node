# Block Stream Filtering — Design Doc

> Status: **Beta / experimental.** Filtering by block item is in scope for
> v1; filtering by entity is deferred. The new RPCs / fields are
> backwards-compatible: omit the filter to get current behaviour.

## 1. Purpose & Goals

Let operators (on the publish side) and clients (on the subscribe / read
side) trim the block stream to just the `BlockItem` kinds they care about.
A filtered item is replaced by a `FilteredSingleItem` carrying the item
hash and the `SubMerkleTree` slot it occupied — so block-proof verification
of the resulting stream remains possible.

Scope for v1:

1. **`BlockStreamFilter` protobuf** — a single message shape applied
   everywhere, so the operator-facing config and the client-facing request
   fields use one schema.
2. **Publish-side filtering** — `StreamPublisherPlugin` drops items at
   ingress before the messaging facility ever sees them. This means the
   block-node never persists or streams what was filtered. Lossy.
3. **Subscribe-side filtering** — `BlockStreamSubscriberSession` honours
   the filter on the `SubscribeStreamRequest`. Lossless on the BN; only the
   outbound stream is trimmed.
4. **Shared helper in `block-node/base`** — one `BlockItemFilter` class
   that both plugins consume, so allow/deny semantics stay consistent.

Out of scope for v1:

- **Entity-level filtering** (filter by account-id, token-id, …). Design
  note only.
- **`block-access` integration** — design captured here (§7), not wired
  into code in this spike.
- **Mandatory items.** The plan suggests filtering should never drop items
  that the block proof / verifier requires (`BlockHeader`, `BlockProof`,
  `BlockFooter`). v1 enforces this by always preserving these regardless
  of filter setting.

## 2. Architecture

```
                ┌───────────────────────────────────────┐
publisher  ──►  │ StreamPublisherPlugin                 │
gRPC stream     │   PublisherConfig.blockStreamFilter   │
                │       │                               │
                │       ▼                               │
                │   BlockItemFilter.apply(items)        │
                │       │                               │
                │       ▼ filtered items                │
                │   messaging facility                  │
                └────────┬──────────────────────────────┘
                         │
            ┌────────────┴────────────────┐
            ▼                             ▼
      verification                  blocks-files-recent / historic

                ┌───────────────────────────────────────┐
subscriber ──►  │ StreamSubscriberPlugin                │
gRPC subscribe  │   SubscribeStreamRequest.filter       │
                │       │                               │
                │       ▼                               │
                │ BlockStreamSubscriberSession          │
                │   BlockItemFilter.apply(items)        │
                │       │                               │
                │       ▼                               │
                │   responsePipeline.onNext(filtered)   │
                └───────────────────────────────────────┘
```

Two filter call sites; one shared helper. No new ring-buffer traffic, no
new plugin.

## 3. `BlockStreamFilter` message

Single shape, used by publish-side config and subscribe-side request.

```proto
// In protobuf-sources/.../shared_message_types.proto
message BlockStreamFilter {
    /**
     * Allow- vs deny-list semantics.
     *
     * If true,  ONLY items whose oneof field number is in
     *           `block_item_types` are forwarded.
     * If false, items whose oneof field number is in
     *           `block_item_types` are DROPPED; all others are
     *           forwarded.
     *
     * BlockHeader (1), BlockProof (9), and BlockFooter (12) are always
     * forwarded regardless of the filter — they are required for block
     * proof verification.
     */
    bool include = 1;

    /**
     * Field numbers of the `BlockItem.item` oneof variants this filter
     * applies to. See block_item.proto for the canonical mapping;
     * common values: 2 EventHeader, 3 RoundHeader, 4 SignedTransaction,
     * 5 TransactionResult, 6 TransactionOutput, 7 StateChanges,
     * 10 RecordFile, 11 TraceData.
     */
    repeated uint32 block_item_types = 2;
}
```

**Supported filter targets.** Only the following `BlockItem.item` field
numbers may appear in `block_item_types`:

| Field | Variant |
|-------|---------|
| 2  | `EventHeader` |
| 3  | `RoundHeader` |
| 4  | `SignedTransaction` |
| 5  | `TransactionResult` |
| 6  | `TransactionOutput` |
| 7  | `StateChanges` |
| 10 | `RecordFile` |
| 11 | `TraceData` |

Any other field number is rejected as an invalid request. In particular:

- **1, 9, 12** (`BlockHeader`, `BlockProof`, `BlockFooter`) are required
  by the block proof tree and cannot be filtered.
- **8, 19** (`FilteredSingleItem`, `RedactedItem`) are already filter
  markers; re-filtering them is meaningless.

The combination `include=true` with an empty `block_item_types` list is
also rejected (it would deny everything).

Encoding choice — `uint32` for the item type tag rather than an enum
mirror — keeps the filter forward-compatible with new oneof variants.
Adding a new `BlockItem.item` variant to the consensus-node proto does
not require a block-node protobuf change; it does, however, require the
implementation's supported-types list to be widened before the new
variant can be filtered.

## 4. `BlockItemFilter` helper (`block-node/base`)

Single, stateless utility in `org.hiero.block.node.base.filter`:

```java
public final class BlockItemFilter {

    /** Field numbers a filter MUST always forward (proof scaffolding). */
    private static final Set<Integer> ALWAYS_FORWARD = Set.of(
            BlockItemUnparsed.ItemOneOfType.BLOCK_HEADER.protoOrdinal(),     // 1
            BlockItemUnparsed.ItemOneOfType.BLOCK_PROOF.protoOrdinal(),      // 9
            BlockItemUnparsed.ItemOneOfType.BLOCK_FOOTER.protoOrdinal());    // 12

    /**
     * @return whether the filter accepts the given item without
     *         transformation.
     */
    public boolean accepts(BlockItemUnparsed item);

    /**
     * Apply the filter to a list of items. Items the filter rejects are
     * replaced by a {@code FilteredSingleItem} carrying:
     *   - item_hash : the SHA-384 of the unparsed item bytes
     *   - tree      : a {@link SubMerkleTree} derived from the original
     *                 field number modulo 10 (see plan §protobuf-sources).
     *
     * Returns a new list; never mutates the input.
     */
    public List<BlockItemUnparsed> apply(List<BlockItemUnparsed> items);
}
```

The filter is constructed from a `BlockStreamFilter` proto. When the proto
is null or its `block_item_types` list is empty, `apply` is the identity
function (returns the input unchanged) so disabled / unset filters are
zero-cost.

`SubMerkleTree` derivation table (from `block_item.proto` mod-10 rule):

| Field number → mod 10 | `SubMerkleTree` value |
|---|---|
| 0 (none in current oneof) | — |
| 1 — `SignedTransaction` (4 % 10 = 4) — see below | INPUT_ITEMS_TREE |
| 2 — `EventHeader` (2), `RoundHeader` (3) | CONSENSUS_HEADER_ITEMS |
| 3 — `StateChanges` (7) | STATE_CHANGE_ITEMS_TREE |
| 4 — `TraceData` (11) | TRACE_DATA_ITEMS_TREE |
| `BlockHeader`(1), `TransactionResult`(5), `TransactionOutput`(6), `RecordFile`(10) | OUTPUT_ITEMS_TREE |
| `BlockProof`(9), `BlockFooter`(12) | NOT_HASHED (never filtered) |

`item_hash` is `SHA-384(BlockItemUnparsed.PROTOBUF.toBytes(item))` — matches
the verifier's existing hash algorithm.

## 5. `stream-publisher` integration

Per the recon, each inbound publisher connection has its own
`PublisherHandler` (created in `LiveStreamPublisherManager.addHandler`).
Filter state is therefore per-connection.

Wire-up:

- **`PublisherConfig`** gains a single optional field —
  `BlockStreamFilter blockStreamFilter`. Default constructed to the
  identity filter (include=false, empty list) → no-op behaviour.
- **`PublisherHandler`** constructor builds a `BlockItemFilter` from the
  config. If the filter is identity, `accepts` is short-circuited.
- **`handleAccept`** (line ~629) inserts a single call
  `final List<BlockItemUnparsed> kept = blockItemFilter.apply(items);`
  before items are placed on `currentBlockQueue` and forwarded to
  `publisherManager.signalDataReady()`.

Notable: this is **lossy** at the BN — filtered items are replaced by
`FilteredSingleItem` *at ingress*, so persistence, verification, and
subscription all see the trimmed stream. The plan calls this out
explicitly.

## 6. `stream-subscriber` integration

Per the recon, each inbound subscribe RPC has its own
`BlockStreamSubscriberSession`. Filter state is per-session.

Wire-up:

- **`SubscribeStreamRequest`** gains a `BlockStreamFilter filter = 3;`
  field (proto-level, optional).
- **`BlockStreamSubscriberSession`** constructor reads the request's
  filter and builds a `BlockItemFilter`. Identity filter when absent.
- **`sendOneBlockItemSet`** (line ~824) calls `apply` on the list of
  `BlockItemUnparsed` before constructing the `BlockItemSetUnparsed` it
  hands to `responsePipeline.onNext`.

This is **lossless** at the BN — the underlying storage and the messaging
ring buffer still see the full stream. Only what this particular
subscriber sees is filtered.

## 7. `block-access` (design only, not implemented)

Goal: same `BlockStreamFilter`, applied per request, on the unary read path.

Wire-up plan:

- Extend `BlockRequest` with an optional `BlockStreamFilter filter`.
- In `BlockAccessServicePlugin.getBlockUnparsed`, after
  `accessor.blockUnparsed()` returns a `BlockUnparsed`, apply
  `BlockItemFilter.apply(block.blockItems())` before returning the
  response.

This is **lossless** at the BN — same property as subscribe.

Implementation is deferred so the spike scope stays tight. The ticket for
this is captured but its acceptance is design + approval, not code.

## 8. Configuration

| Property                                    | Default     | Notes                                          |
|---------------------------------------------|-------------|------------------------------------------------|
| `producer.blockStreamFilter.include`        | `false`     | Disabled by default — full pass-through.        |
| `producer.blockStreamFilter.blockItemTypes` | empty       | Field numbers to deny (or allow when include). |
| `subscriber.blockStreamFilter.include`      | `false`     | Subscriber-side default for clients that don't send one. |
| `subscriber.blockStreamFilter.blockItemTypes` | empty    | —                                              |

A subscriber's request always wins over the server-side default.

## 9. Failure modes

| Failure                                                  | Behaviour                                                                                  |
|----------------------------------------------------------|--------------------------------------------------------------------------------------------|
| Filter names a mandatory item (1/9/12)                    | `BlockItemFilter.from` throws `IllegalArgumentException`. Subscriber call path surfaces `Code.INVALID_REQUEST`; publisher fails fast at startup. |
| Filter names a filter marker (8 or 19)                    | Same as above — invalid request.                                                            |
| Filter names a field number outside the supported list    | Same as above — invalid request.                                                            |
| `include=true` with empty `block_item_types`              | Same as above — invalid request (would deny everything).                                    |
| `include=false` with empty `block_item_types`             | Identity behaviour; `accepts` always true.                                                  |
| Hash computation fails                                    | Should not happen (PBJ serialisation is total over `BlockItemUnparsed`); rethrow as `IllegalStateException`. |

### Observability (deferred — see `expansion.md` §5.2)

Today the helper has no metrics. The recommended additions are:

- **`filter.items_filtered_total{sub_merkle_tree, plugin}`** —
  counter keyed by the `SubMerkleTree` of the dropped item and by
  `plugin` ∈ `{publisher, subscriber, block-access}`. Operators get a
  per-tree drop rate and can compare ingress vs. egress filtering.
- **`filter.invalid_requests_total{plugin, reason}`** — counter for
  rejected filter specs, keyed by rejection reason
  (`unsupported_type`, `empty_allowlist`). Useful for detecting clients
  misusing the API.
- **`filter.active_filters{plugin}`** — gauge of currently-active
  non-identity filters. For the subscriber this is the number of open
  sessions whose request carried a filter; for the publisher it is 0
  or 1 depending on config.

These would live next to the existing publisher / subscriber metrics in
each plugin's `*ServicePlugin` (the registries already exist) and are
captured as a follow-up ticket.

### Filter composition

When the publisher filters at ingress and a subscriber subsequently
applies its own filter, the subscriber's filter sees a stream that is
*already* free of the publisher-filtered items — those slots are
`FilteredSingleItem`. The subscriber filter does NOT re-filter
`FilteredSingleItem` (it's in `ALWAYS_FORWARD`), so the output remains
well-formed and the original drop is not double-counted. In practice:

- **Publisher denylist [STATE_CHANGES]** + **subscriber denylist [STATE_CHANGES]**:
  subscriber sees one `FilteredSingleItem` per state-change slot (from
  the publisher), not two.
- **Publisher denylist [STATE_CHANGES]** + **subscriber denylist [TRACE_DATA]**:
  subscriber sees `FilteredSingleItem` for both kinds.
- **Publisher denylist [STATE_CHANGES]** + **subscriber allowlist [STATE_CHANGES]**:
  subscriber sees no state-change items at all (the publisher already
  dropped them and the subscriber's allowlist does not list
  `FilteredSingleItem`, but `FilteredSingleItem` is in
  `ALWAYS_FORWARD` so it passes anyway). Net: the same stream the
  subscriber would have seen with no filter — composition cannot
  un-drop a publisher-filtered item.

This is intentional: filtering is monotone — once an item is replaced
by its hash, no later filter can recover it.

## 10. Acceptance tests

Unit (`block-node/base`):

1. Allowlist: filter with `include=true, types=[11]` (allow trace_data) drops everything else (except mandatory items) and emits `FilteredSingleItem` for each drop, preserving the original list length.
2. Denylist: filter with `include=false, types=[7]` (drop state_changes) leaves all other variants untouched.
3. `BlockHeader`/`BlockProof`/`BlockFooter` are never replaced — they are mandatory and pass through even when the allowlist omits them.
4. Empty denylist is the identity function.
5. Each generated `FilteredSingleItem.tree` matches the SubMerkleTree the original item maps to.
6. `BlockItemFilter.from` rejects (a) any `block_item_type` outside the supported set, and (b) `include=true` with an empty list.

Plugin-level:

6. `stream-publisher` with config `denylist=[STATE_CHANGES]`: publish a block containing a `StateChanges` item; verify the messaging facility receives the block without state_changes, replaced by `FilteredSingleItem`.
7. `stream-subscriber` with request filter `denylist=[TRANSACTION_RESULT]`: subscribe, publish a block, verify the response stream has `FilteredSingleItem` in place of every `TransactionResult` block item.

E2E (`tools-and-tests/suites`):

8. Boot BlockNodeApp, publish three chained blocks each with a denylist filter set in `PublisherConfig`, then subscribe without a filter and confirm the dropped item types are absent (replaced by `FilteredSingleItem`).

## 11. Open questions

- **Per-block-item filtering vs per-tree filtering.** v1 operates at the
  block-item granularity. Per-`SubMerkleTree` filtering (drop an entire
  sub-tree, emit `FilteredMerkleSubTree`) is a natural extension — see
  `expansion.md`.
- **Verifier interaction.** When the publisher filters at ingress, the
  verification plugin sees `FilteredSingleItem` items in the block. The
  verifier already handles those (the plan calls this out under
  `### verification`); STORY-F6 must include a test that the verifier
  still accepts a filtered block.
- **Entity filtering.** A future iteration adds a higher-level filter
  (e.g. "only events involving account-id X"). This requires parsing the
  signed transaction body, which is a different operation than the
  oneof-level filter here.

## 12. References

- `agent/ref/state-and-filtering/state-and-filtering-plan.md` — source plan.
- `hiero-consensus-node/.../block/stream/block_item.proto` — canonical
  `BlockItem.item` oneof, `FilteredSingleItem`, `SubMerkleTree`.
- `agent/proposals/state-and-filtering/live-state/fyi-to-plan.md` — plan
  corrections (filtering section appended).
- `agent/proposals/state-and-filtering/block-stream-filtering/expansion.md` —
  follow-ups + considerations.
