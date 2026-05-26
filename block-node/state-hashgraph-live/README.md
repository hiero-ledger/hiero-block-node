# `state-hashgraph-live` plugin

> **Status:** *Beta.* Not yet shipped in the default chart manifest. The
> plugin jar is included only in opt-in deployments.

Live Hashgraph state on the Block Node. The plugin subscribes to verified
block-stream notifications, applies `state_changes` items to an in-memory
state store, periodically snapshots that store to disk, and exposes three
gRPC methods for clients that want to read network state without running a
Consensus Node.

## How it works

1. **Subscribe.** On `start()` the plugin registers as a
   `BlockNotificationHandler` and receives every `VerificationNotification`
   with `success == true`.
2. **Queue.** Each verified block is parked in a `ConcurrentSkipListMap` keyed
   by block number.
3. **Apply.** A single-threaded scheduled executor drains the map in strict
   block-number order. For each block, `StateChangeApplier` walks the block
   items and translates every `state_changes` mutation into a
   call on the in-memory `LiveState`:
    | wire variant | LiveState call |
    |---|---|
    | `SingletonUpdateChange` | `updateSingleton(stateId, bytes)` |
    | `MapUpdateChange`       | `updateKv(stateId, keyBytes, valueBytes)` |
    | `MapDeleteChange`       | `removeKv(stateId, keyBytes)` |
    | `QueuePushChange`       | `pushQueue(stateId, bytes)` |
    | `QueuePopChange`        | `popQueue(stateId)` |
4. **Track metadata.** After each block the plugin updates `StateMetadata`
   (block number, round number from `RoundHeader`, SHA-384 state root hash,
   live state size) and emits a `StateUpdateNotification(VERIFIED, …)`.
5. **Snapshot.** A second scheduled executor calls `saveSnapshot()` every
   `state.live.snapshotIntervalMillis`. The snapshot writes the binary
   contents of `LiveState` to
   `<recent>/<blockNumber>/live-state.bin` (atomic), prunes older
   `<recent>/<otherBlock>` directories, and rewrites `stateMetadata.json`
   atomically. A `StateUpdateNotification(SNAPSHOT, …)` follows.

## Storage encoding

All values stored in `LiveState` are PBJ-encoded carriers from
`com.hedera.hapi.block.stream.output`:

- Singletons store the bytes of `SingletonUpdateChange`.
- KV entries use `MapChangeKey` bytes as the key and `MapChangeValue`
  bytes as the value.
- Queues store the bytes of each `QueuePushChange`.

This keeps the apply path codec-free and gives clients a single, consistent
shape to send back through the gRPC reads.

## Configuration

Bound under `@ConfigData("state.live")` in `LiveStateConfig`:

| Property                      | Default                                                | Notes                              |
|-------------------------------|--------------------------------------------------------|------------------------------------|
| `stateMetadataPath`           | `/opt/hiero/block-node/data/state/stateMetadata.json`  | JSON file with the latest metadata |
| `stateSnapshotRecentPath`     | `/opt/hiero/block-node/data/state/snapshot/recent`     | One subdir per most-recent block   |
| `stateSnapshotHistoricPath`   | `/opt/hiero/block-node/data/state/snapshot/historic`   | Reserved for archival (deferred)   |
| `snapshotIntervalMillis`      | `900000` (15 min)                                      | Rate of `saveSnapshot()`           |
| `stateChangesApplyIntervalMillis` | `2000` (2 s)                                       | Rate of the apply loop             |
| `historicCatchUpBatchSize`    | `64`                                                   | Reserved for catch-up replay       |

There is no `enabled` flag. Block-Node plugins are active whenever their jar
is on the classpath; opt-in lives in the deployment manifest.

## gRPC API

The plugin implements `org.hiero.block.api.StateServiceInterface`:

```
rpc getBinaryKV(BinaryStateQuery) returns (BinaryStateQueryResponse);
rpc getBinarySingleton(BinaryStateQuery) returns (BinaryStateQueryResponse);
rpc getBinaryQueue(BinaryStateQuery) returns (BinaryStateQueryResponse);
```

Request rules (enforced in code):

- `state_id` is required.
- `key_bytes` is required for `getBinaryKV`, forbidden for the other two.
- `queue_index` may be set for `getBinaryQueue` (`0` returns the whole queue).
- `block_number` is either `0` (latest) or must equal the current applied
  block; older block numbers are answered with `INVALID_REQUEST`.

Every response carries the current `StateMetadata` so the client can decide
whether to retry against a different Block-Node or wait.

## Failure modes

- **State unavailable at startup.** A corrupt snapshot file is logged at
  `WARNING` and the plugin resets to an empty in-memory state; it does not
  refuse to start.
- **Malformed `state_changes` item.** Raises `IllegalStateException` from the
  applier; the plugin logs and does **not** advance metadata.
- **Concurrent gap.** A block arriving out of order parks in the pending
  map until its predecessor lands. The apply loop never advances past a
  gap.

## Limitations (v1)

- Latest applied state only; no historical state queries.
- Tar archival of older snapshots is not yet wired (`historicPath` exists
  but is unused).
- Full Merkle proof generation is out of scope — the `state_root_hash` in
  `StateMetadata` is the SHA-384 of the canonical encoding (see
  `LiveState.computeHash`), suitable for change detection but not yet
  cryptographically verifiable against the consensus-node's virtual map.

## Roadmap

The next iteration covers tar archival, historical state queries, and an
optional swirlds-state-impl-backed alternative implementation if cross-repo
hash compatibility becomes a hard requirement. See
[design doc](../../docs/design/state/live-state.md) and
[expansion notes](../../agent/proposals/state-and-filtering/live-state/expansion.md)
for follow-ups.
