# `state-management-hashgraph` plugin

> **Status:** *Beta, partial.* This slice covers lifecycle, the apply
> pipeline, and snapshot restore. Snapshot creation and the gRPC query API
> are not implemented yet — see Roadmap.

Live Hashgraph state on the Block Node. The plugin subscribes to verified
block-stream notifications and applies `state_changes` items to an
in-memory state store, backed by the same `VirtualMapState` lifecycle
types the consensus node uses.

## How it works

1. **Subscribe.** On `start()` the plugin registers as a
   `BlockNotificationHandler` and receives every `VerificationNotification`
   with `success == true`.
2. **Queue.** Each verified block is parked in a `ConcurrentSkipListMap`
   keyed by block number.
3. **Catch up.** A one-shot `catchUpFromHistoricalBlocks` task runs after
   `start()`. It compares `metadata.blockNumber` to
   `context.historicalBlockProvider().availableBlocks().max()` and pulls
   missing blocks via `block(n)` in batches of `historicCatchUpBatchSize`.
4. **Apply (lag-1 commit).** A single apply-worker thread blocks on a
   queue and drains the pending map in strict block-number order the
   moment a block arrives (no timer). The pipeline runs **three
   concurrent state versions**: **(3)** the live *mutable*
   (`getMutableState()`) receiving the current block's changes; **(2)**
   `hashingImmutable`, the sealed copy awaiting hash + attestation; and
   **(1)** `attestedImmutable`, the network-confirmed copy. A block is
   applied into the live mutable, but only **exposed** once the *next*
   block's footer confirms its root hash. For each block N:
   - **Hash state 2** (post-(N-1)) *now* — the single point at which a
     state is hashed, on promotion, never eagerly at seal — and validate
     N's `BlockFooter.startOfBlockStateRootHash` against it. On mismatch
     the plugin sets `applyHalted=true`, increments `hashMismatchTotal`,
     and refuses further applies.
   - On match, promote state 2 → `attestedImmutable` and record its
     `StateMetadata` with the just-computed hash.
   - Apply N's `state_changes` to the live mutable `BinaryState`:
     | wire variant | `BinaryState` call |
     |---|---|
     | `SingletonUpdateChange` | `updateSingleton(stateId, bytes)` |
     | `MapUpdateChange`       | `updateKv(stateId, keyBytes, valueBytes)` |
     | `MapDeleteChange`       | `removeKv(stateId, keyBytes)` |
     | `QueuePushChange`       | `pushQueue(stateId, bytes)` |
     | `QueuePopChange`        | `popQueue(stateId)` |
   - `lifecycleManager.copyMutableState()` seals post-N; it stays staged
     until block N+1 attests it.
5. **Restore on start.** If a snapshot directory matching the persisted
   `stateMetadata.json` exists under `stateSnapshotRecentPath`, the plugin
   loads it via `lifecycleManager.loadSnapshot(...)` and seeds the lag-1
   bookkeeping so the restored state is already exposed on boot. Falls
   back to genesis if the metadata is missing or its snapshot can't be
   loaded.

## Storage encoding

All values written to `BinaryState` are PBJ-encoded carriers from
`com.hedera.hapi.block.stream.output`:

- Singletons store the bytes of `SingletonUpdateChange`.
- KV entries use `MapChangeKey` bytes as the key and `MapChangeValue`
  bytes as the value.
- Queues store the bytes of each `QueuePushChange`.

## Configuration

Bound under `@ConfigData("state.management")` in `StateManagementConfig`:

|          Property          |                        Default                        |                       Notes                       |
|----------------------------|-------------------------------------------------------|---------------------------------------------------|
| `stateMetadataPath`        | `/opt/hiero/block-node/data/state/stateMetadata.json` | JSON file with the latest metadata                |
| `stateSnapshotRecentPath`  | `/opt/hiero/block-node/data/state/snapshot/recent`    | Snapshot directories, read on restore             |
| `historicCatchUpBatchSize` | `64`                                                  | Blocks fetched per batch during start-up catch-up |

There is no `enabled` flag. Block-Node plugins are active whenever their
jar is on the classpath; opt-in lives in the deployment manifest.

## Failure modes

- **State unreadable at startup.** A corrupt snapshot directory is logged
  at `WARNING` and the plugin continues with the eagerly-created genesis
  state; it does not refuse to start.
- **Hash mismatch.** When a block's `BlockFooter.startOfBlockStateRootHash`
  doesn't equal the current live root hash, the plugin increments
  `hashMismatchTotal`, sets `applyHalted=true`, logs at `ERROR`, and the
  apply loop short-circuits on `applyHalted` for every subsequent block.
- **Malformed `state_changes` item.** Raises `IllegalStateException` from
  the applier; the plugin logs and does **not** advance metadata.
- **Concurrent gap.** A block arriving out of order parks in the pending
  map until its predecessor lands. The apply loop never advances past a
  gap.

## Limitations

- No snapshot creation yet — the plugin can only restore a snapshot
  produced externally; it never writes one itself.
- No gRPC query API yet.
- Hash-mismatch recovery requires operator intervention; apply stays
  halted until restart and there is no automatic rewind/replay.
- Catch-up is sequential and synchronous within batches; very large
  catch-up windows block `ready=true` proportionally.
- The plugin declares three swirlds-library config records
  (`MerkleDbConfig`, `VirtualMapConfig`, `PathsConfig`) alongside its own
  `StateManagementConfig` in `StateManagementConfigExtension`, since the
  swirlds jars don't self-register them. Cosmetic only — a future plugin
  that also needs `VirtualMapStateLifecycleManager` can safely declare the
  same three types without any registration conflict.

## Roadmap

- Snapshot creation (periodic, with retention/pruning).
- `StateService` gRPC query API (`getBinaryKV`/`getBinarySingleton`/
  `getBinaryQueue`).
- Hash-mismatch recovery protocol.
- Merkle proof RPC exposure.
