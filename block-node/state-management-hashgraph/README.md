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
   `stateMetadata.json` exists under `snapshotRecentPath`, the plugin
   loads it via `lifecycleManager.loadSnapshot(...)` and seeds the lag-1
   bookkeeping so the restored state is already exposed on boot. Falls
   back to genesis only when there is no persisted metadata at all; if
   metadata names a block but its snapshot is missing, unreadable, or its
   hash doesn't match, the plugin halts instead (see Failure modes).

## Storage encoding

All values written to `BinaryState` are the raw payload of the oneof-selected field of each
`com.hedera.hapi.block.stream.output` wrapper, the same bytes the consensus node stores:

- Singletons store the payload of `SingletonUpdateChange`.
- KV entries use the payload of `MapChangeKey` as the key and of `MapChangeValue` as the value.
- Queues store the payload of each `QueuePushChange`.

## Configuration

Bound under `@ConfigData("state.management")` in `StateManagementConfig` — see
`docs/design/state/live-state.md` §4 for the full property table.

There is no `enabled` flag. Block-Node plugins are active whenever their
jar is on the classpath; opt-in lives in the deployment manifest.

## Failure modes

- **State unreadable at startup.** If persisted metadata names a block and its
  snapshot is missing or unreadable (I/O failure), the plugin logs at `ERROR`
  and halts (`applyHalted=true`) rather than silently starting from genesis.
- **Snapshot hash mismatch at startup.** When a loaded snapshot's root
  hash doesn't match the persisted metadata's `stateRootHash`, the
  plugin treats it the same as a block-apply hash mismatch: halts
  (`applyHalted=true`) rather than silently serving a stale/corrupt
  snapshot.
- **Hash mismatch.** When a block's `BlockFooter.startOfBlockStateRootHash`
  doesn't equal the current live root hash, the plugin increments
  `hashMismatchTotal`, sets `applyHalted=true`, logs at `ERROR`, and the
  apply loop short-circuits on `applyHalted` for every subsequent block.
- **Malformed `state_changes` item.** Raises `IllegalStateException` from
  the applier; the plugin logs and does **not** advance metadata.
- **Concurrent gap.** A block arriving out of order parks in the pending
  map until its predecessor lands. A gap retry sweeps everything
  `historicalBlockProvider` currently has from the next expected block
  onward (not just the one missing block), on a delay, until the gap
  fills; `pendingBlocks` is capped (`pendingBlocksMax`) so a long-lived
  gap can't grow it unboundedly.

## Limitations

- No snapshot creation yet — the plugin can only restore a snapshot
  produced externally; it never writes one itself.
- No gRPC query API yet.
- Hash-mismatch recovery requires operator intervention; apply stays
  halted until restart and there is no automatic rewind/replay.
- Catch-up is sequential and synchronous within batches; very large
  catch-up windows block `ready=true` proportionally.
- The plugin also declares `MerkleDbConfig`/`VirtualMapConfig` in
  `StateManagementConfigExtension`, since the swirlds jars don't
  self-register them — see that class's javadoc for details.

## Roadmap

- Snapshot creation (periodic, with retention/pruning).
- `StateService` gRPC query API (`getBinaryKV`/`getBinarySingleton`/
  `getBinaryQueue`).
- Hash-mismatch recovery protocol.
- Merkle proof RPC exposure.
