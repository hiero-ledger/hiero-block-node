# `state-management-hashgraph` plugin

> **Status:** *Beta, partial.* Lifecycle, apply, snapshot restore, and periodic snapshot creation
> are implemented. The gRPC query API is not yet, see Roadmap.

Live Hashgraph state on the Block Node. Subscribes to verified block-stream notifications and
applies their `state_changes` into an in-memory `VirtualMapState`, using the same lifecycle
types the consensus node uses.

## How it works

The apply pipeline runs each block through three state versions, one block behind attestation
("lag-1 commit"):

![Apply pipeline state machine: MUTABLE seals into HASHING, HASHING either promotes to ATTESTED on a matching footer hash or halts on a mismatch](../../docs/assets/state/apply-pipeline-state-machine.svg)

|   Step    |                                                                                                                                       What happens                                                                                                                                        |
|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Subscribe | Registers as a `BlockNotificationHandler`; only `VerificationNotification.success == true` blocks are applied.                                                                                                                                                                            |
| Catch up  | On the catch-up thread, at `start()` and whenever a live block arrives ahead of the next expected one: replays history from the next expected block to the latest, one block at a time. Ready only once the latest block is reached; a stalled sweep is re-driven by the next live block. |
| Apply     | The next expected block is applied inline on the notification thread. A block ahead of it is dropped and triggers a catch-up, so that thread never reads history.                                                                                                                         |
| Restore   | On `start()`, scans `snapshotRecentPath` for the highest-numbered directory, loads it, then checks the loaded state's `BlockStreamInfo.block_number` against the directory name before trusting it.                                                                                       |
| Snapshot  | Every `snapshotIntervalMillis` and on `stop()`, on its own thread: writes the attested state to `snapshotRecentPath/<block>` (staging dir, then atomic rename) and prunes old ones.                                                                                                       |

Full per-block walkthrough: `docs/design/state/live-state.md` §6.

## Storage encoding

All values written to `BinaryState` are the raw payload of the oneof-selected field of each
`com.hedera.hapi.block.stream.output` wrapper, the same bytes the consensus node stores:

- Singletons store the payload of `SingletonUpdateChange`. A partial update is spliced onto the
  prior value first (see `docs/design/state/live-state.md` §7).
- KV entries use the payload of `MapChangeKey` as the key and of `MapChangeValue` as the value.
- Queues store the payload of each `QueuePushChange`.

## Configuration

Bound under `@ConfigData("state.management")` in `StateManagementConfig`, see
`docs/design/state/live-state.md` §4 for the full property table. Snapshot settings:

|            Property            |                      Default                       |        Range         |                                      Notes                                       |
|--------------------------------|----------------------------------------------------|----------------------|----------------------------------------------------------------------------------|
| `snapshotRecentPath`           | `/opt/hiero/block-node/data/state/snapshot/recent` |                      | Snapshot root. Also holds `tmp`, the live MerkleDb, which pruning never touches. |
| `snapshotIntervalMillis`       | `900000` (15m)                                     | `1000` to `86400000` | Fixed delay between snapshots.                                                   |
| `snapshotRecentRetentionCount` | `3`                                                | `1` to `1000`        | Snapshot dirs kept, including the newest.                                        |

There is no `enabled` flag. Block-Node plugins are active whenever their jar is on the
classpath; opt-in lives in the deployment manifest.

## Failure modes

|                                  Failure                                   |                                                                              Behaviour                                                                              |
|----------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| No snapshot directories at startup                                         | Genesis; this is the normal first boot, not a failure.                                                                                                              |
| Snapshot root unlistable, highest directory unreadable or fails self-check | Halt.                                                                                                                                                               |
| No snapshot and first block is N > 0 (no seed state)                       | Halt with a distinct "No seed state" log; not counted as a hash mismatch. Supply a snapshot.                                                                        |
| Footer hash mismatch (`startOfBlockStateRootHash`)                         | Halt; `hashMismatchTotal` incremented.                                                                                                                              |
| Digest-length mismatch (e.g. SHA-256 footer vs. this build's SHA-384)      | Halt, logged distinctly from a genuine divergence.                                                                                                                  |
| Legacy field-2 token-association encoding                                  | Halt with a distinct error, not a generic hash mismatch.                                                                                                            |
| Malformed `state_changes` item                                             | That block's apply aborts; plugin does not advance past it.                                                                                                         |
| Partial update with no prior value                                         | Halt (`PARTIAL_WITHOUT_BASELINE`).                                                                                                                                  |
| Verification gap (block ahead of the next expected)                        | Block dropped; the catch-up thread fetches the missing blocks from `historicalBlockProvider` one at a time. Retried on the next notification if history lacks them. |
| Snapshot write fails                                                       | WARNING, `state_snapshot_failures_total` incremented, staging dir removed; the next run retries.                                                                    |
| Crash mid-snapshot                                                         | Only a `.tmp-<block>` staging dir is left; deleted on the next `start()`.                                                                                           |
| Final snapshot on `stop()` takes over 10 s                                 | WARNING; the write is interrupted and nothing is published. The next start uses the previous snapshot.                                                              |
| Pruning an old snapshot fails                                              | WARNING; the new snapshot is kept and the next snapshot retries the prune.                                                                                          |

A restored snapshot is trusted as a previously stored attested state. Its root hash is first
checked against the chain by the next block's footer, so a node that restores state but ingests
no further blocks relies on snapshot integrity (`docs/design/state/live-state.md` §6.2).

All halts are terminal: recovery is a restart from the last good snapshot, not a live retry.
Snapshot failures never halt apply, and a halted node still snapshots its last attested block.
Full table and recovery semantics: `docs/design/state/live-state.md` §10.

## Reading the attested state

Readers call `reserveAttested()` and read from the returned `ReservedState`, closing it when
done. The reservation keeps the state alive if a newer block is attested meanwhile. It returns
`null` while nothing is attested.

## Limitations

- No gRPC query API yet.
- Hash-mismatch recovery requires a restart; there is no automatic rewind/replay.
- Catch-up applies one block at a time from historical storage; very large catch-up windows
  delay `ready=true` proportionally.
- `StateManagementConfigExtension` also declares `MerkleDbConfig`/`VirtualMapConfig`, since the
  swirlds jars don't self-register them, see that class's javadoc.

## Roadmap

- `StateService` gRPC query API (`getKV`/`getSingleton`/`getQueue`).
- Hash-mismatch recovery protocol.
- Merkle proof RPC exposure.
