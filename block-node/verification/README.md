# verification

Verifies the cryptographic integrity of every block the Block Node receives — whether from a live publisher, backfill, or history replay. Verification results are broadcast back to the rest of the system via `VerificationNotification` so that storage plugins only persist blocks that have passed the check.

---

## Key Files

|                      File                       |                                                                                                                             Purpose                                                                                                                             |
|-------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `VerificationServicePlugin.java`                | Plugin entry point and `BlockNotificationHandler`. Receives block items from the ring buffer, delegates to a per-block `VerificationSession`, persists TSS parameters on first sight of block 0, and broadcasts `VerificationNotification` on block completion. |
| `AllBlocksHasherHandler.java`                   | Maintains a streaming incremental Merkle hasher over every block hash seen by this node. Periodically snaps the hasher state to disk for crash recovery. The root hash is fed into `ExtendedMerkleTreeSession` as `allPreviousBlocksRootHash`.                  |
| `VerificationConfig.java`                       | Configuration record (`@ConfigData("verification")`). Exposes `allBlocksHasherEnabled`, `allBlocksHasherFilePath`, `allBlocksHasherPersistenceInterval` (seconds), and `tssParametersFilePath`.                                                                 |
| `session/HapiVersionSessionFactory.java`        | Routes each block to the correct `VerificationSession` implementation based on the block header's HAPI proto version. Current routing: ≥ 0.72.0 → `ExtendedMerkleTreeSession`; 0.64.0–0.71.x → `DummyVerificationSession`.                                      |
| `session/impl/ExtendedMerkleTreeSession.java`   | Real verification logic. Builds five sub-trees (input, output, consensus header, state changes, trace data), assembles the block root hash, and verifies the TSS threshold signature against the ledger ID.                                                     |
| `session/impl/DummyVerificationSession.java`    | Placeholder that always returns success. Active for HAPI versions 0.64.0–0.71.x. **Must be removed before production deployment** (tracked TODO).                                                                                                               |
| `root_hash_of_all_previous_blocks_hasher.proto` | Protobuf definition for `AllPreviousBlocksRootHashHasherSnapshot` — the serialised state of the Merkle hasher written to disk.                                                                                                                                  |

---

## Notable Logic

### `HapiVersionSessionFactory` — version-gated verification

This is the single most important routing decision in the verification pipeline. Adding support for a new HAPI version or a post-quantum signature scheme means adding a branch here. Ensure the correct session class is wired to the correct version range before any production deployment.

### `ExtendedMerkleTreeSession.getSingle()` — throws on ambiguity

`getSingle(list, predicate)` throws `IllegalStateException` if zero or more than one element matches. The caller checks `if (tssBasedProof != null)` but this null check is unreachable — any block without exactly one TSS proof throws before the null check. Blocks that legitimately have no TSS proof (e.g., pre-TSS era blocks) will cause a crash.

### `AllBlocksHasherHandler.calculateBlockHashFromBlockNumber()` — returns `new byte[0]` for gaps

When a block is missing from the historical store, the method returns an empty byte array instead of `ZERO_BLOCK_HASH` (SHA-384 of `0x00`). Empty bytes used as a Merkle leaf corrupt the root hash computation.

### `VerificationServicePlugin.initializeTssParameters()` — no synchronization

The static fields `tssParametersPersisted`, `activeLedgerId`, and `activeTssPublication` are written without synchronization. Concurrent calls from the live stream handler and backfill handler at block 0 can cause a data race.

### `persistTssParameters()` — direct file write (no temp-then-rename)

Writes directly to the target file path. A JVM crash mid-write will leave a corrupt file that prevents TSS verification on the next startup. Use atomic temp-then-rename (the pattern used in `AllBlocksHasherHandler`) as the fix.
