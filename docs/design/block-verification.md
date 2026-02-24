# Block Verification Design

## Table of Contents

1. [Purpose](#purpose)
2. [Goals](#goals)
3. [Terms](#terms)
4. [Entities](#entities)
5. [Design](#design)
6. [Configuration](#configuration)
7. [Metrics](#metrics)
8. [Exceptions](#exceptions)

## Purpose

The purpose of the Block Verification feature is to ensure that blocks received
from consensus nodes are valid and have not been tampered with. This is achieved
by re-calculating the block hash and verifying it against the signature provided
by the Consensus Node.

## Goals

1. The block-node must re-create the block hash from the block items and verify
   that it matches the hash implied by the signature.
2. If verification fails, the block should be considered invalid, and
   appropriate error-handling procedures must be triggered.

## Terms

<dl>
<dt>Consensus Node (CN)</dt><dd>A node that produces and provides blocks.</dd>
<dt>Block Items</dt><dd>The block data pieces (header, events, transactions,
transaction result, state changes, proof) that make up a block.</dd>
<dt>Block Hash</dt><dd>A cryptographic hash representing the block's integrity.</dd>
<dt>Signature</dt><dd>The cryptographic signature on the block hash, created by the
network aggregation of "share" private keys, as described in the TSS design.</dd>
<dt>Public Key</dt><dd>The public key (a.k.a. Ledger ID) of the network that signed the block.</dd>
<dt>Empty-tree Hash</dt><dd>The hash returned by a streaming Merkle tree hasher when
no leaves were added (i.e. an empty subtree). Since HAPI v0.72 this is
SHA-384(0x00), matching the CN convention.</dd>
</dl>

## Entities

### VerificationServicePlugin

The main plugin class. Implements `BlockNodePlugin`, `BlockItemHandler`, and
`BlockNotificationHandler`. It:

- Receives the stream of block items via the messaging facility.
- On detecting a `BLOCK_HEADER`, creates a `VerificationSession` via
  `HapiVersionSessionFactory` and feeds all subsequent block items into it.
- On session completion, sends a `VerificationNotification` via
  `blockMessaging.sendBlockVerification()`.
- Handles backfilled blocks arriving via `handleBackfilled()`.
- Initialises and owns the `AllBlocksHasherHandler`.
- Tracks all verification metrics.

### HapiVersionSessionFactory

Routes block verification to the correct `VerificationSession` implementation
based on the block's HAPI proto version:

|  HAPI Version   |                          Session                          |
|-----------------|-----------------------------------------------------------|
| >= 0.72.0       | `ExtendedMerkleTreeSession` (real Merkle verification)    |
| 0.64.0 – 0.71.x | `DummyVerificationSession` (placeholder, always succeeds) |
| < 0.64.0        | Throws `IllegalArgumentException`                         |

### VerificationSession

Interface with a single method:

```java
VerificationNotification processBlockItems(BlockItems blockItems) throws ParseException;
```

Returns `null` while the block is incomplete, and a `VerificationNotification`
when the final batch (`isEndOfBlock() == true`) has been processed.

### ExtendedMerkleTreeSession

The full block verification implementation, used for HAPI v0.72.0 and above.

Maintains five `NaiveStreamingTreeHasher` instances — one per block-item
category — that incrementally compute subtree roots as items arrive:

|         Hasher          |                      Block item types                      |
|-------------------------|------------------------------------------------------------|
| `inputTreeHasher`       | `SIGNED_TRANSACTION`                                       |
| `outputTreeHasher`      | `BLOCK_HEADER`, `TRANSACTION_RESULT`, `TRANSACTION_OUTPUT` |
| `consensusHeaderHasher` | `ROUND_HEADER`, `EVENT_HEADER`                             |
| `stateChangesHasher`    | `STATE_CHANGES`                                            |
| `traceDataHasher`       | `TRACE_DATA`                                               |

On finalization, it combines the five subtree roots with the previous block
hash, all-previous-blocks root hash, state root hash, and block timestamp into
a final block hash via `HashingUtilities.computeFinalBlockHash()`. The
signature in the block proof is then verified against that hash.

Empty subtrees use `SHA-384(0x00)` as the empty-tree hash, matching the CN
convention introduced in HAPI v0.72.

### DummyVerificationSession

A placeholder implementation used for HAPI versions 0.64.0 through 0.71.x.
Accumulates block items and unconditionally returns a success notification with
a zeroed hash. Will be replaced with real verification for those versions before
going to production.

### AllBlocksHasherHandler

Maintains a persistent, streaming Merkle tree over the hashes of all
previously verified blocks. Its root hash is included in each block's hash
computation.

On startup it either loads a saved snapshot from disk or rebuilds from the
block store. A scheduled task persists a new snapshot every
`allBlocksHasherPersistenceInterval` seconds. If initialisation fails, the
handler degrades gracefully (`isAvailable()` returns `false`) and verification
falls back to the root hash provided in the block footer.

## Design

1. `VerificationServicePlugin.handleBlockItemsReceived()` receives batches of
   block items from the messaging facility.
2. When a `BLOCK_HEADER` is detected, a new `VerificationSession` is created via
   `HapiVersionSessionFactory`, keyed on the HAPI proto version from the header.
3. Each batch of block items is passed to `currentSession.processBlockItems()`,
   which returns `null` until the final batch (`isEndOfBlock() == true`).
4. `ExtendedMerkleTreeSession` routes each item to the appropriate streaming
   Merkle tree hasher. The `BLOCK_FOOTER` and `BLOCK_PROOF` items are saved for
   finalization.
5. When the final batch arrives, the session computes the final block hash by
   folding the five subtree roots together with the previous-block hash,
   all-previous-blocks root, state root, and timestamp.
6. The TSS-based block proof's signature is verified: `signature == SHA-384(blockHash)`.
7. A `VerificationNotification(success, blockNumber, blockHash, block, source)`
   is returned to the plugin.
8. On success, the plugin sends the notification via `sendBlockVerification()`,
   updates `previousBlockHash`, and appends the block hash to
   `AllBlocksHasherHandler`.
9. On failure, the plugin sends a failure notification (`success = false`).

Sequence Diagram:

```mermaid
sequenceDiagram
    participant M as BlockMessaging
    participant P as VerificationServicePlugin
    participant F as HapiVersionSessionFactory
    participant S as VerificationSession
    participant A as AllBlocksHasherHandler

    M->>P: (1) handleBlockItemsReceived(blockItems)

    alt (2) BLOCK_HEADER detected — start of new block
        P->>A: computeRootHash() / lastBlockHash()
        A-->>P: allPreviousBlocksRootHash, previousBlockHash
        P->>F: createSession(blockNumber, source, hapiVersion, hashes)
        F-->>P: ExtendedMerkleTreeSession or DummyVerificationSession
    end

    P->>S: (3) processBlockItems(blockItems)
    note over S: Accumulates items and hashes; returns null if block incomplete

    alt (4) isEndOfBlock — final batch received
        S->>S: computeFinalBlockHash() via 5 subtree hashers
        S->>S: verifySignature(blockHash, proof.signature)
        S-->>P: VerificationNotification(success, blockNumber, blockHash, block)
    end

    alt (5a) success == true
        P->>M: sendBlockVerification(notification)
        P->>A: appendLatestHashToAllPreviousBlocksStreamingHasher(blockHash)
    else (5b) success == false
        P->>M: sendBlockVerification(failure notification)
    end
```

## Configuration

Configuration class: `VerificationConfig`

|               Property               |                               Default                                |                      Description                       |
|--------------------------------------|----------------------------------------------------------------------|--------------------------------------------------------|
| `allBlocksHasherFilePath`            | `/opt/hiero/block-node/verification/rootHashOfAllPreviousBlocks.bin` | Path for the persistent hasher snapshot                |
| `allBlocksHasherEnabled`             | `true`                                                               | Enable or disable the all-blocks root hash computation |
| `allBlocksHasherPersistenceInterval` | `10` (seconds)                                                       | How often the hasher snapshot is written to disk       |

## Metrics

All metrics are in the `verification` category.

|             Metric             |     Type     |                                Description                                 |
|--------------------------------|--------------|----------------------------------------------------------------------------|
| `verification_blocks_received` | Counter      | Blocks started (one per `BLOCK_HEADER` seen)                               |
| `verification_blocks_verified` | Counter      | Blocks successfully verified                                               |
| `verification_blocks_failed`   | Counter      | Blocks where the header was invalid                                        |
| `verification_blocks_error`    | Counter      | Blocks that triggered an exception or returned a failure notification      |
| `verification_block_time`      | Counter (ns) | Cumulative time spent in the verification handler per block                |
| `hashing_block_time`           | Counter (ns) | Cumulative hashing time, excluding the initial block-header detection step |

## Exceptions

- **SYSTEM_ERROR:** Issues with node configuration or bugs. The node logs
  details, updates metrics, and might attempt recovery or halt.
- **SIGNATURE_INVALID:** If verification fails, `success = false` is reported.
  The block is marked invalid, and publishers may be requested to resend the
  block.

### Signature invalid

If the computed hash does not match the hash signed by the network, the block is
considered unverified. It is marked as such and publishers are requested to
resend the block.
