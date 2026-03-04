# TSS Block Proof Verification

## Purpose

Cryptographically confirms that blocks were signed by a threshold of network stake using TSS.
For the broader block verification design, see
[`docs/design/block-verification.md`](block-verification.md).

## Terms

**LedgerId** — Network identity used as root of trust for `TSS.verifyTSS()`. Published in
block 0 via `LedgerIdPublicationTransactionBody`.

**TssSignedBlockProof** — A `BlockProof` whose `blockSignature` layout depends on WRAPS
availability:

- **Pre-settled** (WRAPS not yet available):
  `vk (1,096) || blsSig (1,632) || aggregate_schnorr_sig (192)` = 2,920 bytes
- **Post-settled** (WRAPS available):
  `vk (1,096) || blsSig (1,632) || wraps_compressed_proof (704)` = 3,432 bytes

Both variants are handled internally by `TSS.verifyTSS()`.

## Design

Block items stream into `ExtendedMerkleTreeSession`, which hashes them into five subtree hashers
by kind. `SIGNED_TRANSACTION` items in block 0 are also scanned for
`LedgerIdPublicationTransactionBody` to bootstrap TSS state.

On end-of-block, the session computes the block root hash and calls
`TSS.verifyTSS(ledgerId, signature, hash)` for signatures >= 1,096 bytes. The library handles
dispatch between the genesis Schnorr path and the settled WRAPS path internally.

## Ledger ID Bootstrap

`ACTIVE_LEDGER_ID` is process-level state initialized from one of three sources:

1. **Persisted file** (`verification.ledgerIdFilePath`) — written by block 0, loaded on restart.
2. **Config string** (`verification.ledgerId`) — runtime-only seed for nodes joining a network
   mid-stream after block 0 has already passed. Never persisted.
3. **Block 0** — always authoritative. Overwrites both in-memory state and the persisted file.

## Configuration

|            Property             |                      Default                       |        Description         |
|---------------------------------|----------------------------------------------------|----------------------------|
| `verification.ledgerId`         | `""`                                               | Hex-encoded ledger ID seed |
| `verification.ledgerIdFilePath` | `/opt/hiero/block-node/verification/ledger-id.bin` | Ledger ID persistence path |
