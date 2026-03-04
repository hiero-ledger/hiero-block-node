# TSS Block Proof Verification

## Purpose

Cryptographically confirms that blocks received from a Consensus Node were signed by a threshold
of network stake using TSS. For the broader block verification design, see
[`docs/design/block-verification.md`](block-verification.md).

## Terms

<dl>
<dt>LedgerId</dt>
<dd>The network identity used as the root of trust for <code>TSS.verifyTSS()</code>. Published
in block 0 via <code>LedgerIdPublicationTransactionBody</code>.</dd>

<dt>TssSignedBlockProof</dt>
<dd>A <code>BlockProof</code> carrying a composite <code>blockSignature</code>:
<code>vk (1,096 bytes) || blsAggregateSig (1,632 bytes) || chainOfTrustProof</code>.
The chain-of-trust proof suffix is either 192 bytes (genesis) or 704 bytes (post-genesis).</dd>

<dt>chainOfTrustProof</dt>
<dd>Suffix of <code>blockSignature</code> that proves the VK was derived from the network address
book. Handled internally by <code>TSS.verifyTSS()</code>.</dd>
</dl>

## Design

As block items stream in, each is routed to one of five subtree hashers:

| Block item kind                            | Subtree hasher        |
|--------------------------------------------|-----------------------|
| `BLOCK_HEADER`                             | outputTreeHasher      |
| `ROUND_HEADER`, `EVENT_HEADER`             | consensusHeaderHasher |
| `SIGNED_TRANSACTION`                       | inputTreeHasher       |
| `TRANSACTION_RESULT`, `TRANSACTION_OUTPUT` | outputTreeHasher      |
| `STATE_CHANGES`                            | stateChangesHasher    |
| `TRACE_DATA`                               | traceDataHasher       |
| `BLOCK_FOOTER`, `BLOCK_PROOF`              | not hashed            |

`SIGNED_TRANSACTION` items in block 0 are also scanned for `LedgerIdPublicationTransactionBody`
to bootstrap TSS native state (address book, WRAPS VK, ledger ID). Scanning stops once found.

On end-of-block, `HashingUtilities.computeFinalBlockHash()` assembles the 48-byte block root
hash, then `verifySignature()` dispatches on `blockSignature.length()`:

- **48 bytes** — legacy path: `SHA384(blockHash)` used by non-TSS test blocks (temporary).
- **&lt; 1,096 bytes** — malformed; returns `false`.
- **&ge; 1,096 bytes** — delegates to `TSS.verifyTSS(ledgerId, signature, hash)`.
  Returns `false` if `ACTIVE_LEDGER_ID` is not yet initialized.

## Ledger ID Bootstrap

`ACTIVE_LEDGER_ID` is process-level state shared across all per-block sessions. It is
initialized from one of three sources, in priority order on startup:

1. **Persisted file** (`verification.ledgerIdFilePath`) — written by block 0, loaded on restart.
2. **Config string** (`verification.ledgerId`) — runtime-only seed for nodes joining an
   established network mid-stream after block 0 has already passed. Never persisted.
3. **Block 0** — always authoritative. Overwrites both in-memory state and the persisted file.

## Configuration

| Property                              | Default                                                          | Description                            |
|---------------------------------------|------------------------------------------------------------------|----------------------------------------|
| `verification.ledgerId`               | `""`                                                             | Hex-encoded ledger ID seed (see above) |
| `verification.ledgerIdFilePath`       | `/opt/hiero/block-node/verification/ledger-id.bin`               | Ledger ID persistence path             |
