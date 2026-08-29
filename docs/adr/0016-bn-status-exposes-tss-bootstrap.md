# 0016 - BN Status Endpoint Exposes TSS Bootstrap Details

Date: 2024-12-01

## Status

Accepted

## Context

When a new Tier 1 Block Node joins the network — or recovers after a reset — it needs to bootstrap critical TSS context that is embedded in the block stream history: the `ledger_id`, the current `roster`, and the `wraps_verification_key`. Without these, a BN cannot verify block proofs from the network or serve verified blocks to consumers.

Rather than requiring new BNs to replay the entire block history to extract this context (which could take hours or days), a lightweight bootstrap path is needed that allows a BN to query its peers for this information and begin verifying blocks immediately.

The special-purpose record-wrapping BN (which watches for TSS system transactions in the record stream) also needs to communicate these details to Tier 1 BNs.

## Decision

Each Block Node exposes TSS bootstrap details on its **`/status` (or `blockNodeStatus`) endpoint**:

- `ledger_id`: The network ledger identifier.
- `roster`: The current Consensus Node roster (node IDs, public keys, weights).
- `wraps_verification_key`: The current hinTS aggregate public key used to verify block proofs.

New or recovering Tier 1 BNs query the special-purpose record-wrapping BN and/or peer Tier 1 BNs to retrieve these details before beginning block verification. Once a BN has bootstrapped TSS context from a trusted peer, it validates the context against block proofs in the stream before accepting it as authoritative.

The `isLiveProofWraps` flag was considered but removed from the endpoint after evaluation — this status is derivable from the block stream itself.

## Consequences

- New BNs can come online and begin verifying blocks within minutes rather than replaying full history.
- The status endpoint becomes a trust anchor for BN-to-BN bootstrapping; its data must be accurate and tamper-evident.
- BN operators should treat the status endpoint as sensitive for TSS data exposure — only accessible to whitelisted BN peers and Hedera internal systems.
- If a BN's status endpoint serves incorrect TSS details (e.g. due to a bug or compromise), it could cause peer BNs to fail verification; cross-referencing from multiple peers mitigates this risk.
- This design enables the special-purpose wrapping BN to serve a dual role: wrapping historical records and providing TSS bootstrap context to Tier 1 BNs.
