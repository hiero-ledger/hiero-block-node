# 0009 - Block Hash Structure: 4-Leaf Binary Merkle Tree

Date: 2024-05-16

## Status

Accepted

## Context

Each block must be cryptographically sealed in a way that: (a) chains blocks together via inclusion of the previous block's hash, (b) commits to the full set of transactions and their outputs, (c) commits to the resulting network state, and (d) produces a single compact root hash that can be signed and verified efficiently.

The "block hash" is a fundamental primitive referenced in Mirror Node, JSON RPC Relay, smart contracts, and historical tooling. It must be computed in a streaming fashion — CNs cannot wait until the entire block (including state hash) is available before beginning to emit block items.

## Decision

Each block's root hash is the root of a **4-leaf binary Merkle tree** with leaves:

1. **Previous Block Root Hash** — cryptographically chains blocks.
2. **Input Items Merkle Root** — streaming Merkle root over event and raw transaction items (computed as items are produced).
3. **Output Items Merkle Root** — streaming Merkle root over transaction output, state change, and other output items.
4. **State Root Hash** — root hash of the network state Merkle map after executing all transactions in the block.

The streaming Merkle algorithm (designed by Leemon Baird) allows each subtree root to be computed incrementally as items are written to the stream, without requiring all data to be available upfront. The hash function is SHA-384.

The block hash of the **first block stream block** uses the running hash at the end of the last record file as the "previous block hash" leaf, ensuring continuity with historical tooling that equates record file running hashes with block hashes.

## Consequences

- CNs can begin emitting block items from leaf 2 (inputs) while leaf 4 (state hash) is still being computed, enabling the low-latency streaming architecture in ADR-0008.
- The state hash (leaf 4) is the last item appended, allowing CN to produce an immutable fast copy of state, hash it, then include the hash in the final block item before sending for signing.
- SHA-384 is used throughout the block hash computation and must be consistently applied in all verification implementations (BN, MN, relay). This matches the SHA-384 already used for record-file running-hash continuity, preserving the bridge between the record stream and block stream eras.
- Historical record file block hashes (used by MN, relay, smart contracts) remain valid for blocks prior to cutover; the first block stream block bridges the two formats.
- The block proof (TSS aggregate signature in Phase 2b; RSA proof in Phase 2a) signs the 4-leaf Merkle root hash — not the state root hash directly.
