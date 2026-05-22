# 0011 - TSS Aggregate Signatures Replace Per-Node RSA Signatures for Block Proofs

Date: 2025-01-01

## Status

Accepted

## Context

Block proofs — the cryptographic attestation that a block is valid and produced by the network — must be verifiable by any consumer (BN, MN, relay) without requiring trust in a central authority. In the record stream era, every Consensus Node signed each record file with its individual RSA private key, and verifiers collected a threshold of signatures to establish consensus. This approach has two limitations: (1) proof size scales linearly with roster size, and (2) all individual node keys must be known and trusted by verifiers, which doesn't scale with a growing, rotating roster.

TSS (Threshold Signature Scheme) produces a single aggregate signature over a threshold of node shares, with proof size constant regardless of roster size. The Hedera-specific construction is **hinTS** (HIP-1200), which uses BLS-based threshold signatures with a publicly verifiable key aggregation ceremony.

The question for Phase 2a was whether to require TSS immediately or to allow a transitional RSA-based proof format.

## Decision

**Phase 2a**: Block proofs use `SignedRecordFileProof` — the existing RSA multi-signature format containing gossiped signatures from all nodes in the current roster. This is the same verification logic already implemented by Mirror Nodes and the WRB CLI. TSS is **not** required.

**Phase 2b (full cutover)**: Block proofs migrate to **hinTS TSS aggregate signatures** per HIP-1200. RSA-based proofs are retired. Consumers (BN, MN, relay) must adopt the TSS verification library before Phase 2b.

The TSS signing ceremony is executed as part of the Phase 2b network upgrade, establishing the initial threshold key aggregation on-chain.

Right after genesis (and during early block stream history), initial blocks may be verified using their aggregate Schnorr signature until a compressed WRAPS proof becomes available.

## Consequences

- Phase 2a decouples BN deployment from TSS readiness, allowing BNs to go live with familiar RSA verification.
- Phase 2b requires coordination across CN (signing), BN (verification), and MN (verification) teams to adopt the TSS library simultaneously.
- Verifiers in Phase 2b no longer need the full per-node public key list — only the aggregated TSS public key and the roster are needed.
- Proof size in Phase 2b becomes constant, making block proofs more efficient at scale and suitable for light client verification.
- The BN exposes TSS bootstrap details (`ledger_id`, `roster`, `wraps_verification_key`) on its status endpoint, allowing new BNs to bootstrap TSS info from peers during Phase 1 and Phase 2a.
- Key rotation (roster changes) requires re-running the TSS key aggregation ceremony on-chain.
