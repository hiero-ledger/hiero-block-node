# 0017 - hinTS as the Threshold Signature Scheme for Block Proofs

Date: 2025-01-01

## Status

Accepted

## Context

The network requires a threshold signature scheme (TSS) that allows a subset of Consensus Node key shares to produce a single aggregate signature over each block's root hash. The aggregate signature must be:
- Verifiable by any consumer using only the network's public key and roster (no per-node key lookup required).
- Compact — constant size regardless of roster size, enabling light client and state proof use cases.
- Compatible with on-chain key management and public verifiability of the key aggregation ceremony.
- Implementable in Java (CN and BN are JVM-based).

Several TSS constructions were evaluated, including BLS-based multi-signature schemes and Schnorr-based TSS. The Hedera-specific requirement for publicly verifiable key aggregation (where any observer can confirm the aggregate key is a valid combination of individual node shares) narrows the field.

## Decision

**hinTS** (Hedera-internal Threshold Signature Scheme, HIP-1200) is adopted. hinTS is a BLS-based threshold signature construction with the following properties:

- Produces a single aggregate signature (constant size) over the block root hash.
- The key aggregation ceremony is publicly verifiable on-chain — any observer can confirm the aggregate key correctly reflects the current roster and share distribution.
- Verification requires only the aggregate public key and roster; no per-node public key enumeration at verification time.
- The TSS signing ceremony is executed as a network upgrade step; key rotation (roster change) triggers a new ceremony.
- Initially, blocks immediately after genesis may be verified using aggregate Schnorr signatures until a compressed WRAPS proof is produced.

## Consequences

- All verification-side consumers (BN, MN, JSON RPC relay, light clients) must integrate the hinTS verification library before Phase 2b.
- The on-chain key aggregation ceremony introduces a new network upgrade step that must complete before block streams go live.
- Roster changes require re-running the ceremony, adding operational complexity to council member changes.
- The BLS curve and parameter choices must be standardized across all implementations (Java CN/BN, and potentially other language clients).
- hinTS enables future light-client and state-proof features that were not possible with per-node RSA signatures due to proof size scaling issues.
- If TSS key generation is delayed or fails, Phase 2a (RSA-based proofs) provides a production fallback that keeps BNs operational until TSS is ready.
