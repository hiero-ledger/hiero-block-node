# 0006 - Staged Cutover: Wrapped Record Blocks Before Full Block Streams

Date: 2025-07-01

## Status

Accepted

## Context

The original plan (v2) called for a single cutover event in which three major paradigm shifts happened simultaneously: (1) Consensus Nodes adopt full Block Streams format, (2) TSS aggregate signatures replace per-node RSA signatures on block proofs, and (3) Block Nodes go live as the authoritative source for Mirror Nodes.

Simultaneously adopting three novel, production-critical systems carries compounding risk — any one of them failing validation would block the others and potentially delay the entire cutover. TSS hardening in particular requires a lengthy soak period that would hold up BN deployment if coupled.

An alternative approach was evaluated: split the single cutover into two staged cutovers gated by an operational readiness review between them.

## Decision

Adopt the **v3 two-stage cutover** strategy (superseding v2):

**Phase 2a — Wrapped Record Block (WRB) Streaming Cutover:**
- CNs stream Wrapped Record Blocks (one record file → one WRB block) to Block Nodes via gRPC, while continuing to produce record files in parallel.
- WRB block proofs use `SignedRecordFileProof` containing gossiped RSA signatures from all nodes in the current roster — the same verification logic already implemented by Mirror Nodes and the WRB CLI.
- The Hedera Mirror Node cuts over to pull from Block Nodes as its primary source, with cloud bucket record files retained as a fallback.
- Third-party Mirror Nodes are unaffected and continue consuming record files.
- TSS is **not** required at this stage.
- Record file production continues until Phase 2b.

**Phase 2b — Full Block Stream Cutover + TSS:**
- CNs permanently adopt the full Block Streams format (HIP-1056) with TSS aggregate block proofs (HIP-1200).
- Record file production ceases permanently.
- Block Streams are uploaded to a new cloud bucket path for third-party MNs; Hedera MN subscribes directly to Tier 1 BNs.
- Gated on Phase 2a operational readiness being demonstrated.

## Consequences

- BNs gain real production load and soak time in Phase 2a before the irreversible cessation of record files in Phase 2b.
- TSS hardening and BN deployment timelines are fully decoupled, allowing parallel team execution.
- Phase 2a requires CN changes (WRB production + gRPC streaming to BNs) and BN RSA verification logic — achievable without TSS.
- Phase 2b remains a one-way decision (record files cease); the Phase 2a gate provides confidence before that commitment.
- Third-party MNs must eventually migrate to Block Streams (Phase 2b), but have a grace period.
- The preview block stream (Phase 0) may be halted during Phase 2a pending validation decision.
