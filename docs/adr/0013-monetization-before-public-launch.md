# 0007 - Monetization Must Precede Public Block Node Access

Date: 2024-10-01

## Status

Accepted

## Context

Block Nodes provide value-added services (streaming, state proofs, snapshots, entity queries) that the network intends to eventually charge for. A key risk is launching public access without a payment mechanism in place: once users build integrations expecting free access, introducing payment later becomes a breaking change that erodes trust and is extremely difficult to enforce retroactively.

The question is whether to ship public Block Nodes early — accepting the risk of setting a free-access expectation — or to defer public access until monetization is in place.

## Decision

**Public Block Node access will not ship before monetization infrastructure is in place.**

- Phase 2 (private BNs) provides BN access only to permissioned partners (Hedera MN, whitelisted Tier 2 BNs) without payment gating.
- Phase 3 (public BNs) introduces HBAR-denominated streaming access for non-permissioned subscribers at the same time as — or before — enabling general public access.
- Any community-facing access in Phase 2 must be explicitly permissioned and whitelisted, never open-access.

## Consequences

- Block Node development and launch timeline is gated on implementing the monetization layer (HIPs, smart contract, payment verification at the gRPC layer).
- Partners operating Tier 2 BNs in Phase 2 must accept that their access is permissioned and may later require payment.
- This precedent protects long-term network economics and BN operator incentive structures.
- The constraint does not apply to cloud bucket access (record files / WRBs), which is governed by a separate requester-pays model.
