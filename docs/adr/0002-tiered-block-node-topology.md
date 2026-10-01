# 0002 - Block Node Tiered Network Topology

Date: 2024-06-01

## Status

Accepted (Tier 2 role refined by ADR-0011 — see Note)

## Note (2026-05-22 review)

The two-tier hierarchy described below remains the load-bearing topology. What has
been refined since this ADR was written is the role of Tier 2 specifically for
disaster recovery: [ADR-0011](0011-tier2-rfh-deployment-posture.md) defines a
specific Tier 2 variant called *Remote Full History* (RFH) — private, cloud-bucket
backed, no state services, pull-backfilling from any available Tier 1 LFH. RFH is a
specialisation of the general Tier 2 concept here, not a replacement for it. The
general Tier 2 BN role (subscribe to Tier 1, serve filtered or public downstream)
remains valid for future community / partner deployments.

## Context

The original record-stream model had Consensus Nodes upload data to cloud buckets, from which any consumer could pull. This created a centralised dependency on GCP/AWS buckets and charged egress costs to consumers (requester-pays model). As the network moves to block streams, a new distribution model is needed that decentralises data availability, enables monetization, and reduces CN-to-consumer coupling.

Options considered:
1. **Flat mesh** — all BNs treated equally, receiving from CNs directly.
2. **Two-tier hierarchy** — small number of permissioned Tier 1 BNs receive from CNs; community-run Tier 2 BNs pull from Tier 1.
3. **CDN-backed** — Tier 1 BNs upload to cloud storage and consumers retrieve via CDN.

Option 3 was explored but rejected: CNs cannot push live streaming gRPC to a CDN; CDNs cannot efficiently cache streaming data; and HTTP/2 via REST for block retrieval would be needed to support CDN caching, but gRPC is the chosen protocol for low-latency block streaming.

## Decision

A **two-tier hierarchy** is adopted:

- **Tier 1 BNs** (permissioned, council-operated or Swirlds-Labs-operated): receive block streams directly from CNs over gRPC; maintain full block history; serve Hedera Mirror Nodes and permissioned Tier 2 BNs.
- **Tier 2 BNs** (community-operated, partner-operated, or private): subscribe to Tier 1 BNs to receive blocks; may offer filtered or public access downstream.

CNs use a configured `block-nodes.json` that lists Tier 1 BN endpoints. Each CN will push to a primary Tier 1 BN with backup failover, rather than broadcasting to all BNs simultaneously.

Tier 1 BNs also upload blocks to a cloud bucket path as a secondary distribution channel for third-party Mirror Nodes that are not yet subscribed to BNs directly.

## Consequences

- Tier 1 BNs are operationally critical — their uptime directly affects MN data availability.
- Tier 2 BN operators must maintain connectivity to at least one Tier 1 BN, creating a soft dependency.
- The bucket upload path provides a backward-compatible fallback during the transition period from record streams (Phase 2a) before full MN cutover (Phase 2b).
- Monetization is planned at the Tier 1 layer for Phase 3 (HBAR-metered streaming to non-permissioned subscribers).
- Block distribution can be expanded without touching CN configuration by simply adding Tier 2 BNs.
