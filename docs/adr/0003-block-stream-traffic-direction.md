# 0003 - Block Stream Traffic Direction: CN→BN Push, MN→BN Pull

Date: 2024-06-01

## Status

Accepted

## Context

When designing the data flow between Consensus Nodes (CNs), Block Nodes (BNs), and Mirror Nodes (MNs), a fundamental question arises: who initiates the connection and in which direction does data flow?

Two directions were considered at each hop:
- **CN → BN**: CNs push blocks to BNs, OR BNs pull blocks from CNs.
- **BN → MN**: BNs push blocks to MNs, OR MNs pull blocks from BNs (subscribe streaming).

The choice has implications for connection management, backpressure, fault tolerance, and operational security.

## Decision

**CN → BN**: CNs **push** block streams to BNs. A BN may not initiate a pull from a CN.

- CNs are configured with a prioritized list of Tier 1 BN endpoints.
- CNs prefer low-latency BNs (geographically close) and will disconnect and reconnect to other BNs based on latency, status, and block availability.
- Each BN has a single primary CN source with multiple backup CNs to avoid all CNs flooding all BNs simultaneously.
- CNs may dedicate a portion of their bandwidth specifically to inbound BN traffic to prevent CN-to-CN gossip starvation.

**MN → BN**: MNs **pull** (subscribe) block streams from BNs. A BN may not push to an MN.

- MNs are configured with a prioritized list of Tier 1 BN endpoints and will reconnect to alternatives on failure.
- MNs prioritize BNs over the cloud bucket fallback path.

**BN backfill**: Each Tier 1 BN is configured to backfill from a defined set of peer Tier 1 BNs to acquire blocks it may have missed. This is peer-to-peer pull, restricted to whitelisted BN peers.

## Consequences

- CNs must be updated to speak the BN gRPC streaming protocol (publishBlockStream) — a mandatory upgrade in the cutover sequence.
- BNs do not require outbound firewall rules toward CNs, simplifying their security posture.
- MNs control their own retry and failover logic; BNs are stateless from a connection perspective.
- The backfill whitelist must be maintained as Tier 1 BN membership changes.
- Reconnection logic on both CNs and MNs must be robust to handle BN restarts and upgrades without data gaps.
