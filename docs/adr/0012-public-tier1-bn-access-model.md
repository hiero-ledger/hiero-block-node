# 0012 - Tier 1 Block Node Public Access Model

Date: 2024-06-01

## Status

Accepted (design intent; bandwidth-group enforcement deferred to infrastructure layer — see Note)

## Note (2026-05-22 review)

The bandwidth-group split described in the Decision section below was a design intent.
A grep of the current codebase finds no application-layer traffic-shaping
implementation (no rate-limiter wired to a partner allowlist in `block-node/app-config`
or the gRPC server). The current implementation uses a single gRPC port (per ADR-0021
note) and any traffic shaping is expected to be applied at the infrastructure layer
(firewall, load balancer, reverse proxy) rather than inside the Block Node process.
The principles still hold; the enforcement boundary has moved.

## Context

Tier 1 Block Nodes (BNs) serve as the primary authoritative data source for the Hedera network's block stream. They receive block streams directly from Consensus Nodes (CNs) and serve that data to Mirror Nodes and Tier 2 BNs. Because they sit at the root of the distribution hierarchy and accept direct CN push connections, they cannot be placed behind a CDN (which would impose caching layers incompatible with live streaming). This exposes them to volumetric traffic risks and bandwidth exhaustion from public consumers.

The question is whether Tier 1 BNs should be fully open to the public, fully private, or partially accessible using traffic shaping — and what mechanism should enforce any such boundary.

## Decision

Tier 1 BNs will be publicly reachable (public IPs, DNS-resolvable) but **partially accessible** via bandwidth group segmentation:

- **Group 1 — Permissioned partners** (Hedera Mirror Nodes and permissioned Tier 2 BNs): allocated ~50% of total outbound bandwidth, receiving priority routing.
- **Group 2 — General community**: allocated the remaining bandwidth for read access, but with no guarantees on throughput.

TLS **will not** be enforced at the BN software layer for the same reasons Consensus Nodes do not enforce it: a decentralised network should not become centralized around a small number of Certificate Authorities. BN deployment infrastructure may layer TLS separately. MNs and Tier 2 BNs should expect to reference IP endpoints rather than hostnames protected by CA-issued certificates.

## Consequences

- Tier 1 BNs are DDoS-exposed without additional infra-level protections (firewall rules, rate limiting at edge).
- Partner Mirror Nodes and Tier 2 BNs receive guaranteed bandwidth headroom even under heavy public load.
- Traffic shaping coordination of partner IP ranges requires operational processes to maintain up-to-date allowlists.
- No CA dependency in the core protocol; this may limit compatibility with CDN services that expect TLS-terminated origins.
- Decision may be revisited once monetization (Phase 3) introduces authenticated streaming with HBAR-denominated access control.
