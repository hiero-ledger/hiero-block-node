# 0021 - Separate Network Ports for Block Stream Ingress and Egress

Date: 2024-06-01

## Status

Deprecated (design intent — single port adopted in implementation; see Note)

## Note (2026-05-22 review)

The current implementation in `block-node/app-config/src/main/java/org/hiero/block/node/app/config/ServerConfig.java`
binds a single gRPC port (`SERVER_PORT`, default 40840) for both CN ingress and
consumer egress. The separate-ports design described below was not adopted in code.
The motivations (firewall isolation, traffic-class prioritisation, independent TLS
termination) remain valid but are now handled at the infrastructure layer — host
firewall rules, load balancer routing, and reverse-proxy configuration — rather
than at distinct application-layer ports. The published default for the BN's
serving port is a single value; if separate ports are later reintroduced, they
should be re-decided in a new ADR that supersedes this one.

## Context

Block Nodes serve two very different traffic classes on the same host: inbound block streams from Consensus Nodes (a small number of high-trust, high-bandwidth connections) and outbound block streams to Mirror Nodes, Tier 2 BNs, and public consumers (potentially many connections at varying trust levels). Mixing these traffic classes on a single port makes it difficult to:
- Apply different firewall rules and rate limits to inbound vs outbound traffic.
- Protect the CN ingress path from consumer-side abuse.
- Prioritize CN traffic during periods of high consumer load.

## Decision

Block Nodes expose **separate TCP ports** for block stream ingestion (CN → BN) and egress (BN → consumers):

- **Ingress port**: Receives block streams from CNs. Firewall rules restrict inbound connections to the known set of CN IP addresses. Rate limiting is not required on this port but may be applied for safety.
- **Egress port**: Serves block streams to MNs, Tier 2 BNs, and public consumers. Rate limiting, bandwidth groups (per ADR-0012), and authentication (Phase 3 monetization) are applied at this port.

Both ports use gRPC over HTTP/2. TLS is not mandated at the software layer (per ADR-0012) but may be applied at the infrastructure layer independently per port.

## Consequences

- Firewall configuration is simplified: inbound CN traffic can be allowlisted by IP on the ingress port independently of egress.
- BN software must be updated to bind and manage two separate listener ports.
- Operators must ensure both ports are accessible from their respective clients and that port numbers are documented in operator runbooks.
- Future per-port TLS termination becomes independently configurable at the infrastructure layer.
