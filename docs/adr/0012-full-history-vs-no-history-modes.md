# 0012 - Block Node Full-History vs No-History Operating Modes

Date: 2024-10-01

## Status

Accepted

## Context

Block Nodes require substantial storage to hold the complete history of blocks from genesis. At 10k TPS, the network produces ~210 GB of block data per day, requiring ~300 TB of HDD for approximately 3 years of history. This hardware specification is cost-prohibitive for many potential operators and narrows the set of vendors able to supply qualifying hardware.

Meanwhile, several use cases — notably CN reconnect support and near-live state proofs — require only a rolling window of recent history (e.g. 24 hours) rather than full history back to genesis. Serving this use case with a full-history node wastes significant storage resources.

Two operating modes were evaluated: one universal mode versus specialized modes for different storage profiles.

## Decision

The Block Node software supports **two configurable operating modes** that run on the same codebase:

- **Full-History Block Node**: Retains all blocks from genesis indefinitely. Required for Tier 1 BNs and any node serving historical archive access. Storage requirement: ~300 TB HDD minimum.
- **No-History (Rolling-Window) Block Node**: Retains only a configurable rolling window of recent blocks (e.g. 24 hours). Used for CN reconnect support, near-live state proofs, and cost-constrained operators who do not need full history. Storage requirement dramatically reduced.

Both modes support the same gRPC streaming APIs; the difference is purely in persistence depth. No-History BNs generate saved-state snapshots on the same schedule as full-history BNs to support CN reconnect from them.

## Consequences

- Reduces the hardware barrier to entry for community and partner BN operators.
- CN reconnect can be served from No-History BNs, reducing the dependency on full-history nodes for network operations.
- Consumers that require historical block access (e.g. explorers, archival Mirror Nodes) must target Full-History nodes; the operating mode is surfaced in the BN status endpoint so consumers can route appropriately.
- The mode is configured at deploy time; switching from No-History to Full-History requires backfilling from peers, which is a supported but potentially slow operation.
- A future variant (stateless full-history distributor) was discussed for hardware-constrained environments where the node serves blocks from slower HDD without maintaining state — this remains an open option for future evaluation.
