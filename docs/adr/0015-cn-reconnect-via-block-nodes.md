# 0013 - Consensus Node Reconnect via Block Nodes

Date: 2024-12-01

## Status

Accepted

## Context

When a Consensus Node falls behind its peers — due to a restart, crash, or network partition — it must reconnect by obtaining a recent saved state and replaying subsequent blocks. Historically, CNs relied on peer CNs or cloud bucket state files for this. As block streams mature and Block Nodes accumulate saved states and rolling block history, they become a natural reconnect source that does not require coordination between CNs directly.

The alternative is to keep the existing CN-to-CN reconnect mechanism (peer gossip, state transfer) or extend the cloud bucket model.

## Decision

Block Nodes will support **CN reconnect** by providing:
- Saved state snapshots generated at configurable intervals by both Full-History and No-History BNs.
- Block streaming from a recent saved state to the current tip, allowing a reconnecting CN to replay blocks to catch up.

CN reconnect from BNs requires the BN endpoint to be **whitelisted** — access is restricted to authenticated CN identities, not open to general consumers. This whitelist is maintained in BN configuration.

The reconnect path from BNs is introduced in Phase 2B (or optionally earlier) and complements rather than replaces CN-to-CN gossip reconnect in the near term.

## Consequences

- BNs become load-bearing for network recovery scenarios, raising their operational criticality.
- No-History BNs can serve reconnect for CNs that are only slightly behind; Full-History BNs are needed for CNs that need to replay from older state.
- BN whitelist management (adding/removing CN identities) becomes an operational requirement that must be coordinated with CN roster changes.
- Saved state format and generation cadence must be agreed upon between CN and BN teams.
- Reduces CN-to-CN coupling for reconnect, which can otherwise create cascading availability problems if multiple CNs need to reconnect simultaneously.
