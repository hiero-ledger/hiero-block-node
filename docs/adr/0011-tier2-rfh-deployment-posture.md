# 0011 - Tier 2 Remote Full History (RFH) Deployment Posture

Date: 2026-05-22

## Status

Accepted

## Context

The Hedera network deploys block storage as a two-tier architecture (ADR-0002). The
Tier 1 layer consists of 7 Local Full History (LFH) Block Nodes, council-operated
and fed directly from CN streams. Behind Tier 1 the network needs a disaster-recovery
substrate that ensures verified blocks are never permanently lost even if all Tier 1
LFHs are unavailable. This role is the Tier 2 *Remote Full History* (RFH) Block Node.

Three operational questions had to be settled before launch:

1. **How many Tier 2 RFHs are needed, and how are they distributed across cloud
   providers and jurisdictions?** A single large cloud already provides many-9s
   durability through multi-region replication, so raw cloud count is not the
   load-bearing argument. Jurisdictional diversity (US versus non-US governance)
   is the meaningful axis for political and regulatory tail risk.
2. **Who operates the Tier 2 RFHs?** Hashgraph (on behalf of Swirlds) is the
   default operator at launch, but council recruitment for additional operator
   roles has historically been slow. Expanding the operator pool beyond council
   members compresses time-to-true-decentralisation.
3. **What is the operational profile of a Tier 2 RFH?** Tier 2 must remain
   available when Tier 1 fails, which constrains its design. It should not share
   failure modes with Tier 1, should not be a serving tier in its own right, and
   should be cost-efficient at the storage and egress scale of full block history.

The supporting analysis is captured in proposal documents under
`agent/proposals/hedera-rfh-deployment/` (rfh-deployment-summary.md,
rfh-deployment-options.html, rfh-deployment-open-questions.html).

## Decision

The Tier 2 layer at launch consists of **three Remote Full History Block Nodes**,
each in a distinct cloud bucket and configured for multi-region replication within
its cloud:

- **RFH-1 — GCP**: operated by Hashgraph on behalf of Swirlds. Long-term retained
  in Hashgraph's portfolio.
- **RFH-2 — OVH (EU)**: operated by a Hedera-vetted and -approved operator under
  the Hedera grant program. Provides cross-jurisdictional diversity against
  US-only political or regulatory action.
- **RFH-3 — AWS S3 / Cloudflare R2 / Backblaze B2 (selection pending)**: operated
  by a Hedera-vetted and -approved operator under the grant program. The specific
  cloud is being vetted; final selection requires a 3-year cost projection,
  governance review of the candidate's operational profile, and operator-pool
  willingness to operate the chosen provider.

The following invariants apply to all Tier 2 RFHs:

- **Private.** No public endpoints, no state services, no streaming subscriber
  API. Only the bulk retrieval path is exposed, and only to known LFH identities
  via mutual TLS or an IP allowlist.
- **Stateless.** No state-replication subsystem and no live block subscription
  service in the RFH process. This lowers the hardware profile, reduces the bug
  surface, and simplifies the operator standard.
- **Multi-region replication.** Single-region buckets are prohibited. GCS
  multi-region, S3 cross-region replication, or the OVH equivalent must be
  configured before the RFH is considered production-ready.
- **Pull-backfill from any Tier 1 LFH.** RFHs ingest from whichever LFH is
  currently fastest or healthiest, not pinned to a single LFH operator. This
  gives RFH ingest resilience to individual LFH incidents.
- **Re-verify blocks before commit.** RFHs re-verify block proofs before writing
  to the cloud bucket; LFH attestation is not trusted across operator boundaries.
- **Operator pool is open.** Council membership is not required to operate an
  RFH; Hedera vetting and approval against the published operator standard is
  the bar.
- **Operator costs covered by grant.** Hashgraph absorbs its own GCP cost as
  part of operating on behalf of Swirlds. Grant-funded operators have their
  cloud costs covered through the Hedera grant program; cost ownership splits
  cleanly along operator boundaries with no pass-through arrangements.

Tier 1 LFHs reserve a portion of their outbound bandwidth specifically for RFH
read traffic so RFH backfill is not starved during peak client load.

## Consequences

- The grant program structure (operator standard, application criteria, grant
  amount, term, attestation cadence) is a pre-launch critical-path artefact and
  is co-owned by engineering and Hedera governance.
- The RFH operator standard published before launch covers SLA, RPO, security
  posture, multi-region replication requirements, hardware profile expectations,
  and attestation cadence. Grant disbursement is gated on adherence.
- OVH's jurisdictional separation claim is subject to legal counsel sign-off.
  Until that confirmation lands, public communications about jurisdictional
  diversity should describe it as "day-to-day governance separation" rather
  than guaranteed political independence.
- The 3rd-cloud selection (Q12 in the proposal open-questions document) remains
  open at this ADR's date. The provider table evaluated AWS S3, Cloudflare R2,
  and Backblaze B2; R2's zero-egress pricing has the strongest cost case but
  requires production-scale validation.
- The stateless RFH design intentionally departs from the Tier 1 BN feature set;
  it reduces hardware costs but means an RFH cannot transparently serve as a
  drop-in replacement for an LFH if Tier 1 capacity is lost. Recovery from
  total Tier 1 loss requires standing up new LFHs and rebuilding their history
  from RFH buckets.
- The single-operator-at-launch state for two of the three RFHs (until grants
  land) is acknowledged as a known time-bounded compromise; the published grant
  program with named recruitment lead is the mitigation. Target for first
  non-Hashgraph RFH live: launch + 3 to 6 months. Target for second: launch +
  6 to 9 months.
- Hybrid LFH+RFH deployment in a single BN process is explicitly out of scope
  under this posture — tier separation is a failure-isolation boundary, not a
  deployment convenience.
