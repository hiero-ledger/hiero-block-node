# 0002 - Tier 1 Block Node Minimum Count and Diversity Requirements

Date: 2024-06-01

## Status

Accepted (terminology and minimum-count target superseded by ADR-0019 on 2026-05-22 — see Note)

## Note (2026-05-22 review)

Two pieces of this ADR have been reframed by [ADR-0019](0019-tier2-rfh-deployment-posture.md):

1. **"RFH" reassigned.** When this ADR was written, RFH stood for *Record-File-History*
   — Block Nodes that wrapped and served historical record files during the
   Phase 2a transition. The acronym has since been reassigned to *Remote Full
   History*: Tier 2 BNs that upload verified blocks to cloud storage as a
   disaster-recovery substrate. The earlier "Record-File-History" concept is
   now captured by the special-purpose wrapping BN described in ADR-0010 and
   does not carry an acronym in current discussion.
2. **Topology count reframed.** The "5 BN minimum (2 RFH + 3 LFH)" target in this
   ADR was written under the assumption that all BNs sat at the same logical
   layer. ADR-0019 splits this into a 7 Tier-1 LFH topology (council-operated,
   CN-fed, serves clients) plus a 3 Tier-2 RFH topology (private, cloud-backed,
   DR substrate). The diversity and cloud-vendor constraints in this ADR's
   Decision section still apply to the *Tier 1* layer.

The original Context and Decision sections remain below for historical record.

## Context

Tier 1 Block Nodes are the network's authoritative persistent store of block streams. If too few operate, or if they share infrastructure, a single failure event — cloud outage, hardware failure, or hostile actor — could result in the loss of the only copy of recent blocks before they propagate to secondary storage. The number of BNs also determines the degree to which the block distribution network is decentralized.

Several constraints apply simultaneously: council member willingness and budget to host hardware, bare-metal availability from vendors that meet spec (24 core/48 thread CPU, 256 GB RAM, 8 TB NVMe, 300 TB HDD, 2×10G NIC), and the security requirement that no single cloud provider host more than a defined fraction of BNs.

## Decision

The target Tier 1 BN topology is structured as follows:

- **Minimum responsible count**: 5 BNs (2 Record-File-History / RFH + 3 Live-Full-History / LFH).
- **Security target**: BN count ≥ ⌊CN count / 2⌋ + 1 to ensure block availability in worst-case CN churn scenarios.
- **RFH count**: At least 2, each on separate cloud providers (e.g. GCP + AWS), preferably 3.
- **Cloud diversity**: No single cloud vendor may host more than 3 Tier 1 BNs; only 1 of those may be an RFH node.
- **CN configuration constraint**: Any given BN may appear in at most 1/3 of CN push configurations to prevent single-BN bottlenecks.

The minimum viable launch threshold is 5 BNs. Fewer than 3 is considered unacceptable even under extreme circumstances; 3 (1 RFH + 2 LFH) is the absolute floor for emergency scenarios and would require auditor justification.

## Consequences

- Vendor procurement is an active constraint — Latitude and OVH are the primary suppliers meeting spec; others are inconsistent or priced unfeasibly.
- Hardware spec may be revisited if a stateless full-history distributor profile is adopted (serving blocks without maintaining state), which would relax storage requirements.
- At 10k TPS, blocks consume ~210 GB/day (~8.5 MB/block). A 300 TB HDD provides ~3 years of headroom with minimal saved-state overhead (~80 TB). This timeline shortens as network throughput grows.
- Council member onboarding pace directly caps how quickly the network achieves target decentralisation.
- Ongoing push to recruit 6+ hosted nodes from Hashgraph and third-party operators in the short term.
