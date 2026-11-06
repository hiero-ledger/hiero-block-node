# Borderline ADRs

This document captures design considerations that were actively debated but did not rise to the level of a formal ADR — either because the decision is still tentative, the scope is narrower/more operational than architectural, or the outcome is likely to change as the system matures. Each item below is a candidate for promotion to a full ADR if circumstances change.

---

## BN-01 — HTTP/2 REST vs gRPC for Block Retrieval (CDN Compatibility)

**Context**: Serving blocks over HTTP/2 REST (rather than gRPC) would allow Tier 1 BNs to sit behind a CDN, shifting egress bandwidth costs and DDoS exposure to CDN infrastructure. The concern is that gRPC live-streaming is incompatible with typical CDN caching semantics.

**Current position**: gRPC is the adopted streaming protocol (ADR-0004). HTTP/2 REST has been noted as a potential addition specifically for CDN-backed block *retrieval* (non-streaming, historical block fetch by number). No decision has been made to implement this.

**Why borderline**: If CDN costs or DDoS risk become pressing, adding an HTTP/2 REST endpoint for block retrieval alongside the existing gRPC streaming API is a meaningful architectural addition worth formalising. Ticket-level, not ADR-level yet.

---

## BN-02 — Configurable Storage Backends / Compression

**Context**: The current on-disk format (Zstd + ZIP, ADR-0020) is internal to BN and acknowledged as not final. Future block volumes and hardware diversity may make alternative compression or container formats preferable. Object storage (S3-compatible) as an alternative to local NVMe+HDD was discussed.

**Current position**: Zstd+ZIP is accepted for now. The team noted that BNs "may have configurable choices in the future."

**Why borderline**: Until there is a concrete proposal for a second storage backend or a performance regression that motivates a change, this remains a future-looking note rather than an actionable architectural decision.

---

## BN-03 — Stateless Full-History Distributor Mode

**Context**: Some operators may be willing to store full block history on slow spinning disk (HDD-only, no NVMe) and serve it without maintaining live state or keeping up with CN block production in real time. This "stateless distributor" mode would lower hardware costs but may not be able to keep up with live CN block production or serve many concurrent streaming clients.

**Current position**: Discussed but not decided. Requires performance testing to determine if it can serve live streams adequately.

**Why borderline**: If vendor procurement constraints worsen, this becomes a practical necessity. Warrants a formal ADR once performance data is available.

---

## BN-04 — Private Tier 2 BNs for Partner Traffic Isolation

**Context**: Rather than having partners access Tier 1 BNs directly (sharing bandwidth with other consumers), a proposal was to deploy private Tier 2 BNs specifically for high-value partner traffic. This was explored as a way to isolate and guarantee SLA for partners.

**Current position**: Rejected for the near term — it "doesn't really help distribution need" and merely moves the problem. Bare-minimum traffic shaping at the Tier 1 level is the current approach.

**Why borderline**: May be revisited in Phase 3 if partner SLA requirements become contractual commitments that cannot be met through bandwidth-group shaping alone.

**2026-05-22 update**: ADR-0011 defines a different private Tier 2 variant — Remote Full History (RFH) — for disaster-recovery rather than partner SLA isolation. The two concepts share the "private Tier 2" label but address different motivations. If partner SLA isolation is later adopted, it would be a separate Tier 2 deployment alongside RFH, not a reuse.

---

## BN-05 — CLIPR State Proof Integration

**Context**: CLIPR (a state-proof-related service) was considered as a consumer of Block Node state proofs. The question was whether CLIPR would use BN-generated state proofs or derive proofs exclusively from CN state directly.

**Current position**: CLIPR will not utilize BNs for state proofs and will involve only CN state proofs.

**Why borderline**: The decision is made, but the rationale is not fully documented. If BN state proof capabilities expand in Phase 3 (BN State Services), this may be worth revisiting and could warrant a formal ADR at that point.

---

## BS-01 — Deduplication and Upload of Wrapped Record Files to Reference Bucket

**Context**: During the historical wrapping process, the deduped source record files are a useful artefact: they could be uploaded to a single-region cloud bucket for long-term reference (e.g. to allow rehashing and provide a reliable public store of the pre-wrap inputs). Wrapped record files themselves do not need to be stored separately as they will be available on Tier 1 BNs.

**Current position**: Tentatively yes for the deduped record files; the need for deep-archive storage is an open question.

**Why borderline**: The storage policy (retention, region, access tier) is an operational decision, not yet formalised. If it involves a long-term cost commitment or public access guarantee, it warrants an ADR.

---

## BS-02 — Preview Block Stream Halt at Phase 2a

**Context**: Preview block streams (Phase 0) have been running since v0.56 alongside record files. At Phase 2a, when CNs begin producing WRBs, the preview streams may become redundant and could be halted to reduce CN overhead.

**Current position**: The team will evaluate at Phase 2a cutover time; a formal decision record is included as a Phase 2a requirement.

**Why borderline**: The decision is pending empirical validation data. Once the Phase 2a cutover is assessed, this should be captured as either a formal ADR or a deployment decision record.

---

## TSS-01 — TSS Key Rotation Process on Roster Changes

**Context**: When the Consensus Node roster changes (a council member is added or removed), the hinTS aggregate key must be updated via a new key aggregation ceremony. The exact process — ceremony timing, ceremony failure handling, interim signing approach during the ceremony window — is not yet formally documented.

**Current position**: Known requirement; design is in progress as part of HIP-1200 implementation.

**Why borderline**: Once the ceremony process is designed and tested, it warrants a formal ADR covering: when to trigger, how to handle ceremony failures, what consumers do during the transition window, and how roster changes are reflected in BN status endpoints.

---

## BN-06 — Block Node Rewards Mechanism

**Context**: Tier 1 BNs are operated by council members and partners who bear significant hardware and operational costs. A rewards mechanism (HBAR-denominated, on-chain) is needed to compensate operators and align incentives for maintaining availability and correctness.

**Current position**: A HIP and implementation are planned as a Phase 2b requirement. Design is in progress.

**Why borderline**: Once the HIP is approved and the on-chain mechanism is specified, the architectural aspects (how BNs report availability, how rewards are calculated and distributed, how consumers interact with the reward layer) should be captured in a dedicated ADR.

---

## BN-07 — Minimum True BN Count for Go-Live

**Context**: The original responsible steward count was 5 (2 RFH + 3 LFH under the original RFH-as-Record-File-History naming). The absolute minimum considered operationally viable was 3 (1 RFH + 2 LFH).

**Current position**: Reframed by ADR-0011. The target topology is now 7 Tier-1 LFHs plus 3 Tier-2 RFHs (Hashgraph-GCP plus two grant-funded operators). Whether the network can go live with fewer than the target Tier-1 count (for example, 5 or 6 LFHs at launch with the remaining onboarded post-launch) is still an open operational question separate from the RFH topology decision.

**Why borderline**: If the Tier-1 LFH go-live count deviates from the 7-LFH target, a formal decision record documenting the risk acceptance and mitigation should be produced. This is currently an escalation item rather than an architectural decision. The Tier-2 RFH minimum is set by ADR-0011 (target 3, with grant-funded slots filled within 3-9 months of launch).

---

## BN-08 — Cloud Provider Selection for RFH-3 (AWS vs Cloudflare R2 vs Backblaze B2)

**Context**: ADR-0011 leaves the third Tier-2 RFH's cloud provider open pending vetting. The candidate set is AWS S3, Cloudflare R2, and Backblaze B2 — all US-jurisdiction, so no diversity-axis gain among them, but they differ materially on egress cost, storage cost, operator familiarity, and governance maturity. Cloudflare R2's zero-egress pricing has the strongest cost case for an egress-dominated workload; AWS S3 has the largest operator pool; Backblaze B2 is cheapest at storage but with smaller operational maturity.

**Current position**: Vetting in progress. Selection requires (a) a 3-year cost projection per candidate, (b) governance review of the candidate's commercial profile, and (c) operator-pool feedback on willingness to operate the chosen provider.

**Why borderline**: Once selected, the choice and its rationale warrant a brief ADR (or an amendment to ADR-0011) so future reviews can revisit the cost / governance tradeoffs against then-current alternatives.

---

## BN-09 — Hedera Grant Program Structure for RFH Operators

**Context**: ADR-0011 commits to a grant-funded operator model for two of three Tier-2 RFHs. The grant program structure — published operator standard (SLA, RPO, security posture, hardware profile, attestation cadence), application criteria, grant amount sizing, grant term, attestation cadence, and the mechanism by which Hedera disburses funds — is a pre-launch deliverable but is not yet specified.

**Current position**: In design. Engineering owns the operator-standard portion; Hedera governance owns the funding mechanism and approval process.

**Why borderline**: Once the program is published, the architectural aspects (how operator compliance is measured, how attestations flow from operators back to the network, what data RFHs publish to demonstrate adherence) deserve a dedicated ADR. The governance-and-finance portions are governance documents, not ADRs.

---

## BN-10 — LFH Outbound Bandwidth Reservation for RFH Backfill

**Context**: ADR-0011 specifies that Tier-1 LFHs reserve a portion of their outbound bandwidth specifically for RFH backfill reads, to avoid starving the RFH ingest path during peak client read load. The mechanism (QoS class on egress, percentage of bandwidth reserved, how it's configured, whether it's enforced in BN software or at the infrastructure layer) is unspecified.

**Current position**: Stated requirement; implementation approach not chosen. Could be application-layer (a configurable byte-budget on the backfill-serving endpoint) or infrastructure-layer (host network QoS, traffic-control rules) — same divergence as ADR-0012's bandwidth groups.

**Why borderline**: Once the mechanism is chosen, it warrants a brief ADR (or a note in ADR-0011) covering where the enforcement lives, what the default reservation is, and how operators tune it. The decision interacts with ADR-0012's bandwidth-group framing.

---

## BN-11 — OVH Jurisdictional Separation Legal Framing

**Context**: ADR-0011 leans on OVH's EU-jurisdiction profile as the load-bearing axis for cross-jurisdictional disaster-recovery diversity. The technical reasoning (family-controlled French company, EU commercial code, GDPR, vertical-integration ops profile) is sound, but the public-facing framing for ecosystem partners, regulators, and council members requires legal counsel sign-off — particularly under scenarios where EU and US regulation might align.

**Current position**: Pending legal counsel review.

**Why borderline**: If legal counsel returns a materially different framing than the proposal documents assumed, a follow-up ADR (or an amendment to ADR-0011) is required to record the refined claim. Until then, the diversity claim should be described as "day-to-day governance separation" rather than guaranteed political independence.

---

## BN-12 — Two-Tier On-Disk Block Storage (Recent NVMe + Historic HDD)

**Context**: The Block Node currently implements two distinct on-disk storage modules — `blocks-file-recent` (live and recent blocks on fast NVMe) and `blocks-file-historic` (older blocks packed into Zstd-compressed ZIP archives on slower HDD, per ADR-0020). The boundary between "recent" and "historic" (rotation policy, threshold, atomic-rotation mechanism, what happens during rotation when a block is read) is a load-bearing operational design choice but is not captured in any ADR.

**Current position**: Implemented in code with config records `FilesRecentConfig` and `FilesHistoricConfig`. Documented behaviour lives in the module source; no ADR records the architectural choice or the rotation contract.

**Why borderline**: This warrants a dedicated ADR covering: rotation threshold/policy, how reads cross the boundary atomically, retention behaviour at the recent/historic boundary, and how the design supports the future stateless-full-history distributor variant (see BN-03). A grounded ADR requires code investigation beyond a high-level review.

---

## BN-13 — Cloud Storage Archive Plugin Family

**Context**: There are at least three cloud archive plugins in the codebase: `CloudStorageArchivePlugin`, `S3ArchivePlugin`, and `ExpandedCloudStoragePlugin` (visible under `block-node/cloud-storage-archive` and related modules). They serve overlapping but not identical roles around uploading blocks to cloud buckets — some are for the recent-blocks bucket (ADR-0017), some for full-history archive, some are S3-specific. The boundary between them is a meaningful design choice.

**Current position**: Implemented; not consolidated into a single conceptual model in documentation.

**Why borderline**: A single ADR articulating the plugin family — which one serves which role, how they interact with Tier-2 RFH (ADR-0011), and what the per-cloud configuration surface looks like — would prevent operator confusion and clarify the design intent. Worth promoting once the RFH-3 cloud is selected (BN-08) since the chosen provider may affect which plugin variant is canonical.

---

## BN-14 — Atomic Persistence Pattern (Write-to-Temp + ATOMIC_MOVE)

**Context**: Several BN components persist state to disk (TSS bootstrap data, RSA roster bootstrap, address book, block range sets). Recent work in PR-2738 introduced an atomic write-to-temp-then-rename pattern for some files, while others still use direct `Files.write`. The team has discussed but not formally decided whether all persistence paths should adopt the atomic pattern uniformly, and whether a small shared helper should encapsulate the idiom.

**Current position**: Adopted ad hoc; not yet uniform across all persistence paths.

**Why borderline**: A short ADR could record the convention ("all internal state persistence uses `Files.move` with `ATOMIC_MOVE + REPLACE_EXISTING` via a shared helper") and the rationale (avoids torn-state observations after crashes). Useful as a coding standard backed by an architectural rationale, but small enough that it could equally live as a contributing-style document.
