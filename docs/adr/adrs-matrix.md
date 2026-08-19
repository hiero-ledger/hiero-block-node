# ADR Relationship Matrix

This document describes how the architecture decisions under `docs/adr/`
relate to each other so an engineer can see what depends on what without
clicking through every file. For an interactive graph view, open
[adrs-graph.html](adrs-graph.html) in a browser.

The ADRs are numbered in **decreasing order of impact**: 0001 is the
program-defining decision; 0021 is the most narrowly scoped (and
currently Deprecated). New engineers reading the ADRs in numeric order
will be introduced to the largest-impact decisions first, with each
subsequent ADR building on the ones before it.

---

## Tier Overview

|                  Tier                   |  Numbers  |                                                Theme                                                |
|-----------------------------------------|-----------|-----------------------------------------------------------------------------------------------------|
| **1 — Program-defining foundations**    | 0001-0005 | What is the block stream, who feeds whom, how is it shaped on the wire and how is each block sealed |
| **2 — Cutover and cryptography**        | 0006-0009 | How the network transitions from record streams to block streams, and what proofs seal each block   |
| **3 — Operations and topology counts**  | 0010-0013 | How many BNs exist at each tier, who runs them, who pays                                            |
| **4 — Operational modes and endpoints** | 0014-0017 | How BNs are configured to serve different roles, what they expose, where they upload                |
| **5 — Implementation architecture**     | 0018-0021 | Code-level architectural choices that enable the above                                              |

---

## Full Matrix

Columns:

- **Depends on** — ADRs whose decisions must be understood before this one makes sense; the conceptual prerequisites.
- **Referenced by** — later ADRs that build on or refine this one. Helps when reading this ADR to see what comes next.
- **Themes** — short tags for cross-cutting topics so you can scan for "everything TSS-related" or "everything storage-related".

|  #   |                    Title                    | Tier |    Depends on    |        Referenced by         |            Themes            |                Status                |
|------|---------------------------------------------|------|------------------|------------------------------|------------------------------|--------------------------------------|
| 0001 | Block Stream Format Replaces Record Streams | 1    | —                | 0006, 0007, 0017, 0018, 0020 | block-stream, cutover        | Accepted                             |
| 0002 | Tiered Block Node Topology                  | 1    | 0001             | 0010, 0011, 0012, 0014, 0021 | topology                     | Accepted (refined by 0011)           |
| 0003 | Block Stream Traffic Direction              | 1    | 0002             | 0021                         | topology, protocol           | Accepted                             |
| 0004 | gRPC Chunked Block Items Protocol           | 1    | 0001, 0003       | 0005, 0019                   | protocol, block-stream       | Accepted                             |
| 0005 | Block Hash 4-Leaf Merkle Tree               | 1    | 0001, 0004       | 0008                         | crypto, block-stream         | Accepted                             |
| 0006 | Staged WRB → Full Block Stream Cutover      | 2    | 0001, 0008       | 0007, 0009                   | cutover                      | Accepted                             |
| 0007 | Historical Record File Wrapping             | 2    | 0001, 0006       | 0010, 0016, 0017             | cutover, history             | Accepted                             |
| 0008 | TSS Aggregate Signatures                    | 2    | 0005, 0006       | 0009, 0016                   | tss, crypto                  | Accepted                             |
| 0009 | hinTS TSS Scheme Selection                  | 2    | 0008             | 0016                         | tss, crypto                  | Accepted                             |
| 0010 | Tier 1 BN Minimum Count and Diversity       | 3    | 0002, 0007       | 0014                         | topology, ops                | Accepted (target superseded by 0011) |
| 0011 | Tier 2 RFH Deployment Posture               | 3    | 0002, 0007       | 0018, 0019                   | topology, ops, governance    | Accepted                             |
| 0012 | Tier 1 Public Access Model                  | 3    | 0002             | 0013, 0021                   | topology, access, governance | Accepted (enforcement deferred)      |
| 0013 | Monetization Before Public Launch           | 3    | 0012             | —                            | governance, access           | Accepted                             |
| 0014 | Full-History vs No-History Operating Modes  | 4    | 0002, 0010       | 0015                         | ops, storage                 | Accepted                             |
| 0015 | CN Reconnect via Block Nodes                | 4    | 0014             | —                            | ops, recovery                | Accepted                             |
| 0016 | BN Status Endpoint Exposes TSS Bootstrap    | 4    | 0007, 0008, 0009 | —                            | tss, ops                     | Accepted                             |
| 0017 | New Cloud Bucket Path for Block Streams     | 4    | 0001, 0007       | —                            | storage, cutover             | Accepted                             |
| 0018 | Plugin SPI for BN Extension Architecture    | 5    | 0001, 0011       | 0019                         | implementation               | Accepted                             |
| 0019 | LMAX Disruptor Block Messaging Bus          | 5    | 0004, 0018, 0011 | —                            | implementation, performance  | Accepted                             |
| 0020 | On-Disk Block Archive Format                | 5    | 0001             | —                            | implementation, storage      | Accepted                             |
| 0021 | Separate Ingress/Egress Ports               | 5    | 0003, 0012       | —                            | implementation, topology     | Deprecated (single port adopted)     |

---

## Cross-Reference Graph (text view)

If you read the ADRs in numeric order, the dependency arrows look like
this. Each arrow points from the dependent ADR to the foundation it
depends on (read "0010 → 0002" as "0010 depends on 0002").

```
                  0001  Block Stream Format
                   │
       ┌───────────┼────────────────────────────────┐
       ▼           ▼                                ▼
     0002 Tier   0006 Cutover ── 0008 TSS ── 0009 hinTS
       │           │              │             │
       │           ▼              ▼             ▼
       │         0007 Hist Wrap   │           0016 Status/TSS
       │           │              │              ▲
       │           │              └──────────────┘
       │           │
       │           │ 0004 gRPC ── 0005 Merkle
       │           │   ▲             │
       │           │   └────── 0003 Traffic Direction
       │           │             ▲
       ├──────────►│             │
       │           │             │
   ┌───┴──────────────┐          │
   ▼                  ▼          │
 0010 Min Count     0011 RFH    0012 Public Access
   │                  │            │
   ▼                  │            ▼
 0014 Modes           │          0013 Monetization
   │                  │
   ▼                  │
 0015 Reconnect       │          0017 Bucket Path ◄── 0001, 0007
                      │
                      ▼
                   0018 Plugin SPI
                      │
                      ▼
                   0019 Disruptor ◄── 0004, 0011

                   0020 Archive Format ◄── 0001
                   0021 Ports (deprecated) ◄── 0003, 0012
```

---

## By Theme

Use this when you're investigating a specific area and want to read
everything relevant in one pass.

### Block stream format (the wire and the seal)

- **0001** Block Stream Format Replaces Record Streams — what a block is
- **0004** gRPC Chunked Block Items Protocol — how it's sent
- **0005** Block Hash 4-Leaf Merkle Tree — how each block is sealed
- **0017** Cloud Bucket Path for Block Streams — where it's mirrored
- **0020** On-Disk Block Archive Format — where it's stored at rest

### Cutover from record streams

- **0006** Staged Cutover (Phase 2a / 2b)
- **0007** Historical Record File Wrapping (genesis through cutover)
- **0001** Block Stream Format (the destination format)

### TSS and cryptographic proofs

- **0008** TSS Aggregate Signatures Replace RSA
- **0009** hinTS as the TSS Scheme
- **0016** BN Status Endpoint Exposes TSS Bootstrap
- **0005** Block Hash Structure (the thing being signed)

### Network topology and operator structure

- **0002** Tiered BN Topology (the canonical hierarchy)
- **0003** Block Stream Traffic Direction (CN push, MN pull)
- **0010** Tier 1 BN Minimum Count and Diversity
- **0011** Tier 2 RFH Deployment Posture (this is the recent verdict)
- **0012** Tier 1 Public Access Model
- **0013** Monetization Before Public Launch
- **0015** CN Reconnect via BNs
- **0021** Separate Ingress/Egress Ports (Deprecated)

### Operating modes and serving roles

- **0014** Full-History vs No-History Modes
- **0011** Tier 2 RFH Deployment Posture (private, stateless)
- **0015** CN Reconnect (can be served by either mode)

### Code-level architecture

- **0018** Plugin SPI (BlockNodePlugin / ServiceLoader / JPMS)
- **0019** LMAX Disruptor (inter-plugin messaging)
- **0020** On-Disk Block Archive Format (Zstd + ZIP)

---

## Reading Paths

Different roles often only need a subset of the ADRs. Suggested first-read paths:

- **New backend engineer working on BN code:** 0001, 0002, 0003, 0004, 0005, 0018, 0019, 0020 — in that order.
- **Operator / SRE deploying a BN:** 0002, 0010, 0011, 0012, 0014, 0015, 0017, 0021 — focus on topology, modes, and ports.
- **Cryptography / verification reviewer:** 0005, 0008, 0009, 0016 — block hash + TSS.
- **Cutover or release coordinator:** 0001, 0006, 0007, 0008, 0009, 0010 — what changes when and why.
- **Governance / partnerships:** 0010, 0011, 0012, 0013 — counts, operators, costs, public access.

---

## Currency and Drift

ADRs marked as **Deprecated** or with a Status note like "design intent —
implementation deferred" represent decisions that have drifted from the
codebase or been refined by later ADRs:

- **0021 (Separate Ingress/Egress Ports) — Deprecated.** Implementation
  uses a single port; bandwidth shaping is deferred to the infrastructure
  layer. Listed last in the numbering because it's the least applicable
  decision today.
- **0010 (Tier 1 Min Count and Diversity) — Target reframed.** Original
  "5 BN minimum (2 RFH + 3 LFH)" replaced by 0011's 7+3 topology and a
  reassignment of the RFH acronym to "Remote Full History".
- **0012 (Public Access) — Bandwidth-group enforcement deferred.** The
  design intent is intact; enforcement is now at the infrastructure layer.
- **0002 (Tiered Topology) — Tier 2 role refined.** 0011 defines a
  specific private-cloud-backed Tier 2 variant (RFH); the general Tier 2
  concept remains.

Each drifted ADR carries a "Note (2026-05-22 review)" section at the top
explaining the drift and pointing at the current source of truth.

For decisions that are *under consideration* but not yet ADR-worthy, see
[possible-adrs.md](possible-adrs.md).
