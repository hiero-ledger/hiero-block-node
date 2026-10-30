# 0017 - New Cloud Bucket Path for Block Streams

Date: 2024-12-01

## Status

Accepted

## Context

Record files, sidecar files, and event files were historically uploaded to cloud buckets (GCP and AWS) under well-known path prefixes. Mirror Nodes and third-party consumers reference these paths. As the network transitions to block streams, a decision is needed on whether to reuse existing bucket paths (potentially causing consumer confusion between old and new formats) or to introduce a clean new path structure.

Additionally, the block stream cloud bucket serves a different role than the Tier 1 BN streaming API: the bucket is a secondary distribution channel for consumers who have not yet subscribed to BNs, and serves as a fallback for Hedera MN during Phase 2a.

## Decision

**A new cloud bucket path** is used for block stream block files, separate from all existing record/event/sidecar paths. The path structure is defined in HIP-1193 and standardized across GCP and AWS buckets.

- RFH (Record-File-History) BNs additionally upload individual blocks to a **recent-blocks cloud bucket** with a short retention policy (approximately 1 month), providing a rolling window of recent blocks for operational validation and community access without requiring full-history BN access.
- The record stream bucket paths remain unchanged and continue to serve record files through the end of Phase 2a. They are retired at Phase 2b cutover.
- Block stream bucket paths are read-only for consumers; BNs are the primary writers via their bucket upload plugin.

## Consequences

- Third-party Mirror Nodes can continue consuming record files from the old bucket paths through Phase 2b, giving them time to adopt the Block Streams format.
- The new bucket path must be communicated to all ecosystem partners well in advance of Phase 2b.
- The recent-blocks bucket (1-month retention) provides a safety net for operational debugging and simplifies validation during the Phase 2a soak period.
- BNs must implement a bucket-upload plugin that writes to S3-compatible storage (GCP and AWS). This is a separately configurable plugin and not core to the BN's streaming function.
- Bucket costs should be evaluated: block files are larger than individual record files but deduplication means fewer total files; net cost impact depends on retention policy and access patterns.
