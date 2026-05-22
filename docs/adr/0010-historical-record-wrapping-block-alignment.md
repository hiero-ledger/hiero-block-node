# 0010 - Historical Record File Wrapping and Block Number Alignment at Cutover

Date: 2024-12-01

## Status

Accepted

## Context

Hedera has years of history stored in record files. Block Nodes and the block stream ecosystem are built around a single, continuous block-numbered history from genesis. To provide a consistent format without a discontinuity at the moment of cutover, historical record files must be wrapped into the Wrapped Record Block (WRB) format.

Additionally, at the moment CNs switch from record files to block streams, the block number must be set correctly. Record files and block streams close blocks on different boundaries (record files close on user transaction handling + time period elapsed; block streams close every ~2 seconds on round boundaries), so simply continuing the block count from preview block streams would create a growing gap.

Two issues require explicit decisions:
1. How to produce a consistent block history from genesis through cutover.
2. How to align block numbers between the record stream era and the block stream era.

## Decision

**Historical wrapping**: An offline command-line tool (`record_wrap_history`) downloads, deduplicates, verifies, and wraps historical record files into WRB block files. The tool produces:
- A chain of wrapped blocks from genesis through the most recent record file prior to cutover.
- A `record_wrap_history` export ("jumpstart file") containing: the latest `block_number`, the `latest_right_side_block_history_merkle_hash`, and approximately 20–30 of the most recent block hashes for continuity.
- The jumpstart file is included in the network upgrade bundle (council-signed), allowing CNs to bootstrap their block hash Merkle tree at upgrade time.

A special-purpose BN runs the offline wrapping tool and serves as the backfill source for Tier 1 BNs prior to cutover, ensuring Tier 1 BNs are close to live when the cutover occurs.

**Block number alignment**: At cutover, CNs set the first post-cutover block number to `last_record_file_block_number + 1`. This ensures:
- No gaps in block numbering visible to consumers.
- MNs, relay, and smart contracts see a continuous block sequence.
- If preview block streams and record streams ran concurrently, a divergence in block numbers was observed during the preview period — this is resolved at cutover by using only the record-file-derived block number as the source of truth.

## Consequences

- The wrapping tool must achieve close-to-live processing speed (~60 seconds or better for the most recent blocks) to minimize the gap at cutover.
- The jumpstart file must be ready and included in the upgrade file 7–9 days before the release to allow council signing.
- CNs must implement jumpstart file ingestion and one-time record file wrapping logic at upgrade, with a fail-safe to skip and continue normally if the step fails.
- BN operators must deploy the special-purpose wrapping BN ahead of cutover and ensure Tier 1 BNs backfill from it.
- Preview block stream block numbers are effectively discarded at cutover; any consumers referencing preview block numbers must account for this discontinuity.
- Historical record files are **not** required to be present on Tier 1 BN disks prior to cutover; BNs will backfill from peers and the wrapping BN.
