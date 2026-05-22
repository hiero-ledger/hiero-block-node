# 0015 - Block Stream Format Replaces Fragmented Record/Event/State Streams

Date: 2024-04-01

## Status

Accepted

## Context

The Hedera network historically produced four parallel, fragmented data streams: record files (transaction inputs and outputs), event files (gossip events), sidecar files (smart contract state changes), and signature files (per-node RSA signatures). Consumers — primarily Mirror Nodes — had to ingest and correlate all four streams to reconstruct a complete picture of network activity. This fragmentation created complexity, duplication, and significant storage costs.

As the network grows, this model does not scale: storage is duplicated across streams, streams must be independently deduplicated, and consumers must implement correlation logic that is fragile across format changes.

The alternative is a single, unified, self-contained block stream that carries all network data in one serialized, content-addressed format.

## Decision

A unified **Block Stream format** (HIP-1056) replaces all existing record, event, sidecar, and signature streams. Each block contains:

- **Block header**: Block number, timestamp, software version, hash of previous block.
- **Input items**: Event items and raw transaction inputs (in order of consensus).
- **Output items**: Transaction results, state changes (including smart contract state), system transaction outputs.
- **Block proof**: Cryptographic attestation (RSA multi-sig in Phase 2a, TSS aggregate in Phase 2b).

A block closes on round boundaries approximately every 2 seconds of consensus time, replacing the record-file closure model (which was driven by user transaction handling + a time period).

Serialization uses Protocol Buffers. The stream is designed to be parsed incrementally (streaming) rather than requiring the full block to be buffered before processing.

## Consequences

- All four legacy stream formats are replaced; consumers that relied on the old formats must migrate.
- Historical record files are wrapped into a compatible WRB format (ADR-0010) to maintain a contiguous chain from genesis.
- Block numbers are continuous across the record→block stream boundary, with the first block stream block set to `last_record_file_block + 1`.
- Storage is significantly reduced by deduplication (record files contained significant redundancy with sidecar and event files).
- Mirror Nodes must implement Block Stream parsing to continue operating after Phase 2b cutover. Third-party MNs have a grace period via the cloud bucket fallback.
- The block closure boundary change (round-based every ~2 seconds vs transaction-based) means the number of blocks per day changes — tooling and dashboards that assume record-file block counts will need updating.
