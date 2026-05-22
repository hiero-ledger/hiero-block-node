# 0008 - Block Stream Network Protocol: gRPC with Chunked Block Items

Date: 2024-09-01

## Status

Accepted

## Context

The block streaming protocol between CN→BN and BN→MN (and BN→BN) requires a network API design. The fundamental tradeoff is between **latency** (consumers want to begin processing block items as early as possible, before the block is sealed with a signature) and **throughput/simplicity** (fewer, larger messages are easier to implement and less taxing on gRPC infrastructure).

Four options were evaluated:

1. **Stream of Block Items** — each item sent as a separate gRPC message. Maximum granularity and lowest latency. Rejected: gRPC off-the-shelf implementations struggle at message rates above ~tens of thousands per second; expected block item rates are 30k–50k/s. A custom gRPC client would be required for all consumers.
2. **Stream of complete Blocks** — one gRPC message per sealed, signed block. Simplest. Rejected: protobuf requires message size up front, meaning the consumer cannot receive a byte until the block proof signature is complete. Consumers lose ~1 second of processing time per block that they could have used to hash, parse, and convert block items.
3. **Stream of repeated Block Items in chunks** — items batched and sent in groups as they become available; the proof is sent as a final chunk. Adopted.
4. **Two async streams (items + proofs)** — items and block proofs sent on separate gRPC streams. Considered for future; adds implementation complexity requiring consumers to pipeline and match streams.

## Decision

**Option 3 — Stream of repeated Block Items in chunks** is adopted (HIP-1081).

- Items are sent in batches as they become available from the CN execution pipeline; the block proof is the final batch item.
- Maximum chunk size target: ~1,000 items per message, yielding approximately 30–50 gRPC stream messages per second — well within gRPC comfort zone.
- Consumers (BN, MN) begin parsing, hashing, and converting items as soon as the first chunk arrives, gaining nearly a full second of processing headroom before receiving the block proof.
- The CN may send all non-proof items for block N+1 immediately after completing block N's proof, before N+1's proof is available — enabling block pipelining.

## Consequences

- Consumers must implement incremental processing logic to handle partial blocks before the proof arrives.
- Block Nodes must pass chunks through to downstream consumers with minimal parsing/re-serialization to preserve the latency benefit.
- gRPC message rate stays low enough to use standard client implementations without custom high-performance gRPC clients.
- Option 4 (dual streams) remains available as a future optimization if sub-second TSS signatures make the latency difference negligible.
- The CN pipelining architecture processes multiple blocks concurrently (different pipeline stages for different blocks), and the streaming protocol must be compatible with this concurrent production model.
