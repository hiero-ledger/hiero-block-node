# Block Node Application

## Overview

The Block Node Application receives the block stream produced by Hiero Consensus Nodes, verifies
each block's cryptographic integrity, stores the verified blockchain, and distributes blocks to
downstream clients via gRPC streaming APIs.

## Configuration

Refer to the [Configuration](../docs/block-node/configuration.md) for configuration options.

## Quickstart

Refer to the [Docker Compose Quickstart](../docs/block-node/docker-compose-quickstart.md) for a
quick guide on how to run the application locally.

## Metrics

Refer to the [Metrics](../docs/block-node/metrics.md) for metrics available in the system.

## Design

### Block Persistence

Refer to the [Block Persistence](../docs/design/persistence/block-persistence.md) for details on
how blocks are persisted.

### Bi-directional Producer/Consumer Streaming with gRPC

Refer to the [Bi-directional Streaming](../docs/design/streaming/bidi-producer-consumers-streaming.md)
for details on how the gRPC streaming is implemented.
