# Block Node Application

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Configuration](#configuration)
4. [Metrics](#metrics)
5. [Design](#design)
   1. [Block Persistence](#block-persistence)
   2. [Bi-directional Producer/Consumer Streaming with gRPC](#bi-directional-producerconsumer-streaming-with-grpc)

## Overview

The Block Stream Application is designed handle the output of a Hiero Node, which would be in form of Block Stream.
By handling we can understand verifying, storing and applying needed state changes.
It uses various configuration sources and dependency injection to manage its components.

## Configuration

Refer to the [Configuration](docs/configuration.md) for configuration options.

## Quickstart

Refer to the [Quickstart](docs/quickstart.md) for a quick guide on how to get started with the application.

## Metrics

Refer to the [Metrics](docs/metrics.md) for metrics available in the system.

## Design

### Block Persistence

Refer to the [Block Persistence](docs/design/block-persistence.md) for details on how blocks are persisted.

### Bi-directional Producer/Consumer Streaming with gRPC

Refer to the [Bi-directional Producer/Consumer Streaming with gRPC](docs/design/live-streaming/bidi-producer-consumers-streaming.md) for details on how the gRPC streaming is implemented.
