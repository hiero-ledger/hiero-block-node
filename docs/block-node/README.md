# Server

The Block Node Server Application is designed to handle the block streams output of a Hiero Consensus Node, parsing and
storing the blocks and network state information. It will also optionally provide many value adding APIs to allow
downstream applications to access block and state information.

![block-node-network-architecture](./../assets/block-node-network-architecture.svg)

## Key Technologies

- **Java**: Primary programming languages.
- **gRPC**: For streaming and communication.
- **Protobuf**: For specification, serialization and deserialization of block data.
- **Helidon**: Web server framework.
- **PBJ** and **PBJ-Helidon**: Java extension for PBJ and Helidon.
- **Gradle** and **Kotlin**: Build and dependency management.
- **LMAX Disruptor**: For high performance inter-thread messaging.
- **Log4J**: For logging.
- **Zstd** and **tar**: For compression.

## Quickstart

Refer to the [Quickstart](quickstart.md) for a quick guide on how to get started with the application.

## High-Level Architecture
BlockNode is a modular, event-driven web-server built on [Helidon](https://helidon.io/) framework.
The server is primarily designed to process gRPC streams of Block Items and distribute them efficiently across system
components using a plugin-based architecture for additional service processing.
The system is highly extensible, allowing developers to add new functionality via plugins.

Additional details are captured in [Architecture Overview](architecture/architecture-overview.md).

## Configuration

Refer to the [Configuration](configuration.md) for configuration options.
