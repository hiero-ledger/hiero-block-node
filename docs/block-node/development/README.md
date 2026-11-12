# Development

## Key Technologies

- **Java**: Primary programming language.
- **gRPC**: For streaming and communication.
- **Protobuf**: For specification, serialization and deserialization of block data.
- **Helidon**: Web server framework.
- **PBJ** and **PBJ-Helidon**: Protocol buffer compiler and Helidon server extension.
- **Gradle**: Build and dependency management.
- **LMAX Disruptor**: For high performance inter-thread messaging.
- **System.Logger**: For logging (JUL backend). See the [Logging Reference](../operate/logging.md), [Logging Architecture](../../design/architecture/Logging-Architecture.md), and [Logging Guidelines](../../logging-guidelines.md).
- **Zstd**, **Zip** and **tar**: For file compression at rest and archival.

## High-Level Architecture

The Block Node is a modular, event-driven network server built on the [Helidon](https://helidon.io/) framework.
The system is highly extensible, allowing developers to add and remove functionality via plugins.

## Documents

|                            Document                            |                        Description                        |
|----------------------------------------------------------------|-----------------------------------------------------------|
| [Architecture overview](architecture/architecture-overview.md) | High-level system architecture and component interactions |
| [Data flow](architecture/data-flow.md)                         | How block data moves through the system                   |
| [Plugin system](architecture/plugins.md)                       | Plugin lifecycle, interfaces, and extension points        |
| [Design docs](../../design/README.md)                          | In-depth design documents for Block Node features         |
| [Simulator](../../simulator/README.md)                         | Block Node simulator for testing and development          |
