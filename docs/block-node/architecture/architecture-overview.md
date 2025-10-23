# Architecture Overview

The BlockNode is primarily designed to process gRPC streams of Block Items and distribute them efficiently across system
components and to clients using a plugin-based architecture for additional service processing.

## Key Concepts

- **Event-Driven:** BlockNode receives gRPC streams of Block Items, which are distributed to plugins and drive the processing logic.
- **Plugin System:** All major features are implemented as plugins, conforming to the `BlockNodePlugin` interface.
  Plugins are dynamically loaded and initialized at startup.
- **Messaging:** The `BlockMessagingFacility` is responsible for distributing event messages (via LMAX Disruptor) to registered handlers defined in plugins.
- **Block Management:** Historical block creation is managed by `HistoricalBlockFacilityImpl`, which aggregates multiple
  block providers and exposes a unified view of available blocks.

## Modules

The repo structure is organized into multiple Java modules, each encapsulating specific functionality.
Plugin modules are loaded dynamically at runtime using the JPMS service loader mechanism.

### Main Modules

The following modules under `block-node` directory form the core of the BlockNode system.
- `app`: Main application logic and entrypoint (`BlockNodeApp.java`).
- `spi`: Service Provider Interfaces for plugins and facilities.
- `messaging`: Core messaging facilities for distributing block items.
- `health`: Health check and monitoring plugins.

### Additional Modules

The following modules provide additional functionality and are loaded as plugins if present:
- `block-access`: Interfaces and implementations for accessing block data.
- `block-providers`: Implementations for various block storage backends.
- `s3-archive`: S3-based block archiving plugin.
- `server-status`: Server status monitoring plugin.
- `stream-publisher`: Stream publishing plugin for external systems.
- `stream-subscriber`: Stream subscribing plugin for external systems.
- `verification`: Block verification plugin.

## System Architecture Diagram

```mermaid
---
config:
  theme: redux
  layout: elk
---
flowchart TD
 subgraph subGraph0["gRPC Publisher Client"]
        Q["gRPC BlockStreamPublish Request"]
  end
 subgraph subGraph1["gRPC Subscriber Client"]
        R["gRPC BlockStreamSubscribe Request"]
  end
 subgraph subGraph2["gRPC Block Client"]
        S["gRPC BlockAccess Request"]
        T["gRPC NodeStatus Request"]
  end
 subgraph X["BlockNode Core Modules"]
        B["SPI"]
        C["BlockMessagingFacility"]
        D["BlockProviders"]
        E["Messaging"]
        F["Health"]
  end
 subgraph Y["BlockNode Add-On Modules"]
        G["BlockAccess"]
        H["S3-Archive"]
        I["ServerStatus"]
        J["StreamPublisher"]
        K["StreamSubscriber"]
        L["Verification"]
  end
 subgraph subGraph4["BlockNode Server"]
        A["BlockNodeApp"]
        X
        Y
  end
    A --> X & Y
    Q <--> J
    R <--> K
    S <--> G
    T <--> I
```
![block-node-app-logic](./../../assets/block-node-app-logic.svg)

Additional details regarding Service interactions are illustrated in [Block-Node-Nano-Services](./../../assets/Block-Node-Nano-Services.svg) diagram.

## Plugins
BlockNode's functionality is extended through a variety of plugins, each implementing the `BlockNodePlugin` interface. Key plugins include:
- **BackfillPlugin:** Guarantees the stored block stream is complete by retrieving missing blocks from other Block Nodes.
- **BlockAccessServicePlugin:** Provides block retrieval APIs.
- **BlocksFilesHistoricPlugin:** Archives and retrieves older persisted blocks.
- **BlocksFilesRecentPlugin:** Handles block persistence, retrieval, and deletion of recently streamed blocks.
- **HealthServicePlugin:** Provides kubernetes health check endpoints.
- **S3ArchivePlugin:** Archives blocks to S3 API compatible remote storage services.
- **ServerStatusServicePlugin:** Provides block node status API endpoints.
- **StreamPublisherPlugin:** Implements block stream publishing API as documented in the [communication protocol](./../../design/communication-protocol/README.md).
- **SubscriberServicePlugin:** Implements the unverified block subscription API.
- **VerificationServicePlugin:** Verifies incoming blocks for integrity prior to persistence.

For additional details on plugins, refer to the [Plugins](./plugins.md).

## How The Block Node Works

1. **Startup:** `BlockNodeApp` loads configuration, initializes plugins, registers API services and starts the web server.
2. **Event Distribution:** Incoming gRPC block streams from a publisher are received and passed to `BlockMessagingFacility`,
   which distributes block items via a block items ring buffer to registered plugins. Additionally, registered plugins may
   put notification events onto a notification ring buffer to inform other services of available actions.
3. **Plugin Processing:** Each registered plugin processes block items and notifications independently, enabling modular and extensible workflows.

## API Data Flows

Multiple API data flows occur within BlockNode, primarily centered around block item processing and distribution. Key flows include:
- **Block Stream Publish API Flow:** Incoming block items from gRPC streams are handled via the `StreamPublisherPlugin` and distributed to plugins via the `BlockMessagingFacility`.
- **Block Access API Flow:** Block access requests from gRPC clients are routed to the appropriate block provider plugins for retrieval.
- **Block Stream Subscription API Flow:** Block streams are served to subscribers via the `SubscriberServicePlugin`.
- **Backfilling Flow:** Missing historical blocks are retrieved by the `BackfillPlugin`.

These flows are illustrated in detail in the [Data Flow](data-flow.md) document.
