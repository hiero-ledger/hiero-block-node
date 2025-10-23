# BlockNode Data Flow

This document describes multiple data flows between system components using its event-driven architecture.

## Block Item Publish Data Flow Overview

```mermaid
---
config:
  theme: redux
  layout: elk
---
sequenceDiagram
    participant Client as gRPC Client
    participant App as StreamPublisherPlugin
    participant Messaging as BlockMessagingFacility
    participant Plugin1 as VerificationServicePlugin
    participant Plugin2 as BlockFileRecentPlugin

    Client->>App: Send Block Items (gRPC)
    App->>Messaging: Pass Block Items
    Messaging->>Plugin1: Deliver Block Items
    Plugin1->>Messaging: Publish VerificationNotification
    Messaging->>Plugin2: Deliver VerificationNotification
    Plugin2->>Messaging: Publish PersistedNotification
    Messaging->>App: Waits on Notifications
    App->>Client: Sends Acknowledgment
```
![block-item-publish-flow](./../../assets/block-item-publish-flow.svg)

Note: Each plugin processes items independently making use of thread isolation

1. **gRPC Client** sends a stream of Block Items to BlockNode.
2. **BlockNodeApp** receives the stream and forwards Block Items to the `BlockMessagingFacility`.
3. **BlockMessagingFacility** distributes Block Items to all registered plugins (handlers). Each handler runs on its own thread, ensuring isolation and scalability.
4. **Plugins** process Block Items according to their logic (e.g., storing, verifying, publishing, etc.).
5. Back pressure is applied if any plugin is slow, ensuring system stability.



