# BlockNode API Data Flows

This document describes multiple data flows between system components using its event-driven architecture.

## Block Stream Publish API Flow

** Overview should cover block items flow, merkle tree building, verification, persistence, and notification. **

![block-item-publish-flow](./../../assets/block-item-publish-flow.svg)
or
![block-n-acknowledgment-flow](./../../assets/block-n-acknowledgement-flow.svg)


1. gRPC publisher client sends a stream of Block Items to BlockNode.
2. Block Node server receives and routes the stream to the `BlockStreamPublishService` in the `StreamPublisherPlugin` implementation.
3. `StreamPublisherPlugin` publishes the stream to the `BlockMessagingFacility`.
4. `BlockMessagingFacility` broadcasts Block Items to all registered plugins (handlers). Each handler runs on its own thread, ensuring isolation and scalability.
5. `VerificationServicePlugin` process Block Items building the block merkle tree and verifying it upon receipt of a block proof. It publishes a `VerificationNotification` upon successful verification.
6. `BlockFileRecentPlugin` listens for a `VerificationNotification` and persists a verified block accordingly. It also publishes a `PersistedNotification` upon completions
7. `StreamPublisherPlugin` waits for both the `VerificationNotification` and `PersistedNotification` before sending an acknowledgment back to the gRPC client.

Note: Each plugin processes items independently making use of thread isolation

## Block Access API Flow


## Block Stream Subscription API Flow


## Backfilling Flow

