# Design Document Template

## Table of Contents

1. [Purpose](#purpose)
2. [Goals](#goals)
3. [Terms](#terms)
4. [Entities](#entities)
5. [Design](#design)
6. [Diagram](#diagram)
7. [Configuration](#configuration)
8. [Metrics](#metrics)
9. [Exceptions](#exceptions)
10. [Acceptance Tests](#acceptance-tests)

## Purpose

We intend to create a new Block Node plugin (TSSBootstrapPlugin) that queries another Block Node's detailed status to pick up TSS configuration details needed to initiate TSS verification.

## Goals

- Deliver TSS booststrap information to a Block Node that does not have access to the network Block 0 transactions.
  - A new Block Node needs to learn the current TSS state (roster, verification key, ledger ID)
    - Plugin implements the existing BlockNodePlugin interfaces
    - The planned plugin queries a peer BN's serverStatusDetail gRPC endpoint to retrieve TSS information
      - gRPC peer communication follows the same WebClient pattern used elsewhere in the codebase
    - The planned plugin will persist the TSS data locally and load it during plugin init() allowing it to be ready when block proofs arrive with TSS Signatures.
    - If TSS data is not present locally, the plugin will check to see if the TSS Data is stored in its config. TssBootstrapPlugin config can be used for both testing and temporary initialization for a Block Node
    - The planned plugin will periodically query its peers for TSS data updates.
  - TSS data is exposed to other plugins
    - The BlockNode plugin interface will be refactored to have an onContextUpdated method that will be called by the BlockNode application when the context is updated.
    - The StatusDetailPlugin will implement ```onContextUpdate()``` and receive TSS data updates via the

## Terms

<dl>
  <dt>Term</dt>
  <dd>A Term is any word or short phrase used in this document that
      requires clear definition and/or explanation.</dd>
</dl>

## Entities

### BlockNodeContext

- Will contain the ```TssData``` information and is passed to plugins in the ```BlockNodePlugin.init()``` call.
- Could possibly contain the ```PluginManager``` so that plugins can pass context updates directly to the ```PluginManager```
- The BlockNodeContext will also be sent to the plugins via the ```BlockNodePlugin.onContextUpdate()``` callback function on the plugins

### ServiceStatusServicePlugin

- The plugin responsible for the ```serverStatus```  and ```serverStatusDetail``` gRPC calls.
- Will implement the ```BlockNodePlugin.onContextUpdate()``` to receive ```TssData``` updates
- Will respond to peer requests for ```TssData``` via the ```serverStatusDetail``` call.

### TssBootstrapConfig

- Used to configure the BN Peers that will be used to query serverStatusDetail
- Used to bootstrap TSS data via the Config
- Should only be used if there is no locally persisted data

### TssBootstrapPlugin

- Will contact peer BNs by querying the ```serverStatusDetail``` gRPC call.
- Will persist TssData locally and use that data during init.
- TssData can also be configured using the TssBootstrapConfig
- It will notify the PluginManager when it has a TssData update.

### VerificationPlugin

- Verifies block 0
- Will notify the  PluginManager when it detects changes to ```TssData```
- Will implement the ```BlockNodePlugin.onContextUpdate()``` to receive ```TssData``` updates

### PluginManager

- A new entity responsible for updating the plugins when context changes.
- It will call the ```BlockNodePlugin.onContextUpdate()``` on all plugins when the ```BlockNodeContext``` changes.
- It could register for ```BlockNodeContext``` Change notifications, validate the change if possible, and notify all plugins if a valid change has been received
- It could be passed directly to the plugins during initialization so that plugins can access a method for updating context changes
- It will be notified of ```BlockNodeContext``` changes by the ```TssBootstrapPlugin``` and the ```VerificationPlugin```
- It will be the central entity, part of the BlockNodeApp that is responsible for notifying plugins of ```BlockNodeContext``` changes

## Design

- Oustanding design decisions
  - How should the plugin manager be contacted by plugins for context changes?
    - ContextChangeNotification?
    - Passed to plugins via BlockNodeContext and direct method access

## Diagram

TBD

Consider using mermaid to generate one or more of the following:
- [User Journey](https://mermaid.js.org/syntax/userJourney.html)
- [Requirement Diagram](https://mermaid.js.org/syntax/requirementDiagram.html)
- [Sequence Diagram](https://mermaid.js.org/syntax/sequenceDiagram.html)
- [Class Diagram](https://mermaid.js.org/syntax/classDiagram.html)
- [Block Diagram](https://mermaid.js.org/syntax/block.html)
- [Architecture Diagram](https://mermaid.js.org/syntax/architecture.html)
- [Flowchart](https://mermaid.js.org/syntax/flowchart.html)
- [State Diagram](https://mermaid.js.org/syntax/stateDiagram.html)
- [Entity Relationship Diagram](https://mermaid.js.org/syntax/entityRelationshipDiagram.html)**

## Configuration

TBD

## Metrics

TBD

## Exceptions

TBD

## Acceptance Tests

TBD

## FAQ

1. Plugin ordering — how does bootstrap init before verification? ServiceLoader doesn't guarantee order.
   - Plugins will be updated using the ```BlockNodePlugin.onContextUpdate()```.
   - Plugins will be updated after ```BlockNodePlugin.init()```.
2. Will TssBootstrap plugin persist the .bin file into the Verification PVC?
   - Plugins should not share configuration information.
   - The verification plugin should manage its own configuration
   - When the verification plugins detects new TssData, ie Block 0, it will notify the plugin manager.
   - The plugin manager will validate the TssData and update the other plugins via ```BlockNodePlugin.onContextUpdate()```
3. Is this plugin intended to run only once at BN first run.
   - The plugin will periodically query its peer BN servers to see if they have any TssData updates.
4. Where and how it will get the BN source(s) for trusted peers to get the TSS data from? We already have the concept of backfill peers.
   - Plugins should not share configuration information.
   - The TssBootstrapPlugin will manage its own configuration.
