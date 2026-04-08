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
    - Plugin implements the existing `BlockNodePlugin` interfaces
    - The planned plugin queries a peer BN's serverStatusDetail gRPC endpoint to retrieve TSS information
      - gRPC peer communication follows the same WebClient pattern used elsewhere in the codebase
    - An `ApplicationStateFacility` interface will be created.
      - Responsible for handling requests to change `TssData`
      - Responsible for persisting the `TssData`
      - Responsible for notifying plugins when the `BlockNodeContext` changes
      - Will initially be implemented by the `BlockNodeApp`
    - The plugin will check to see if `TSSData` is stored in its config. `TssBootstrapPlugin` config can be used for both testing and temporary initialization for a Block Node
    - The planned plugin will periodically query its peers for TSS data updates.
  - TSS data is exposed to other plugins
    - The BlockNode plugin interface will be refactored to have an onContextUpdated method that will be called by the BlockNode application when the context is updated.
    - The `StatusDetailPlugin` will implement `onContextUpdate()` and receive TSS data updates via the `ApplicationStateFacility`

## Terms

## Entities

### ApplicationStataFacility

- A new entity responsible for updating the plugins when context changes.
- It will call the `BlockNodePlugin.onContextUpdate()` on all plugins when the `BlockNodeContext` changes.
- It will be passed directly to the plugins as a parameter to init()
- Plugins can use the `updateTssData` method to update the `TssData` in the `BlockNodeContext`
- It will process requests to change `TssData`
- It Will persist `TssData`
- It Will load persisted `TssData` prior to plugin initialization
- Will initially be implemented by the `BlockNodeApp`

### BlockNodeContext

- Will contain the `TssData` information and is passed to plugins in the `BlockNodePlugin.init()` call.
- The BlockNodeContext will also be sent to the plugins via the `BlockNodePlugin.onContextUpdate()` callback function on the plugins

### ServiceStatusServicePlugin

- The plugin responsible for the `serverStatus`  and `serverStatusDetail` gRPC calls.
- Will implement the `BlockNodePlugin.onContextUpdate()` to receive `TssData` updates
- Will respond to peer requests for `TssData` via the `serverStatusDetail` call.

### TssBootstrapConfig

- Used to configure the BN Peers that will be used to query `serverStatusDetail`
- Used to bootstrap TSS data via the Config
- Will override initial data provided by the `ApplicationStateFacility`

### TssBootstrapPlugin

- Will contact peer BNs by querying the `serverStatusDetail` gRPC call.
- `TssData` can also be configured using the `TssBootstrapConfig`
- `TssBootstrapConfig` data will override the `TssData` received during `init()`.
- It will notify the `ApplicationStateFacility` when it has a `TssData` update.

### VerificationPlugin

- Verifies block 0
- Will notify the `ApplicationStateFacility` when it detects changes to `TssData`
- Will implement the `BlockNodePlugin.onContextUpdate()` to receive `TssData` updates

### TssData

- The protobuf `TssData` used in the `serverStatusDetailsResponse`

```markdown
/**
* TSS information for a Block-Node.<br/>
* `TssData` contains the Ledger Id, wraps validation key, and TssRoster information
*
* All fields in this message SHALL be filled in, or none.
*/
  message TssData {
  /**
    * The ledger id
    */
      bytes ledger_id = 1;
   /**
    * The wraps verification key
    */
      bytes wraps_verification_key = 2;
   /**
    * The current TSS roster
    */
      TssRoster current_roster = 3;
    }

/**
* TSS Roster information.
*
* `TssRoster` contains a list of roster entry
*/
  message TssRoster {
    /**
     * The list of `RosterEntry`
     */
      repeated RosterEntry roster_entries = 1;
      }

/**
* A single TSS Roster entry.
*
* All fields are REQUIRED.<br/>
* The `node_id` field SHALL match the same field for an entry in the Node Store
* in consensus network state.<br/>
* The `schnorr_public_key` MAY be a placeholder value if the associated node failed
* to participate in the roster election within the required time limit.
*/
  message RosterEntry {
  /**
   * The node id
   */
      uint64 node_id = 1;
   /**
    * The node weight
    */
      uint64 weight = 2;
   /**
    * The schnorr public key
    */
      bytes schnorr_public_key = 3;
}
```

## Design

- Oustanding design decisions
  - Should the TssData or TssRoster messages have the starting block number that the TssData is valid from. Either

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
   - Initial `TssData` will be provided in the BlockNodeContext.
2. Will the `TssBootstrapPlugin` persist the .bin file into the Verification PVC?
   - Plugins should not share configuration information.
   - The verification plugin should manage its own configuration
   - The `ApplicationStateFacility` will be responsible for persisting the `TssData`
   - When the verification plugins detects new `TssData`, ie Block 0, it will notify the `ApplicationStateFacility`.
   - The `ApplicationStateFacility` will update all plugins of BlockNodeContext changes via `BlockNodePlugin.onContextUpdate()`
3. Is this plugin intended to run only once at BN first run.
   - The plugin will periodically query its peer BN servers to see if they have any `TssData` updates.
4. Where and how it will get the BN source(s) for trusted peers to get the TSS data from? We already have the concept of backfill peers.
   - Plugins should not share configuration information.
   - The `TssBootstrapPlugin` will manage its own configuration.
   - Perhaps the `ApplicationStateFacility` should manage the BN Peer information
