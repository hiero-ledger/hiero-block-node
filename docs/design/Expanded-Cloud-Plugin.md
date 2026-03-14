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

The Expanded Cloud Storage Plugin (ECSP) is a data storage plugin for the block node
that stores individual block files in cloud storage systems.

## Goals

The ECSP must store every block, as received, after verification.
The ECSP must store each block as a single ZStandard-compressed file.
The ECSP must adhere to a file pattern as defined below.

## Terms

<dl>
  <dt>Cloud Storage</dt>
  <dd>Any cloud storage API supported by the plugin and enabled in
      configuration.</dd>
</dl>

## Entities

## Design

1. 

1. `VerificationServicePlugin.handleBlockItemsReceived()` receives batches of
   block items from the messaging facility.
2. When a `BLOCK_HEADER` is detected, a new `VerificationSession` is created via
   `HapiVersionSessionFactory`, keyed on the HAPI proto version from the header.
3. Each batch of block items is passed to `currentSession.processBlockItems()`,
   which returns `null` until the final batch (`isEndOfBlock() == true`).
4. `ExtendedMerkleTreeSession` routes each item to the appropriate streaming
   Merkle tree hasher. The `BLOCK_FOOTER` and `BLOCK_PROOF` items are saved for
   finalization.
5. When the final batch arrives, the session computes the final block hash by
   folding the five subtree roots together with the previous-block hash,
   all-previous-blocks root, state root, and timestamp.
6. The TSS-based block proof's signature is verified: `signature == SHA-384(blockHash)`.
7. A `VerificationNotification(success, blockNumber, blockHash, block, source)`
   is returned to the plugin.
8. On success, the plugin sends the notification via `sendBlockVerification()`,
   updates `previousBlockHash`, and appends the block hash to
   `AllBlocksHasherHandler`.
9. On failure, the plugin sends a failure notification (`success = false`).

## Diagram

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

## Metrics

## Exceptions

## Acceptance Tests
