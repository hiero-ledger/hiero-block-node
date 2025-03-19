# Design Document Template

## Table of Contents

1. [Purpose](#purpose)
2. [Goals](#goals)
3. [Terms](#terms)
4. [Entities](#entities)
5. [Design](#design)
6. [Sequence Diagram](#sequence-diagram)
7. [Configuration](#configuration)
8. [Metrics](#metrics)
9. [Exceptions](#exceptions)
10. [Acceptance Tests](#acceptance-tests)

## Purpose

The purpose of this document is to define the design of the BlockContentProof feature.
The BlockContentProof API allows the BN to provide a proof of the content of a block to the client.

## Goals

1. Define the BlockContentProof struct
2. Define the BlockContentProof API
3. BN should be able to calculate the BlockContentProof given a BlockNumber and BlockItem (or BlockItemHash or BlockItemIndex)
4. A Client should be able to verify the BlockContentProof easily.

## Terms

<dl>
  <dt>BlockNumber</dt>
  <dd>A value that represents the unique number (identifier) of a given `Block`.
  It is a strictly increasing `long` value, starting from zero (`0`).</dd>

  <dt>BlockItem</dt>
  <dd>Every block is comprised of a repeated set of block_items, there are many types of block_items, but for the purpose of understanding the BlockContentProof we can divide them into 2 large groups, `inputs` and `outputs` each Kind of BlockItem can belong to these groups and this is very important in order to calculate correctly the BlockRootHash and a BlockContentProof. </dd>

  <dt>BlockItemHash</dt>
  <dd>Is the SHA-384 hash of the block_item </dd>

  <dt>MerkleSiblingHashes</dt>
  <dd>Are the sibling missing hashes from the item all the way up to the BlockRootHash.</dd>

  <dt>BlockContentProof</dt>
  <dd>A data structure that holds all the needed information to prove a given block item is part of a given block number. Is at least comprised of `BlockNumber, `BlockItemHash`, `MerkleSiblingHashes`, `BlockSignature`</dd>

  <dt>BlockRootHash</dt>
  <dd>Is a "special" merkle tree defined by the `Hiero` DLT that is comprised by `previousBlockHash`, `inputsMerkleTree`, `outputsMerkleTree` and `stateRootHash`.</dd>

  <dt>Block Signature</dt>
  <dd>Is the Signed BlockRootHash by the Network, its purpose is to verify that the BlockRootHash of a given block was signed by the network.</dd>

</dl>

## API

### block_proof.proto

```protobuf
message MerkleSiblingHash {
    bool is_first = 1;
    bytes sibling_hash = 2;
}
```

### block_proof_service.proto

```protobuf
 message BlockContentProof {
  com.hedera.hapi.block.stream.BlockItem block_item = 1;
  repeated MerkleSiblingHash sibling_hashes = 2;
  bytes block_signature = 3;
}

message BlockContentProofRequest {
    uint64 block_number = 1;
    oneof block_item {
        com.hedera.hapi.block.stream.BlockItem block_item = 1;
        bytes block_item_hash = 2;
        uint32 block_item_index = 3;
    }
}

enum BlockContentProofResponseCode {
    BLOCK_CONTENT_PROOF_UNKNOWN = 0;
    BLOCK_CONTENT_PROOF_SUCCESS = 1;
    BLOCK_CONTENT_PROOF_NOT_FOUND = 2;
    BLOCK_CONTENT_PROOF_INSUFFICIENT_BALANCE = 3;
    BLOCK_CONTENT_PROOF_INTERNAL_ERROR = 4;
    BLOCK_CONTENT_PROOF_DUPLICATE_HASH_ITEM_FOUND = 5;
}

message BlockContentProofResponse {
    BlockContentProofResponseCode status = 1;
    com.hedera.hapi.block.stream.BlockContentProof block_content_proof = 2;
}

service BlockContentProofService {
    rpc getBlockContentProof(BlockContentProofRequest) returns (BlockContentProofResponse);
}
```

## Entities

### BlockContentProof

BlockContentProof is a new record that contains all the necessary data to compute the block content proof. It contains the following fields:
- block_number: long,
- block_item: BlockItem,
- block_item_hash: Bytes,
- sibling_hashes: List< MerkleSiblingHash>,
- block_root_hash: Bytes,
- block_signature: Bytes

### PBJBlockContentProofServiceProxy

PBJBlockContentProofServiceProxy is the entity responsible for handling the block content proof requests. It provides the implementation for the `getBlockContentProof` rpc endpoint.
Handles the verification of webServerStatus, handles exceptions, and wraps the response from the BlockContentProofService in a `BlockContentProofResponse` message.

### BlockContentProofService

Requests the Block to the BlockReader (Persistence Module), waits to get it and once it gets the Block it creates a new BlockMerkleTreeInfo record out of that block, after that it uses it to create the BlockContentProof and returns it to the PBJBlockContentProofServiceProxy.

### BlockReader

The BlockReader is responsible for reading the block items from the block store and providing them to the BlockContentProofService.

### BlockMerkleTreeInfo

Is a new record type that contains all the necessary data to compute any block item proof in the block. it contains:
- inputsMerkleTree: List< List< Bytes>>,
- outputsMerkleTree: List< List< Bytes>>,
- previousBlockHash: Bytes,
- stateRootHash: Bytes,
- blockHash: Bytes,

## Design

1. The `PBJBlockContentProofServiceProxy` receives the block content proof requests.
2. If the request is valid and the web server is running, it forwards the request to the `BlockContentProofService`.
3. The `BlockContentProofService` requests the block from the `BlockReader`.
4. The `BlockReader` reads the block items from the block store and provides them to the `BlockContentProofService`.
5. `BlockContentProofService` calculates the `BlockMerkleTreeInfo` and creates the `BlockContentProof` using the `BlockMerkleTreeInfo`.
6. The `BlockContentProofService` returns the `BlockContentProof` to the `PBJBlockContentProofServiceProxy`.
7. The `PBJBlockContentProofServiceProxy` wraps the `BlockContentProof` in a `BlockContentProofResponse` and sends it back to the client.

## Sequence Diagram

```mermaid
sequenceDiagram
    participant Client as Client
    participant Proxy as PBJBlockContentProofServiceProxy
    participant Service as BlockContentProofService
    participant Reader as BlockReader

    Client->>Proxy:  BlockContentProofRequest
    Proxy->>Proxy:  Validates request & check web server status
    Proxy->>Service: BlockContentProofRequest
    Service->>Reader: Requests Block Number
    Reader-->>Service: Returns Block items
    Service->>Service: Calculate BlockMerkleTreeInfo
    Service->>Service: Create BlockContentProof using BlockMerkleTreeInfo
    Service-->>Proxy: Return BlockContentProof
    Proxy->>Client: BlockContentProofResponse

```

## Configuration

## Metrics

1. BlockContentProofRequest_Success Counter
2. BlockContentProofRequest_Error Counter
3. BlockContentProofResponse_Latency Histogram

**Note:** Once we are able to add labels we should add `status` label to the metrics

## Exceptions

Any exception will be handled and return `BLOCK_CONTENT_PROOF_INTERNAL_ERROR` response code to the client.

## Acceptance Tests

### Happy Path:

1. Client sends a valid BlockContentProofRequest to the BN for an existing block and block_item.
2. BN receives the request and returns a complete BlockContentProofResponse
3. Client receives the BlockContentProofResponse and verifies the block_item using the BlockContentProof provided by the BN.

### Edge Case duplicate block_item (Failure Case):

1. Client sends a BlockContentProofRequest using `block_item_hash` to the BN for a block_item that has a duplicate hash in the block.
2. BN receives the request and returns a `BLOCK_CONTENT_PROOF_DUPLICATE_HASH_ITEM_FOUND` response code to the client.
3. Client receives the response and verify is the correct response code.

### Edge Case duplicate block_item (Success Case):

1. Client sends a BlockContentProofRequest using `block_item_index` to the BN for a block_item that has a duplicate hash in the block.
2. BN receives the request and returns a complete BlockContentProofResponse.
3. Client receives the BlockContentProofResponse and verifies the block_item using the BlockContentProof provided by the BN.
4. Client verifies that the block_item is the correct one by pre-calculating the merkle proof path before hand for the right item and then comparing the given proof with the expected proof.

### Block Number not found:

1. Client sends a BlockContentProofRequest to the BN for a non-existing block number.
2. BN receives the request and returns a `BLOCK_CONTENT_PROOF_NOT_FOUND` response code to the client.

### Block item not found:

1. Client sends a BlockContentProofRequest to the BN for a non-existing block item. (variations of block_item, block_item_hash, block_item_index)
2. BN receives the request and returns a `BLOCK_CONTENT_PROOF_NOT_FOUND` response code to the client.

### Block item hash is invalid:

1. Client sends a BlockContentProofRequest to the BN for a block_item with an invalid hash.
2. BN receives the request and returns a `BLOCK_CONTENT_PROOF_NOT_FOUND` response code to the client.
