---
hip: TBD
title: Historical Record File Wrapping Conversion
author: Jasper Potts <jasper.potts@swirldslabs.com>
working-group: Hiero Block Node Team
type: Standards Track
category: Core
needs-council-approval: Yes
status: Draft
created: 2025-01-XX
discussions-to: https://github.com/hiero-ledger/hiero-block-node/discussions
requires: 1056, 1081, 1200
---

## Abstract

This HIP defines a standard method for converting Hedera's historical blockchain data from legacy
Record Stream file formats (versions 2, 5, and 6) into the unified Block Stream format specified
by HIP-1081. The conversion process is lossless and reversible, preserving all original
cryptographic signatures and verification capabilities while enabling a single, modern format
for the entire blockchain history.

Each historical record file is wrapped into a Block Stream block containing exactly four items:
a BlockHeader with versioning metadata, a RecordFileItem containing the record file contents
normalized to V6 format along with any sidecar files and amendments, a BlockFooter with hash
chain links, and a BlockProof containing a SignedRecordFileProof with the original RSA node
signatures.

Once the first live Block Stream block is produced with a TSS (Threshold Signature Scheme)
signature after the last wrapped block, additional TSS-based StateProof block proofs are
appended to each historical block. This enables verification of any historical block using
only the network's Ledger ID, eliminating the need to track historical address books.

Additionally, this HIP specifies amendments for historically missing data: the initial network
state at Stream-Start (Block 0) representing 15,368 accounts and other entities created before
the record stream began, and 113 missing transactions from three separate bug periods between
2019 and 2023.

## Motivation

The Hedera network's blockchain history exists in three different binary formats (Record Stream
versions 2, 5, and 6), each requiring dedicated parsing code and signature verification logic.
This creates several problems:

1. **Format Complexity**: Developers must implement three different parsers with version-specific
   hash computation algorithms to work with historical data.

2. **Redundant Storage**: The current architecture stores approximately 30 copies of the record
   stream across consensus nodes, resulting in roughly 20x redundant storage.

3. **Incomplete Genesis State**: The network operated for nearly a year (2018-12-16 to 2019-09-13)
   before Stream-Start, creating 15,368 accounts whose initial balances are not recorded in the
   stream. This makes complete balance validation impossible.

4. **Missing Transactions**: Historical bugs caused 113 transactions to be omitted from the
   record stream across three separate periods (2019, 2022, and 2023).

5. **Verification Complexity**: Each format version uses different hash computation methods,
   requiring version-specific signature verification.

By converting all historical data to Block Stream format with amendments, downstream consumers
(Block Nodes, Mirror Nodes, analytics platforms, and applications) can work with a single format
that provides complete, cryptographically verifiable blockchain history.

## Rationale

### Why Wrap Rather Than Parse and Re-encode

The wrapping approach preserves the original RSA signatures from consensus nodes. Re-encoding
the data would invalidate these signatures and require either:
- Signing 81+ million blocks with TSS (computationally prohibitive)
- Maintaining two separate trust chains (operationally complex)

By wrapping, the original signatures remain valid and can be verified by reconstructing the
record file bytes and computing the version-specific hash.

### Why Four Block Items (Initially)

The initial four-item structure mirrors the standard Block Stream format:
1. **BlockHeader**: Provides consistent metadata interface
2. **RecordFileItem**: Contains the wrapped data in a normalized container
3. **BlockFooter**: Maintains the hash chain linking blocks
4. **BlockProof**: Contains verification data (SignedRecordFileProof for historical blocks)

This structure allows existing Block Stream tooling to process wrapped blocks with minimal
modification. After the first TSS-signed live block is produced, a fifth item (TSS StateProof)
is appended to enable simplified verification.

### Why Two Block Proofs

Each wrapped block eventually contains two block proofs serving different purposes:

1. **SignedRecordFileProof**: Preserves the original RSA signatures from consensus nodes at
   the time the record file was created. This provides historical trust anchored to the
   original signing event but requires knowledge of the address book at that time.

2. **TSS StateProof**: Added after wrapping, this proof uses the `root_hash_of_all_block_hashes_tree`
   merkle tree to create a concise proof linking the historical block to a TSS-signed live block.
   Verification requires only the network's Ledger ID.

The dual-proof approach provides both backward compatibility (original signatures remain
verifiable) and forward simplicity (TSS verification with just the Ledger ID).

### Why Store Version in SignedRecordFileProof

The record file format version determines how the signed hash is computed. Storing the version
in the proof allows verification code to select the correct hash algorithm without parsing the
wrapped record file contents.

### Why RecordStreamFile as the Container Format

Record Stream V6 uses protobuf encoding via the `RecordStreamFile` message, which cleanly
encapsulates the transaction data. Converting V2 and V5 data to this format provides:
- A single, well-documented structure
- Lossless, reversible conversion
- Clean protobuf APIs for data access

## User Stories

**As a Block Explorer Developer**, I want to query any block from genesis to present using a
single API and data format, so that I don't need to maintain multiple parsing implementations.

**As an Auditor**, I want to cryptographically verify any historical block using only the
Ledger ID and the block data, so that I can independently validate blockchain integrity without
tracking historical address book changes.

**As an Archive Operator**, I want to store deduplicated blockchain history without maintaining
multiple format-specific storage systems, reducing storage costs by approximately 20x.

**As an Analytics Platform Operator**, I want access to complete blockchain data including
pre-Stream-Start state and historically missing transactions, so that my balance calculations
are accurate.

**As a New Validator**, I want to sync the complete blockchain history using the same format
as live blocks, so that I don't need special code paths for historical data.

**As a Wallet Developer**, I want to verify historical transactions using the same TSS
verification method as live blocks, so that I don't need to implement multiple signature
verification algorithms or maintain address book history.

## Specification

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT",
"RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in
RFC 2119.

### Wrapped Block Structure

Each historical record file SHALL be converted to a Block Stream block containing exactly
four BlockItems in this order:

#### BlockHeader

```protobuf
message BlockHeader {
    // HAPI protocol version from the original record file
    SemanticVersion hapi_proto_version = 1;

    // Software version - SHALL be null (not available in historical files)
    SemanticVersion software_version = 2;

    // Sequential block number starting from 0
    int64 number = 3;

    // Timestamp of the first transaction in the block
    Timestamp block_timestamp = 4;

    // Hash algorithm - SHALL be SHA2_384
    BlockHashAlgorithm hash_algorithm = 5;
}
```

#### RecordFileItem

```protobuf
message RecordFileItem {
    // Consensus time from the record file name
    Timestamp creation_time = 1;

    // Record file contents converted to V6 RecordStreamFile format
    RecordStreamFile record_file_contents = 2;

    // Zero or more sidecar files associated with this record file
    repeated SidecarFile sidecar_file_contents = 3;

    // Amendments for missing or corrected data
    repeated RecordStreamItem amendments = 4;
}
```

#### BlockFooter

```protobuf
message BlockFooter {
    // Block Stream hash of the previous wrapped block
    // For Block 0: SHA384(0x00) (EMPTY_TREE_HASH)
    bytes previous_block_root_hash = 1;

    // Merkle root of all block hashes from Block 0 to previous block
    bytes root_hash_of_all_block_hashes_tree = 2;

    // State root hash at block start
    // SHALL be EMPTY_TREE_HASH (state hashes not in record files)
    bytes start_of_block_state_root_hash = 3;
}
```

#### BlockProof with SignedRecordFileProof

```protobuf
message BlockProof {
    oneof proof {
        SignedRecordFileProof signed_record_file_proof = 3;
    }
}

message SignedRecordFileProof {
    // Original record file format version (2, 5, or 6)
    uint32 version = 1;

    // RSA signatures from consensus nodes
    repeated RecordFileSignature record_file_signatures = 2;
}

message RecordFileSignature {
    // RSA signature bytes
    bytes signature_bytes = 1;

    // Node ID that created this signature
    uint64 node_id = 2;
}
```

### Record File Reconstruction

To verify a wrapped block, the original record file bytes MUST be reconstructed from the
`RecordFileItem` contents. The following pseudocode describes the reconstruction procedure.

All multi-byte integers are big-endian unless otherwise specified.

#### Version 2 Reconstruction

```
PROCEDURE reconstructV2RecordFile(hapiVersion, previousBlockHash, recordStreamFile):
    buffer = new ByteBuffer()

    // Header
    buffer.writeInt32(2)                           // Version number
    buffer.writeInt32(hapiVersion.major)           // HAPI major version only
    buffer.writeByte(0x01)                         // Previous file hash marker
    buffer.writeBytes(previousBlockHash)           // 48-byte SHA-384 hash

    // Record stream items
    FOR EACH item IN recordStreamFile.recordStreamItems:
        buffer.writeByte(0x02)                     // Record marker
        txnBytes = protobufEncode(item.transaction)
        buffer.writeInt32(txnBytes.length)
        buffer.writeBytes(txnBytes)
        recordBytes = protobufEncode(item.record)
        buffer.writeInt32(recordBytes.length)
        buffer.writeBytes(recordBytes)
    END FOR

    RETURN buffer.toByteArray()
END PROCEDURE
```

#### Version 5 Reconstruction

```
PROCEDURE reconstructV5RecordFile(hapiVersion, recordStreamFile):
    buffer = new ByteBuffer()

    // Header
    buffer.writeInt32(5)                           // Version number
    buffer.writeInt32(hapiVersion.major)           // HAPI major
    buffer.writeInt32(hapiVersion.minor)           // HAPI minor
    buffer.writeInt32(hapiVersion.patch)           // HAPI patch
    buffer.writeInt32(1)                           // Object stream version

    // Start object running hash (V5 HashObject format)
    writeV5HashObject(buffer, recordStreamFile.startObjectRunningHash)

    // Record stream items
    FOR EACH item IN recordStreamFile.recordStreamItems:
        buffer.writeLong(0xe370929ba5429d8b)       // RecordStreamObject class ID
        buffer.writeInt32(1)                        // Class version
        recordBytes = protobufEncode(item.record)
        buffer.writeInt32(recordBytes.length)       // Note: record before transaction in V5
        buffer.writeBytes(recordBytes)
        txnBytes = protobufEncode(item.transaction)
        buffer.writeInt32(txnBytes.length)
        buffer.writeBytes(txnBytes)
    END FOR

    // End object running hash (V5 HashObject format)
    writeV5HashObject(buffer, recordStreamFile.endObjectRunningHash)

    RETURN buffer.toByteArray()
END PROCEDURE

PROCEDURE writeV5HashObject(buffer, hashObject):
    buffer.writeLong(0xf422da83a251741e)           // Hash class ID
    buffer.writeInt32(1)                            // Class version
    buffer.writeInt32(0x58ff811b)                   // Digest type (SHA-384)
    buffer.writeInt32(48)                           // Hash length
    buffer.writeBytes(hashObject.hash)              // 48-byte hash
END PROCEDURE
```

**V5 HashObject Structure (68 bytes total)**:

| Offset | Size | Field | Value |
|--------|------|-------|-------|
| 0 | 8 | Class ID | `0xf422da83a251741e` |
| 8 | 4 | Class Version | `1` |
| 12 | 4 | Digest Type | `0x58ff811b` (SHA-384) |
| 16 | 4 | Hash Length | `48` |
| 20 | 48 | Hash Bytes | SHA-384 hash |

#### Version 6 Reconstruction

```
PROCEDURE reconstructV6RecordFile(recordStreamFile):
    buffer = new ByteBuffer()

    // Header
    buffer.writeInt32(6)                           // Version number

    // Protobuf-encoded RecordStreamFile
    buffer.writeBytes(protobufEncode(recordStreamFile))

    RETURN buffer.toByteArray()
END PROCEDURE
```

### Record File Hash Computation

The hash computation differs by version. The computed hash is what the node signatures verify.

#### Version 2 Hash (Two-Stage)

V2 uses a two-stage hash where content is hashed separately from the header:

```
CONSTANT V2_HEADER_LENGTH = 57   // 4 + 4 + 1 + 48 bytes

PROCEDURE computeV2SignedHash(recordFileBytes):
    // Step 1: Hash the content (everything after header)
    contentHash = SHA384(recordFileBytes[V2_HEADER_LENGTH:])

    // Step 2: Hash header concatenated with content hash
    signedHash = SHA384(recordFileBytes[0:V2_HEADER_LENGTH] || contentHash)

    RETURN signedHash
END PROCEDURE
```

#### Version 5 and 6 Hash (Entire File)

V5 and V6 use a simple whole-file hash:

```
PROCEDURE computeV5V6SignedHash(recordFileBytes):
    RETURN SHA384(recordFileBytes)
END PROCEDURE
```

### Running Hash Computation (V5/V6)

The running hash chains all record stream items together. It is computed from the start
running hash through each item.

```
CONSTANT HASH_CLASS_ID = 0xf422da83a251741e      // -854880720348154850 as signed long
CONSTANT HASH_CLASS_VERSION = 1
CONSTANT RECORD_STREAM_OBJECT_CLASS_ID = 0xe370929ba5429d8b
CONSTANT RECORD_STREAM_OBJECT_VERSION = 1

PROCEDURE computeRunningHash(recordStreamFile):
    runningHash = recordStreamFile.startObjectRunningHash.hash

    FOR EACH item IN recordStreamFile.recordStreamItems:
        // Hash the individual item
        itemHash = hashRecordStreamItem(item)

        // Combine with running hash
        runningHash = combineHashes(runningHash, itemHash)
    END FOR

    RETURN runningHash
END PROCEDURE

PROCEDURE hashRecordStreamItem(item):
    buffer = new ByteBuffer()

    // Class ID (little endian for this specific encoding)
    buffer.writeLongLE(RECORD_STREAM_OBJECT_CLASS_ID)

    // Class version (little endian)
    buffer.writeInt32LE(RECORD_STREAM_OBJECT_VERSION)

    // Transaction record length and bytes (big endian lengths)
    recordBytes = protobufEncode(item.record)
    buffer.writeInt32(recordBytes.length)
    buffer.writeBytes(recordBytes)

    // Transaction length and bytes
    txnBytes = protobufEncode(item.transaction)
    buffer.writeInt32(txnBytes.length)
    buffer.writeBytes(txnBytes)

    RETURN SHA384(buffer.toByteArray())
END PROCEDURE

PROCEDURE combineHashes(previousRunningHash, itemHash):
    buffer = new ByteBuffer()

    // First hash object descriptor (little endian)
    buffer.writeLongLE(HASH_CLASS_ID)
    buffer.writeInt32LE(HASH_CLASS_VERSION)
    buffer.writeBytes(previousRunningHash)         // 48 bytes

    // Second hash object descriptor (little endian)
    buffer.writeLongLE(HASH_CLASS_ID)
    buffer.writeInt32LE(HASH_CLASS_VERSION)
    buffer.writeBytes(itemHash)                    // 48 bytes

    RETURN SHA384(buffer.toByteArray())
END PROCEDURE
```

**Byte Order Note**: The running hash computation uses **little endian** for class IDs and
versions but **big endian** for length fields. This matches the original Swirlds serialization.

### Block Stream Block Hash Computation

Wrapped blocks use the same 16-leaf fixed Merkle tree structure as standard Block Stream blocks.
This hash is stored in the `previous_block_root_hash` field of subsequent blocks.

```
CONSTANT LEAF_PREFIX = 0x00
CONSTANT SINGLE_CHILD_PREFIX = 0x01
CONSTANT TWO_CHILDREN_PREFIX = 0x02
CONSTANT EMPTY_TREE_HASH = SHA384(LEAF_PREFIX)    // Hash of empty tree

PROCEDURE computeBlockHash(block, allBlocksMerkleTreeRoot):
    // Extract required components
    header = block.getBlockHeader()
    footer = block.getBlockFooter()
    previousBlockHash = footer.previousBlockRootHash
    stateRootHash = EMPTY_TREE_HASH               // Not available in record files

    // Build item category trees
    consensusHeadersTree = new StreamingMerkleTree()
    inputItemsTree = new StreamingMerkleTree()
    outputItemsTree = new StreamingMerkleTree()
    stateChangesTree = new StreamingMerkleTree()
    traceItemsTree = new StreamingMerkleTree()

    FOR EACH item IN block.items:
        itemBytes = protobufEncode(item)
        SWITCH item.type:
            CASE EVENT_HEADER, ROUND_HEADER:
                consensusHeadersTree.addLeaf(itemBytes)
            CASE SIGNED_TRANSACTION:
                inputItemsTree.addLeaf(itemBytes)
            CASE BLOCK_HEADER, RECORD_FILE, TRANSACTION_RESULT, TRANSACTION_OUTPUT:
                outputItemsTree.addLeaf(itemBytes)
            CASE STATE_CHANGES:
                stateChangesTree.addLeaf(itemBytes)
            CASE TRACE_DATA:
                traceItemsTree.addLeaf(itemBytes)
            CASE BLOCK_FOOTER, BLOCK_PROOF:
                // Not included in hash
    END FOR

    // Compute fixed 16-leaf tree root
    timestampHash = hashLeaf(protobufEncode(header.blockTimestamp))

    leftLeftLeft = hashInternal(previousBlockHash, allBlocksMerkleTreeRoot)
    leftLeftRight = hashInternal(stateRootHash, consensusHeadersTree.root())
    leftLeft = hashInternal(leftLeftLeft, leftLeftRight)

    leftRightLeft = hashInternal(inputItemsTree.root(), outputItemsTree.root())
    leftRightRight = hashInternal(stateChangesTree.root(), traceItemsTree.root())
    leftRight = hashInternal(leftRightLeft, leftRightRight)

    left = hashInternal(leftLeft, leftRight)
    fixedRootTree = hashInternal(left, null)      // Right side reserved

    RETURN hashInternal(timestampHash, fixedRootTree)
END PROCEDURE

PROCEDURE hashLeaf(data):
    RETURN SHA384(LEAF_PREFIX || data)
END PROCEDURE

PROCEDURE hashInternal(leftChild, rightChild):
    IF rightChild IS null:
        RETURN SHA384(SINGLE_CHILD_PREFIX || leftChild)
    ELSE:
        RETURN SHA384(TWO_CHILDREN_PREFIX || leftChild || rightChild)
END PROCEDURE
```

### Appending TSS StateProof Block Proofs

After wrapping all historical record files and producing the first live Block Stream block with
a TSS signature, TSS-based StateProof block proofs SHALL be appended to each wrapped block.

#### Process

1. **Prerequisite**: The first live Block Stream block after the last wrapped block is produced
   and signed with a TSS signature per HIP-1200.

2. **Merkle Tree Construction**: The `root_hash_of_all_block_hashes_tree` in each block's
   `BlockFooter` forms a streaming Merkle tree of all previous block hashes. This tree enables
   efficient proof construction for any historical block.

3. **StateProof Generation**: For each wrapped block N:
   - Construct a Merkle proof from block N's hash to the root of the all-blocks-hashes tree
     in the first TSS-signed live block
   - Create a `StateProof` containing this Merkle path and the TSS signature

4. **Appending**: The `StateProof` block proof is appended as an additional `BlockItem` at the
   end of each wrapped block file, after the existing `SignedRecordFileProof`.

#### Result

After this process, each wrapped block contains:
- BlockHeader
- RecordFileItem (with optional StateChanges for block 0)
- BlockFooter
- BlockProof (SignedRecordFileProof) - original RSA signatures
- BlockProof (StateProof) - TSS-based proof via Merkle tree

#### StateProof Structure

```protobuf
message BlockProof {
    oneof proof {
        TssSignedBlockProof signed_block_proof = 1;
        StateProof block_state_proof = 2;
        SignedRecordFileProof signed_record_file_proof = 3;
    }
}

message StateProof {
    // Merkle path from this block's hash to the all-blocks-hashes tree root
    repeated bytes merkle_path = 1;

    // Block number of the TSS-signed block containing the tree root
    int64 anchor_block_number = 2;

    // TSS signature from the anchor block
    bytes tss_signature = 3;
}
```

### Signature Verification Algorithms

Wrapped blocks support two verification methods. The TSS StateProof method (recommended) is
simpler and requires only the Ledger ID. The SignedRecordFileProof method preserves original
verification capability but requires the historical address book.

#### Method 1: TSS StateProof Verification (Recommended)

This method verifies the block using the appended TSS StateProof and requires only the
network's Ledger ID.

```
PROCEDURE verifyWithTssStateProof(block, ledgerId):
    // Step 1: Find the StateProof in the block
    stateProof = block.getBlockProofByType(STATE_PROOF)
    IF stateProof IS null:
        RETURN VERIFICATION_NOT_AVAILABLE  // Fall back to RSA method
    END IF

    // Step 2: Compute the block hash
    blockHash = computeBlockHash(block, block.footer.rootHashOfAllBlockHashesTree)

    // Step 3: Verify the Merkle path from this block to the anchor block's tree root
    currentHash = blockHash
    FOR EACH siblingHash IN stateProof.merklePath:
        currentHash = hashInternal(currentHash, siblingHash)
    END FOR

    // Step 4: Verify the TSS signature against the Ledger ID
    // The TSS signature signs the block hash of the anchor block
    RETURN verifyTssSignature(ledgerId, currentHash, stateProof.tssSignature)
END PROCEDURE
```

**Advantages**:
- Requires only the Ledger ID (a single well-known value)
- No need to track historical address books
- Unified verification for all blocks (historical and live)
- Verifies amendments along with original data

#### Method 2: SignedRecordFileProof Verification (Original)

This method uses the original RSA signatures from when the record file was created.

```
PROCEDURE verifyWithSignedRecordFileProof(block, addressBook):
    // Step 1: Extract components
    recordFileItem = block.getRecordFileItem()
    signedProof = block.getBlockProofByType(SIGNED_RECORD_FILE_PROOF)
    version = signedProof.version

    // Step 2: Get previous block hash for reconstruction
    previousHash = recordFileItem.recordFileContents.startObjectRunningHash.hash

    // Step 3: Reconstruct record file bytes
    SWITCH version:
        CASE 2:
            recordFileBytes = reconstructV2RecordFile(
                block.header.hapiProtoVersion,
                previousHash,
                recordFileItem.recordFileContents
            )
        CASE 5:
            recordFileBytes = reconstructV5RecordFile(
                block.header.hapiProtoVersion,
                recordFileItem.recordFileContents
            )
        CASE 6:
            recordFileBytes = reconstructV6RecordFile(
                recordFileItem.recordFileContents
            )
    END SWITCH

    // Step 4: Compute version-specific hash
    IF version == 2:
        signedHash = computeV2SignedHash(recordFileBytes)
    ELSE:
        signedHash = computeV5V6SignedHash(recordFileBytes)
    END IF

    // Step 5: Verify signatures
    validSignatures = 0
    totalNodes = addressBook.nodeAddresses.size

    FOR EACH sig IN signedProof.recordFileSignatures:
        nodeAddress = addressBook.getNodeById(sig.nodeId)
        publicKey = nodeAddress.rsaPublicKey

        IF verifyRsaSha384Signature(publicKey, signedHash, sig.signatureBytes):
            validSignatures = validSignatures + 1
        END IF
    END FOR

    // Step 6: Check threshold (Byzantine fault tolerance)
    // Required: more than 1/3 of nodes must have valid signatures
    requiredSignatures = (totalNodes / 3) + 1

    RETURN validSignatures >= requiredSignatures
END PROCEDURE
```

**Note**: This method verifies only the original record file data. Amendments are NOT covered
by the SignedRecordFileProof and require trust in the amendment source or TSS verification.

### Block 0 Amendment: Initial State

Block 0 SHALL include state changes representing the network state at Stream-Start. This data
comes from a saved state snapshot at round 33,485,415 (end of Block 0) with the single Block 0
transaction reversed.

#### Source Data

- **Saved State Round**: 33,485,415
- **Consensus Timestamp**: 2019-09-13T21:53:51.916679Z
- **Method**: Reverse the balance changes from transaction `0.0.11337@1568411616.448357000`

#### Contents Summary

| Property | Value |
|----------|-------|
| Accounts | 15,368 (15,267 normal + 101 smart contracts) |
| Total Balance | 5,000,000,000,000,000,000 tinybars (50 billion HBAR) |
| Files | 130 |
| Contract Bytecodes | 101 |
| Contract Storage Entries | 95 |

The amendment SHALL be encoded as `StateChanges` block items representing:
- Account creations with initial balances
- File creations with metadata
- Smart contract deployments with bytecode
- Contract storage key-value pairs

### Transaction Amendments

113 transactions are missing from the historical record stream due to three separate bugs:

| Category | Period | Count | Description |
|----------|--------|-------|-------------|
| Insufficient Fee Funding | 2019-09-14 to 2019-09-18 | 31 | Payer couldn't afford fees, record omitted |
| FAIL_INVALID NFT (2022) | 2022-07-31 to 2022-08-09 | 70 | NFT ownership list bug |
| FAIL_INVALID NFT (2023) | 2023-02-10 to 2023-02-14 | 12 | NFT wipe/burn bookkeeping bug |
| **Total** | | **113** | |

Each missing transaction SHALL be added to the `amendments` field of the corresponding block's
`RecordFileItem`. The amendment data format:

```protobuf
message RecordStreamItem {
    Transaction transaction = 1;
    TransactionRecord record = 2;
}
```

Amendment source files are stored in the Mirror Node repository at:
`hedera-mirror-importer/src/main/resources/errata/mainnet/missingtransactions/`

Each `.bin` file contains:
```
[4-byte big-endian length][TransactionRecord bytes][4-byte big-endian length][Transaction bytes]
```

#### Failed Transfers Correction

Between 2019-09-14 and 2019-10-03, there were 1,177 transactions that failed due to insufficient
account balance. The attempted transfers were nonetheless listed in the transaction record.
These transactions exist in the record stream but with incorrect transfer lists.

For these transactions, amendments SHALL contain corrected `RecordStreamItem` entries with the
erroneous transfers removed from the `TransactionRecord.transferList`.

**Detection Criteria** (from Mirror Node):
- Transaction type: CRYPTOTRANSFER (14)
- Failed result (not SUCCESS/22)
- Timestamp before 1577836799000000000 (Oct 3, 2019 23:59:59 UTC)
- Contains credit to non-system account (not 0.0.3 through 0.0.27 or 0.0.98)

## Backwards Compatibility

This HIP is backwards compatible:

1. **Lossless Conversion**: The wrapping process preserves all original data. Record files can
   be exactly reconstructed from wrapped blocks.

2. **Original Signatures Valid**: RSA signatures from consensus nodes remain verifiable against
   the reconstructed record file hash.

3. **New Verification Path**: Wrapped blocks can also be verified using Block Stream hash
   computation for the `previous_block_root_hash` chain.

4. **No Consensus Changes**: This conversion is performed offline and does not require changes
   to consensus node software.

## Security Implications

### Trust Model Overview

Wrapped blocks provide two independent trust paths:

1. **TSS Trust (Recommended)**: Ledger ID → TSS Signature → Merkle Path → Block Hash → Block Contents
2. **Original Trust (Legacy)**: Address Book → RSA Signatures → Record File Hash → Record File Contents

The TSS trust path is recommended because it:
- Requires only a single well-known value (the Ledger ID)
- Provides unified verification across all blocks (historical and live)
- Cryptographically covers amendments in addition to original data
- Does not require tracking historical address book changes

### TSS Verification Security

The TSS StateProof verification security derives from:

1. **Ledger ID Trust**: The Ledger ID is the network's master public key for TSS verification.
   As long as the verifier trusts the current network and its Ledger ID, all historical blocks
   can be verified.

2. **Merkle Tree Integrity**: The `root_hash_of_all_block_hashes_tree` in each block's footer
   creates an append-only Merkle tree. The StateProof contains a path from the historical
   block's hash to a TSS-signed root, providing cryptographic proof of inclusion.

3. **Amendment Coverage**: Unlike the SignedRecordFileProof, the TSS StateProof signs the
   complete Block Stream block hash, which includes the `amendments` field. This provides
   cryptographic trust for corrected data.

### SignedRecordFileProof Verification Security

For consumers who need to verify original data independent of current network trust:

1. **Address Book Requirement**: Verification requires the address book (node public keys)
   active when the record file was created. Address books are available from:
   - Block 0: Initial address book from genesis
   - Subsequent blocks: Address book updates in file `0.0.101`

2. **Amendment Limitation**: The SignedRecordFileProof signs only the original record file
   hash. Amendments are NOT covered and must be trusted separately through:
   - Mirror Node errata documentation
   - Saved state snapshots (for Block 0 initial state)
   - Community verification

3. **Use Case**: This verification method is primarily useful for verifying historical trust
   at the point of original creation, or for consumers who specifically distrust the current
   network but trusted it historically (an unlikely scenario).

### Choosing a Verification Method

| Scenario | Recommended Method |
|----------|-------------------|
| Standard block verification | TSS StateProof |
| Verifying amendments are correct | TSS StateProof |
| Auditing with minimal trust assumptions | TSS StateProof (trust current Ledger ID) |
| Verifying original data at creation time | SignedRecordFileProof |
| No trust in current network | SignedRecordFileProof (requires historical address book) |

### Amendment Trust Details

For SignedRecordFileProof verification, amendments require separate trust:
- **Block 0 Initial State**: Derived from saved state snapshot at round 33,485,415
- **Missing Transactions**: Sourced from Mirror Node errata repository
- **Failed Transfers Corrections**: Sourced from Mirror Node errata repository

Consumers who require only originally-signed data MAY ignore the `amendments` field, though
this will result in incomplete balance calculations and missing transaction history.

## Reference Implementation

Reference implementation is available in the Hiero Block Node repository:

- `RecordBlockConverter.java` - Core conversion logic between formats
- `ParsedRecordFile.java` - Record file parsing and reconstruction
- `BlockStreamBlockHasher.java` - Block Stream hash computation
- `SerializationV5Utils.java` - V5 HashObject serialization
- `MainnetBlockZeroState.java` - Block 0 initial state construction
- `ToWrappedBlocksCommand.java` - CLI conversion tool

## Rejected Ideas

### Option 1: Don't Fix Anything

Leave record stream as-is, requiring consumers to parse multiple formats and maintain their
own amendment databases.

**Rejected because**: Perpetuates complexity and incomplete data access.

### Option 3a: TSS Amendments with State Proofs

Sign amendments using TSS with state proofs from historical saved states.

**Rejected because**: Constructing state proofs from old saved states is complex, and many
historical states are not available in formats that support proof generation.

### Option 3b: TSS Pre-Signed Every Block

Generate individual TSS signatures for each of the 81+ million historical blocks before any
live Block Stream blocks exist.

**Rejected because**: Computationally prohibitive and would require significant consensus node
resources to sign each block individually.

**Note**: The adopted approach (appending TSS StateProofs via Merkle tree) differs from this
rejected option. Instead of signing each block individually, a single TSS signature on the
first live block, combined with Merkle proofs from the `root_hash_of_all_block_hashes_tree`,
provides verification for all historical blocks efficiently.

### Option 4: Fork History and Re-sign

Create a reformatted blockchain with all corrections, signed with TSS.

**Rejected because**: Creates a forked history requiring dual trust paths and ongoing
operational burden to maintain both versions.

## References

- [HIP-1081: Block Stream](https://hips.hedera.com/hip/hip-1081)
- [HIP-1056: Block Node](https://hips.hedera.com/hip/hip-1056)
- [HIP-1200: Threshold Signature Scheme (TSS)](https://hips.hedera.com/hip/hip-1200)
- [HIP-435: Record File V6 Format](https://hips.hedera.com/hip/hip-435)
- [Mirror Node Errata Documentation](https://github.com/hiero-ledger/hiero-mirror-node/blob/main/docs/database/README.md#errata)
- [RFC 2119: Key words for use in RFCs](https://www.ietf.org/rfc/rfc2119)

## Copyright

This document is licensed under the Apache License, Version 2.0 -- see [LICENSE](../LICENSE)
or (https://www.apache.org/licenses/LICENSE-2.0)
