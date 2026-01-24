// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

import static org.hiero.block.tools.utils.TestBlocks.V2_TEST_BLOCK_ADDRESS_BOOK;
import static org.hiero.block.tools.utils.TestBlocks.V2_TEST_BLOCK_BYTES;
import static org.hiero.block.tools.utils.TestBlocks.V2_TEST_BLOCK_HASH;
import static org.hiero.block.tools.utils.TestBlocks.V2_TEST_BLOCK_NUMBER;
import static org.hiero.block.tools.utils.TestBlocks.V2_TEST_BLOCK_RECORD_FILE_NAME;
import static org.hiero.block.tools.utils.TestBlocks.V5_TEST_BLOCK_ADDRESS_BOOK;
import static org.hiero.block.tools.utils.TestBlocks.V5_TEST_BLOCK_BYTES;
import static org.hiero.block.tools.utils.TestBlocks.V5_TEST_BLOCK_HASH;
import static org.hiero.block.tools.utils.TestBlocks.V5_TEST_BLOCK_NUMBER;
import static org.hiero.block.tools.utils.TestBlocks.V5_TEST_BLOCK_RECORD_FILE_NAME;
import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_ADDRESS_BOOK;
import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_BYTES;
import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_HASH;
import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_NUMBER;
import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_RECORD_FILE_NAME;
import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_SIDECAR_BYTES;
import static org.hiero.block.tools.utils.TestBlocks.loadV2SignatureFiles;
import static org.hiero.block.tools.utils.TestBlocks.loadV5SignatureFiles;
import static org.hiero.block.tools.utils.TestBlocks.loadV6SignatureFiles;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.streams.SidecarFile;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import org.hiero.block.tools.blocks.WrappedBlockIndex;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Tests for RecordBlockConverter to verify lossless round-trip conversion between
 * ParsedRecordBlock and Block formats for V2, V5, and V6 record files.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RecordBlockConverterTest {
    /** Dummy root hash for a block hashes merkle tree (48 bytes for SHA-384) */
    private static final byte[] DUMMY_ROOT_HASH = new byte[48];
    /** Dummy previous block hash (48 bytes for SHA-384) */
    private static final byte[] DUMMY_PREVIOUS_BLOCK_HASH = HexFormat.of()
            .parseHex(
                    "52939c10ed7d382366e636c4c071287ed0a4ebcab144a750b6293a4dee11f5a021e81f3e093a29b5b53bf3d1fd870080");

    /** V2 parsed block for testing */
    private ParsedRecordBlock v2ParsedBlock;

    /** V5 parsed block for testing */
    private ParsedRecordBlock v5ParsedBlock;

    /** V6 parsed block for testing */
    private ParsedRecordBlock v6ParsedBlock;

    @Test
    @Order(1)
    @DisplayName("Setup V2 ParsedRecordBlock")
    void setupV2Block() {
        final InMemoryFile recordFile = new InMemoryFile(Path.of(V2_TEST_BLOCK_RECORD_FILE_NAME), V2_TEST_BLOCK_BYTES);
        final ParsedRecordFile parsedRecordFile = ParsedRecordFile.parse(recordFile);
        final List<ParsedSignatureFile> signatureFiles = loadV2SignatureFiles();

        v2ParsedBlock = new ParsedRecordBlock(parsedRecordFile, signatureFiles, Collections.emptyList());

        assertNotNull(v2ParsedBlock);
        assertEquals(7, signatureFiles.size(), "V2 block should have 7 signature files");
    }

    @Test
    @Order(2)
    @DisplayName("Setup V5 ParsedRecordBlock")
    void setupV5Block() {
        final InMemoryFile recordFile = new InMemoryFile(Path.of(V5_TEST_BLOCK_RECORD_FILE_NAME), V5_TEST_BLOCK_BYTES);
        final ParsedRecordFile parsedRecordFile = ParsedRecordFile.parse(recordFile);
        final List<ParsedSignatureFile> signatureFiles = loadV5SignatureFiles();

        v5ParsedBlock = new ParsedRecordBlock(parsedRecordFile, signatureFiles, Collections.emptyList());

        assertNotNull(v5ParsedBlock);
        assertEquals(12, signatureFiles.size(), "V5 block should have 12 signature files");
    }

    @Test
    @Order(3)
    @DisplayName("Setup V6 ParsedRecordBlock")
    void setupV6Block() {
        assertDoesNotThrow(() -> {
            final InMemoryFile recordFile =
                    new InMemoryFile(Path.of(V6_TEST_BLOCK_RECORD_FILE_NAME), V6_TEST_BLOCK_BYTES);
            final ParsedRecordFile parsedRecordFile = ParsedRecordFile.parse(recordFile);
            final List<ParsedSignatureFile> signatureFiles = loadV6SignatureFiles();
            final SidecarFile sidecarFile = SidecarFile.PROTOBUF.parse(Bytes.wrap(V6_TEST_BLOCK_SIDECAR_BYTES));

            v6ParsedBlock = new ParsedRecordBlock(parsedRecordFile, signatureFiles, List.of(sidecarFile));

            assertNotNull(v6ParsedBlock);
            assertEquals(12, signatureFiles.size(), "V6 block should have 12 signature files");
            assertEquals(1, v6ParsedBlock.sidecarFiles().size(), "V6 block should have 1 sidecar file");
        });
    }

    @Test
    @Order(4)
    @DisplayName("Test V2 round trip: ParsedRecordBlock -> Block -> ParsedRecordBlock")
    void testV2RoundTrip() {
        assertNotNull(v2ParsedBlock);

        // Convert to Block
        Block block = RecordBlockConverter.toBlock(
                v2ParsedBlock,
                V2_TEST_BLOCK_NUMBER,
                DUMMY_PREVIOUS_BLOCK_HASH,
                DUMMY_ROOT_HASH,
                V2_TEST_BLOCK_ADDRESS_BOOK);
        assertNotNull(block);
        assertEquals(4, block.items().size(), "Block should have 4 items (header, record file, footer, proof)");

        // Convert back to ParsedRecordBlock
        ParsedRecordBlock roundTripBlock = RecordBlockConverter.toRecordFile(block, V2_TEST_BLOCK_ADDRESS_BOOK);
        assertNotNull(roundTripBlock);

        // Verify record file is lossless
        assertArrayEquals(
                v2ParsedBlock.recordFile().recordFileContents(),
                roundTripBlock.recordFile().recordFileContents(),
                "V2 record file bytes should be lossless through round trip");

        // Verify block hash is preserved
        assertArrayEquals(
                v2ParsedBlock.recordFile().blockHash(),
                roundTripBlock.recordFile().blockHash(),
                "V2 block hash should be preserved");

        // Verify signature count matches
        assertEquals(
                v2ParsedBlock.signatureFiles().size(),
                roundTripBlock.signatureFiles().size(),
                "V2 signature count should match");

        // Verify signature bytes are preserved (metadata may be lost, which is by design)
        for (int i = 0; i < v2ParsedBlock.signatureFiles().size(); i++) {
            assertArrayEquals(
                    v2ParsedBlock.signatureFiles().get(i).signatureBytes(),
                    roundTripBlock.signatureFiles().get(i).signatureBytes(),
                    "V2 signature bytes should be preserved for signature " + i);
        }

        // Verify no sidecar files (V2 doesn't have them)
        assertEquals(0, roundTripBlock.sidecarFiles().size(), "V2 should have no sidecar files");
    }

    @Test
    @Order(5)
    @DisplayName("Test V5 round trip: ParsedRecordBlock -> Block -> ParsedRecordBlock")
    void testV5RoundTrip() {
        assertNotNull(v5ParsedBlock);

        // Convert to Block
        Block block = RecordBlockConverter.toBlock(
                v5ParsedBlock,
                V5_TEST_BLOCK_NUMBER,
                DUMMY_PREVIOUS_BLOCK_HASH,
                DUMMY_ROOT_HASH,
                V5_TEST_BLOCK_ADDRESS_BOOK);
        assertNotNull(block);
        assertEquals(4, block.items().size(), "Block should have 4 items (header, record file, footer, proof)");

        // Convert back to ParsedRecordBlock
        ParsedRecordBlock roundTripBlock = RecordBlockConverter.toRecordFile(block, V5_TEST_BLOCK_ADDRESS_BOOK);
        assertNotNull(roundTripBlock);

        // Verify record file is lossless
        assertArrayEquals(
                v5ParsedBlock.recordFile().recordFileContents(),
                roundTripBlock.recordFile().recordFileContents(),
                "V5 record file bytes should be lossless through round trip");

        // Verify block hash is preserved
        assertArrayEquals(
                v5ParsedBlock.recordFile().blockHash(),
                roundTripBlock.recordFile().blockHash(),
                "V5 block hash should be preserved");

        // Verify signature count matches
        assertEquals(
                v5ParsedBlock.signatureFiles().size(),
                roundTripBlock.signatureFiles().size(),
                "V5 signature count should match");

        // Verify signature bytes are preserved (metadata may be lost, which is by design)
        for (int i = 0; i < v5ParsedBlock.signatureFiles().size(); i++) {
            assertArrayEquals(
                    v5ParsedBlock.signatureFiles().get(i).signatureBytes(),
                    roundTripBlock.signatureFiles().get(i).signatureBytes(),
                    "V5 signature bytes should be preserved for signature " + i);
        }

        // Verify no sidecar files (V5 doesn't have them)
        assertEquals(0, roundTripBlock.sidecarFiles().size(), "V5 should have no sidecar files");
    }

    @Test
    @Order(6)
    @DisplayName("Test V6 round trip: ParsedRecordBlock -> Block -> ParsedRecordBlock")
    void testV6RoundTrip() {
        assertNotNull(v6ParsedBlock);

        // Convert to Block
        Block block = RecordBlockConverter.toBlock(
                v6ParsedBlock,
                V6_TEST_BLOCK_NUMBER,
                DUMMY_PREVIOUS_BLOCK_HASH,
                DUMMY_ROOT_HASH,
                V6_TEST_BLOCK_ADDRESS_BOOK);
        assertNotNull(block);
        assertEquals(4, block.items().size(), "Block should have 4 items (header, record file, footer, proof)");

        // Convert back to ParsedRecordBlock
        ParsedRecordBlock roundTripBlock = RecordBlockConverter.toRecordFile(block, V6_TEST_BLOCK_ADDRESS_BOOK);
        assertNotNull(roundTripBlock);

        // Verify record file is lossless
        assertArrayEquals(
                v6ParsedBlock.recordFile().recordFileContents(),
                roundTripBlock.recordFile().recordFileContents(),
                "V6 record file bytes should be lossless through round trip");

        // Verify block hash is preserved
        assertArrayEquals(
                v6ParsedBlock.recordFile().blockHash(),
                roundTripBlock.recordFile().blockHash(),
                "V6 block hash should be preserved");

        // Verify signature count matches
        assertEquals(
                v6ParsedBlock.signatureFiles().size(),
                roundTripBlock.signatureFiles().size(),
                "V6 signature count should match");

        // Verify signature bytes are preserved (metadata may be lost, which is by design)
        for (int i = 0; i < v6ParsedBlock.signatureFiles().size(); i++) {
            assertArrayEquals(
                    v6ParsedBlock.signatureFiles().get(i).signatureBytes(),
                    roundTripBlock.signatureFiles().get(i).signatureBytes(),
                    "V6 signature bytes should be preserved for signature " + i);
        }

        // Verify sidecar files are lossless
        assertEquals(
                v6ParsedBlock.sidecarFiles().size(),
                roundTripBlock.sidecarFiles().size(),
                "V6 sidecar file count should match");

        // Verify sidecar file contents are preserved
        for (int i = 0; i < v6ParsedBlock.sidecarFiles().size(); i++) {
            byte[] originalSidecarBytes = SidecarFile.PROTOBUF
                    .toBytes(v6ParsedBlock.sidecarFiles().get(i))
                    .toByteArray();
            byte[] roundTripSidecarBytes = SidecarFile.PROTOBUF
                    .toBytes(roundTripBlock.sidecarFiles().get(i))
                    .toByteArray();
            assertArrayEquals(
                    originalSidecarBytes,
                    roundTripSidecarBytes,
                    "V6 sidecar file " + i + " should be lossless through round trip");
        }
    }

    @Test
    @Order(7)
    @DisplayName("Test V2 block validation after round trip")
    void testV2BlockValidationAfterRoundTrip() {
        assertNotNull(v2ParsedBlock);

        // Convert to Block and back
        Block block = RecordBlockConverter.toBlock(
                v2ParsedBlock,
                V2_TEST_BLOCK_NUMBER,
                DUMMY_PREVIOUS_BLOCK_HASH,
                DUMMY_ROOT_HASH,
                V2_TEST_BLOCK_ADDRESS_BOOK);
        ParsedRecordBlock roundTripBlock = RecordBlockConverter.toRecordFile(block, V2_TEST_BLOCK_ADDRESS_BOOK);

        // Validate the round-trip block with correct previous hash
        byte[] previousBlockHash = new byte[48]; // V2 block 0 has all zeros for previous hash
        assertDoesNotThrow(() -> {
            byte[] validatedBlockHash = roundTripBlock.validate(previousBlockHash, V2_TEST_BLOCK_ADDRESS_BOOK);
            assertNotNull(validatedBlockHash);
            assertArrayEquals(
                    V2_TEST_BLOCK_HASH,
                    validatedBlockHash,
                    "V2 block hash after round trip should match expected value");
        });
    }

    @Test
    @Order(8)
    @DisplayName("Test V5 block validation after round trip")
    void testV5BlockValidationAfterRoundTrip() {
        assertNotNull(v5ParsedBlock);

        // Convert to Block and back
        Block block = RecordBlockConverter.toBlock(
                v5ParsedBlock,
                V5_TEST_BLOCK_NUMBER,
                DUMMY_PREVIOUS_BLOCK_HASH,
                DUMMY_ROOT_HASH,
                V5_TEST_BLOCK_ADDRESS_BOOK);
        ParsedRecordBlock roundTripBlock = RecordBlockConverter.toRecordFile(block, V5_TEST_BLOCK_ADDRESS_BOOK);

        // Validate the round-trip block with the correct previous hash
        byte[] previousBlockHash = v5ParsedBlock.recordFile().previousBlockHash();
        assertDoesNotThrow(() -> {
            byte[] validatedBlockHash = roundTripBlock.validate(previousBlockHash, V5_TEST_BLOCK_ADDRESS_BOOK);
            assertNotNull(validatedBlockHash);
            assertArrayEquals(
                    V5_TEST_BLOCK_HASH,
                    validatedBlockHash,
                    "V5 block hash after round trip should match expected value");
        });
    }

    @Test
    @Order(9)
    @DisplayName("Test V6 block validation after round trip")
    void testV6BlockValidationAfterRoundTrip() {
        assertNotNull(v6ParsedBlock);

        // Convert to Block and back
        Block block = RecordBlockConverter.toBlock(
                v6ParsedBlock,
                V6_TEST_BLOCK_NUMBER,
                DUMMY_PREVIOUS_BLOCK_HASH,
                DUMMY_ROOT_HASH,
                V6_TEST_BLOCK_ADDRESS_BOOK);
        ParsedRecordBlock roundTripBlock = RecordBlockConverter.toRecordFile(block, V6_TEST_BLOCK_ADDRESS_BOOK);

        // Validate the round-trip block with the correct previous hash
        byte[] previousBlockHash = v6ParsedBlock.recordFile().previousBlockHash();
        assertDoesNotThrow(() -> {
            byte[] validatedBlockHash = roundTripBlock.validate(previousBlockHash, V6_TEST_BLOCK_ADDRESS_BOOK);
            assertNotNull(validatedBlockHash);
            assertArrayEquals(
                    V6_TEST_BLOCK_HASH,
                    validatedBlockHash,
                    "V6 block hash after round trip should match expected value");
        });
    }

    @Test
    @Order(10)
    @DisplayName("Test Block structure for V2")
    void testV2BlockStructure() {
        assertNotNull(v2ParsedBlock);

        Block block = RecordBlockConverter.toBlock(
                v2ParsedBlock,
                V2_TEST_BLOCK_NUMBER,
                DUMMY_PREVIOUS_BLOCK_HASH,
                DUMMY_ROOT_HASH,
                V2_TEST_BLOCK_ADDRESS_BOOK);

        // Verify block items
        assertTrue(block.items().get(0).hasBlockHeader(), "First item should be BlockHeader");
        assertTrue(block.items().get(1).hasRecordFile(), "Second item should be RecordFile");
        assertTrue(block.items().get(2).hasBlockFooter(), "Third item should be BlockFooter");
        assertTrue(block.items().get(3).hasBlockProof(), "Fourth item should be BlockProof");

        // Verify BlockHeader
        var blockHeaderItem = block.items().get(0);
        assertNotNull(blockHeaderItem);
        assertEquals(V2_TEST_BLOCK_NUMBER, blockHeaderItem.blockHeaderOrThrow().number(), "Block number should match");

        // Verify RecordFile contains sidecar files (should be empty for V2)
        var recordFile = block.items().get(1).recordFile();
        assertNotNull(recordFile);
        assertEquals(0, recordFile.sidecarFileContents().size(), "V2 should have no sidecar files");

        // Verify BlockProof has signatures
        assertTrue(
                block.items().get(3).blockProofOrThrow().hasSignedRecordFileProof(),
                "BlockProof should have SignedRecordFileProof");
        assertEquals(
                2,
                block.items()
                        .get(3)
                        .blockProofOrThrow()
                        .signedRecordFileProofOrThrow()
                        .version(),
                "V2 should have record format version 2");
    }

    @Test
    @Order(11)
    @DisplayName("Test Block structure for V6 with sidecar")
    void testV6BlockStructureWithSidecar() {
        assertNotNull(v6ParsedBlock);

        Block block = RecordBlockConverter.toBlock(
                v6ParsedBlock,
                V6_TEST_BLOCK_NUMBER,
                DUMMY_PREVIOUS_BLOCK_HASH,
                DUMMY_ROOT_HASH,
                V6_TEST_BLOCK_ADDRESS_BOOK);

        // Verify RecordFile contains sidecar files
        var recordFileItem = block.items().get(1);
        assertNotNull(recordFileItem);
        assertEquals(
                1, recordFileItem.recordFileOrThrow().sidecarFileContents().size(), "V6 should have 1 sidecar file");

        // Verify BlockProof version
        var blockProof = block.items().get(3);
        assertNotNull(blockProof);
        assertEquals(
                6,
                blockProof.blockProofOrThrow().signedRecordFileProofOrThrow().version(),
                "V6 should have record format version 6");
    }

    @Test
    @Order(12)
    @DisplayName("Test block structure with genesis STATE_CHANGES inserted at correct position")
    void testBlockStructureWithGenesisStateChangesInserted() {
        assertNotNull(v2ParsedBlock);

        // Convert to Block (without amendments)
        Block block = RecordBlockConverter.toBlock(
                v2ParsedBlock,
                V2_TEST_BLOCK_NUMBER,
                DUMMY_PREVIOUS_BLOCK_HASH,
                DUMMY_ROOT_HASH,
                V2_TEST_BLOCK_ADDRESS_BOOK);

        // Verify initial structure: [HEADER, RECORD_FILE, FOOTER, PROOF]
        assertEquals(4, block.items().size(), "Block should have 4 items before amendment");
        assertTrue(block.items().get(0).hasBlockHeader(), "Index 0 should be BLOCK_HEADER");
        assertTrue(block.items().get(1).hasRecordFile(), "Index 1 should be RECORD_FILE");
        assertTrue(block.items().get(2).hasBlockFooter(), "Index 2 should be BLOCK_FOOTER");
        assertTrue(block.items().get(3).hasBlockProof(), "Index 3 should be BLOCK_PROOF");

        // Simulate inserting genesis STATE_CHANGES at WrappedBlockIndex.STATE_CHANGES (index 2)
        // Create mock STATE_CHANGES items
        var stateChanges = com.hedera.hapi.block.stream.output.StateChanges.newBuilder()
                .consensusTimestamp(com.hedera.hapi.node.base.Timestamp.newBuilder()
                        .seconds(1568411631L)
                        .nanos(396440000)
                        .build())
                .build();
        var stateChangesItem = BlockItem.newBuilder().stateChanges(stateChanges).build();

        // Insert at index 2 (WrappedBlockIndex.STATE_CHANGES)
        List<BlockItem> items = new ArrayList<>(block.items());
        items.add(WrappedBlockIndex.STATE_CHANGES.index(), stateChangesItem);
        Block amendedBlock = new Block(items);

        // Verify amended structure: [HEADER, RECORD_FILE, STATE_CHANGES, FOOTER, PROOF]
        assertEquals(5, amendedBlock.items().size(), "Block should have 5 items after amendment");
        assertTrue(amendedBlock.items().get(0).hasBlockHeader(), "Index 0 should be BLOCK_HEADER");
        assertTrue(amendedBlock.items().get(1).hasRecordFile(), "Index 1 should be RECORD_FILE");
        assertTrue(amendedBlock.items().get(2).hasStateChanges(), "Index 2 should be STATE_CHANGES");
        assertTrue(amendedBlock.items().get(3).hasBlockFooter(), "Index 3 should be BLOCK_FOOTER");
        assertTrue(amendedBlock.items().get(4).hasBlockProof(), "Index 4 should be BLOCK_PROOF");
    }
}
