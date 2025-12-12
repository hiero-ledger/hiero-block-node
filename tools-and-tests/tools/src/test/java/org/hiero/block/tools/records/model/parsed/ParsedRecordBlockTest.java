// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

import static org.hiero.block.tools.utils.TestBlocks.*;
import static org.junit.jupiter.api.Assertions.*;

import com.google.common.jimfs.Jimfs;
import com.hedera.hapi.streams.SidecarFile;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Tests for parsing and validating ParsedRecordBlock with V2, V5, and V6 Hedera record blocks.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS) // Single instance, shared state
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParsedRecordBlockTest {

    /** The parsed V2 block, created in test (1) */
    private ParsedRecordBlock v2ParsedBlock;

    /** The parsed V5 block, created in test (2) */
    private ParsedRecordBlock v5ParsedBlock;

    /** The parsed V6 block, created in test (3) */
    private ParsedRecordBlock v6ParsedBlock;

    @Test
    @Order(1)
    @DisplayName("Test creating V2 ParsedRecordBlock")
    void testV2ParsedRecordBlockCreation() {
        assertDoesNotThrow(() -> {
            final InMemoryFile recordFile =
                    new InMemoryFile(Path.of(V2_TEST_BLOCK_RECORD_FILE_NAME), V2_TEST_BLOCK_BYTES);
            final ParsedRecordFile parsedRecordFile = ParsedRecordFile.parse(recordFile);
            final List<ParsedSignatureFile> signatureFiles = loadV2SignatureFiles();

            v2ParsedBlock = new ParsedRecordBlock(parsedRecordFile, signatureFiles, Collections.emptyList());

            assertNotNull(v2ParsedBlock);
            assertEquals(7, signatureFiles.size(), "V2 block should have 7 signature files (nodes 3-9)");
            assertEquals(0, v2ParsedBlock.sidecarFiles().size(), "V2 block should have no sidecar files");
        });
    }

    @Test
    @Order(2)
    @DisplayName("Test creating V5 ParsedRecordBlock")
    void testV5ParsedRecordBlockCreation() {
        assertDoesNotThrow(() -> {
            final InMemoryFile recordFile =
                    new InMemoryFile(Path.of(V5_TEST_BLOCK_RECORD_FILE_NAME), V5_TEST_BLOCK_BYTES);
            final ParsedRecordFile parsedRecordFile = ParsedRecordFile.parse(recordFile);
            final List<ParsedSignatureFile> signatureFiles = loadV5SignatureFiles();

            v5ParsedBlock = new ParsedRecordBlock(parsedRecordFile, signatureFiles, Collections.emptyList());

            assertNotNull(v5ParsedBlock);
            assertEquals(12, signatureFiles.size(), "V5 block should have 12 signature files (nodes 10-18, 20-22)");
            assertEquals(0, v5ParsedBlock.sidecarFiles().size(), "V5 block should have no sidecar files");
        });
    }

    @Test
    @Order(3)
    @DisplayName("Test creating V6 ParsedRecordBlock with sidecar")
    void testV6ParsedRecordBlockCreation() {
        assertDoesNotThrow(() -> {
            final InMemoryFile recordFile =
                    new InMemoryFile(Path.of(V6_TEST_BLOCK_RECORD_FILE_NAME), V6_TEST_BLOCK_BYTES);
            final ParsedRecordFile parsedRecordFile = ParsedRecordFile.parse(recordFile);
            final List<ParsedSignatureFile> signatureFiles = loadV6SignatureFiles();

            // Parse sidecar file
            final SidecarFile sidecarFile = SidecarFile.PROTOBUF.parse(Bytes.wrap(V6_TEST_BLOCK_SIDECAR_BYTES));

            v6ParsedBlock = new ParsedRecordBlock(parsedRecordFile, signatureFiles, List.of(sidecarFile));

            assertNotNull(v6ParsedBlock);
            assertEquals(
                    12, signatureFiles.size(), "V6 block should have 12 signature files (nodes 3, 21, 25, 28-35, 37)");
            assertEquals(1, v6ParsedBlock.sidecarFiles().size(), "V6 block should have 1 sidecar file");
        });
    }

    @Test
    @Order(4)
    @DisplayName("Test V2 block time")
    void testV2BlockTime() {
        assertNotNull(v2ParsedBlock);
        assertNotNull(v2ParsedBlock.blockTime());
        assertEquals(
                V2_TEST_BLOCK_DATE_STR.replace('_', ':'),
                v2ParsedBlock.blockTime().toString(),
                "V2 block time should match expected value");
    }

    @Test
    @Order(5)
    @DisplayName("Test V5 block time")
    void testV5BlockTime() {
        assertNotNull(v5ParsedBlock);
        assertNotNull(v5ParsedBlock.blockTime());
        assertEquals(
                V5_TEST_BLOCK_DATE_STR.replace('_', ':'),
                v5ParsedBlock.blockTime().toString(),
                "V5 block time should match expected value");
    }

    @Test
    @Order(6)
    @DisplayName("Test V6 block time")
    void testV6BlockTime() {
        assertNotNull(v6ParsedBlock);
        assertNotNull(v6ParsedBlock.blockTime());
        assertEquals(
                V6_TEST_BLOCK_DATE_STR.replace('_', ':'),
                v6ParsedBlock.blockTime().toString(),
                "V6 block time should match expected value");
    }

    @Test
    @Order(7)
    @DisplayName("Test V2 block validation")
    void testV2BlockValidation() {
        assertNotNull(v2ParsedBlock);

        // For block 0 (V2), the previous block hash is all zeros
        byte[] previousBlockHash = new byte[48];

        assertDoesNotThrow(() -> {
            byte[] validatedBlockHash = v2ParsedBlock.validate(previousBlockHash, V2_TEST_BLOCK_ADDRESS_BOOK);
            assertNotNull(validatedBlockHash);
            assertArrayEquals(
                    V2_TEST_BLOCK_HASH, validatedBlockHash, "Validated V2 block hash should match expected value");
        });
    }

    @Test
    @Order(8)
    @DisplayName("Test V5 block validation")
    void testV5BlockValidation() {
        assertNotNull(v5ParsedBlock);

        // Use the start running hash as the previous block hash for V5
        byte[] previousBlockHash = v5ParsedBlock.recordFile().previousBlockHash();

        assertDoesNotThrow(() -> {
            byte[] validatedBlockHash = v5ParsedBlock.validate(previousBlockHash, V5_TEST_BLOCK_ADDRESS_BOOK);
            assertNotNull(validatedBlockHash);
            assertArrayEquals(
                    V5_TEST_BLOCK_HASH, validatedBlockHash, "Validated V5 block hash should match expected value");
        });
    }

    @Test
    @Order(9)
    @DisplayName("Test V6 block validation with sidecar")
    void testV6BlockValidation() {
        assertNotNull(v6ParsedBlock);

        // Use the start running hash as the previous block hash for V6
        byte[] previousBlockHash = v6ParsedBlock.recordFile().previousBlockHash();

        assertDoesNotThrow(() -> {
            byte[] validatedBlockHash = v6ParsedBlock.validate(previousBlockHash, V6_TEST_BLOCK_ADDRESS_BOOK);
            assertNotNull(validatedBlockHash);
            assertArrayEquals(
                    V6_TEST_BLOCK_HASH, validatedBlockHash, "Validated V6 block hash should match expected value");
        });
    }

    @Test
    @Order(10)
    @DisplayName("Test V2 block validation fails with wrong previous hash")
    void testV2BlockValidationFailsWithWrongPreviousHash() {
        assertNotNull(v2ParsedBlock);

        byte[] wrongPreviousHash = new byte[48];
        wrongPreviousHash[0] = 1; // Make it different from all zeros

        assertThrows(
                ValidationException.class,
                () -> v2ParsedBlock.validate(wrongPreviousHash, V2_TEST_BLOCK_ADDRESS_BOOK),
                "V2 block validation should fail with wrong previous hash");
    }

    @Test
    @Order(11)
    @DisplayName("Test V2 block write and read back")
    void testV2BlockWriteAndReadBack() throws Exception {
        assertNotNull(v2ParsedBlock);

        try (var fs = Jimfs.newFileSystem()) {
            var testDir = fs.getPath("test");
            Files.createDirectories(testDir);

            // Write the block (not gzipped)
            v2ParsedBlock.write(testDir, false);

            // Verify record file was written
            var recordFilePath = testDir.resolve(V2_TEST_BLOCK_RECORD_FILE_NAME);
            assertTrue(Files.exists(recordFilePath), "Record file should exist");

            // Verify signature files were written
            for (int nodeId = 3; nodeId < 10; nodeId++) {
                var sigFilePath = testDir.resolve("node_0.0." + nodeId + ".rcd_sig");
                assertTrue(Files.exists(sigFilePath), "Signature file for node " + nodeId + " should exist");
            }

            // Read back and verify record file content
            byte[] writtenRecordBytes = Files.readAllBytes(recordFilePath);
            assertArrayEquals(V2_TEST_BLOCK_BYTES, writtenRecordBytes, "Written V2 record file should match original");
        }
    }

    @Test
    @Order(12)
    @DisplayName("Test V6 block write with sidecar and verify bytes")
    void testV6BlockWriteWithSidecar() throws Exception {
        assertNotNull(v6ParsedBlock);

        try (var fs = Jimfs.newFileSystem()) {
            var testDir = fs.getPath("test");
            Files.createDirectories(testDir);

            // Write the block (not gzipped)
            v6ParsedBlock.write(testDir, false);

            // Read back and verify record file bytes
            var recordFilePath = testDir.resolve(V6_TEST_BLOCK_RECORD_FILE_NAME);
            assertTrue(Files.exists(recordFilePath), "V6 record file should exist");
            byte[] writtenRecordBytes = Files.readAllBytes(recordFilePath);
            assertArrayEquals(V6_TEST_BLOCK_BYTES, writtenRecordBytes, "Written V6 record file should match original");

            // Read back and verify sidecar file bytes
            var sidecarFilePath = testDir.resolve(V6_TEST_BLOCK_DATE_STR + "_1.rcd");
            assertTrue(Files.exists(sidecarFilePath), "V6 sidecar file should exist");
            byte[] writtenSidecarBytes = Files.readAllBytes(sidecarFilePath);
            assertArrayEquals(
                    V6_TEST_BLOCK_SIDECAR_BYTES, writtenSidecarBytes, "Written V6 sidecar file should match original");

            // Read back and verify signature files (12 files for V6)
            List<ParsedSignatureFile> v6SignatureFiles = loadV6SignatureFiles();
            for (ParsedSignatureFile signatureFile : v6SignatureFiles) {
                int nodeId = signatureFile.accountNum();
                var sigFilePath = testDir.resolve("node_0.0." + nodeId + ".rcd_sig");
                assertTrue(Files.exists(sigFilePath), "Signature file for node " + nodeId + " should exist");

                // Get original signature file bytes from parsed signature file
                byte[] originalSigBytes = signatureFile.signatureFileBytes();

                // Verify signature file bytes match original
                byte[] writtenSigBytes = Files.readAllBytes(sigFilePath);
                assertArrayEquals(
                        originalSigBytes,
                        writtenSigBytes,
                        "Written V6 signature file for node " + nodeId + " should match original");
            }
        }
    }

    @Test
    @Order(13)
    @DisplayName("Test V5 block write gzipped and verify bytes")
    void testV5BlockWriteGzipped() throws Exception {
        assertNotNull(v5ParsedBlock);

        try (var fs = Jimfs.newFileSystem()) {
            var testDir = fs.getPath("test");
            Files.createDirectories(testDir);

            // Write the block (gzipped)
            v5ParsedBlock.write(testDir, true);

            // Read back and verify gzipped record file
            var recordFilePath = testDir.resolve(V5_TEST_BLOCK_RECORD_FILE_NAME + ".gz");
            assertTrue(Files.exists(recordFilePath), "Gzipped V5 record file should exist");
            assertTrue(Files.size(recordFilePath) > 0, "Gzipped V5 record file should not be empty");

            // Verify it's smaller than original (gzip compression)
            assertTrue(
                    Files.size(recordFilePath) < V5_TEST_BLOCK_BYTES.length,
                    "Gzipped file should be smaller than original");

            // Decompress and verify bytes match original
            byte[] decompressedBytes;
            try (var gzipStream = new java.util.zip.GZIPInputStream(Files.newInputStream(recordFilePath));
                    var baos = new java.io.ByteArrayOutputStream()) {
                gzipStream.transferTo(baos);
                decompressedBytes = baos.toByteArray();
            }
            assertArrayEquals(
                    V5_TEST_BLOCK_BYTES, decompressedBytes, "Decompressed V5 record file should match original");

            // Read back and verify signature files (12 files for V5, not gzipped)
            List<ParsedSignatureFile> v5SignatureFiles = loadV5SignatureFiles();
            for (ParsedSignatureFile signatureFile : v5SignatureFiles) {
                int nodeId = signatureFile.accountNum();
                var sigFilePath = testDir.resolve("node_0.0." + nodeId + ".rcd_sig");
                assertTrue(Files.exists(sigFilePath), "Signature file for node " + nodeId + " should exist");

                // Get original signature file bytes from parsed signature file
                byte[] originalSigBytes = signatureFile.signatureFileBytes();

                // Verify signature file bytes match original
                byte[] writtenSigBytes = Files.readAllBytes(sigFilePath);
                assertArrayEquals(
                        originalSigBytes,
                        writtenSigBytes,
                        "Written V5 signature file for node " + nodeId + " should match original");
            }
        }
    }
}
