// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

import static org.hiero.block.tools.records.RecordFileDates.instantToRecordFileName;
import static org.hiero.block.tools.utils.TestBlocks.V2_TEST_BLOCK_ADDRESS_BOOK;
import static org.hiero.block.tools.utils.TestBlocks.V2_TEST_BLOCK_BYTES;
import static org.hiero.block.tools.utils.TestBlocks.V2_TEST_BLOCK_HASH;
import static org.hiero.block.tools.utils.TestBlocks.loadV2SignatureFiles;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.jimfs.Jimfs;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Tests for parsing and validating ParsedRecordFile with a V2 Hedera record stream file.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS) // Single instance, shared state
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParsedV2RecordFileTest {

    /** The parse V2 block, parsed in test (1) */
    private ParsedRecordFile v2BlockParsedRecordFile;

    @Test
    @Order(1)
    @DisplayName("Test parsing V2 record file")
    void testV2TestParse() {
        assertDoesNotThrow(() -> {
            final InMemoryFile v2RecordFile =
                    new InMemoryFile(Path.of("2019-09-13T21_53_51.396440Z.rcd"), V2_TEST_BLOCK_BYTES);
            v2BlockParsedRecordFile = ParsedRecordFile.parse(v2RecordFile);
        });
    }

    @Test
    @Order(2)
    @DisplayName("Test V2 block hash")
    void testV2BlockHash() {
        byte[] blockHash = v2BlockParsedRecordFile.blockHash();
        System.out.println("blockHash = " + HexFormat.of().formatHex(blockHash));
        assertArrayEquals(
                V2_TEST_BLOCK_HASH, blockHash, "V2 block hash does not match expected value from mirror node");
        // recompute block hash to verify deterministic result
        byte[] recomputedHash = v2BlockParsedRecordFile.recomputeBlockHash();
        assertArrayEquals(
                V2_TEST_BLOCK_HASH,
                recomputedHash,
                "Recomputed V2 block hash does not match expected value from mirror node");
    }

    @Test
    @Order(3)
    @DisplayName("Test V2 signatures")
    void testV2Signatures() {
        List<ParsedSignatureFile> signatures = loadV2SignatureFiles();
        for (ParsedSignatureFile signatureFile : signatures) {
            assertTrue(
                    signatureFile.isValid(v2BlockParsedRecordFile.signedHash(), V2_TEST_BLOCK_ADDRESS_BOOK),
                    "V2 signature file for node " + signatureFile.accountNum() + " is not valid");
        }
    }

    @Test
    @Order(4)
    @DisplayName("Test V2 ParsedRecordFile reconstruction via parse method")
    void testV2OtherParseMethod() {
        ParsedRecordFile reconstructedRecordFile = ParsedRecordFile.parse(
                v2BlockParsedRecordFile.blockTime(),
                2,
                v2BlockParsedRecordFile.hapiProtoVersion(),
                v2BlockParsedRecordFile.previousBlockHash(),
                v2BlockParsedRecordFile.recordStreamFile());
        assertArrayEquals(
                V2_TEST_BLOCK_HASH,
                reconstructedRecordFile.blockHash(),
                "Reconstructed V2 ParsedRecordFile block hash does not match expected value from mirror node");
        assertArrayEquals(
                V2_TEST_BLOCK_BYTES,
                reconstructedRecordFile.recordFileContents(),
                "Reconstructed V2 ParsedRecordFile bytes do not match original bytes");
    }

    @Test
    @Order(5)
    @DisplayName("Test V2 write methods")
    void testV2WriteMethods() throws Exception {
        try (ByteArrayOutputStream bout = new ByteArrayOutputStream(V2_TEST_BLOCK_BYTES.length);
                WritableStreamingData out = new WritableStreamingData(bout)) {
            v2BlockParsedRecordFile.write(out);
            assertArrayEquals(
                    V2_TEST_BLOCK_BYTES,
                    bout.toByteArray(),
                    "Written V2 ParsedRecordFile bytes do not match original bytes");
        }
        // create JimFS in-memory file system and write the record file to it
        try (var fs = Jimfs.newFileSystem()) {
            var pathTest = fs.getPath("test");
            Files.createDirectories(pathTest);
            v2BlockParsedRecordFile.write(pathTest, false);
            // read back the written file and compare bytes (filename has padded nanoseconds)
            var writtenPath = pathTest.resolve(instantToRecordFileName(v2BlockParsedRecordFile.blockTime()));
            byte[] writtenBytes = Files.readAllBytes(writtenPath);
            assertArrayEquals(
                    V2_TEST_BLOCK_BYTES,
                    writtenBytes,
                    "Written to file V2 ParsedRecordFile bytes do not match original bytes");
        }
    }
}
