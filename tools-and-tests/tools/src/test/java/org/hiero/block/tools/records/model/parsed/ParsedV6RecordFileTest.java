// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_ADDRESS_BOOK;
import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_BYTES;
import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_HASH;
import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_RECORD_FILE_NAME;
import static org.hiero.block.tools.utils.TestBlocks.loadV6SignatureFiles;
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
 * Tests for parsing and validating ParsedRecordFile with a V6 Hedera record stream file.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS) // Single instance, shared state
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParsedV6RecordFileTest {

    /** The parse V6 block, parsed in test (1) */
    private ParsedRecordFile v6BlockParsedRecordFile;

    @Test
    @Order(1)
    @DisplayName("Test parsing V6 record file")
    void testV6TestParse() {
        assertDoesNotThrow(() -> {
            final InMemoryFile v6RecordFile =
                    new InMemoryFile(Path.of(V6_TEST_BLOCK_RECORD_FILE_NAME), V6_TEST_BLOCK_BYTES);
            v6BlockParsedRecordFile = ParsedRecordFile.parse(v6RecordFile);
        });
    }

    @Test
    @Order(2)
    @DisplayName("Test V6 block hash")
    void testV6BlockHash() {
        byte[] blockHash = v6BlockParsedRecordFile.blockHash();
        System.out.println("blockHash = " + HexFormat.of().formatHex(blockHash));
        assertArrayEquals(
                V6_TEST_BLOCK_HASH, blockHash, "V6 block hash does not match expected value from mirror node");
        // recompute block hash to verify deterministic result
        byte[] recomputedHash = v6BlockParsedRecordFile.recomputeBlockHash();
        assertArrayEquals(
                V6_TEST_BLOCK_HASH,
                recomputedHash,
                "Recomputed V6 block hash does not match expected value from mirror node");
    }

    @Test
    @Order(3)
    @DisplayName("Test V6 signatures")
    void testV6Signatures() {
        // V6 has signature files for nodes 3, 21, 25, 28-35, 37
        List<ParsedSignatureFile> signatures = loadV6SignatureFiles();
        for (ParsedSignatureFile signatureFile : signatures) {
            assertTrue(
                    signatureFile.isValid(v6BlockParsedRecordFile.signedHash(), V6_TEST_BLOCK_ADDRESS_BOOK),
                    "V6 signature file for node " + signatureFile.accountNum() + " is not valid");
        }
    }

    @Test
    @Order(4)
    @DisplayName("Test V6 ParsedRecordFile reconstruction via parse method")
    void testV6OtherParseMethod() {
        ParsedRecordFile reconstructedRecordFile = ParsedRecordFile.parse(
                v6BlockParsedRecordFile.blockTime(),
                6,
                v6BlockParsedRecordFile.hapiProtoVersion(),
                v6BlockParsedRecordFile.previousBlockHash(),
                v6BlockParsedRecordFile.recordStreamFile());
        assertArrayEquals(
                V6_TEST_BLOCK_HASH,
                reconstructedRecordFile.blockHash(),
                "Reconstructed V6 ParsedRecordFile block hash does not match expected value from mirror node");
        assertArrayEquals(
                V6_TEST_BLOCK_BYTES,
                reconstructedRecordFile.recordFileContents(),
                "Reconstructed V6 ParsedRecordFile bytes do not match original bytes");
    }

    @Test
    @Order(5)
    @DisplayName("Test V6 write methods")
    void testV6WriteMethods() throws Exception {
        try (ByteArrayOutputStream bout = new ByteArrayOutputStream(V6_TEST_BLOCK_BYTES.length);
                WritableStreamingData out = new WritableStreamingData(bout)) {
            v6BlockParsedRecordFile.write(out);
            assertArrayEquals(
                    V6_TEST_BLOCK_BYTES,
                    bout.toByteArray(),
                    "Written V6 ParsedRecordFile bytes do not match original bytes");
        }
        // create JimFS in-memory file system and write the record file to it
        try (var fs = Jimfs.newFileSystem()) {
            var pathTest = fs.getPath("test");
            Files.createDirectories(pathTest);
            v6BlockParsedRecordFile.write(pathTest, false);
            // read back the written file and compare bytes
            var writtenPath = pathTest.resolve(V6_TEST_BLOCK_RECORD_FILE_NAME);
            byte[] writtenBytes = Files.readAllBytes(writtenPath);
            assertArrayEquals(
                    V6_TEST_BLOCK_BYTES,
                    writtenBytes,
                    "Written to file V6 ParsedRecordFile bytes do not match original bytes");
        }
    }
}
