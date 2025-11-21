// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

import static org.hiero.block.tools.utils.TestBlocks.V5_TEST_BLOCK_ADDRESS_BOOK;
import static org.hiero.block.tools.utils.TestBlocks.V5_TEST_BLOCK_BYTES;
import static org.hiero.block.tools.utils.TestBlocks.V5_TEST_BLOCK_DATE_STR;
import static org.hiero.block.tools.utils.TestBlocks.V5_TEST_BLOCK_HASH;
import static org.hiero.block.tools.utils.TestBlocks.V5_TEST_BLOCK_RECORD_FILE_NAME;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.jimfs.Jimfs;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Tests for parsing and validating ParsedRecordFile with a V5 Hedera record stream file.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS) // Single instance, shared state
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParsedV5RecordFileTest {

    /** The parse V5 block, parsed in test (1) */
    private ParsedRecordFile v5BlockParsedRecordFile;

    @Test
    @Order(1)
    @DisplayName("Test parsing V5 record file")
    void testV5TestParse() {
        assertDoesNotThrow(() -> {
            final InMemoryFile v5RecordFile =
                    new InMemoryFile(Path.of(V5_TEST_BLOCK_RECORD_FILE_NAME), V5_TEST_BLOCK_BYTES);
            v5BlockParsedRecordFile = ParsedRecordFile.parse(v5RecordFile);
        });
    }

    @Test
    @Order(2)
    @DisplayName("Test V5 block hash")
    void testV5BlockHash() {
        byte[] blockHash = v5BlockParsedRecordFile.blockHash();
        System.out.println("blockHash = " + HexFormat.of().formatHex(blockHash));
        assertArrayEquals(
                V5_TEST_BLOCK_HASH, blockHash, "V5 block hash does not match expected value from mirror node");
        // recompute block hash to verify deterministic result
        byte[] recomputedHash = v5BlockParsedRecordFile.recomputeBlockHash();
        assertArrayEquals(
                V5_TEST_BLOCK_HASH,
                recomputedHash,
                "Recomputed V5 block hash does not match expected value from mirror node");
    }

    @Test
    @Order(3)
    @DisplayName("Test V5 signatures")
    void testV5Signatures() {
        // V5 has signature files for nodes 10-18, 20-22
        List<ParsedSignatureFile> signatures = Stream.concat(
                        IntStream.rangeClosed(10, 18).boxed(),
                        IntStream.rangeClosed(20, 22).boxed())
                .map(nodeId -> {
                    try {
                        final byte[] sigBytes = Objects.requireNonNull(
                                        ParsedV5RecordFileTest.class.getResourceAsStream("/record-files/example-v5/"
                                                + V5_TEST_BLOCK_DATE_STR + "/node_0.0." + nodeId + ".rcd_sig"))
                                .readAllBytes();
                        return new ParsedSignatureFile(
                                new InMemoryFile(Path.of("node_0.0." + nodeId + ".rcd_sig"), sigBytes));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();
        for (ParsedSignatureFile signatureFile : signatures) {
            assertTrue(
                    signatureFile.isValid(v5BlockParsedRecordFile.signedHash(), V5_TEST_BLOCK_ADDRESS_BOOK),
                    "V5 signature file for node " + signatureFile.accountNum() + " is not valid");
        }
    }

    @Test
    @Order(4)
    @DisplayName("Test V5 ParsedRecordFile reconstruction via parse method")
    void testV5OtherParseMethod() {
        ParsedRecordFile reconstructedRecordFile = ParsedRecordFile.parse(
                v5BlockParsedRecordFile.blockTime(),
                5,
                v5BlockParsedRecordFile.hapiProtoVersion(),
                v5BlockParsedRecordFile.previousBlockHash(),
                v5BlockParsedRecordFile.recordStreamFile());
        assertArrayEquals(
                V5_TEST_BLOCK_HASH,
                reconstructedRecordFile.blockHash(),
                "Reconstructed V5 ParsedRecordFile block hash does not match expected value from mirror node");
        assertArrayEquals(
                V5_TEST_BLOCK_BYTES,
                reconstructedRecordFile.recordFileContents(),
                "Reconstructed V5 ParsedRecordFile bytes do not match original bytes");
    }

    @Test
    @Order(5)
    @DisplayName("Test V5 write methods")
    void testV5WriteMethods() throws Exception {
        try (ByteArrayOutputStream bout = new ByteArrayOutputStream(V5_TEST_BLOCK_BYTES.length);
                WritableStreamingData out = new WritableStreamingData(bout)) {
            v5BlockParsedRecordFile.write(out);
            assertArrayEquals(
                    V5_TEST_BLOCK_BYTES,
                    bout.toByteArray(),
                    "Written V5 ParsedRecordFile bytes do not match original bytes");
        }
        // create JimFS in-memory file system and write the record file to it
        try (var fs = Jimfs.newFileSystem()) {
            var pathTest = fs.getPath("test");
            Files.createDirectories(pathTest);
            v5BlockParsedRecordFile.write(pathTest, false);
            // read back the written file and compare bytes
            var writtenPath = pathTest.resolve(V5_TEST_BLOCK_RECORD_FILE_NAME);
            byte[] writtenBytes = Files.readAllBytes(writtenPath);
            assertArrayEquals(
                    V5_TEST_BLOCK_BYTES,
                    writtenBytes,
                    "Written to file V5 ParsedRecordFile bytes do not match original bytes");
        }
    }
}
