// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.hiero.block.tools.records.RecordFileDates.extractRecordFileTime;
import static org.hiero.block.tools.utils.TestBlocks.V2_TEST_BLOCK_ADDRESS_BOOK;
import static org.hiero.block.tools.utils.TestBlocks.V2_TEST_BLOCK_BYTES;
import static org.hiero.block.tools.utils.TestBlocks.V2_TEST_BLOCK_DATE_STR;
import static org.hiero.block.tools.utils.TestBlocks.V2_TEST_BLOCK_NUMBER;
import static org.hiero.block.tools.utils.TestBlocks.V2_TEST_BLOCK_RECORD_FILE_NAME;
import static org.hiero.block.tools.utils.TestBlocks.V5_TEST_BLOCK_ADDRESS_BOOK;
import static org.hiero.block.tools.utils.TestBlocks.V5_TEST_BLOCK_BYTES;
import static org.hiero.block.tools.utils.TestBlocks.V5_TEST_BLOCK_DATE_STR;
import static org.hiero.block.tools.utils.TestBlocks.V5_TEST_BLOCK_NUMBER;
import static org.hiero.block.tools.utils.TestBlocks.V5_TEST_BLOCK_RECORD_FILE_NAME;
import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_ADDRESS_BOOK;
import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_BYTES;
import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_DATE_STR;
import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_NUMBER;
import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_RECORD_FILE_NAME;
import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_SIDECAR_BYTES;
import static org.hiero.block.tools.utils.TestBlocks.V6_TEST_BLOCK_SIDECAR_FILE_NAME;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.hiero.block.tools.records.model.parsed.ParsedRecordBlock;
import org.hiero.block.tools.records.model.parsed.ParsedV2RecordFileTest;
import org.hiero.block.tools.records.model.parsed.RecordBlockConverter;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Generates Wrapped Record Block (WRB) test fixtures from sample V2, V5, and V6 record files.
 * Each test creates an UnparsedRecordBlock from raw files, parses it, converts to a Block,
 * serializes to gzipped protobuf, saves as a .blk.gz fixture, and validates round-trip fidelity.
 */
class WrbFixtureGeneratorTest {

    /** Output directory for generated WRB fixtures (relative to subproject working directory). */
    private static final Path WRB_OUTPUT_DIR = Path.of("src/test/resources/record-files/wrb");

    @Test
    @DisplayName("Generate and validate V2 WRB fixture")
    void generateV2Wrb() throws IOException {
        // Load primary record file
        final InMemoryFile primaryRecordFile =
                new InMemoryFile(Path.of(V2_TEST_BLOCK_RECORD_FILE_NAME), V2_TEST_BLOCK_BYTES);
        final Instant recordFileTime = extractRecordFileTime(V2_TEST_BLOCK_RECORD_FILE_NAME);

        // Load signature files as raw InMemoryFile objects
        final List<InMemoryFile> signatureFiles = loadRawSignatureFiles(
                "/record-files/example-v2/" + V2_TEST_BLOCK_DATE_STR + "/",
                IntStream.rangeClosed(3, 9).toArray());

        // Create unparsed block, parse, and convert
        final UnparsedRecordBlock unparsedBlock = UnparsedRecordBlock.newInMemoryBlock(
                recordFileTime,
                primaryRecordFile,
                Collections.emptyList(),
                signatureFiles,
                Collections.emptyList(),
                Collections.emptyList());
        final ParsedRecordBlock parsedBlock = unparsedBlock.parse();
        final Block block = RecordBlockConverter.toBlock(
                parsedBlock,
                V2_TEST_BLOCK_NUMBER,
                EMPTY_TREE_HASH,
                EMPTY_TREE_HASH,
                V2_TEST_BLOCK_ADDRESS_BOOK,
                AmendmentProvider.createAmendmentProvider("none"));

        // Validate block structure
        assertBlockStructure(block, 2);

        // Serialize and save
        final Path outputPath = saveBlockAsGzip(block, "v2-block.blk.gz");
        assertTrue(Files.exists(outputPath), "V2 WRB fixture file should exist");
        assertTrue(Files.size(outputPath) > 0, "V2 WRB fixture file should not be empty");

        // Round-trip: Block -> ParsedRecordBlock -> verify original record file bytes
        final ParsedRecordBlock roundTrip = RecordBlockConverter.toRecordFile(block, V2_TEST_BLOCK_ADDRESS_BOOK);
        assertArrayEquals(
                V2_TEST_BLOCK_BYTES,
                roundTrip.recordFile().recordFileContents(),
                "V2 round-trip record file bytes should match original");
    }

    @Test
    @DisplayName("Generate and validate V5 WRB fixture")
    void generateV5Wrb() throws IOException {
        // Load primary record file
        final InMemoryFile primaryRecordFile =
                new InMemoryFile(Path.of(V5_TEST_BLOCK_RECORD_FILE_NAME), V5_TEST_BLOCK_BYTES);
        final Instant recordFileTime = extractRecordFileTime(V5_TEST_BLOCK_RECORD_FILE_NAME);

        // Load signature files (nodes 10-18, 20-22)
        final int[] v5NodeIds = Stream.concat(
                        IntStream.rangeClosed(10, 18).boxed(),
                        IntStream.rangeClosed(20, 22).boxed())
                .mapToInt(Integer::intValue)
                .toArray();
        final List<InMemoryFile> signatureFiles =
                loadRawSignatureFiles("/record-files/example-v5/" + V5_TEST_BLOCK_DATE_STR + "/", v5NodeIds);

        // Create unparsed block, parse, and convert
        final UnparsedRecordBlock unparsedBlock = UnparsedRecordBlock.newInMemoryBlock(
                recordFileTime,
                primaryRecordFile,
                Collections.emptyList(),
                signatureFiles,
                Collections.emptyList(),
                Collections.emptyList());
        final ParsedRecordBlock parsedBlock = unparsedBlock.parse();
        final Block block = RecordBlockConverter.toBlock(
                parsedBlock,
                V5_TEST_BLOCK_NUMBER,
                EMPTY_TREE_HASH,
                EMPTY_TREE_HASH,
                V5_TEST_BLOCK_ADDRESS_BOOK,
                AmendmentProvider.createAmendmentProvider("none"));

        // Validate block structure
        assertBlockStructure(block, 5);

        // Serialize and save
        final Path outputPath = saveBlockAsGzip(block, "v5-block.blk.gz");
        assertTrue(Files.exists(outputPath), "V5 WRB fixture file should exist");
        assertTrue(Files.size(outputPath) > 0, "V5 WRB fixture file should not be empty");

        // Round-trip: Block -> ParsedRecordBlock -> verify original record file bytes
        final ParsedRecordBlock roundTrip = RecordBlockConverter.toRecordFile(block, V5_TEST_BLOCK_ADDRESS_BOOK);
        assertArrayEquals(
                V5_TEST_BLOCK_BYTES,
                roundTrip.recordFile().recordFileContents(),
                "V5 round-trip record file bytes should match original");
    }

    @Test
    @DisplayName("Generate and validate V6 WRB fixture")
    void generateV6Wrb() throws IOException {
        // Load primary record file
        final InMemoryFile primaryRecordFile =
                new InMemoryFile(Path.of(V6_TEST_BLOCK_RECORD_FILE_NAME), V6_TEST_BLOCK_BYTES);
        final Instant recordFileTime = extractRecordFileTime(V6_TEST_BLOCK_RECORD_FILE_NAME);

        // Load sidecar file
        final InMemoryFile sidecarFile =
                new InMemoryFile(Path.of(V6_TEST_BLOCK_SIDECAR_FILE_NAME), V6_TEST_BLOCK_SIDECAR_BYTES);

        // Load signature files (nodes 3, 21, 25, 28-35, 37)
        final int[] v6NodeIds = Stream.of(
                        Stream.of(3, 21, 25, 37), IntStream.rangeClosed(28, 35).boxed())
                .flatMap(s -> s)
                .mapToInt(Integer::intValue)
                .toArray();
        final List<InMemoryFile> signatureFiles =
                loadRawSignatureFiles("/record-files/example-v6/" + V6_TEST_BLOCK_DATE_STR + "/", v6NodeIds);

        // Create unparsed block, parse, and convert
        final UnparsedRecordBlock unparsedBlock = UnparsedRecordBlock.newInMemoryBlock(
                recordFileTime,
                primaryRecordFile,
                Collections.emptyList(),
                signatureFiles,
                List.of(sidecarFile),
                Collections.emptyList());
        final ParsedRecordBlock parsedBlock = unparsedBlock.parse();
        final Block block = RecordBlockConverter.toBlock(
                parsedBlock,
                V6_TEST_BLOCK_NUMBER,
                EMPTY_TREE_HASH,
                EMPTY_TREE_HASH,
                V6_TEST_BLOCK_ADDRESS_BOOK,
                AmendmentProvider.createAmendmentProvider("none"));

        // Validate block structure
        assertBlockStructure(block, 6);
        assertEquals(
                1,
                block.items().stream()
                        .filter(item -> item.hasRecordFile())
                        .findFirst()
                        .orElseThrow()
                        .recordFileOrThrow()
                        .sidecarFileContents()
                        .size(),
                "V6 block should contain 1 sidecar file");

        // Serialize and save
        final Path outputPath = saveBlockAsGzip(block, "v6-block.blk.gz");
        assertTrue(Files.exists(outputPath), "V6 WRB fixture file should exist");
        assertTrue(Files.size(outputPath) > 0, "V6 WRB fixture file should not be empty");

        // Round-trip: Block -> ParsedRecordBlock -> verify original record file bytes
        final ParsedRecordBlock roundTrip = RecordBlockConverter.toRecordFile(block, V6_TEST_BLOCK_ADDRESS_BOOK);
        assertArrayEquals(
                V6_TEST_BLOCK_BYTES,
                roundTrip.recordFile().recordFileContents(),
                "V6 round-trip record file bytes should match original");
    }

    // === Round-trip from disk tests
    // ====================================================================================

    @Nested
    @DisplayName("Read WRB fixtures from disk and validate round-trip")
    class ReadFixturesFromDisk {

        @Test
        @DisplayName("Read V2 WRB fixture from disk and round-trip to original record file bytes")
        void readV2WrbFromDisk() throws Exception {
            final Block block = loadBlockFromResource("/record-files/wrb/v2-block.blk.gz");
            assertBlockStructure(block, 2);
            assertEquals(
                    V2_TEST_BLOCK_NUMBER,
                    block.items().get(0).blockHeaderOrThrow().number());

            final ParsedRecordBlock roundTrip = RecordBlockConverter.toRecordFile(block, V2_TEST_BLOCK_ADDRESS_BOOK);
            assertArrayEquals(
                    V2_TEST_BLOCK_BYTES,
                    roundTrip.recordFile().recordFileContents(),
                    "V2 fixture read from disk should round-trip to original record file bytes");
            assertEquals(2, roundTrip.recordFile().recordFormatVersion(), "Record format version should be 2");
        }

        @Test
        @DisplayName("Read V5 WRB fixture from disk and round-trip to original record file bytes")
        void readV5WrbFromDisk() throws Exception {
            final Block block = loadBlockFromResource("/record-files/wrb/v5-block.blk.gz");
            assertBlockStructure(block, 5);
            assertEquals(
                    V5_TEST_BLOCK_NUMBER,
                    block.items().get(0).blockHeaderOrThrow().number());

            final ParsedRecordBlock roundTrip = RecordBlockConverter.toRecordFile(block, V5_TEST_BLOCK_ADDRESS_BOOK);
            assertArrayEquals(
                    V5_TEST_BLOCK_BYTES,
                    roundTrip.recordFile().recordFileContents(),
                    "V5 fixture read from disk should round-trip to original record file bytes");
            assertEquals(5, roundTrip.recordFile().recordFormatVersion(), "Record format version should be 5");
        }

        @Test
        @DisplayName("Read V6 WRB fixture from disk and round-trip to original record file bytes")
        void readV6WrbFromDisk() throws Exception {
            final Block block = loadBlockFromResource("/record-files/wrb/v6-block.blk.gz");
            assertBlockStructure(block, 6);
            assertEquals(
                    V6_TEST_BLOCK_NUMBER,
                    block.items().get(0).blockHeaderOrThrow().number());

            // Verify sidecar is present
            assertEquals(
                    1,
                    block.items().stream()
                            .filter(item -> item.hasRecordFile())
                            .findFirst()
                            .orElseThrow()
                            .recordFileOrThrow()
                            .sidecarFileContents()
                            .size(),
                    "V6 fixture should contain 1 sidecar file");

            final ParsedRecordBlock roundTrip = RecordBlockConverter.toRecordFile(block, V6_TEST_BLOCK_ADDRESS_BOOK);
            assertArrayEquals(
                    V6_TEST_BLOCK_BYTES,
                    roundTrip.recordFile().recordFileContents(),
                    "V6 fixture read from disk should round-trip to original record file bytes");
            assertEquals(6, roundTrip.recordFile().recordFormatVersion(), "Record format version should be 6");
            assertEquals(1, roundTrip.sidecarFiles().size(), "V6 round-trip should preserve 1 sidecar file");
        }
    }

    // === Helper Methods ==============================================================================================

    /**
     * Loads raw signature files as InMemoryFile objects from the test resources.
     *
     * @param resourceDir the resource directory path (e.g. "/record-files/example-v2/2019-.../")
     * @param nodeIds the node account IDs to load signature files for
     * @return list of InMemoryFile objects for each signature file
     */
    private static List<InMemoryFile> loadRawSignatureFiles(String resourceDir, int[] nodeIds) throws IOException {
        final List<InMemoryFile> result = new ArrayList<>();
        for (int nodeId : nodeIds) {
            final String sigFileName = "node_0.0." + nodeId + ".rcd_sig";
            try (InputStream is = ParsedV2RecordFileTest.class.getResourceAsStream(resourceDir + sigFileName)) {
                Objects.requireNonNull(is, "Missing signature file: " + resourceDir + sigFileName);
                result.add(new InMemoryFile(Path.of(sigFileName), is.readAllBytes()));
            }
        }
        return result;
    }

    /**
     * Loads a gzipped Block protobuf from a classpath resource.
     *
     * @param resourcePath the classpath resource path (e.g. "/record-files/wrb/v2-block.blk.gz")
     * @return the parsed Block
     */
    private static Block loadBlockFromResource(String resourcePath) throws Exception {
        try (InputStream is = WrbFixtureGeneratorTest.class.getResourceAsStream(resourcePath)) {
            Objects.requireNonNull(is, "Missing resource: " + resourcePath);
            try (GZIPInputStream gzipIn = new GZIPInputStream(is)) {
                return Block.PROTOBUF.parse(Bytes.wrap(gzipIn.readAllBytes()));
            }
        }
    }

    /**
     * Asserts the basic structure of a wrapped record block.
     *
     * @param block the block to validate
     * @param expectedRecordFormatVersion the expected record format version in the proof
     */
    private static void assertBlockStructure(Block block, int expectedRecordFormatVersion) {
        assertNotNull(block, "Block should not be null");
        assertEquals(4, block.items().size(), "Block should have 4 items (header, record file, footer, proof)");
        assertTrue(block.items().get(0).hasBlockHeader(), "First item should be BlockHeader");
        assertTrue(block.items().get(1).hasRecordFile(), "Second item should be RecordFile");
        assertTrue(block.items().get(2).hasBlockFooter(), "Third item should be BlockFooter");
        assertTrue(block.items().get(3).hasBlockProof(), "Fourth item should be BlockProof");
        assertTrue(
                block.items().get(3).blockProofOrThrow().hasSignedRecordFileProof(),
                "BlockProof should have SignedRecordFileProof");
        assertEquals(
                expectedRecordFormatVersion,
                block.items()
                        .get(3)
                        .blockProofOrThrow()
                        .signedRecordFileProofOrThrow()
                        .version(),
                "Record format version in proof should be " + expectedRecordFormatVersion);
    }

    /**
     * Serializes a Block to gzipped protobuf bytes and writes to the WRB output directory.
     *
     * @param block the block to serialize
     * @param fileName the output file name (e.g. "v2-block.blk.gz")
     * @return the path to the written file
     */
    private static Path saveBlockAsGzip(Block block, String fileName) throws IOException {
        final byte[] protobufBytes = Block.PROTOBUF.toBytes(block).toByteArray();
        final Path outputPath =
                Path.of(System.getProperty("user.dir")).resolve(WRB_OUTPUT_DIR).resolve(fileName);
        Files.createDirectories(outputPath.getParent());
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(protobufBytes);
            gzipOut.finish();
            Files.write(outputPath, baos.toByteArray());
        }
        return outputPath;
    }
}
