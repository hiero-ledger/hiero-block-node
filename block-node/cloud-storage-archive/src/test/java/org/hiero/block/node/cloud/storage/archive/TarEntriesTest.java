// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder.generateBlocksInRange;
import static org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder.sampleProofUnparsed;
import static org.hiero.block.node.base.ParseHelper.standardParse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.base.CompressionType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/// Unit tests for [TarEntries].
class TarEntriesTest {

    @Test
    @DisplayName("Test writing a tar file and extracting it, and checking the contents")
    void testWritingTar(@TempDir Path tempDir) throws Exception {
        final List<TestBlock> blocks = generateBlocksInRange(0, 3);
        final Path tarFile = tempDir.resolve("test.tar");
        try (var outputStream = Files.newOutputStream(tarFile)) {
            for (TestBlock block : blocks) {
                outputStream.write(TarEntries.toTarEntry(block.blockUnparsed(), block.number()));
            }
            // end-of-archive marker: two 512-byte zero blocks
            outputStream.write(new byte[1024]);
        }
        assertTrue(checkBlockTarFileContents(tempDir, tarFile, blocks));
    }

    @Test
    @DisplayName("Test writing a tar file with huge blocks and extracting it, and checking the contents")
    void testWritingTarLargeBlocks(@TempDir Path tempDir) throws Exception {
        final List<TestBlock> blocks = TestBlockBuilder.generateLargeBlocksInRange(0, 10);
        final Path tarFile = tempDir.resolve("test.tar");
        try (var outputStream = Files.newOutputStream(tarFile)) {
            for (TestBlock block : blocks) {
                outputStream.write(TarEntries.toTarEntry(block.blockUnparsed(), block.number()));
            }
            // end-of-archive marker: two 512-byte zero blocks
            outputStream.write(new byte[1024]);
        }
        assertTrue(checkBlockTarFileContents(tempDir, tarFile, blocks));
    }

    @Test
    @DisplayName("Tar entry modification time is the block's consensus time")
    void testModificationTimeIsConsensusTime() throws IOException {
        final Instant consensusTime = Instant.parse("2024-01-02T03:04:05Z");
        final TestBlock block = generateBlocksInRange(7, 7, consensusTime, Duration.ofSeconds(2))
                .getFirst();

        final byte[] entry = TarEntries.toTarEntry(block.blockUnparsed(), block.number());

        // UStar mtime field: 11 octal digits and a NUL terminator at header offset 136.
        final String modificationTimeOctal = new String(entry, 136, 11, StandardCharsets.US_ASCII);
        assertEquals(consensusTime.getEpochSecond(), Long.parseLong(modificationTimeOctal, 8));
    }

    @Test
    @DisplayName("Consensus time fails when the block does not start with a block header")
    void testConsensusTimeWithoutHeaderThrows() {
        final BlockUnparsed blockWithoutHeader =
                BlockUnparsed.newBuilder().blockItems(sampleProofUnparsed(7)).build();

        assertThrows(IOException.class, () -> TarEntries.consensusTimeSeconds(blockWithoutHeader));
    }

    @Test
    @DisplayName("Consensus time fails when the block header has no timestamp")
    void testConsensusTimeWithoutTimestampThrows() {
        final Bytes headerWithoutTimestamp =
                BlockHeader.PROTOBUF.toBytes(BlockHeader.newBuilder().number(7).build());
        final BlockUnparsed block = BlockUnparsed.newBuilder()
                .blockItems(BlockItemUnparsed.newBuilder()
                        .blockHeader(headerWithoutTimestamp)
                        .build())
                .build();

        assertThrows(IOException.class, () -> TarEntries.consensusTimeSeconds(block));
    }

    @Test
    @DisplayName("Consensus time fails when the block header cannot be parsed")
    void testConsensusTimeWithUnparsableHeaderThrows() {
        // Field 1 with the invalid wire type 7.
        final Bytes unparsableHeader = Bytes.wrap(new byte[] {(byte) 0x0F});
        final BlockUnparsed block = BlockUnparsed.newBuilder()
                .blockItems(BlockItemUnparsed.newBuilder()
                        .blockHeader(unparsableHeader)
                        .build())
                .build();

        assertThrows(IOException.class, () -> TarEntries.consensusTimeSeconds(block));
    }

    private boolean checkBlockTarFileContents(Path tempDir, Path tarFile, List<TestBlock> blocks) throws Exception {
        final Path tarTempDir = Files.createDirectories(tempDir.resolve("tar-output"));
        final ProcessBuilder processBuilder =
                new ProcessBuilder("tar", "-xf", tarFile.toString(), "-C", tarTempDir.toString());
        processBuilder.redirectErrorStream(true);
        final int exitCode = processBuilder.start().waitFor();
        if (exitCode != 0) {
            fail("Failed to untar file. Exit code: " + exitCode);
        }
        try (Stream<Path> files = Files.list(tarTempDir)) {
            for (Path file : files.toList()) {
                final String fileName = file.getFileName().toString();
                if (fileName.endsWith(".blk.zstd")) {
                    final long blockNumber = Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
                    final TestBlock expected = blocks.stream()
                            .filter(b -> b.number() == blockNumber)
                            .findFirst()
                            .orElseThrow();
                    return checkBlockFileContents(file, expected);
                } else {
                    fail("Unexpected file name: " + fileName);
                    return false;
                }
            }
        }
        return false;
    }

    private boolean checkBlockFileContents(Path blockFile, TestBlock expected) throws IOException, ParseException {
        final byte[] bytes = Files.readAllBytes(blockFile);
        final Block block = standardParse(Block.PROTOBUF, Bytes.wrap(CompressionType.ZSTD.decompress(bytes)));
        return expected.block().equals(block);
    }
}
