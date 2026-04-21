// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder.generateBlocksInRange;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
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
        final Block block = Block.PROTOBUF.parse(Bytes.wrap(CompressionType.ZSTD.decompress(bytes)));
        return expected.block().equals(block);
    }
}
