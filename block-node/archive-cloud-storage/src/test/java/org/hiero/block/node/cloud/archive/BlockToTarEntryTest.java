// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.archive;

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

/// Unit tests for [BlockToTarEntry].
class BlockToTarEntryTest {

    @Test
    @DisplayName("Test writing a tar file and extracting it, and checking the contents")
    void testWritingTar(@TempDir Path tempDir) throws Exception {
        final List<TestBlock> blocks = generateBlocksInRange(0, 3);
        // write to temp file
        Path tarFile = tempDir.resolve("test.tar");
        try (var outputStream = Files.newOutputStream(tarFile)) {
            for (TestBlock block : blocks) {
                outputStream.write(BlockToTarEntry.toTarEntry(block.blockUnparsed(), block.number()));
            }
            // end-of-archive marker: two 512-byte zero blocks
            outputStream.write(new byte[1024]);
        }
        // check the contents of the tar file
        assertTrue(checkBlockTarFileContents(tempDir, tarFile, blocks));
    }

    @Test
    @DisplayName("Test writing a tar file with huge blocks and extracting it, and checking the contents")
    void testWritingTarLargeBlocks(@TempDir Path tempDir) throws Exception {
        final List<TestBlock> blocks = TestBlockBuilder.generateLargeBlocksInRange(0, 10);
        // write to temp file
        Path tarFile = tempDir.resolve("test.tar");
        try (var outputStream = Files.newOutputStream(tarFile)) {
            for (TestBlock block : blocks) {
                outputStream.write(BlockToTarEntry.toTarEntry(block.blockUnparsed(), block.number()));
            }
            // end-of-archive marker: two 512-byte zero blocks
            outputStream.write(new byte[1024]);
        }
        // check the contents of the tar file
        assertTrue(checkBlockTarFileContents(tempDir, tarFile, blocks));
    }

    /// Check the contents of the tar file by extracting it and checking the contents of each block file.
    ///
    /// @param tempDir the temporary directory where to create subdirectory to extract the tar file
    /// @param tarFile the tar file to check
    /// @param blocks the expected blocks that should be in the tar
    /// @throws IOException if there is an error reading the file
    /// @throws InterruptedException if the process is interrupted
    private boolean checkBlockTarFileContents(Path tempDir, Path tarFile, List<TestBlock> blocks) throws Exception {
        // now try to un-tar the file using command line tar command
        Path tarTempDir = Files.createDirectories(tempDir.resolve("tar-output"));
        // Use ProcessBuilder to execute the command
        ProcessBuilder processBuilder =
                new ProcessBuilder("tar", "-xf", tarFile.toString(), "-C", tarTempDir.toString());
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            fail("Failed to untar file. Exit code: " + exitCode);
        }
        // list the directory contents
        try (Stream<Path> files = Files.list(tarTempDir)) {
            for (Path file : files.toList()) {
                // check the file name
                String fileName = file.getFileName().toString();
                if (fileName.endsWith(".blk.zstd")) {
                    long blockNumber = Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
                    TestBlock expected = blocks.stream()
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

    /// Check the contents of the block file against the expected block.
    ///
    /// @param blockFile the block file to check
    /// @param expected the expected block
    /// @throws IOException if there is an error reading the file
    /// @throws ParseException if there is an error parsing the block
    private boolean checkBlockFileContents(Path blockFile, TestBlock expected) throws IOException, ParseException {
        byte[] bytes = Files.readAllBytes(blockFile);
        Block block = Block.PROTOBUF.parse(Bytes.wrap(CompressionType.ZSTD.decompress(bytes)));
        return expected.block().equals(block);
    }
}
