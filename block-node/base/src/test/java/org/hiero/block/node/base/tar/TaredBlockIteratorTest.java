// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.tar;

import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.createNumberOfLargeBlocks;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlockAccessors;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.github.luben.zstd.Zstd;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.shaded.org.bouncycastle.util.Arrays;

/**
 * Test class for TaredBlockIterator.
 */
public class TaredBlockIteratorTest {

    @Test
    @DisplayName("Test writing a tar file and extracting it, and checking the contents")
    public void testWritingTar(@TempDir Path tempDir) throws IOException, InterruptedException {
        BlockAccessor[] blockAccessors = createNumberOfVerySimpleBlockAccessors(0, 3);
        TaredBlockIterator taredBlockIterator =
                new TaredBlockIterator(Format.ZSTD_PROTOBUF, new Arrays.Iterator<>(blockAccessors));
        // write to temp file
        Path tarFile = tempDir.resolve("test.tar");
        try (var outputStream = Files.newOutputStream(tarFile)) {
            while (taredBlockIterator.hasNext()) {
                byte[] bytesChunk = taredBlockIterator.next();
                outputStream.write(bytesChunk);
            }
            // check that an extra call to next() throws an exception
            assertThrows(NoSuchElementException.class, taredBlockIterator::next);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write tar file", e);
        }
        // check the contents of the tar file
        checkBlockTarFileContents(tempDir, tarFile, blockAccessors);
    }

    @Test
    @DisplayName("Test writing a tar file with huge block and extracting it, and checking the contents")
    public void testWritingTarLargeBlocks(@TempDir Path tempDir) throws IOException, InterruptedException {
        BlockAccessor[] blockAccessors = LongStream.range(0, 10)
                .mapToObj(bn -> {
                    BlockItem[] blockItems = createNumberOfLargeBlocks(bn, bn);
                    Block block = new Block(java.util.Arrays.asList(blockItems));
                    return new BlockAccessor() {
                        @Override
                        public long blockNumber() {
                            return bn;
                        }

                        @Override
                        public Block block() {
                            return block;
                        }
                    };
                })
                .toArray(BlockAccessor[]::new);

        TaredBlockIterator taredBlockIterator =
                new TaredBlockIterator(Format.ZSTD_PROTOBUF, new Arrays.Iterator<>(blockAccessors));
        // write to temp file
        Path tarFile = tempDir.resolve("test.tar");
        try (var outputStream = Files.newOutputStream(tarFile)) {
            while (taredBlockIterator.hasNext()) {
                byte[] bytesChunk = taredBlockIterator.next();
                outputStream.write(bytesChunk);
            }
            // check that an extra call to next() throws an exception
            assertThrows(NoSuchElementException.class, taredBlockIterator::next);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write tar file", e);
        }
        // check the contents of the tar file
        checkBlockTarFileContents(tempDir, tarFile, blockAccessors);
    }

    /**
     * Check the contents of the tar file by extracting it and checking the contents of each block file.
     *
     * @param tempDir the temporary directory where to create subdirectory to extract the tar file
     * @param tarFile the tar file to check
     * @param blockAccessors the accessors for the expected blocks that should be in tar
     * @throws IOException if there is an error reading the file
     * @throws InterruptedException if the process is interrupted
     */
    void checkBlockTarFileContents(Path tempDir, Path tarFile, BlockAccessor[] blockAccessors)
            throws IOException, InterruptedException {
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
            files.forEach(file -> {
                try {
                    // check the file name
                    String fileName = file.getFileName().toString();
                    if (fileName.endsWith(".blk.zstd")) {
                        int blockNumber = Integer.parseInt(fileName.substring(0, fileName.indexOf('.')));
                        // check the contents of the file
                        BlockAccessor blockAccessor = blockAccessors[blockNumber];
                        checkBlockFileContents(file, blockAccessor);
                    } else {
                        fail("Unexpected file name: " + fileName);
                    }
                } catch (IOException | ParseException e) {
                    throw new RuntimeException("Failed to read file: " + file, e);
                }
            });
        }
    }

    /**
     * Check the contents of the block file against the expected block from BlockAccessor.
     *
     * @param blockFile the block file to check
     * @param blockAccessor the accessor for the expected block
     * @throws IOException if there is an error reading the file
     * @throws ParseException if there is an error parsing the block
     */
    private void checkBlockFileContents(Path blockFile, BlockAccessor blockAccessor)
            throws IOException, ParseException {
        byte[] bytes = Files.readAllBytes(blockFile);
        Block block = Block.PROTOBUF.parse(Bytes.wrap(Zstd.decompress(bytes, (int) Zstd.getFrameContentSize(bytes))));
        assertEquals(blockAccessor.block(), block, "Block file contents do not match expected block");
    }
}
