// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher.hashBlock;
import static org.hiero.block.tools.utils.Sha256.SHA_256_HASH_SIZE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.hiero.block.tools.blocks.model.BlockArchiveType;
import org.hiero.block.tools.blocks.model.BlockReader;
import org.hiero.block.tools.blocks.model.BlockWriter;
import org.hiero.block.tools.blocks.model.BlockWriter.BlockPath;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import picocli.CommandLine;

/**
 * End-to-end test for {@code blocks repair-zips}: wraps the genesis day in zip mode, deletes a few
 * block entries, refills them from the source day archive and checks every refilled block hashes
 * to its registry entry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class RepairZipsCommandTest {

    private static final long[] DELETED_BLOCKS = {5, 6, 700};

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Missing blocks are refilled with hashes matching the registry")
    void refilledBlocksMatchRegistry() throws Exception {
        assumeTrue(isZstdAvailable(), "zstd not available");
        final Path dayFile = resource("/2019-09-13.tar.zstd");
        final Path blockTimes = resource("/metadata/block_times.bin");
        final Path dayBlocks = resource("/metadata/day_blocks.json");

        final Path inputDir = tempDir.resolve("input");
        Files.createDirectories(inputDir);
        Files.copy(dayFile, inputDir.resolve(dayFile.getFileName()));
        final Path outputDir = tempDir.resolve("wrapped");
        assertEquals(
                0,
                new CommandLine(new ToWrappedBlocksCommand())
                        .execute(
                                "-i", inputDir.toString(),
                                "-o", outputDir.toString(),
                                "-b", blockTimes.toString(),
                                "-d", dayBlocks.toString()),
                "wrap should succeed");

        for (final long blockNumber : DELETED_BLOCKS) {
            final BlockPath blockPath =
                    BlockWriter.computeBlockPath(outputDir, blockNumber, BlockArchiveType.UNCOMPRESSED_ZIP);
            try (FileSystem zipFs = FileSystems.newFileSystem(blockPath.zipFilePath())) {
                Files.delete(zipFs.getPath(blockPath.blockFileName()));
            }
        }
        // The saved hasher state is past the deleted blocks, and MissingBlockFiller only fast-forwards
        // it, so drop it to make the filler replay from block 0.
        Files.delete(outputDir.resolve("streamingMerkleTree.bin"));
        Files.deleteIfExists(outputDir.resolve("streamingMerkleTree.bin.bak"));

        assertEquals(
                0,
                new CommandLine(new RepairZipsCommand())
                        .execute(
                                outputDir.toString(),
                                "-i",
                                inputDir.toString(),
                                "-b",
                                blockTimes.toString(),
                                "-d",
                                dayBlocks.toString()),
                "repair-zips should fill every deleted block");

        try (BlockStreamBlockHashRegistry registry =
                new BlockStreamBlockHashRegistry(outputDir.resolve("blockStreamBlockHashes.bin"))) {
            for (final long blockNumber : DELETED_BLOCKS) {
                final byte[] hash = hashBlock(BlockReader.readBlock(outputDir, blockNumber));
                assertEquals(SHA_256_HASH_SIZE, hash.length);
                assertArrayEquals(registry.getBlockHash(blockNumber), hash, "block " + blockNumber);
            }
        } finally {
            BlockReader.closeCachedZipFs();
        }
    }

    private Path resource(final String name) throws Exception {
        return Path.of(Objects.requireNonNull(getClass().getResource(name)).toURI());
    }

    private static boolean isZstdAvailable() {
        try {
            Process p = new ProcessBuilder("which", "zstd").start();
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
