// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.zip.GZIPInputStream;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;

/*
 * Utility class for getting test blocks.
 * */
@SuppressWarnings("unused")
public final class BlockUtils {

    /**
     * Converts Block to a List of BlockUnparsed
     *
     * @param block the Block to convert
     * @return BlockUnparsed representation of the BlockItem
     */
    public static BlockUnparsed toBlockUnparsed(Block block) {
        try {
            return BlockUnparsed.PROTOBUF.parse(Block.PROTOBUF.toBytes(block));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Converts a BlockUnparsed to a Block
     *
     * @param block the BlockUnparsed to convert
     * @return the Block representation of the BlockUnparsed
     */
    public static Block toBlock(BlockUnparsed block) {
        try {
            return Block.PROTOBUF.parse(BlockUnparsed.PROTOBUF.toBytes(block));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets a SampleBlockInfo, out of the defined sample blocks enum.
     */
    public static SampleBlockInfo getSampleBlockInfo(SAMPLE_BLOCKS sampleBlocks) throws IOException, ParseException {
        BlockUnparsed blockUnparsed;
        var stream = TestUtils.class.getModule().getResourceAsStream("test-blocks/" + sampleBlocks.blockName);
        try (final var gzipInputStream = new GZIPInputStream(stream)) {
            // Read the bytes from the GZIPInputStream
            byte[] bytes = gzipInputStream.readAllBytes();
            // Parse the bytes into a BlockUnparsed object
            blockUnparsed = BlockUnparsed.PROTOBUF.parse(Bytes.wrap(bytes));
        }

        // Get the block root hash and block number
        Bytes blockRootHash = sampleBlocks.getBlockHash();
        long blockNumber = sampleBlocks.getBlockNumber();
        // Return a SampleBlockInfo object with the block root hash, block number, and BlockUnparsed object
        return new SampleBlockInfo(blockRootHash, blockNumber, blockUnparsed);
    }

    /**
     * SampleBlockInfo is a simple record that contains the block root hash, block number, and BlockUnparsed object for convenience
     * */
    public record SampleBlockInfo(Bytes blockRootHash, Long blockNumber, BlockUnparsed blockUnparsed) {}

    /**
     * Sample blocks for testing.
     * These blocks are used for testing purposes only.
     */
    public enum SAMPLE_BLOCKS {
        HAPI_0_66_0_BLOCK_10(
                "HAPI-0-66-0/000000000000000000000000000000000010.blk.gz",
                "30783030", // dummy hash
                10),
        HAPI_0_68_0_BLOCK_14(
                "HAPI-0-68-0/000000000000000000000000000000000014.blk.gz",
                "d4924fe896fd32375ced195f29238f36b50f1a04d7b0e34e00b82758a2e9cabd37c98945181788e309d2c9588830bbcb",
                14);

        private final String blockName;
        private final Bytes blockHash;
        private final long blockNumber;

        SAMPLE_BLOCKS(String blockName, String blockHash, long blockNumber) {
            this.blockName = blockName;
            this.blockHash = Bytes.fromHex(blockHash);
            this.blockNumber = blockNumber;
        }

        public String getBlockName() {
            return blockName;
        }

        public Bytes getBlockHash() {
            return blockHash;
        }

        public long getBlockNumber() {
            return blockNumber;
        }
    }

    /**
     * A simple file visitor to recursively delete files and directories up to
     * the provided root.
     */
    private static class RecursiveFileDeleteVisitor extends SimpleFileVisitor<Path> {
        @Override
        @NonNull
        public FileVisitResult visitFile(@NonNull final Path file, @NonNull final BasicFileAttributes attrs)
                throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
        }

        @Override
        @NonNull
        public FileVisitResult postVisitDirectory(@NonNull final Path dir, @Nullable final IOException e)
                throws IOException {
            if (e == null) {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            } else {
                // directory iteration failed
                throw e;
            }
        }
    }
}
