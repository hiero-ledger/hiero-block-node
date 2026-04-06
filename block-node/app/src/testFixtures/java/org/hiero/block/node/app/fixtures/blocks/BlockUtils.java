// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.Codec;
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
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

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
            blockUnparsed = BlockUnparsed.PROTOBUF.parse(
                    Bytes.wrap(bytes).toReadableSequentialData(),
                    false,
                    false,
                    Codec.DEFAULT_MAX_DEPTH,
                    BlockAccessor.MAX_BLOCK_SIZE_BYTES);
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
        /** Genesis block — bootstraps TSS parameters and ledger ID. */
        BLOCK_0(
                "CN_0_73_TSS_WRAPS/0.blk.gz",
                "3de47629fe289fc7c4c6757b78c90d5ae41dae532d252512854d7db16dd06715adb34ca54c33561f58a4661c2394849f",
                0),
        /** Sequential block 1 (pre-settled Schnorr signature). */
        BLOCK_1(
                "CN_0_73_TSS_WRAPS/1.blk.gz",
                "a08777a11f74ec6c572c0bb72edff5f5ca9830f0cc4738534960ce2167ca46d58ddffc8f52c398170865d7526f96486b",
                1),
        /** Sequential block 2 (pre-settled Schnorr signature). */
        BLOCK_2(
                "CN_0_73_TSS_WRAPS/2.blk.gz",
                "faa4dd0e83e9db4861833a574187d9c538006e33a63cdd25c0f907da317720b21d79ac80952519ea87ef765153051c34",
                2),
        /** Sequential block 3 (pre-settled Schnorr signature). */
        BLOCK_3(
                "CN_0_73_TSS_WRAPS/3.blk.gz",
                "7e06bd1f69e149e3e04e7ee57f723edcab0a84283d0c592ca184d75dedd86aec5eaf61e50b4379adb4a4c90296f73a9b",
                3),
        /** Sequential block 4 (pre-settled Schnorr signature). */
        BLOCK_4(
                "CN_0_73_TSS_WRAPS/4.blk.gz",
                "83181d7d40842495c6bf9a19a5fc93dea992dae4dd95e669e6f4a4bcbf4dcc64fdce15d6b725b8db5ea9f58459ba8919",
                4),
        /** Transition block — first block with WRAPS signature (Schnorr to Wraps transition, oversized ~13MB). */
        BLOCK_466(
                "CN_0_73_TSS_WRAPS/466.blk.gz",
                "ad532f179da5abfc1f982a2a1dbc3d5c0c2e27b47126d559356a03fb656f81c862b3030ba13e1a98242e6390756d9c14",
                466),
        /** Post-settled block — has WRAPS signature (settled TSS). */
        BLOCK_467(
                "CN_0_73_TSS_WRAPS/467.blk.gz",
                "a7986473fa0a42a55a74f04eca352ec7cb6dc3715375500c141f01b1eae01c466f1fc88bd67bf84c84ab80c58fd1918e",
                467);

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
