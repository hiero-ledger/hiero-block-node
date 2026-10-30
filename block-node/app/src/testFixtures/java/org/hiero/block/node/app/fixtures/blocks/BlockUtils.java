// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import static org.hiero.block.node.base.ParseHelper.standardParse;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.zip.GZIPInputStream;
import org.hiero.block.internal.BlockUnparsed;

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
            return standardParse(BlockUnparsed.PROTOBUF, Block.PROTOBUF.toBytes(block));
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
            return standardParse(Block.PROTOBUF, BlockUnparsed.PROTOBUF.toBytes(block));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets a SampleBlockInfo from any sample block enum.
     */
    public static SampleBlockInfo getSampleBlockInfo(SampleBlock sampleBlock) throws IOException, ParseException {
        BlockUnparsed blockUnparsed;
        try (InputStream stream = Files.newInputStream(
                        ResourceTestBlockBuilder.testBlocksRoot().resolve(sampleBlock.getBlockName()));
                final GZIPInputStream gzipInputStream = new GZIPInputStream(stream)) {
            byte[] bytes = gzipInputStream.readAllBytes();
            blockUnparsed = standardParse(BlockUnparsed.PROTOBUF, Bytes.wrap(bytes), Integer.MAX_VALUE);
        }

        return new SampleBlockInfo(sampleBlock.getBlockHash(), sampleBlock.getBlockNumber(), blockUnparsed);
    }

    /**
     * SampleBlockInfo is a simple record that contains the block root hash, block number, and BlockUnparsed object for convenience
     * */
    public record SampleBlockInfo(Bytes blockRootHash, Long blockNumber, BlockUnparsed blockUnparsed) {}

    /**
     * Common interface for sample block enums providing block metadata for test fixtures.
     */
    public interface SampleBlock {
        String getBlockName();

        Bytes getBlockHash();

        long getBlockNumber();
    }

    /**
     * Sample blocks for testing.
     * These blocks are used for testing purposes only.
     */
    public enum SAMPLE_BLOCKS implements SampleBlock {
        /** Genesis block — canary for BlockHasherTest.PositiveBlockHasher only. */
        BLOCK_0(
                "tss/direct/varied/local-cn-11-12/0.blk.gz",
                "3c421aac698b04fccdd46acad435125ea53af91fe404e787c8ccf4c23a11aa69190468ea1dd797af11d75335f4840749",
                0),
        /**
         * A large (31 MB) real block with a TSS signature from a TSS library before
         * hedera-cryptography 3.17. Kept for the backfill and block access tests that need a large real block
         * without verifying its proof; the harness cannot synthesize a block of this size.
         */
        BLOCK_322(
                "tss/direct/varied/local-cn-11-12-sha2-384/322.blk.gz",
                "15c9a3e7d027c112e3378a1a66277e5b1e3d9b1aa4ec67749ad3ef0e4d99859e0784672c8c5f838717a5da8e6fcbf67f",
                322);

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
     * Sample blocks containing state proofs from a TSS capture made with a TSS library before
     * hedera-cryptography 3.17. Every 5th block (0, 5, ...) is directly signed;
     * blocks in between have state proofs referencing the next signed block. Block 0 contains
     * LedgerIdPublicationTransactionBody for TSS initialization. Same fixture files as
     * ResourceTestBlockBuilder.StateProof.
     */
    public enum SAMPLE_BLOCKS_STATE_PROOFS implements SampleBlock {
        /** Genesis block — bootstraps TSS parameters and ledger ID. Direct Schnorr proof. */
        BLOCK_0(
                "tss/indirect/varied/local-cn-11-12/0.blk.gz",
                "85d7117a94f091156a97e1f94fb11b640c12431527106e50aa7ce8e4e02af287f3f28591dc43d4c3942a67e7064e4148",
                0),
        /** Indirect proof — 4-gap state proof, references signed block 5. */
        BLOCK_1(
                "tss/indirect/varied/local-cn-11-12/1.blk.gz",
                "9799848eff3420617d5c2eb8e092f903bde6de8662887105f3bd17363d7c1cd3629814f9fc58a9d8f5e1e6ed3fdd53c1",
                1),
        /** Indirect proof — 3-gap state proof, references signed block 5. */
        BLOCK_2(
                "tss/indirect/varied/local-cn-11-12/2.blk.gz",
                "0facbf0e5d94d4576635796c6be83f2d16203e6be633e475b70942f2b22c880944c156e1badcd27be96ffc7ee7b7e194",
                2),
        /** Indirect proof — 2-gap state proof, references signed block 5. */
        BLOCK_3(
                "tss/indirect/varied/local-cn-11-12/3.blk.gz",
                "9d1ac9ba0c57558ab99691f97414d19698e068ff6d600502c3896fccfe3f58c261515f00dc3c116107961385be9f8eec",
                3),
        /** Indirect proof — 1-gap state proof, references signed block 5. */
        BLOCK_4(
                "tss/indirect/varied/local-cn-11-12/4.blk.gz",
                "e313796875ef100613684c0ee4ef1a80a13b73aa9c97f5f9592f9b9a25c798b33e36e397fa0e1456b17cee2155b8d7ac",
                4),
        /** Direct Schnorr TSS proof — the signed block referenced by blocks 1-4. */
        BLOCK_5(
                "tss/indirect/varied/local-cn-11-12/5.blk.gz",
                "1c417b370965dd85e274b568694d0a5b3325b39f619a840df0161339864a30e389ba3aec4c7763236d2f4c505d031921",
                5);

        private final String blockName;
        private final Bytes blockHash;
        private final long blockNumber;

        SAMPLE_BLOCKS_STATE_PROOFS(String blockName, String blockHash, long blockNumber) {
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
