// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.Codec;
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
     * Loads the RSA {@link NodeAddressBook} fixture for a WRB sample-block network.  Looks up
     * {@code test-blocks/WRB/<network>/address-book.json} on the test classpath and parses it
     * with the PBJ JSON codec.  See {@code WrbAddressBookFixtureGeneratorTest} for how this
     * file is produced from {@code genesis-network.json}.
     *
     * @param network the WRB network folder name, e.g. {@code "SOLO_4N"}
     * @return the parsed address book
     */
    public static NodeAddressBook getSampleAddressBook(String network) throws IOException, ParseException {
        final String resourcePath = "test-blocks/WRB/" + network + "/address-book.json";
        try (InputStream stream = TestUtils.class.getModule().getResourceAsStream(resourcePath)) {
            if (stream == null) {
                throw new IOException("Address book fixture not found on classpath: " + resourcePath);
            }
            return NodeAddressBook.JSON.parse(Bytes.wrap(stream.readAllBytes()));
        }
    }

    /**
     * Gets a SampleBlockInfo from any sample block enum.
     */
    public static SampleBlockInfo getSampleBlockInfo(SampleBlock sampleBlock) throws IOException, ParseException {
        BlockUnparsed blockUnparsed;
        try (InputStream stream =
                        TestUtils.class.getModule().getResourceAsStream("test-blocks/" + sampleBlock.getBlockName());
                final GZIPInputStream gzipInputStream = new GZIPInputStream(stream)) {
            byte[] bytes = gzipInputStream.readAllBytes();
            blockUnparsed = BlockUnparsed.PROTOBUF.parse(
                    Bytes.wrap(bytes).toReadableSequentialData(),
                    false,
                    false,
                    Codec.DEFAULT_MAX_DEPTH,
                    BlockAccessor.MAX_BLOCK_SIZE_BYTES);
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
     * Sample wrapped record blocks (WRB) for the V6 {@code SignedRecordFileProof} verification path.
     * Each constant maps to a {@code test-blocks/WRB/<network>/<blockNumber>.blk.gz} resource and
     * carries the name of the network folder so callers can fetch the matching
     * {@link NodeAddressBook} via {@link #getSampleAddressBook(String)}.
     *
     * <p>Hashes are intentionally empty: the integration tests that consume these blocks assert that
     * the verifier emits a non-null block hash, but not its specific value. Populate them via
     * {@code WrbAddressBookFixtureGeneratorTest} if a strict-hash assertion is needed later.
     *
     * <p>Only solo-network blocks are wired in right now. The {@code v6-block.blk.gz} sample under
     * {@code tools-and-tests/.../record-files/wrb/} was evaluated as a second source but its block
     * header carries HAPI 0.63.x, below the 0.72.0 minimum at which {@code HapiVersionSessionFactory}
     * dispatches to the WRB-capable {@code ExtendedMerkleTreeSession}. Add it back once the
     * verifier accepts older HAPI versions, or once a newer mainnet WRB capture is available.
     */
    public enum SAMPLE_BLOCKS_WRB implements SampleBlock {
        /** Solo-network genesis WRB block — the only one in this batch containing tss-init metadata. */
        SOLO_4N_BLOCK_0("WRB/SOLO_4N/0.blk.gz", "", 0, "SOLO_4N"),
        SOLO_4N_BLOCK_1("WRB/SOLO_4N/1.blk.gz", "", 1, "SOLO_4N"),
        SOLO_4N_BLOCK_2("WRB/SOLO_4N/2.blk.gz", "", 2, "SOLO_4N"),
        SOLO_4N_BLOCK_3("WRB/SOLO_4N/3.blk.gz", "", 3, "SOLO_4N"),
        SOLO_4N_BLOCK_4("WRB/SOLO_4N/4.blk.gz", "", 4, "SOLO_4N");

        private final String blockName;
        private final Bytes blockHash;
        private final long blockNumber;
        private final String network;

        SAMPLE_BLOCKS_WRB(String blockName, String blockHash, long blockNumber, String network) {
            this.blockName = blockName;
            this.blockHash = Bytes.fromHex(blockHash);
            this.blockNumber = blockNumber;
            this.network = network;
        }

        @Override
        public String getBlockName() {
            return blockName;
        }

        @Override
        public Bytes getBlockHash() {
            return blockHash;
        }

        @Override
        public long getBlockNumber() {
            return blockNumber;
        }

        /** Network folder for this fixture (also the {@code <network>} argument to {@link #getSampleAddressBook(String)}). */
        public String network() {
            return network;
        }
    }

    /**
     * Sample blocks containing state proofs from a hapiTestWraps capture with Schnorr TSS signatures.
     * Every 4th block (0, 4, ...) is directly signed; blocks in between have state proofs.
     * Block 0 contains LedgerIdPublicationTransactionBody for TSS initialization.
     */
    public enum SAMPLE_BLOCKS_STATE_PROOFS implements SampleBlock {
        /** Genesis block — bootstraps TSS parameters and ledger ID. Direct Schnorr proof. */
        BLOCK_0(
                "CN_0_73_TSS_SCHNORR/0.blk.gz",
                "abca002469f5a59badf0634f8e759895891c65a0df3c3c7f9b78ef0920bc53670e9c5da867ab20d95a4bafb7a192e7f8",
                0),
        /** Indirect proof — 3-gap state proof (15 siblings), references signed block 4. */
        BLOCK_1(
                "CN_0_73_TSS_SCHNORR/1.blk.gz",
                "16be13db66b0ee4b8b5c1f1173eb081fe267197e219273bc501a293c51eedd4a301edd9db12a452e23a874eab71b32d5",
                1),
        /** Indirect proof — 2-gap state proof (11 siblings), references signed block 4. */
        BLOCK_2(
                "CN_0_73_TSS_SCHNORR/2.blk.gz",
                "ebb69bd6d03e4a152baf68f2bdd3cebb0bf90fbe5d3b7e6f0f4134f043945355f170cc162a352367230a377312e4efc8",
                2),
        /** Indirect proof — 1-gap state proof (7 siblings), references signed block 4. */
        BLOCK_3(
                "CN_0_73_TSS_SCHNORR/3.blk.gz",
                "ab58d1104bb0a09b2562927b5dbd46e47c2e9962f6e0938de529babe43077636e8ee3f67473be490c28e781998294a33",
                3),
        /** Direct Schnorr TSS proof — the signed block referenced by blocks 1-3. */
        BLOCK_4(
                "CN_0_73_TSS_SCHNORR/4.blk.gz",
                "fedfb3dafcb18673938f71da192efdf246cfb1e6e80016aba18cc97a09484f610acc087ae1f468d46e34dd00e92dcfc0",
                4);

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
