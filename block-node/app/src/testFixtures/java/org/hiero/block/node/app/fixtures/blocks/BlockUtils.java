// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import static org.hiero.block.node.base.ParseHelper.standardParse;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.node.base.NodeAddressBook;
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
            return standardParse(NodeAddressBook.JSON, Bytes.wrap(stream.readAllBytes()));
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
                "CN_11_12_TSS_WRAPS/0.blk.gz",
                "3c421aac698b04fccdd46acad435125ea53af91fe404e787c8ccf4c23a11aa69190468ea1dd797af11d75335f4840749",
                0),
        /**
         * Transition block — the first block with a WRAPS (post-settled) signature after the
         * Schnorr → WRAPS TSS transition. Kept for {@code BackfillPluginTest.testBackfillOnDemandTssWrapsBlock}
         * which specifically exercises backfill of a transition block; the harness can't
         * synthesize the transition boundary.
         */
        BLOCK_322(
                "CN_11_12_TSS_WRAPS/322.blk.gz",
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
        SOLO_4N_BLOCK_0(
                "WRB/SOLO_4N/0.blk.gz",
                "cc2c7c849e9863458d2deb779dcca9563bec6ef4943b5709d8623803af78f9372d5e38ce8f9ae594fedc74909aece65d",
                0,
                "SOLO_4N"),
        SOLO_4N_BLOCK_1(
                "WRB/SOLO_4N/1.blk.gz",
                "8b70cecb8deabc57acb12fa8696bcc27e696b7b1ede3febc2e3f0aaeef422bd55a043b319f326cbecbd886852f392b7a",
                1,
                "SOLO_4N"),
        SOLO_4N_BLOCK_2(
                "WRB/SOLO_4N/2.blk.gz",
                "6cfb9066c335aebe27563d8b7ffc2fae0baad4898880e82753827a9147b3b63455bc5fc7b794adeda0810198d717ee30",
                2,
                "SOLO_4N"),
        SOLO_4N_BLOCK_3(
                "WRB/SOLO_4N/3.blk.gz",
                "6204e2c36e154b152d43e9e5322346ef4691080d70bb589c8609bef12f63e4ccc1cfa386a27ab66a16c80679f368ac9f",
                3,
                "SOLO_4N"),
        SOLO_4N_BLOCK_4(
                "WRB/SOLO_4N/4.blk.gz",
                "32806f4d9977cf41ba2750e9b4254545c19870ff01a1783654b50429b201f2a310bae9cc549821f263935454de88bc43",
                4,
                "SOLO_4N");

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
     * Sample blocks containing state proofs from a Schnorr TSS capture. Every 5th block
     * (0, 5, ...) is directly signed; blocks in between have state proofs referencing the
     * next signed block. Block 0 contains LedgerIdPublicationTransactionBody for TSS
     * initialization. Same fixture files as ResourceTestBlockBuilder.StateProof.
     */
    public enum SAMPLE_BLOCKS_STATE_PROOFS implements SampleBlock {
        /** Genesis block — bootstraps TSS parameters and ledger ID. Direct Schnorr proof. */
        BLOCK_0(
                "CN_11_12_TSS_SCHNORR/0.blk.gz",
                "85d7117a94f091156a97e1f94fb11b640c12431527106e50aa7ce8e4e02af287f3f28591dc43d4c3942a67e7064e4148",
                0),
        /** Indirect proof — 4-gap state proof, references signed block 5. */
        BLOCK_1(
                "CN_11_12_TSS_SCHNORR/1.blk.gz",
                "9799848eff3420617d5c2eb8e092f903bde6de8662887105f3bd17363d7c1cd3629814f9fc58a9d8f5e1e6ed3fdd53c1",
                1),
        /** Indirect proof — 3-gap state proof, references signed block 5. */
        BLOCK_2(
                "CN_11_12_TSS_SCHNORR/2.blk.gz",
                "0facbf0e5d94d4576635796c6be83f2d16203e6be633e475b70942f2b22c880944c156e1badcd27be96ffc7ee7b7e194",
                2),
        /** Indirect proof — 2-gap state proof, references signed block 5. */
        BLOCK_3(
                "CN_11_12_TSS_SCHNORR/3.blk.gz",
                "9d1ac9ba0c57558ab99691f97414d19698e068ff6d600502c3896fccfe3f58c261515f00dc3c116107961385be9f8eec",
                3),
        /** Indirect proof — 1-gap state proof, references signed block 5. */
        BLOCK_4(
                "CN_11_12_TSS_SCHNORR/4.blk.gz",
                "e313796875ef100613684c0ee4ef1a80a13b73aa9c97f5f9592f9b9a25c798b33e36e397fa0e1456b17cee2155b8d7ac",
                4),
        /** Direct Schnorr TSS proof — the signed block referenced by blocks 1-4. */
        BLOCK_5(
                "CN_11_12_TSS_SCHNORR/5.blk.gz",
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
