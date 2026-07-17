// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import static org.hiero.block.node.app.fixtures.blocks.TestBlock.MAX_BLOCK_MESSAGE_DEPTH;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;

public class ResourceTestBlockBuilder {
    /// A simple interface to define a resource block identifier enum
    public interface ResourceBlock {
        String resourceName();

        Bytes blockRootHash();

        long blockNumber();

        default BlockUnparsed loadBlock() throws IOException, ParseException {
            try (final InputStream stream =
                            TestUtils.class.getModule().getResourceAsStream("test-blocks/" + resourceName());
                    final GZIPInputStream gzipInputStream = new GZIPInputStream(stream)) {
                final byte[] bytes = gzipInputStream.readAllBytes();
                return BlockUnparsed.PROTOBUF.parse(
                        Bytes.wrap(bytes).toReadableSequentialData(),
                        false,
                        true,
                        MAX_BLOCK_MESSAGE_DEPTH,
                        Integer.MAX_VALUE);
            }
        }
    }

    /// TSS WRAPS test blocks.
    public enum WRAPS implements ResourceBlock {
        /// Genesis block — bootstraps TSS parameters and ledger ID.
        BLOCK_0(
                "CN_0_73_TSS_WRAPS/0.blk.gz",
                "3de47629fe289fc7c4c6757b78c90d5ae41dae532d252512854d7db16dd06715adb34ca54c33561f58a4661c2394849f",
                0),
        /// Sequential block 1 (pre-settled Schnorr signature).
        BLOCK_1(
                "CN_0_73_TSS_WRAPS/1.blk.gz",
                "a08777a11f74ec6c572c0bb72edff5f5ca9830f0cc4738534960ce2167ca46d58ddffc8f52c398170865d7526f96486b",
                1),
        /// Sequential block 2 (pre-settled Schnorr signature).
        BLOCK_2(
                "CN_0_73_TSS_WRAPS/2.blk.gz",
                "faa4dd0e83e9db4861833a574187d9c538006e33a63cdd25c0f907da317720b21d79ac80952519ea87ef765153051c34",
                2),
        /// Sequential block 3 (pre-settled Schnorr signature).
        BLOCK_3(
                "CN_0_73_TSS_WRAPS/3.blk.gz",
                "7e06bd1f69e149e3e04e7ee57f723edcab0a84283d0c592ca184d75dedd86aec5eaf61e50b4379adb4a4c90296f73a9b",
                3),
        /// Sequential block 4 (pre-settled Schnorr signature).
        BLOCK_4(
                "CN_0_73_TSS_WRAPS/4.blk.gz",
                "83181d7d40842495c6bf9a19a5fc93dea992dae4dd95e669e6f4a4bcbf4dcc64fdce15d6b725b8db5ea9f58459ba8919",
                4),
        /// Transition block — first block with WRAPS signature (Schnorr to Wraps transition, oversized ~13MB).
        BLOCK_466(
                "CN_0_73_TSS_WRAPS/466.blk.gz",
                "ad532f179da5abfc1f982a2a1dbc3d5c0c2e27b47126d559356a03fb656f81c862b3030ba13e1a98242e6390756d9c14",
                466),
        /// Post-settled block — has WRAPS signature (settled TSS).
        BLOCK_467(
                "CN_0_73_TSS_WRAPS/467.blk.gz",
                "a7986473fa0a42a55a74f04eca352ec7cb6dc3715375500c141f01b1eae01c466f1fc88bd67bf84c84ab80c58fd1918e",
                467);
        private final String resourceName;
        private final Bytes blockRootHash;
        private final long blockNumber;

        WRAPS(final String resourceName, final String blockRootHash, final long blockNumber) {
            this.resourceName = resourceName;
            this.blockRootHash = Bytes.fromHex(blockRootHash);
            this.blockNumber = blockNumber;
        }

        @Override
        public String resourceName() {
            return resourceName;
        }

        @Override
        public Bytes blockRootHash() {
            return blockRootHash;
        }

        @Override
        public long blockNumber() {
            return blockNumber;
        }
    }

    /// Sample wrapped record blocks (WRB) for the V6 `SignedRecordFileProof` verification path.
    /// Each constant maps to a `test-blocks/WRB/<network>/<blockNumber>.blk.gz` resource and
    /// carries the name of the network folder so callers can fetch the matching
    /// [NodeAddressBook] via [#loadAddressBook(String)].
    public enum WRB implements ResourceBlock {
        /// Solo-network genesis WRB block — the only one in this batch containing tss-init metadata.
        SOLO_4N_BLOCK_0(
                "WRB/SOLO_4N/0.blk.gz",
                "52ad6e3386c1ba1c976cb0c1abd348becefaf3541fc0bb7ea5d4fa19dbe596d16cbbb4899328081b0eec6d88befb1340",
                0,
                "SOLO_4N"),
        SOLO_4N_BLOCK_1(
                "WRB/SOLO_4N/1.blk.gz",
                "8b420d35ba2f564444fcea007a6b74a3f3564c2da670ef059fa51f2e54397db8716803222abf0f3dce9f8b66218c6705",
                1,
                "SOLO_4N"),
        SOLO_4N_BLOCK_2(
                "WRB/SOLO_4N/2.blk.gz",
                "a35667c3417ea24fcd1aeb7272d6094bc8a3d2b2ed04ee1cb7050bede41e29231eeb12b74cc85e4ba04a63bbc1ff04ff",
                2,
                "SOLO_4N"),
        SOLO_4N_BLOCK_3(
                "WRB/SOLO_4N/3.blk.gz",
                "19b193e72f90d86bd3b848e17208a08567114f09d19479c1b43231751b238db99d0547b484469183948192216da3eb25",
                3,
                "SOLO_4N"),
        SOLO_4N_BLOCK_4(
                "WRB/SOLO_4N/4.blk.gz",
                "dcf7b335327e738b7cb98461af62f42c56b76b0b51be363f340f1edd22362eeb5eba93522f433113586bc8be4720dbd6",
                4,
                "SOLO_4N");
        private final String resourceName;
        private final Bytes blockRootHash;
        private final long blockNumber;
        private final String network;

        WRB(final String resourceName, final String blockRootHash, final long blockNumber, final String network) {
            this.resourceName = resourceName;
            this.blockRootHash = Bytes.fromHex(blockRootHash);
            this.blockNumber = blockNumber;
            this.network = network;
        }

        @Override
        public String resourceName() {
            return resourceName;
        }

        @Override
        public Bytes blockRootHash() {
            return blockRootHash;
        }

        @Override
        public long blockNumber() {
            return blockNumber;
        }

        /// Network folder for this fixture (also the `<network>` argument to [#loadAddressBook(String)]).
        public String network() {
            return network;
        }

        /// Load the address book based on network.
        public NodeAddressBook loadAddressBook() throws IOException, ParseException {
            final String resourcePath = "test-blocks/WRB/" + network + "/address-book.json";
            try (final InputStream stream = TestUtils.class.getModule().getResourceAsStream(resourcePath)) {
                if (stream == null) {
                    throw new IOException("Address book fixture not found on classpath: " + resourcePath);
                }
                return NodeAddressBook.JSON.parse(Bytes.wrap(stream.readAllBytes()));
            }
        }
    }

    /// Sample blocks containing state proofs from a hapiTestWraps capture with Schnorr TSS signatures.
    /// Every 4th block (0, 4, ...) is directly signed; blocks in between have state proofs.
    /// Block 0 contains LedgerIdPublicationTransactionBody for TSS initialization.
    public enum StateProof implements ResourceBlock {
        /// Genesis block — bootstraps TSS parameters and ledger ID. Direct Schnorr proof.
        BLOCK_0(
                "CN_0_77_TSS_SCHNORR/0.blk.gz",
                "bfeea5194985638c8dfd3fd56ef8627ff65982fae48a2f4c29eda4accd400b5d2c0414a8496770de531903b7d781522d",
                0),
        /// Indirect proof — 3-gap state proof (15 siblings), references signed block 4.
        BLOCK_1(
                "CN_0_77_TSS_SCHNORR/1.blk.gz",
                "e2c09c05295e4e73133e7d59b8374aab9f274d0a5dfb0d5e5e9a76487cf71443b56525664cf044f3a893145744067f86",
                1),
        /// Indirect proof — 2-gap state proof (11 siblings), references signed block 4.
        BLOCK_2(
                "CN_0_77_TSS_SCHNORR/2.blk.gz",
                "9d25ef1473fe5665ff1c8ac1ceab8f0d77c5d0e5030cf93980ea8dcef21fa73be0bdd5f0deb0fe2d5e5658ad30a50b64",
                2),
        /// Indirect proof — 1-gap state proof (7 siblings), references signed block 4.
        BLOCK_3(
                "CN_0_77_TSS_SCHNORR/3.blk.gz",
                "8ced3ee04edad4c4eae5955523c9f00cc3c6712a47491bd8521d43a67ff3ebacbced21bf45dd31de3128ecdb1ac6db36",
                3),
        /// Direct Schnorr TSS proof — the signed block referenced by blocks 1-3.
        BLOCK_4(
                "CN_0_77_TSS_SCHNORR/4.blk.gz",
                "5a6dda11d4d05184a1eda31db0c9a798ff465c7f551f6454f2bd739ee8e141539652170a737b21d7bfef31bbb86ce54a",
                4);
        private final String resourceName;
        private final Bytes blockRootHash;
        private final long blockNumber;

        StateProof(final String resourceName, final String blockRootHash, final long blockNumber) {
            this.resourceName = resourceName;
            this.blockRootHash = Bytes.fromHex(blockRootHash);
            this.blockNumber = blockNumber;
        }

        @Override
        public String resourceName() {
            return resourceName;
        }

        @Override
        public Bytes blockRootHash() {
            return blockRootHash;
        }

        @Override
        public long blockNumber() {
            return blockNumber;
        }
    }

    public static ResourceTestBlock load(final WRAPS wrapsBlock) throws IOException, ParseException {
        return new ResourceTestBlock(wrapsBlock.blockNumber(), wrapsBlock.loadBlock(), wrapsBlock.blockRootHash());
    }

    public static List<ResourceTestBlock> loadMultiple(final WRAPS... wrapsBlocks) throws IOException, ParseException {
        final List<ResourceTestBlock> result = new ArrayList<>();
        for (final WRAPS wrapsBlock : wrapsBlocks) {
            result.add(load(wrapsBlock));
        }
        return result;
    }

    public static ResourceTestWRBBlock load(final WRB wrbBlock) throws IOException, ParseException {
        return new ResourceTestWRBBlock(
                wrbBlock.blockNumber(), wrbBlock.loadBlock(), wrbBlock.blockRootHash(), wrbBlock.loadAddressBook());
    }

    public static List<ResourceTestWRBBlock> loadMultiple(final WRB... wrbBlocks) throws IOException, ParseException {
        final List<ResourceTestWRBBlock> result = new ArrayList<>();
        for (final WRB wrbBlock : wrbBlocks) {
            result.add(load(wrbBlock));
        }
        return result;
    }

    public static ResourceTestBlock load(final StateProof stateProofBlock) throws IOException, ParseException {
        return new ResourceTestBlock(
                stateProofBlock.blockNumber(), stateProofBlock.loadBlock(), stateProofBlock.blockRootHash());
    }

    public static List<ResourceTestBlock> loadMultiple(final StateProof... stateProofBlocks)
            throws IOException, ParseException {
        final List<ResourceTestBlock> result = new ArrayList<>();
        for (final StateProof stateProofBlock : stateProofBlocks) {
            result.add(load(stateProofBlock));
        }
        return result;
    }
}
