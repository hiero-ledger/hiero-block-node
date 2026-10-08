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
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.junit.jupiter.params.provider.Arguments;

public class ResourceTestBlockBuilder {
    /// The real captured fixtures, one consecutive chain per proof family, each starting at
    /// block 0. These arrays are the single source of truth for the real-data tests: the hasher
    /// tests hash every block of every chain and the plugin tests feed each chain in order.
    /// The legacy V2/V5 wrapped record blocks are standalone fixtures and not part of a chain.
    public static final WRAPS[] consecutiveWRAPSBlocks = new WRAPS[] {
        WRAPS.BLOCK_0, WRAPS.BLOCK_1, WRAPS.BLOCK_2, WRAPS.BLOCK_3, WRAPS.BLOCK_4, WRAPS.BLOCK_5,
    };
    /// Consecutive wrapped record blocks of the four node Solo network, see [#consecutiveWRAPSBlocks].
    public static final WRB[] consecutiveWRBBlocks = new WRB[] {
        WRB.SOLO_4N_BLOCK_0,
        WRB.SOLO_4N_BLOCK_1,
        WRB.SOLO_4N_BLOCK_2,
        WRB.SOLO_4N_BLOCK_3,
        WRB.SOLO_4N_BLOCK_4,
        WRB.SOLO_4N_BLOCK_5,
    };
    /// Consecutive state proof blocks, see [#consecutiveWRAPSBlocks].
    public static final StateProof[] consecutiveStateProofBlocks = new StateProof[] {
        StateProof.BLOCK_0,
        StateProof.BLOCK_1,
        StateProof.BLOCK_2,
        StateProof.BLOCK_3,
        StateProof.BLOCK_4,
        StateProof.BLOCK_5,
    };

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

    /// Real TSS WRAPS test blocks, the consecutive blocks 0 to 5 of a consensus node capture.
    /// They are the real-data oracle for {@code BlockHasherTest.PositiveBlockHasher} (the "our
    /// hasher matches the real-CN captured hash" invariant) and for
    /// {@code VerificationServicePluginTest.RealBlocksTests}. All other WRAPS tests generate
    /// their blocks via the harness ({@code HarnessChainBuilder}); the E2E lifecycle workflow
    /// generates its blocks at CI time via {@code :block-verification:generateHarnessBlocks}.
    public enum WRAPS implements ResourceBlock {
        /// Genesis block: bootstraps TSS parameters and ledger ID.
        BLOCK_0("CN_11_12_TSS_WRAPS/0.blk.gz", "3c5008ebb8a7b75dea226bc80070de857f0013c4e66cd06ab4f76e005bd41848", 0),
        BLOCK_1("CN_11_12_TSS_WRAPS/1.blk.gz", "d3ccd9f1189828ddb26280a02073c3cddb990c0dd56dca0fb45a7a6f528a17a7", 1),
        BLOCK_2("CN_11_12_TSS_WRAPS/2.blk.gz", "3ecbe0f59d5c4e3c76d90f14c41a680fdad6e526b1182cd2cf127f5e0cb69c98", 2),
        BLOCK_3("CN_11_12_TSS_WRAPS/3.blk.gz", "85ee335f1141be23772617fd40efe5a0931b3ae5b3aeeb4719561a8102a97630", 3),
        BLOCK_4("CN_11_12_TSS_WRAPS/4.blk.gz", "b962cb94bdc10673d620f91fe3bf739c3001cfb7ea84d62ec078a5fcaf65552e", 4),
        BLOCK_5("CN_11_12_TSS_WRAPS/5.blk.gz", "e083ae5de11ba44fe5dc546f7e948de85d031535054d34f4ca9a3c608e6ea337", 5);
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
                "bc3e4d7dfe22ba99d04da01757371cd3458ae414a8fcee25d0bee48f3a5cc05d",
                0,
                "SOLO_4N"),
        SOLO_4N_BLOCK_1(
                "WRB/SOLO_4N/1.blk.gz",
                "f10b060cb824f1f63d6be21a15fbd356c8a21a6a2630aaf7e858200763261b7f",
                1,
                "SOLO_4N"),
        SOLO_4N_BLOCK_2(
                "WRB/SOLO_4N/2.blk.gz",
                "4908e561241ceddcf51c5efdb02a983ec279734b9c74531417f71a9bd138c964",
                2,
                "SOLO_4N"),
        SOLO_4N_BLOCK_3(
                "WRB/SOLO_4N/3.blk.gz",
                "6ee3b7cb4ae6aead865fc0791ea4a3e919ff0c7396af0ca3dddea57b8133048f",
                3,
                "SOLO_4N"),
        SOLO_4N_BLOCK_4(
                "WRB/SOLO_4N/4.blk.gz",
                "1a645f93213adafd7d516e953c8ce4ef5aaea7fd39f89c6ec76f089630b15a1a",
                4,
                "SOLO_4N"),
        SOLO_4N_BLOCK_5(
                "WRB/SOLO_4N/5.blk.gz",
                "3e61ddc29b36fe34611080972c4925636dc5e35dfe2ed0fdb0572cbd0a4625c6",
                5,
                "SOLO_4N"),
        /// Real genesis block wrapped from a v2 record file
        /// (2019-09-13T21_53_51.396440Z) with its original RSA signatures; the address book
        /// carries the genesis-era keys with nodeId = accountNum - 3.
        V2_BLOCK_0(
                "WRB/V2/0.blk.gz",
                "2262876a73c10cdd6c7780723b149d73d5f47196916104b9e34e675d950baef07d429173d48bc845de158ea8e7c0bacc",
                0,
                "V2"),
        /// Real block wrapped from a v5 record file (2022-01-01T00_00_00.252365821Z)
        /// with its original RSA signatures; the address book carries the era keys with
        /// nodeId = accountNum - 3.
        V5_BLOCK_26591040(
                "WRB/V5/26591040.blk.gz",
                "2e23d5e423bbe37ad32917109b0baf92ef5f890ad2c226b7d7e01d4f9c5d90217a2645ba2e832f948b76e200ecd03c70",
                26591040,
                "V5");
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
    /// Every 5th block (0, 5, ...) is directly signed with Schnorr; blocks in between carry
    /// state proofs referencing the next signed block. Block 0 contains
    /// LedgerIdPublicationTransactionBody for TSS initialization.
    public enum StateProof implements ResourceBlock {
        /// Genesis block — bootstraps TSS parameters and ledger ID. Direct Schnorr proof.
        BLOCK_0("CN_11_12_TSS_SCHNORR/0.blk.gz", "45595f0f6fbc1cc9f01a8844b4558446f38961c728ac3e22a60dadd4374d8c5f", 0),
        /// Indirect proof — 4-gap state proof, references signed block 5.
        BLOCK_1("CN_11_12_TSS_SCHNORR/1.blk.gz", "2324591602f1fc8a7e7c2800fd64afab23d1d2129a0462ff6d9d1534fdcb605d", 1),
        /// Indirect proof — 3-gap state proof, references signed block 5.
        BLOCK_2("CN_11_12_TSS_SCHNORR/2.blk.gz", "51359533ee8ecbe67c637f254f6948ea9ff0872217f04ac0b0548f404d2bd3fe", 2),
        /// Indirect proof — 2-gap state proof, references signed block 5.
        BLOCK_3("CN_11_12_TSS_SCHNORR/3.blk.gz", "09dd130bef705f63563d5a5393b409b50d81d8c86a0e134c2bdf954056a30408", 3),
        /// Indirect proof — 1-gap state proof, references signed block 5.
        BLOCK_4("CN_11_12_TSS_SCHNORR/4.blk.gz", "bdf0d0861ebfe21ddc90b32a4d020012ffac50aebf919e8cb7f6df6d272175d3", 4),
        /// Direct Schnorr TSS proof — the signed block referenced by blocks 1-4.
        BLOCK_5("CN_11_12_TSS_SCHNORR/5.blk.gz", "f1f74fc92ee40e71f046a08ddc62a859f8120fd0d4fc7c2f0ac13df2f2f05ad6", 5);
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

    /// One real fixture chain: a display name and its loaded consecutive blocks.
    public record RealBlockChain(String name, List<? extends ResourceTestBlock> blocks) {}

    /// Loads the real fixture chains, one per proof family (see [#consecutiveWRAPSBlocks]).
    /// This is the single definition behind [#realBlockChains()] and [#realBlocks()].
    public static List<RealBlockChain> loadRealBlockChains() throws IOException, ParseException {
        return List.of(
                new RealBlockChain("WRB SOLO_4N", loadMultiple(consecutiveWRBBlocks)),
                new RealBlockChain("StateProof CN_11_12", loadMultiple(consecutiveStateProofBlocks)),
                new RealBlockChain("WRAPS CN_11_12", loadMultiple(consecutiveWRAPSBlocks)));
    }

    /// The real fixture chains as parameterized test arguments: the chain name and the loaded
    /// blocks of one chain per invocation. Tests reference it through `@MethodSource` by the
    /// fully qualified name of this method.
    public static Stream<Arguments> realBlockChains() throws IOException, ParseException {
        return loadRealBlockChains().stream().map(chain -> Arguments.of(chain.name(), chain.blocks()));
    }

    /// Every block of the real fixture chains as parameterized test arguments: a display name
    /// of the form `<chain name> block <number>` and the block, one block per invocation.
    /// Tests reference it through `@MethodSource` by the fully qualified name of this method.
    public static Stream<Arguments> realBlocks() throws IOException, ParseException {
        return loadRealBlockChains().stream().flatMap(chain -> chain.blocks().stream()
                .map(block -> Arguments.of(chain.name() + " block " + block.number(), block)));
    }
}
