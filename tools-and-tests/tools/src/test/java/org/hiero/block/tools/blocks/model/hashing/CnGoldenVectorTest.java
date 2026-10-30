// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.hiero.block.node.base.ParseHelper.standardParse;
import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Golden vectors for the CLI's SHA-256 block hashing, taken from blocks produced by a consensus node.
 *
 * <p>The blocks are the CN-captured chains of the {@code ci-test-blocks} archive (fetched by {@code fetchTestBlocks}
 * in the root build), the same files the block node tests use. The expected roots are the constants in
 * {@code ResourceTestBlockBuilder} ({@code WRB.SOLO_4N_BLOCK_0..5} and {@code WRAPS.BLOCK_0..5}), repeated here so the
 * tools module stays independent of block node test fixture code.
 *
 * <p>{@code startOfBlockStateRootHash} is not checked: CN blocks carry a real state hash, while the CLI has no state
 * and writes {@link HashingUtils#EMPTY_TREE_HASH} on purpose.
 */
class CnGoldenVectorTest {
    /** Expected block root hashes of the wrb/v6/varied/local-solo-4n chain, blocks 0 to 5 */
    private static final List<String> WRB_SOLO_4N_ROOTS = List.of(
            "bc3e4d7dfe22ba99d04da01757371cd3458ae414a8fcee25d0bee48f3a5cc05d",
            "f10b060cb824f1f63d6be21a15fbd356c8a21a6a2630aaf7e858200763261b7f",
            "4908e561241ceddcf51c5efdb02a983ec279734b9c74531417f71a9bd138c964",
            "6ee3b7cb4ae6aead865fc0791ea4a3e919ff0c7396af0ca3dddea57b8133048f",
            "1a645f93213adafd7d516e953c8ce4ef5aaea7fd39f89c6ec76f089630b15a1a",
            "3e61ddc29b36fe34611080972c4925636dc5e35dfe2ed0fdb0572cbd0a4625c6");
    /** Expected block root hashes of the tss/direct/varied/local-cn-11-12 chain, blocks 0 to 5 */
    private static final List<String> CN_11_12_TSS_WRAPS_ROOTS = List.of(
            "3c5008ebb8a7b75dea226bc80070de857f0013c4e66cd06ab4f76e005bd41848",
            "d3ccd9f1189828ddb26280a02073c3cddb990c0dd56dca0fb45a7a6f528a17a7",
            "3ecbe0f59d5c4e3c76d90f14c41a680fdad6e526b1182cd2cf127f5e0cb69c98",
            "85ee335f1141be23772617fd40efe5a0931b3ae5b3aeeb4719561a8102a97630",
            "b962cb94bdc10673d620f91fe3bf739c3001cfb7ea84d62ec078a5fcaf65552e",
            "e083ae5de11ba44fe5dc546f7e948de85d031535054d34f4ca9a3c608e6ea337");

    /** The CN chains (set folders in the test blocks archive), each with its expected block root hashes */
    private static Stream<Arguments> cnChains() {
        return Stream.of(
                Arguments.of("wrb/v6/varied/local-solo-4n", WRB_SOLO_4N_ROOTS),
                Arguments.of("tss/direct/varied/local-cn-11-12", CN_11_12_TSS_WRAPS_ROOTS));
    }

    /**
     * Hashes each block of a CN chain and checks the root, the previous block link and the all-blocks tree root in
     * the footer. Block 0 is the genesis block, so its footer also shows the CN's genesis conventions, which must be
     * the ones the CLI writes: {@link HashingUtils#EMPTY_TREE_HASH} for both the previous block hash and the root of
     * the (empty) all-blocks tree.
     */
    @ParameterizedTest
    @MethodSource("cnChains")
    @DisplayName("CLI hashing matches the CN block roots and footer links")
    void cliHashingMatchesCnChain(final String chain, final List<String> expectedRoots) throws Exception {
        final StreamingHasher allBlocksHasher = new StreamingHasher();
        Bytes previousRoot = Bytes.wrap(EMPTY_TREE_HASH);
        for (int blockNumber = 0; blockNumber < expectedRoots.size(); blockNumber++) {
            final BlockUnparsed block = loadBlock(chain, blockNumber);
            final BlockFooter footer = footerOf(block);
            final String label = chain + " block " + blockNumber;

            assertEquals(previousRoot, footer.previousBlockRootHash(), label + " previousBlockRootHash");
            assertEquals(
                    Bytes.wrap(allBlocksHasher.computeRootHash()),
                    footer.rootHashOfAllBlockHashesTree(),
                    label + " rootHashOfAllBlockHashesTree");

            final Bytes root = Bytes.wrap(BlockStreamBlockHasher.hashBlock(block));
            assertEquals(Bytes.fromHex(expectedRoots.get(blockNumber)), root, label + " root hash");

            allBlocksHasher.addNodeByHash(root.toByteArray());
            previousRoot = root;
        }
    }

    /**
     * The empty tree hash is SHA-256 of a single {@code 0x00} byte. The expected value is the block node's (the
     * {@code SHA2_256} constant in {@code common}'s {@code HashAlgorithm}), not a CN capture; the CN agreement is
     * shown by the genesis footers in {@link #cliHashingMatchesCnChain(String, List)}.
     */
    @Test
    @DisplayName("EMPTY_TREE_HASH matches the block node's SHA-256 empty tree hash")
    void emptyTreeHashMatchesBlockNode() {
        assertEquals(
                "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
                HexFormat.of().formatHex(EMPTY_TREE_HASH));
    }

    /**
     * Loads one gzipped block of a CN chain from the test blocks directory.
     *
     * @param chain the set folder in the test blocks archive
     * @param blockNumber the block number, which is also the file name
     * @return the unparsed block
     */
    private static BlockUnparsed loadBlock(final String chain, final int blockNumber) throws Exception {
        final String dir = System.getProperty("hiero.testBlocks.dir");
        assertNotNull(dir, "System property hiero.testBlocks.dir not set; run tests through Gradle (fetchTestBlocks)");
        final Path file = Path.of(dir, chain, blockNumber + ".blk.gz");
        try (InputStream in = Files.newInputStream(file);
                GZIPInputStream gzipIn = new GZIPInputStream(in)) {
            return standardParse(BlockUnparsed.PROTOBUF, Bytes.wrap(gzipIn.readAllBytes()), Integer.MAX_VALUE);
        }
    }

    /**
     * Parses the first block footer of a block, failing the test if there is none.
     *
     * @param block the block to read the footer from
     * @return the parsed footer
     */
    private static BlockFooter footerOf(final BlockUnparsed block) throws Exception {
        BlockFooter footer = null;
        for (final BlockItemUnparsed item : block.blockItems()) {
            if (footer == null && item.hasBlockFooter()) {
                footer = standardParse(BlockFooter.PROTOBUF, item.blockFooterOrThrow());
            }
        }
        assertNotNull(footer, "Block has no footer");
        return footer;
    }
}
