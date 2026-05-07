// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.VerificationServicePlugin;
import org.hiero.block.node.verification.session.impl.ExtendedMerkleTreeSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestVerificationHapiVersions {

    @BeforeAll
    static void bootstrapTssState() throws IOException, ParseException {
        // Process block 0 to initialize TSS static state so non-genesis blocks can verify
        VerificationServicePlugin.activeLedgerId = null;
        VerificationServicePlugin.activeTssPublication = null;
        VerificationServicePlugin.tssParametersPersisted = false;
        BlockUtils.SampleBlockInfo block0 = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_0);
        List<BlockItemUnparsed> items = block0.blockUnparsed().blockItems();
        long blockNumber = BlockHeader.PROTOBUF
                .parse(items.getFirst().blockHeaderOrThrow())
                .number();
        ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                blockNumber, BlockSource.UNKNOWN, null, null, null, Map.of(), null, null, null);
        session.processBlockItems(new BlockItems(items, blockNumber, true, true));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("sampleBlocks")
    @DisplayName("Verify HAPI version compatibility with real blocks")
    @Timeout(value = 10) // seconds; avoid accidental hangs
    void verifyHapiVersionCompatibility(final String sampleName, final BlockUtils.SampleBlockInfo sampleBlockInfo)
            throws ParseException {

        final List<BlockItemUnparsed> blockItems =
                sampleBlockInfo.blockUnparsed().blockItems();

        // Quick sanity checks so failures are obvious
        assertNotNull(blockItems, sampleName + ": blockItems should not be null");
        assertFalse(blockItems.isEmpty(), sampleName + ": blockItems should not be empty");

        final BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeaderOrThrow());
        final long blockNumber = blockHeader.number();

        // If session creation or processing ever regresses, we want fast, high-signal failures
        final VerificationSession session = assertTimeoutPreemptively(
                Duration.ofSeconds(2),
                () -> HapiVersionSessionFactory.createSession(
                        blockNumber,
                        BlockSource.UNKNOWN,
                        blockHeader.hapiProtoVersion(),
                        null,
                        null,
                        VerificationServicePlugin.activeLedgerId),
                sampleName + ": creating verification session exceeded time budget");

        final VerificationNotification note = assertTimeoutPreemptively(
                Duration.ofSeconds(3),
                () -> {
                    BlockItems blockItemsMessage = new BlockItems(blockItems, blockNumber, true, true);
                    return session.processBlockItems(blockItemsMessage);
                },
                sampleName + ": processing block items exceeded time budget");

        // Basic assertions that apply to all verification sessions
        assertNotNull(note, sampleName + ": Verification notification should not be null");
        assertEquals(blockNumber, note.blockNumber(), sampleName + ": Block number should match");
        assertTrue(note.success(), sampleName + ": Verification should be successful");

        assertEquals(sampleBlockInfo.blockRootHash(), note.blockHash(), sampleName + ": Block hash should match");
    }

    /** Supply the concrete samples you want to cover. Add more here as they're added to fixtures. */
    private static Stream<Arguments> sampleBlocks() throws IOException, ParseException {
        final BlockUtils.SampleBlockInfo block0 = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_0);

        final BlockUtils.SampleBlockInfo block1 = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_1);

        final BlockUtils.SampleBlockInfo block4 = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_4);

        return Stream.of(
                Arguments.of("CN_0_73_BLOCK_0", block0),
                Arguments.of("CN_0_73_BLOCK_1", block1),
                Arguments.of("CN_0_73_BLOCK_4", block4));
    }
}
