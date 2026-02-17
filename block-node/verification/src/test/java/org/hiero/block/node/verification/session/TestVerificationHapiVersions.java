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
import java.util.stream.Stream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.session.impl.DummyVerificationSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestVerificationHapiVersions {

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
                        blockNumber, BlockSource.UNKNOWN, blockHeader.hapiProtoVersion(), null, null),
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

        // DummyVerificationSession returns a dummy hash (0x00), so skip hash comparison for it
        if (!(session instanceof DummyVerificationSession)) {
            assertEquals(sampleBlockInfo.blockRootHash(), note.blockHash(), sampleName + ": Block hash should match");
        }
    }

    /** Supply the concrete samples you want to cover. Add more here as they're added to fixtures. */
    private static Stream<Arguments> sampleBlocks() throws IOException, ParseException {
        // Use readable case names to make failures obvious in the parameterized display
        final BlockUtils.SampleBlockInfo s1 =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_68_0_BLOCK_14);

        final BlockUtils.SampleBlockInfo s2 =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_66_0_BLOCK_10);

        final BlockUtils.SampleBlockInfo s3 =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_69_0_BLOCK_240);

        final BlockUtils.SampleBlockInfo s4 =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_71_0_BLOCK_21);

        return Stream.of(
                Arguments.of("HAPI_0_68_0_BLOCK_14", s1),
                Arguments.of("HAPI_0_66_0_BLOCK_10", s2),
                Arguments.of("HAPI_0_69_0_BLOCK_240", s3),
                Arguments.of("HAPI_0_71_0_BLOCK_21", s4));
    }
}
