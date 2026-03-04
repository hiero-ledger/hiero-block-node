// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ExtendedMerkleTreeSessionTest {

    @BeforeEach
    void resetLedgerState() {
        // Reset process-level TSS state to ensure test isolation.
        ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.set(null);
    }

    @Test
    @DisplayName("happy path - HAPI 0.72.0 block")
    void happyPath() throws IOException, ParseException {
        BlockUtils.SampleBlockInfo sampleBlockInfo =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_72_0_BLOCK_21);
        List<BlockItemUnparsed> blockItems = sampleBlockInfo.blockUnparsed().blockItems();

        BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeaderOrThrow());
        long blockNumber = blockHeader.number();

        ExtendedMerkleTreeSession session =
                new ExtendedMerkleTreeSession(blockNumber, BlockSource.PUBLISHER, null, null);

        BlockItems blockItemsMessage = new BlockItems(blockItems, blockNumber, true, true);
        VerificationNotification blockNotification = session.processBlockItems(blockItemsMessage);

        assertArrayEquals(
                blockItems.toArray(),
                session.blockItems.toArray(),
                "The internal block items should be the same as ones sent in");

        assertArrayEquals(
                blockItems.toArray(),
                blockNotification.block().blockItems().toArray(),
                "The notification's block items should be the same as ones sent in");

        assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");

        assertEquals(
                sampleBlockInfo.blockRootHash(),
                blockNotification.blockHash(),
                "The block hash should be the same as the one in the block header");

        assertTrue(blockNotification.success(), "The block notification should be successful");

        assertEquals(
                sampleBlockInfo.blockUnparsed(),
                blockNotification.block(),
                "The block should be the same as the one sent");
    }

    @Test
    @DisplayName("should verify TssWraps block 0 through the full session pipeline")
    void shouldVerifyTssWrapsBlock_throughSession() throws IOException, ParseException {
        BlockUnparsed block = loadBlock("test-blocks/tss/TssWraps/0.blk.gz");
        VerificationNotification notification = runSession(block);
        assertTrue(
                notification.success(),
                "TssWraps block 0 should verify successfully through ExtendedMerkleTreeSession");
    }

    @Test
    @DisplayName("should initialize TSS ledger state from LedgerIdPublicationTransactionBody in block 0")
    void shouldInitializeTssStateFromBlock0() throws IOException, ParseException {
        BlockUnparsed block = loadBlock("test-blocks/tss/TssWraps/0.blk.gz");
        runSession(block);
        assertNotNull(
                ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.get(),
                "ACTIVE_LEDGER_ID must be set after processing a block 0 with LedgerIdPublicationTransactionBody");
    }

    @Test
    @DisplayName("should reject a malformed 10-byte signature as too short for VK prefix")
    void shouldRejectMalformedShortSignature() {
        ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(0L, BlockSource.PUBLISHER, null, null);
        Bytes hash = Bytes.wrap(new byte[48]);
        Bytes shortSignature = Bytes.wrap(new byte[10]);
        assertFalse(session.verifySignature(hash, shortSignature), "A 10-byte signature must be rejected as too short");
    }

    @Test
    @DisplayName("should reject a zero-filled 2920-byte garbage TssWraps signature")
    void shouldRejectGarbageTssWrapsSignature() {
        // ACTIVE_LEDGER_ID is null (reset by @BeforeEach), so verifySignature returns false
        // before even calling TSS.verifyTSS().  This guards the null-ledger-ID path for TssWraps-sized inputs.
        ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(0L, BlockSource.PUBLISHER, null, null);
        Bytes hash = Bytes.wrap(new byte[48]);
        Bytes garbageSignature = Bytes.wrap(new byte[2920]);
        assertFalse(
                session.verifySignature(hash, garbageSignature), "A zero-filled 2920-byte signature must not verify");
    }

    private static VerificationNotification runSession(BlockUnparsed block) throws ParseException {
        List<BlockItemUnparsed> items = block.blockItems();
        long blockNumber = BlockHeader.PROTOBUF
                .parse(items.getFirst().blockHeaderOrThrow())
                .number();
        ExtendedMerkleTreeSession session =
                new ExtendedMerkleTreeSession(blockNumber, BlockSource.PUBLISHER, null, null);
        return session.processBlockItems(new BlockItems(items, blockNumber, true, true));
    }

    private static BlockUnparsed loadBlock(String resourcePath) throws IOException, ParseException {
        try (InputStream stream = TestUtils.class.getModule().getResourceAsStream(resourcePath);
                GZIPInputStream gzip = new GZIPInputStream(stream)) {
            return BlockUnparsed.PROTOBUF.parse(Bytes.wrap(gzip.readAllBytes()));
        }
    }
}
