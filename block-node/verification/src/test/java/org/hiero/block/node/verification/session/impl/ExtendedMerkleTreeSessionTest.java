// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.hiero.block.node.verification.VerificationServicePlugin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ExtendedMerkleTreeSessionTest {

    @BeforeEach
    void resetTssState() {
        VerificationServicePlugin.activeLedgerId = null;
        VerificationServicePlugin.activeTssPublication = null;
        VerificationServicePlugin.tssParametersPersisted = false;
    }

    @Test
    @DisplayName("happy path - CN 0.73 block")
    void happyPath() throws IOException, ParseException {
        // Use block 0 which self-bootstraps TSS state (genesis aggregate Schnorr signature)
        BlockUtils.SampleBlockInfo sampleBlockInfo = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_0);
        List<BlockItemUnparsed> blockItems = sampleBlockInfo.blockUnparsed().blockItems();

        BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeaderOrThrow());
        long blockNumber = blockHeader.number();

        ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                blockNumber, BlockSource.PUBLISHER, null, null, null, Map.of(), null, null, null);

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
        BlockUnparsed block = loadBlock("test-blocks/CN_0_73_TSS_WRAPS/0.blk.gz");
        List<BlockItemUnparsed> items = block.blockItems();
        long blockNumber = BlockHeader.PROTOBUF
                .parse(items.getFirst().blockHeaderOrThrow())
                .number();
        ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                blockNumber, BlockSource.PUBLISHER, null, null, null, Map.of(), null, null, null);
        VerificationNotification notification =
                session.processBlockItems(new BlockItems(items, blockNumber, true, true));
        assertTrue(
                notification.success(),
                "TssWraps block 0 should verify successfully through ExtendedMerkleTreeSession");
    }

    @Test
    @DisplayName("should initialize TSS parameters on plugin when processing block 0")
    void shouldInitializeTssParametersFromBlock0() throws IOException, ParseException {
        BlockUnparsed block = loadBlock("test-blocks/CN_0_73_TSS_WRAPS/0.blk.gz");
        createAndProcessSession(block, null);
        assertNotNull(
                VerificationServicePlugin.activeLedgerId,
                "activeLedgerId must be set after processing block 0 with LedgerIdPublicationTransactionBody");
        assertNotNull(
                VerificationServicePlugin.activeTssPublication,
                "activeTssPublication must be set after processing block 0");
    }

    @Test
    @DisplayName("should reject a malformed 10-byte signature as too short for VK prefix")
    void shouldRejectMalformedShortSignature() {
        ExtendedMerkleTreeSession session =
                new ExtendedMerkleTreeSession(0L, BlockSource.PUBLISHER, null, null, null, Map.of(), null, null, null);
        Bytes hash = Bytes.wrap(new byte[48]);
        Bytes shortSignature = Bytes.wrap(new byte[10]);
        assertFalse(session.verifySignature(hash, shortSignature), "A 10-byte signature must be rejected as too short");
    }

    @Test
    @DisplayName("should reject a zero-filled 2920-byte garbage TssWraps signature when no ledger ID")
    void shouldRejectGarbageTssWrapsSignature() {
        // No ledgerId provided, so verifySignature returns false before calling TSS.verifyTSS()
        ExtendedMerkleTreeSession session =
                new ExtendedMerkleTreeSession(0L, BlockSource.PUBLISHER, null, null, null, Map.of(), null, null, null);
        Bytes hash = Bytes.wrap(new byte[48]);
        Bytes garbageSignature = Bytes.wrap(new byte[2920]);
        assertFalse(
                session.verifySignature(hash, garbageSignature), "A zero-filled 2920-byte signature must not verify");
    }

    @Test
    @DisplayName("should fail verification when block contains duplicate TSS proofs")
    void shouldFailWithDuplicateTssProofs() throws IOException, ParseException {
        BlockUtils.SampleBlockInfo sampleBlockInfo = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_0);
        List<BlockItemUnparsed> originalItems = sampleBlockInfo.blockUnparsed().blockItems();
        long blockNumber = BlockHeader.PROTOBUF
                .parse(originalItems.getFirst().blockHeaderOrThrow())
                .number();

        BlockItemUnparsed tssProofItem = null;
        for (BlockItemUnparsed item : originalItems) {
            if (item.item().kind() == BlockItemUnparsed.ItemOneOfType.BLOCK_PROOF) {
                BlockProof proof = BlockProof.PROTOBUF.parse(item.blockProofOrThrow());
                if (proof.hasSignedBlockProof()) {
                    tssProofItem = item;
                    break;
                }
            }
        }
        assertNotNull(tssProofItem, "Test block must contain a TSS proof item");

        List<BlockItemUnparsed> items = new ArrayList<>(originalItems);
        items.add(tssProofItem);

        ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                blockNumber, BlockSource.PUBLISHER, null, null, null, Map.of(), null, null, null);
        VerificationNotification notification =
                session.processBlockItems(new BlockItems(items, blockNumber, true, true));

        assertNotNull(notification, "Session must produce a notification for a malformed block");
        assertFalse(notification.success(), "Duplicate TSS proofs must not verify successfully");
        assertEquals(FailureType.BAD_BLOCK_PROOF, notification.failureType());
    }

    private static ExtendedMerkleTreeSession createAndProcessSession(BlockUnparsed block, Bytes ledgerId)
            throws ParseException {
        List<BlockItemUnparsed> items = block.blockItems();
        long blockNumber = BlockHeader.PROTOBUF
                .parse(items.getFirst().blockHeaderOrThrow())
                .number();
        ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                blockNumber, BlockSource.PUBLISHER, null, null, ledgerId, Map.of(), null, null, null);
        session.processBlockItems(new BlockItems(items, blockNumber, true, true));
        return session;
    }

    private static BlockUnparsed loadBlock(String resourcePath) throws IOException, ParseException {
        try (InputStream stream = TestUtils.class.getModule().getResourceAsStream(resourcePath);
                GZIPInputStream gzip = new GZIPInputStream(stream)) {
            return BlockUnparsed.PROTOBUF.parse(Bytes.wrap(gzip.readAllBytes()));
        }
    }
}
