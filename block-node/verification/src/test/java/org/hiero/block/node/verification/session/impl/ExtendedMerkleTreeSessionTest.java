// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static org.hiero.block.node.base.ParseHelper.standardParse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.hiero.block.node.verification.VerificationServicePlugin;
import org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder;
import org.hiero.block.node.verification.session.VerificationProofMetrics;
import org.hiero.block.signing.TssBlockSigner;
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
    @DisplayName("happy path - locally-signed genesis block passes end-to-end")
    void happyPath() throws ParseException {
        final LegacyHarnessChainBuilder.Signed genesis =
                LegacyHarnessChainBuilder.create(TssBlockSigner.create()).genesisWithPublication();
        final long blockNumber = genesis.block().number();
        final List<BlockItemUnparsed> items = genesis.block().blockUnparsed().blockItems();

        final ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                blockNumber, BlockSource.PUBLISHER, null, null, null, Map.of(), VerificationProofMetrics.NONE);
        final VerificationNotification notification =
                session.processBlockItems(new BlockItems(items, blockNumber, true, true));

        assertArrayEquals(
                items.toArray(),
                session.blockItems.toArray(),
                "The internal block items should be the same as ones sent in");
        assertArrayEquals(
                items.toArray(),
                notification.block().blockItems().toArray(),
                "The notification's block items should be the same as ones sent in");
        assertEquals(blockNumber, notification.blockNumber(), "The block number should match the header");
        assertEquals(genesis.rootHash(), notification.blockHash(), "The block hash should match the computed root");
        assertTrue(notification.success(), "The block notification should be successful");
        assertEquals(genesis.block().blockUnparsed(), notification.block(), "The block should round-trip identically");
    }

    @Test
    @DisplayName("should verify a locally-signed TssWraps genesis block through the full session pipeline")
    void shouldVerifyTssWrapsBlock_throughSession() throws ParseException {
        final LegacyHarnessChainBuilder.Signed genesis =
                LegacyHarnessChainBuilder.create(TssBlockSigner.create()).genesisWithPublication();
        final long blockNumber = genesis.block().number();
        final ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                blockNumber, BlockSource.PUBLISHER, null, null, null, Map.of(), VerificationProofMetrics.NONE);
        final VerificationNotification notification = session.processBlockItems(
                new BlockItems(genesis.block().blockUnparsed().blockItems(), blockNumber, true, true));
        assertTrue(
                notification.success(),
                "TssWraps block 0 should verify successfully through ExtendedMerkleTreeSession");
    }

    @Test
    @DisplayName("should initialize TSS parameters on plugin when processing block 0")
    void shouldInitializeTssParametersFromBlock0() throws ParseException {
        final LegacyHarnessChainBuilder.Signed genesis =
                LegacyHarnessChainBuilder.create(TssBlockSigner.create()).genesisWithPublication();
        final ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                genesis.block().number(),
                BlockSource.PUBLISHER,
                null,
                null,
                null,
                Map.of(),
                VerificationProofMetrics.NONE);
        session.processBlockItems(new BlockItems(
                genesis.block().blockUnparsed().blockItems(), genesis.block().number(), true, true));
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
        ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                0L, BlockSource.PUBLISHER, null, null, null, Map.of(), VerificationProofMetrics.NONE);
        Bytes hash = Bytes.wrap(new byte[48]);
        Bytes shortSignature = Bytes.wrap(new byte[10]);
        assertFalse(session.verifySignature(hash, shortSignature), "A 10-byte signature must be rejected as too short");
    }

    @Test
    @DisplayName("should reject a zero-filled 2920-byte garbage TssWraps signature when no ledger ID")
    void shouldRejectGarbageTssWrapsSignature() {
        // No ledgerId provided, so verifySignature returns false before calling TSS.verifyTSS()
        ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                0L, BlockSource.PUBLISHER, null, null, null, Map.of(), VerificationProofMetrics.NONE);
        Bytes hash = Bytes.wrap(new byte[48]);
        Bytes garbageSignature = Bytes.wrap(new byte[2920]);
        assertFalse(
                session.verifySignature(hash, garbageSignature), "A zero-filled 2920-byte signature must not verify");
    }

    @Test
    @DisplayName("should return BAD_BLOCK_PROOF failure when no block footer is present")
    void shouldReturnBadBlockProofWhenNoBlockFooter() throws ParseException {
        final long blockNumber = 42L;
        final ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                blockNumber, BlockSource.PUBLISHER, null, null, null, Map.of(), VerificationProofMetrics.NONE);
        final BlockItemUnparsed roundHeaderItem = BlockItemUnparsed.newBuilder()
                .roundHeader(Bytes.wrap(new byte[32]))
                .build();
        final VerificationNotification notification =
                session.processBlockItems(new BlockItems(List.of(roundHeaderItem), blockNumber, false, true));

        assertNotNull(notification, "A notification must be returned when isEndOfBlock=true");
        assertFalse(notification.success(), "Verification must fail when footer is absent");
        assertEquals(FailureType.BAD_BLOCK_PROOF, notification.failureInfo().failureType());
        assertNotNull(notification.block(), "Block bytes must be present for diagnostics even on failure");
    }

    @Test
    @DisplayName("should fail verification when block contains duplicate TSS proofs")
    void shouldFailWithDuplicateTssProofs() throws ParseException {
        final LegacyHarnessChainBuilder.Signed genesis =
                LegacyHarnessChainBuilder.create(TssBlockSigner.create()).genesisWithPublication();
        final TestBlock block = genesis.block();
        final long blockNumber = block.number();

        BlockItemUnparsed tssProofItem = null;
        for (final BlockItemUnparsed item : block.blockUnparsed().blockItems()) {
            if (item.item().kind() == BlockItemUnparsed.ItemOneOfType.BLOCK_PROOF) {
                final BlockProof proof = standardParse(BlockProof.PROTOBUF, item.blockProofOrThrow());
                if (proof.hasSignedBlockProof()) {
                    tssProofItem = item;
                    break;
                }
            }
        }
        assertNotNull(tssProofItem, "Generated block must contain a TSS proof item");

        final List<BlockItemUnparsed> items =
                new ArrayList<>(block.blockUnparsed().blockItems());
        items.add(tssProofItem);

        final ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                blockNumber, BlockSource.PUBLISHER, null, null, null, Map.of(), VerificationProofMetrics.NONE);
        final VerificationNotification notification =
                session.processBlockItems(new BlockItems(items, blockNumber, true, true));

        assertNotNull(notification, "Session must produce a notification for a malformed block");
        assertFalse(notification.success(), "Duplicate TSS proofs must not verify successfully");
        assertEquals(FailureType.BAD_BLOCK_PROOF, notification.failureInfo().failureType());
        assertNotNull(notification.block(), "Block bytes must be present for diagnostics even on failure");
    }
}
