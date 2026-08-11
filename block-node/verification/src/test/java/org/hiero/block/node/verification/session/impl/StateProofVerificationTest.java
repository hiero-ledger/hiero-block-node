// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static org.hiero.block.node.base.ParseHelper.standardParse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.SiblingNode;
import com.hedera.hapi.block.stream.StateProof;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils.SAMPLE_BLOCKS_STATE_PROOFS;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils.SampleBlockInfo;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.VerificationServicePlugin;
import org.hiero.block.node.verification.session.VerificationProofMetrics;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies that indirect (state) proof verification works end-to-end, including both the
 * merkle path walk and the TSS signature verification.
 *
 * <p>State proofs are produced when TSS signing is delayed (e.g., after restarts or upgrades).
 * Instead of a direct TSS signature, the proof contains merkle path siblings that chain the
 * target block's hash through one or more gap blocks to a later directly-signed block.
 *
 * <p>Test blocks come from a real Schnorr TSS capture where every 5th block is directly
 * signed. Block 0 contains {@code LedgerIdPublicationTransactionBody} for TSS initialization.
 * Block 1 has a 4-gap state proof (19 siblings), block 2 has a 3-gap (15 siblings), block 3
 * has a 2-gap (11 siblings), and block 4 has a 1-gap (7 siblings). All indirect proofs
 * reference block 5 as the directly-signed target.
 *
 * @see ExtendedMerkleTreeSession#verifyStateProof
 */
@Execution(ExecutionMode.SAME_THREAD)
class StateProofVerificationTest {

    private static SampleBlockInfo block0;
    private static SampleBlockInfo block1;
    private static SampleBlockInfo block2;
    private static SampleBlockInfo block3;
    private static SampleBlockInfo block4;
    private static SampleBlockInfo block5;

    private Bytes activeLedgerId;

    @BeforeAll
    static void loadBlocks() throws IOException, ParseException {
        block0 = BlockUtils.getSampleBlockInfo(SAMPLE_BLOCKS_STATE_PROOFS.BLOCK_0);
        block1 = BlockUtils.getSampleBlockInfo(SAMPLE_BLOCKS_STATE_PROOFS.BLOCK_1);
        block2 = BlockUtils.getSampleBlockInfo(SAMPLE_BLOCKS_STATE_PROOFS.BLOCK_2);
        block3 = BlockUtils.getSampleBlockInfo(SAMPLE_BLOCKS_STATE_PROOFS.BLOCK_3);
        block4 = BlockUtils.getSampleBlockInfo(SAMPLE_BLOCKS_STATE_PROOFS.BLOCK_4);
        block5 = BlockUtils.getSampleBlockInfo(SAMPLE_BLOCKS_STATE_PROOFS.BLOCK_5);
    }

    @BeforeEach
    void initializeLedgerState() throws ParseException {
        // Reset static TSS state for test isolation
        VerificationServicePlugin.activeLedgerId = null;
        VerificationServicePlugin.activeTssPublication = null;
        VerificationServicePlugin.tssParametersPersisted = false;
        // Process block 0 through a session to initialize native TSS state and extract ledger ID
        ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                block0.blockNumber(), BlockSource.PUBLISHER, null, null, null, Map.of(), VerificationProofMetrics.NONE);
        session.processBlockItems(
                new BlockItems(block0.blockUnparsed().blockItems(), block0.blockNumber(), true, true));
        assertNotNull(VerificationServicePlugin.activeLedgerId, "Block 0 must set the active ledger ID");
        this.activeLedgerId = VerificationServicePlugin.activeLedgerId;
    }

    @Test
    void shouldVerifyBlock3With2GapStateProof() throws ParseException {
        // Block 3 is an indirect proof with 2 gap blocks (11 siblings in path 1)
        VerificationNotification notification = verifyBlock(block3);
        assertNotNull(notification, "Block 3 must produce a verification notification");
        assertTrue(notification.success(), "Block 3 state proof (2-gap) should verify successfully");
        assertNotNull(notification.blockHash(), "Verified block must have a non-null block hash");
        assertNotNull(notification.block(), "Verified block must include the block data");
        assertEquals(block3.blockRootHash(), notification.blockHash(), "Block hash must match expected root hash");
    }

    @Test
    void shouldVerifyBlock2With3GapStateProof() throws ParseException {
        // Block 2 is an indirect proof with 3 gap blocks (15 siblings in path 1)
        VerificationNotification notification = verifyBlock(block2);
        assertNotNull(notification, "Block 2 must produce a verification notification");
        assertTrue(notification.success(), "Block 2 state proof (3-gap) should verify successfully");
        assertEquals(block2.blockRootHash(), notification.blockHash(), "Block hash must match expected root hash");
    }

    @Test
    void shouldVerifyBlock1With4GapStateProof() throws ParseException {
        // Block 1 is an indirect proof with 4 gap blocks (19 siblings in path 1)
        VerificationNotification notification = verifyBlock(block1);
        assertNotNull(notification, "Block 1 must produce a verification notification");
        assertTrue(notification.success(), "Block 1 state proof (4-gap) should verify successfully");
        assertEquals(block1.blockRootHash(), notification.blockHash(), "Block hash must match expected root hash");
    }

    @Test
    void shouldVerifySiblingCountMonotonicWithGapSize() throws ParseException {
        // A larger gap must require more siblings in the merkle path, monotonic ordering must
        // hold whatever the exact per-gap formula is under the current merkle-path encoding.
        final int block3Siblings = extractSiblingCount(block3.blockUnparsed());
        final int block2Siblings = extractSiblingCount(block2.blockUnparsed());
        final int block1Siblings = extractSiblingCount(block1.blockUnparsed());
        assertTrue(block3Siblings < block2Siblings, "3-gap proof must have more siblings than 2-gap");
        assertTrue(block2Siblings < block1Siblings, "4-gap proof must have more siblings than 3-gap");
    }

    @Test
    void shouldRejectTamperedSiblingHash() throws ParseException {
        // Tamper with a sibling hash — the reconstructed root will differ, TSS verification must fail
        BlockUnparsed tamperedBlock = tamperSiblingHash(block3.blockUnparsed());
        VerificationNotification notification = verifyBlock(block3.blockNumber(), tamperedBlock);
        assertNotNull(notification, "Tampered block must still produce a notification");
        assertFalse(notification.success(), "Tampered sibling hash must cause verification to fail");
    }

    @Test
    void shouldRejectTamperedTimestampLeaf() throws ParseException {
        // Tamper with the timestamp leaf in path 0 — the signed block root will differ
        BlockUnparsed tamperedBlock = tamperTimestampLeaf(block3.blockUnparsed());
        VerificationNotification notification = verifyBlock(block3.blockNumber(), tamperedBlock);
        assertNotNull(notification, "Tampered block must still produce a notification");
        assertFalse(notification.success(), "Tampered timestamp leaf must cause verification to fail");
    }

    @Test
    void shouldRejectTamperedBlockContentWithValidStateProof() throws ParseException {
        // Tamper with a transaction in the block body while leaving the state proof unchanged.
        // The independently computed blockRootHash will differ from what the state proof chain
        // encodes, so the first-iteration integrity check must reject the block.
        BlockUnparsed tamperedBlock = tamperSignedTransaction(block3.blockUnparsed());
        VerificationNotification notification = verifyBlock(block3.blockNumber(), tamperedBlock);
        assertNotNull(notification, "Tampered block must still produce a notification");
        assertFalse(
                notification.success(),
                "Tampered block content with valid state proof must cause verification to fail");
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4})
    void shouldRejectInvalidSiblingCount(int siblingCount) throws ParseException {
        // 1 and 2 fail the "< 3" guard; 4 fails the "(count - 3) % 4 == 0" guard.
        // Block 3 has 7 siblings, so truncating to 1, 2, or 4 always produces an invalid count.
        BlockUnparsed tamperedBlock = withSiblingCount(block3.blockUnparsed(), siblingCount);
        VerificationNotification notification = verifyBlock(block3.blockNumber(), tamperedBlock);
        assertNotNull(notification, "Block with " + siblingCount + " sibling(s) must still produce a notification");
        assertFalse(notification.success(), "State proof with " + siblingCount + " sibling(s) must fail verification");
    }

    @Test
    void shouldVerifyDirectTssProofStillWorks() throws ParseException {
        // Block 0 and block 5 have direct TSS proofs (every-5th cycle) — ensure the existing path still works
        VerificationNotification notification0 = verifyBlock(block0);
        assertNotNull(notification0, "Block 0 must produce a verification notification");
        assertTrue(notification0.success(), "Block 0 direct TSS proof should still verify");
        assertEquals(block0.blockRootHash(), notification0.blockHash());

        VerificationNotification notification5 = verifyBlock(block5);
        assertNotNull(notification5, "Block 5 must produce a verification notification");
        assertTrue(notification5.success(), "Block 5 direct TSS proof should still verify");
        assertEquals(block5.blockRootHash(), notification5.blockHash());
    }

    // Helpers

    private VerificationNotification verifyBlock(SampleBlockInfo blockInfo) throws ParseException {
        return verifyBlock(blockInfo.blockNumber(), blockInfo.blockUnparsed());
    }

    private VerificationNotification verifyBlock(long blockNumber, BlockUnparsed block) throws ParseException {
        ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                blockNumber,
                BlockSource.PUBLISHER,
                null,
                null,
                activeLedgerId,
                Map.of(),
                VerificationProofMetrics.NONE);
        return session.processBlockItems(new BlockItems(block.blockItems(), blockNumber, true, true));
    }

    private static int extractSiblingCount(BlockUnparsed block) throws ParseException {
        for (BlockItemUnparsed item : block.blockItems().reversed()) {
            if (item.item().kind() != BlockItemUnparsed.ItemOneOfType.BLOCK_PROOF) {
                continue;
            }
            BlockProof proof = standardParse(BlockProof.PROTOBUF, item.blockProofOrThrow());
            if (proof.hasBlockStateProof()) {
                return proof.blockStateProof().paths().get(1).siblings().size();
            }
        }
        throw new IllegalStateException("No state proof found in block");
    }

    /**
     * Creates a copy of the block with a tampered sibling hash in the state proof's path 1.
     * Flips the first byte of the first sibling's hash.
     */
    private static BlockUnparsed tamperSiblingHash(BlockUnparsed block) throws ParseException {
        List<BlockItemUnparsed> items = new ArrayList<>(block.blockItems());
        for (int i = items.size() - 1; i >= 0; i--) {
            BlockItemUnparsed item = items.get(i);
            if (item.item().kind() != BlockItemUnparsed.ItemOneOfType.BLOCK_PROOF) {
                continue;
            }
            BlockProof proof = standardParse(BlockProof.PROTOBUF, item.blockProofOrThrow());
            if (!proof.hasBlockStateProof()) {
                continue;
            }
            StateProof stateProof = proof.blockStateProof();
            List<SiblingNode> siblings =
                    new ArrayList<>(stateProof.paths().get(1).siblings());
            SiblingNode original = siblings.get(0);
            byte[] tamperedHash = original.hash().toByteArray();
            tamperedHash[0] = (byte) ~tamperedHash[0];
            siblings.set(0, new SiblingNode(original.isLeft(), Bytes.wrap(tamperedHash)));

            StateProof tamperedStateProof = stateProof
                    .copyBuilder()
                    .paths(List.of(
                            stateProof.paths().get(0),
                            stateProof
                                    .paths()
                                    .get(1)
                                    .copyBuilder()
                                    .siblings(siblings)
                                    .build(),
                            stateProof.paths().get(2)))
                    .build();
            BlockProof tamperedProof =
                    proof.copyBuilder().blockStateProof(tamperedStateProof).build();
            Bytes tamperedProofBytes = BlockProof.PROTOBUF.toBytes(tamperedProof);
            items.set(
                    i,
                    BlockItemUnparsed.newBuilder()
                            .blockProof(tamperedProofBytes)
                            .build());
            break;
        }
        return new BlockUnparsed(items);
    }

    /**
     * Creates a copy of the block with the state proof's path 1 siblings truncated to {@code count} entries.
     * Used to exercise the sibling-count validation in {@code verifyStateProof}.
     */
    private static BlockUnparsed withSiblingCount(BlockUnparsed block, int count) throws ParseException {
        List<BlockItemUnparsed> items = new ArrayList<>(block.blockItems());
        for (int i = items.size() - 1; i >= 0; i--) {
            BlockItemUnparsed item = items.get(i);
            if (item.item().kind() != BlockItemUnparsed.ItemOneOfType.BLOCK_PROOF) {
                continue;
            }
            BlockProof proof = standardParse(BlockProof.PROTOBUF, item.blockProofOrThrow());
            if (!proof.hasBlockStateProof()) {
                continue;
            }
            StateProof stateProof = proof.blockStateProof();
            List<SiblingNode> truncated =
                    new ArrayList<>(stateProof.paths().get(1).siblings().subList(0, count));
            StateProof tamperedStateProof = stateProof
                    .copyBuilder()
                    .paths(List.of(
                            stateProof.paths().get(0),
                            stateProof
                                    .paths()
                                    .get(1)
                                    .copyBuilder()
                                    .siblings(truncated)
                                    .build(),
                            stateProof.paths().get(2)))
                    .build();
            BlockProof tamperedProof =
                    proof.copyBuilder().blockStateProof(tamperedStateProof).build();
            items.set(
                    i,
                    BlockItemUnparsed.newBuilder()
                            .blockProof(BlockProof.PROTOBUF.toBytes(tamperedProof))
                            .build());
            return new BlockUnparsed(items);
        }
        throw new IllegalStateException("No state proof found in block");
    }

    /**
     * Creates a copy of the block with a tampered signed transaction item.
     * Flips the first byte of the first signed transaction's raw bytes, leaving the state proof intact.
     */
    private static BlockUnparsed tamperSignedTransaction(BlockUnparsed block) {
        List<BlockItemUnparsed> items = new ArrayList<>(block.blockItems());
        for (int i = 0; i < items.size(); i++) {
            BlockItemUnparsed item = items.get(i);
            if (item.item().kind() != BlockItemUnparsed.ItemOneOfType.SIGNED_TRANSACTION) {
                continue;
            }
            byte[] tamperedTx = item.signedTransactionOrThrow().toByteArray();
            tamperedTx[0] = (byte) ~tamperedTx[0];
            items.set(
                    i,
                    BlockItemUnparsed.newBuilder()
                            .signedTransaction(Bytes.wrap(tamperedTx))
                            .build());
            return new BlockUnparsed(items);
        }
        throw new IllegalStateException("No SIGNED_TRANSACTION found in block");
    }

    /**
     * Creates a copy of the block with a tampered timestamp leaf in the state proof's path 0.
     * Flips the first byte of the timestamp leaf data.
     */
    private static BlockUnparsed tamperTimestampLeaf(BlockUnparsed block) throws ParseException {
        List<BlockItemUnparsed> items = new ArrayList<>(block.blockItems());
        for (int i = items.size() - 1; i >= 0; i--) {
            BlockItemUnparsed item = items.get(i);
            if (item.item().kind() != BlockItemUnparsed.ItemOneOfType.BLOCK_PROOF) {
                continue;
            }
            BlockProof proof = standardParse(BlockProof.PROTOBUF, item.blockProofOrThrow());
            if (!proof.hasBlockStateProof()) {
                continue;
            }
            StateProof stateProof = proof.blockStateProof();
            byte[] tamperedTimestamp = stateProof.paths().get(0).timestampLeaf().toByteArray();
            tamperedTimestamp[0] = (byte) ~tamperedTimestamp[0];

            StateProof tamperedStateProof = stateProof
                    .copyBuilder()
                    .paths(List.of(
                            stateProof
                                    .paths()
                                    .get(0)
                                    .copyBuilder()
                                    .timestampLeaf(Bytes.wrap(tamperedTimestamp))
                                    .build(),
                            stateProof.paths().get(1),
                            stateProof.paths().get(2)))
                    .build();
            BlockProof tamperedProof =
                    proof.copyBuilder().blockStateProof(tamperedStateProof).build();
            Bytes tamperedProofBytes = BlockProof.PROTOBUF.toBytes(tamperedProof);
            items.set(
                    i,
                    BlockItemUnparsed.newBuilder()
                            .blockProof(tamperedProofBytes)
                            .build());
            break;
        }
        return new BlockUnparsed(items);
    }
}
