// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.cryptography.hints.HintsLibraryBridge;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Verifies that HintsLibraryBridge.verifyAggregate() correctly validates real TSS block proofs.
 *
 * <p>Tests both TSS-only and TssWraps variants, and both "before settled" (block 0, with hinTS
 * preprocessing state) and "after settled" (block 50, no TSS-specific state changes) boundaries.
 *
 * <p>The {@code blockSignature} field in {@code TssSignedBlockProof} is a composite byte array:
 * {@code verificationKey (1480 bytes) || blsAggregateSignature || [chainOfTrustProof]}. The VK and
 * BLS signature are sliced out and passed to {@code verifyAggregate} separately.
 */
class TssBlockProofVerificationTest {

    /** Byte length of the hinTS verification key embedded at the start of every blockSignature. */
    private static final int VK_LENGTH = 1480;

    private static final HintsLibraryBridge HINTS_BRIDGE = HintsLibraryBridge.getInstance();

    private static BlockUnparsed tssBlock0;
    private static BlockUnparsed tssBlock50;
    private static BlockUnparsed wrapsBlock0;
    private static BlockUnparsed wrapsBlock50;

    @BeforeAll
    static void setUp() throws IOException, ParseException {
        tssBlock0 = loadBlock("test-blocks/tss/TSS/000000000000000000000000000000000000.blk.gz");
        tssBlock50 = loadBlock("test-blocks/tss/TSS/000000000000000000000000000000000050.blk.gz");
        wrapsBlock0 = loadBlock("test-blocks/tss/TssWraps/000000000000000000000000000000000000.blk.gz");
        wrapsBlock50 = loadBlock("test-blocks/tss/TssWraps/000000000000000000000000000000000050.blk.gz");
    }

    @Test
    void shouldVerifyTssBlock0_beforeSettled() throws ParseException {
        Bytes blockSig = extractSignature(tssBlock0);
        Bytes vk = blockSig.slice(0, VK_LENGTH);
        Bytes sig = blockSig.slice(VK_LENGTH, blockSig.length() - VK_LENGTH);
        Bytes hash = computeBlockHash(tssBlock0);
        boolean verified = HINTS_BRIDGE.verifyAggregate(sig.toByteArray(), hash.toByteArray(), vk.toByteArray(), 1L, 2L);
        assertTrue(verified, "TSS block 0 BLS aggregate signature should verify successfully");
    }

    @Test
    void shouldVerifyTssBlock50_afterSettled() throws ParseException {
        Bytes blockSig = extractSignature(tssBlock50);
        Bytes vk = blockSig.slice(0, VK_LENGTH);
        Bytes sig = blockSig.slice(VK_LENGTH, blockSig.length() - VK_LENGTH);
        Bytes hash = computeBlockHash(tssBlock50);
        boolean verified = HINTS_BRIDGE.verifyAggregate(sig.toByteArray(), hash.toByteArray(), vk.toByteArray(), 1L, 2L);
        assertTrue(verified, "TSS block 50 BLS aggregate signature should verify successfully");
    }

    @Test
    void shouldVerifyTssWrapsBlock0_beforeSettled() throws ParseException {
        Bytes blockSig = extractSignature(wrapsBlock0);
        Bytes vk = blockSig.slice(0, VK_LENGTH);
        Bytes sig = blockSig.slice(VK_LENGTH, blockSig.length() - VK_LENGTH);
        Bytes hash = computeBlockHash(wrapsBlock0);
        boolean verified = HINTS_BRIDGE.verifyAggregate(sig.toByteArray(), hash.toByteArray(), vk.toByteArray(), 1L, 2L);
        assertTrue(verified, "TssWraps block 0 BLS aggregate signature should verify successfully");
    }

    @Test
    void shouldVerifyTssWrapsBlock50_afterSettled() throws ParseException {
        Bytes blockSig = extractSignature(wrapsBlock50);
        Bytes vk = blockSig.slice(0, VK_LENGTH);
        Bytes sig = blockSig.slice(VK_LENGTH, blockSig.length() - VK_LENGTH);
        Bytes hash = computeBlockHash(wrapsBlock50);
        boolean verified = HINTS_BRIDGE.verifyAggregate(sig.toByteArray(), hash.toByteArray(), vk.toByteArray(), 1L, 2L);
        assertTrue(verified, "TssWraps block 50 BLS aggregate signature should verify successfully");
    }

    @Test
    void shouldRejectTamperedBlockHash() throws ParseException {
        Bytes blockSig = extractSignature(tssBlock0);
        Bytes vk = blockSig.slice(0, VK_LENGTH);
        Bytes sig = blockSig.slice(VK_LENGTH, blockSig.length() - VK_LENGTH);
        byte[] tamperedHash = computeBlockHash(tssBlock0).toByteArray();
        tamperedHash[0] = (byte) ~tamperedHash[0];
        boolean verified = HINTS_BRIDGE.verifyAggregate(sig.toByteArray(), tamperedHash, vk.toByteArray(), 1L, 2L);
        assertFalse(verified, "BLS aggregate signature should not verify against a tampered block hash");
    }

    /**
     * Loads a block from a gzipped resource in the test fixtures module.
     */
    private static BlockUnparsed loadBlock(String resourcePath) throws IOException, ParseException {
        try (InputStream stream = TestUtils.class.getModule().getResourceAsStream(resourcePath);
                GZIPInputStream gzip = new GZIPInputStream(stream)) {
            return BlockUnparsed.PROTOBUF.parse(Bytes.wrap(gzip.readAllBytes()));
        }
    }

    /**
     * Computes the block root hash using ExtendedMerkleTreeSession.
     */
    private static Bytes computeBlockHash(BlockUnparsed block) throws ParseException {
        long blockNumber = BlockHeader.PROTOBUF
                .parse(block.blockItems().getFirst().blockHeaderOrThrow())
                .number();
        ExtendedMerkleTreeSession session =
                new ExtendedMerkleTreeSession(blockNumber, BlockSource.PUBLISHER, null, null);
        BlockItems message = new BlockItems(block.blockItems(), blockNumber, true, true);
        VerificationNotification notification = session.processBlockItems(message);
        assertNotNull(notification, "Session must produce a VerificationNotification");
        return notification.blockHash();
    }

    /**
     * Extracts the composite blockSignature bytes from the block's BlockProof item.
     *
     * <p>The returned bytes have the format:
     * {@code verificationKey (1480 bytes) || blsAggregateSignature || [chainOfTrustProof]}.
     */
    private static Bytes extractSignature(BlockUnparsed block) throws ParseException {
        for (BlockItemUnparsed item : block.blockItems().reversed()) {
            if (item.item().kind() != BlockItemUnparsed.ItemOneOfType.BLOCK_PROOF) {
                continue;
            }
            BlockProof proof = BlockProof.PROTOBUF.parse(item.blockProofOrThrow());
            if (proof.hasSignedBlockProof()) {
                return proof.signedBlockProof().blockSignature();
            }
        }
        throw new IllegalStateException("No BlockProof with signedBlockProof found in block");
    }
}
