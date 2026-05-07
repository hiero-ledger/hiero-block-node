// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.cryptography.tss.TSS;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.VerificationServicePlugin;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that TSS.verifyTSS() correctly validates real TssWraps block proofs.
 *
 * <p>Block 0 uses the genesis (aggregate Schnorr, 2,920-byte) path. Block 467 uses the
 * settled WRAPS path (3,432-byte blockSignature).
 */
class TssBlockProofVerificationTest {

    private static BlockUnparsed wrapsBlock0;
    private static BlockUnparsed wrapsBlock467;

    /** Ledger ID initialized from block 0 via the plugin's static TSS state. */
    private Bytes activeLedgerId;

    @BeforeAll
    static void setUp() throws IOException, ParseException {
        wrapsBlock0 = loadBlock("test-blocks/CN_0_73_TSS_WRAPS/0.blk.gz");
        wrapsBlock467 = loadBlock("test-blocks/CN_0_73_TSS_WRAPS/467.blk.gz");
    }

    @BeforeEach
    void initializeLedgerState() throws ParseException {
        // Reset static TSS state for test isolation
        VerificationServicePlugin.activeLedgerId = null;
        VerificationServicePlugin.activeTssPublication = null;
        VerificationServicePlugin.tssParametersPersisted = false;
        // Process block 0 through a session to initialize native TSS state and extract ledger ID
        long blockNumber = BlockHeader.PROTOBUF
                .parse(wrapsBlock0.blockItems().getFirst().blockHeaderOrThrow())
                .number();
        ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                blockNumber, BlockSource.PUBLISHER, null, null, null, Map.of(), null, null, null);
        session.processBlockItems(new BlockItems(wrapsBlock0.blockItems(), blockNumber, true, true));
        assertNotNull(VerificationServicePlugin.activeLedgerId, "Block 0 must set the active ledger ID");
        this.activeLedgerId = VerificationServicePlugin.activeLedgerId;
    }

    @Test
    void shouldVerifyTssWrapsBlock0BeforeSettled() throws ParseException {
        Bytes hash = computeBlockHash(wrapsBlock0, null);
        Bytes signature = extractSignature(wrapsBlock0);
        // genesis: vk (1096) + blsSig (1632) + aggregate Schnorr (192) = 2920
        assertEquals(2_920, signature.length(), "Block 0 signature must be 2920 bytes (genesis path)");
        assertTrue(
                TSS.verifyTSS(activeLedgerId.toByteArray(), signature.toByteArray(), hash.toByteArray()),
                "TssWraps block 0 aggregate Schnorr signature should verify successfully");
    }

    @Test
    void shouldVerifyTssWrapsBlock467SettledPath() throws ParseException {
        Bytes hash = computeBlockHash(wrapsBlock467, activeLedgerId);
        Bytes signature = extractSignature(wrapsBlock467);
        // settled path: vk (1096) + blsSig (1632) + WRAPS proof (704) = 3432
        assertEquals(3_432, signature.length(), "Block 467 signature must be 3432 bytes (settled WRAPS path)");
        assertTrue(
                TSS.verifyTSS(activeLedgerId.toByteArray(), signature.toByteArray(), hash.toByteArray()),
                "TssWraps block 467 settled WRAPS signature should verify successfully");
    }

    @Test
    void shouldRejectTamperedSignature() throws ParseException {
        Bytes hash = computeBlockHash(wrapsBlock0, null);
        byte[] sig = extractSignature(wrapsBlock0).toByteArray();
        sig[0] = (byte) ~sig[0];
        assertFalse(
                TSS.verifyTSS(activeLedgerId.toByteArray(), sig, hash.toByteArray()),
                "Tampered signature should not verify against a valid block hash");
    }

    @Test
    void shouldRejectTamperedBlockHash() throws ParseException {
        byte[] hash = computeBlockHash(wrapsBlock0, null).toByteArray();
        hash[0] = (byte) ~hash[0];
        Bytes signature = extractSignature(wrapsBlock0);
        assertFalse(
                TSS.verifyTSS(activeLedgerId.toByteArray(), signature.toByteArray(), hash),
                "BLS aggregate signature should not verify against a tampered block hash");
    }

    private static BlockUnparsed loadBlock(String resourcePath) throws IOException, ParseException {
        try (InputStream stream = TestUtils.class.getModule().getResourceAsStream(resourcePath);
                GZIPInputStream gzip = new GZIPInputStream(stream)) {
            return BlockUnparsed.PROTOBUF.parse(Bytes.wrap(gzip.readAllBytes()));
        }
    }

    private static Bytes computeBlockHash(BlockUnparsed block, Bytes ledgerId) throws ParseException {
        long blockNumber = BlockHeader.PROTOBUF
                .parse(block.blockItems().getFirst().blockHeaderOrThrow())
                .number();
        ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                blockNumber, BlockSource.PUBLISHER, null, null, ledgerId, Map.of(), null, null, null);
        BlockItems message = new BlockItems(block.blockItems(), blockNumber, true, true);
        VerificationNotification notification = session.processBlockItems(message);
        assertNotNull(notification, "Session must produce a VerificationNotification");
        return notification.blockHash();
    }

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
