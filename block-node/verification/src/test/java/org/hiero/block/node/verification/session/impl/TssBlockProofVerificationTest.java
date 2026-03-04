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
import java.util.zip.GZIPInputStream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Verifies that TSS.verifyTSS() correctly validates real TssWraps block proofs.
 *
 * <p>Block 0 uses the genesis (aggregate Schnorr, 2,920-byte) path. Block 50 (captured from
 * block 631 of the fixture run) uses the settled WRAPS path (3,432-byte blockSignature).
 */
class TssBlockProofVerificationTest {

    private static BlockUnparsed wrapsBlock0;
    private static BlockUnparsed wrapsBlock50;

    @BeforeAll
    static void setUp() throws IOException, ParseException {
        wrapsBlock0 = loadBlock("test-blocks/tss/TssWraps/0.blk.gz");
        wrapsBlock50 = loadBlock("test-blocks/tss/TssWraps/50.blk.gz");
    }

    @BeforeEach
    void initializeLedgerState() throws ParseException {
        ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.set(null);
        computeBlockHash(wrapsBlock0); // initializes ACTIVE_LEDGER_ID from block 0
    }

    @Test
    void shouldVerifyTssWrapsBlock0_beforeSettled() throws ParseException {
        Bytes hash = computeBlockHash(wrapsBlock0);
        Bytes signature = extractSignature(wrapsBlock0);
        // genesis: vk (1096) + blsSig (1632) + aggregate Schnorr (192) = 2920
        assertEquals(2_920, signature.length(), "Block 0 signature must be 2920 bytes (genesis path)");
        Bytes ledgerId = ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.get();
        assertTrue(
                TSS.verifyTSS(ledgerId.toByteArray(), signature.toByteArray(), hash.toByteArray()),
                "TssWraps block 0 aggregate Schnorr signature should verify successfully");
    }

    @Disabled(
            "Requires a settled-path fixture (3432-byte blockSignature); blocked on CN generating a WRAPS-settled block")
    @Test
    void shouldVerifyTssWrapsBlock50_settledPath() throws ParseException {
        Bytes hash = computeBlockHash(wrapsBlock50);
        Bytes signature = extractSignature(wrapsBlock50);
        // settled path: vk (1096) + blsSig (1632) + WRAPS proof (704) = 3432
        assertEquals(3_432, signature.length(), "Block 50 signature must be 3432 bytes (settled WRAPS path)");
        Bytes ledgerId = ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.get();
        assertTrue(
                TSS.verifyTSS(ledgerId.toByteArray(), signature.toByteArray(), hash.toByteArray()),
                "TssWraps block 50 settled WRAPS signature should verify successfully");
    }

    @Test
    void shouldRejectTamperedSignature() throws ParseException {
        Bytes hash = computeBlockHash(wrapsBlock0);
        byte[] sig = extractSignature(wrapsBlock0).toByteArray();
        sig[0] = (byte) ~sig[0];
        Bytes ledgerId = ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.get();
        assertFalse(
                TSS.verifyTSS(ledgerId.toByteArray(), sig, hash.toByteArray()),
                "Tampered signature should not verify against a valid block hash");
    }

    @Test
    void shouldRejectTamperedBlockHash() throws ParseException {
        byte[] hash = computeBlockHash(wrapsBlock0).toByteArray();
        hash[0] = (byte) ~hash[0];
        Bytes signature = extractSignature(wrapsBlock0);
        Bytes ledgerId = ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.get();
        assertFalse(
                TSS.verifyTSS(ledgerId.toByteArray(), signature.toByteArray(), hash),
                "BLS aggregate signature should not verify against a tampered block hash");
    }

    private static BlockUnparsed loadBlock(String resourcePath) throws IOException, ParseException {
        try (InputStream stream = TestUtils.class.getModule().getResourceAsStream(resourcePath);
                GZIPInputStream gzip = new GZIPInputStream(stream)) {
            return BlockUnparsed.PROTOBUF.parse(Bytes.wrap(gzip.readAllBytes()));
        }
    }

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
