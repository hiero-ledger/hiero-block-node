// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static org.hiero.block.node.base.ParseHelper.standardParse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.cryptography.tss.TSS;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Map;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.VerificationServicePlugin;
import org.hiero.block.node.verification.session.VerificationProofMetrics;
import org.hiero.block.signing.TssBlockSigner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link TSS#verifyTSS} correctly validates TSS block proofs produced by the
 * {@link TssBlockSigner} harness — both the genesis (aggregate-Schnorr, 2920-byte) and the
 * settled (WRAPS, 3432-byte) paths.
 */
class TssBlockProofVerificationTest {

    @BeforeEach
    void resetStatics() {
        VerificationServicePlugin.activeLedgerId = null;
        VerificationServicePlugin.activeTssPublication = null;
        VerificationServicePlugin.tssParametersPersisted = false;
    }

    @Test
    void shouldVerifyTssWrapsBlock0BeforeSettled() throws ParseException {
        final TssBlockSigner signer = TssBlockSigner.create();
        provisionPluginFrom(signer);
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
        final Bytes hash = computeBlockHash(block, VerificationServicePlugin.activeLedgerId);
        final Bytes signature =
                signer.signBlockProof(block.number(), hash).signedBlockProof().blockSignature();
        // genesis: vk (1096) + blsSig (1632) + aggregate Schnorr (192) = 2920
        assertEquals(2_920, signature.length(), "Genesis-path signature must be 2920 bytes");
        assertTrue(
                TSS.verifyTSS(
                        VerificationServicePlugin.activeLedgerId.toByteArray(),
                        signature.toByteArray(),
                        hash.toByteArray()),
                "Aggregate-Schnorr signature should verify successfully");
    }

    @Test
    void shouldVerifyTssWrapsBlock467SettledPath() throws ParseException {
        final TssBlockSigner signer = TssBlockSigner.createDeterministicSettled();
        provisionPluginFrom(signer);
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(467L);
        final Bytes hash = computeBlockHash(block, VerificationServicePlugin.activeLedgerId);
        final Bytes signature =
                signer.signBlockProof(block.number(), hash).signedBlockProof().blockSignature();
        // settled path: vk (1096) + blsSig (1632) + WRAPS proof (704) = 3432
        assertEquals(3_432, signature.length(), "Settled-path signature must be 3432 bytes");
        assertTrue(
                TSS.verifyTSS(
                        VerificationServicePlugin.activeLedgerId.toByteArray(),
                        signature.toByteArray(),
                        hash.toByteArray()),
                "Settled WRAPS signature should verify successfully");
    }

    @Test
    void shouldRejectTamperedSignature() throws ParseException {
        final TssBlockSigner signer = TssBlockSigner.create();
        provisionPluginFrom(signer);
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
        final Bytes hash = computeBlockHash(block, VerificationServicePlugin.activeLedgerId);
        final byte[] signature = signer.signBlockProof(block.number(), hash)
                .signedBlockProof()
                .blockSignature()
                .toByteArray();
        signature[0] = (byte) ~signature[0];
        assertFalse(
                TSS.verifyTSS(VerificationServicePlugin.activeLedgerId.toByteArray(), signature, hash.toByteArray()),
                "Tampered signature should not verify against a valid block hash");
    }

    @Test
    void shouldRejectTamperedBlockHash() throws ParseException {
        final TssBlockSigner signer = TssBlockSigner.create();
        provisionPluginFrom(signer);
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
        final Bytes hash = computeBlockHash(block, VerificationServicePlugin.activeLedgerId);
        final Bytes signature =
                signer.signBlockProof(block.number(), hash).signedBlockProof().blockSignature();
        final byte[] tamperedHash = hash.toByteArray();
        tamperedHash[0] = (byte) ~tamperedHash[0];
        assertFalse(
                TSS.verifyTSS(
                        VerificationServicePlugin.activeLedgerId.toByteArray(), signature.toByteArray(), tamperedHash),
                "BLS aggregate signature should not verify against a tampered block hash");
    }

    /**
     * Extracts the signer's ledger-id publication and pushes it through the same
     * {@link VerificationServicePlugin#initializeTssParameters} entry point block 0 processing
     * would use, so the JVM-wide {@link TSS} native state matches the signer's roster.
     */
    private static void provisionPluginFrom(final TssBlockSigner signer) throws ParseException {
        final SignedTransaction signedTx =
                standardParse(SignedTransaction.PROTOBUF, signer.genesisLedgerIdSignedTransaction());
        final TransactionBody body = standardParse(TransactionBody.PROTOBUF, signedTx.bodyBytes());
        final LedgerIdPublicationTransactionBody publication = body.ledgerIdPublicationOrThrow();
        VerificationServicePlugin.initializeTssParameters(publication);
        assertNotNull(VerificationServicePlugin.activeLedgerId, "initializeTssParameters must set the ledger ID");
    }

    private static Bytes computeBlockHash(final TestBlock block, final Bytes ledgerId) throws ParseException {
        final ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                block.number(), BlockSource.PUBLISHER, null, null, ledgerId, Map.of(), VerificationProofMetrics.NONE);
        final BlockItems message = new BlockItems(block.blockUnparsed().blockItems(), block.number(), true, true);
        final VerificationNotification notification = session.processBlockItems(message);
        assertNotNull(notification, "Session must produce a VerificationNotification");
        return notification.blockHash();
    }
}
