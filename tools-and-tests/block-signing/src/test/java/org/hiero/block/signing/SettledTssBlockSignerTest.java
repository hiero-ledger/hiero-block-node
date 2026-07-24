// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.signing;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.cryptography.tss.TSS;
import com.hedera.cryptography.wraps.WRAPSVerificationKey;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.junit.jupiter.api.Test;

/// Verifies the SETTLED (WRAPS) path: [TssBlockSigner#createDeterministicSettled] produces a proof
/// whose 704-byte compressed WRAPS `abProof` (loaded from the committed resource) is accepted by
/// `TSS.verifyTSS`. Verifying a WRAPS proof needs no proving artifacts, so this runs everywhere.
class SettledTssBlockSignerTest {

    /// hVK(1096) + hintsSignature(1632) + compressed WRAPS proof(704).
    private static final int EXPECTED_SETTLED_SIGNATURE_LENGTH = 1096 + 1632 + 704;

    @Test
    void producesSettledSignatureAcceptedByVerifyTss() {
        final TssBlockSigner signer = TssBlockSigner.createDeterministicSettled();
        final Bytes blockRootHash = sha384("settled-block-root-hash");

        final BlockProof proof = signer.signBlockProof(9L, blockRootHash);

        assertThat(proof.hasSignedBlockProof()).isTrue();
        final Bytes signature = proof.signedBlockProof().blockSignature();
        assertThat(signature.length()).isEqualTo(EXPECTED_SETTLED_SIGNATURE_LENGTH);

        final TssData tssData = signer.verificationMaterial().tssData();
        provisionVerifier(tssData);

        assertThat(TSS.verifyTSS(
                        tssData.ledgerId().toByteArray(), signature.toByteArray(), blockRootHash.toByteArray()))
                .isTrue();
    }

    @Test
    void settledSignatureDoesNotVerifyForADifferentBlockRootHash() {
        final TssBlockSigner signer = TssBlockSigner.createDeterministicSettled();
        final BlockProof proof = signer.signBlockProof(1L, sha384("the-signed-root"));
        final TssData tssData = signer.verificationMaterial().tssData();
        provisionVerifier(tssData);

        assertThat(TSS.verifyTSS(
                        tssData.ledgerId().toByteArray(),
                        proof.signedBlockProof().blockSignature().toByteArray(),
                        sha384("a-different-root").toByteArray()))
                .isFalse();
    }

    private static void provisionVerifier(final TssData tssData) {
        final List<RosterEntry> entries = tssData.currentRoster().rosterEntries();
        final byte[][] schnorrPublicKeys = new byte[entries.size()][];
        final long[] weights = new long[entries.size()];
        final long[] nodeIds = new long[entries.size()];
        for (int i = 0; i < entries.size(); i++) {
            final RosterEntry entry = entries.get(i);
            schnorrPublicKeys[i] = entry.schnorrPublicKey().toByteArray();
            weights[i] = entry.weight();
            nodeIds[i] = entry.nodeId();
        }
        TSS.setAddressBook(schnorrPublicKeys, weights, nodeIds);
        WRAPSVerificationKey.setCurrentKey(tssData.wrapsVerificationKey().toByteArray());
    }

    private static Bytes sha384(final String text) {
        try {
            return Bytes.wrap(MessageDigest.getInstance("SHA-384").digest(text.getBytes()));
        } catch (final NoSuchAlgorithmException fatal) {
            throw new IllegalStateException(fatal);
        }
    }
}
