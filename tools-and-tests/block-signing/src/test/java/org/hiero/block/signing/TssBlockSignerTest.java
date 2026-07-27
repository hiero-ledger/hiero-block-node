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

/// Verifies that [TssBlockSigner] produces signatures the block-verification `TSSVerifier` accepts.
///
/// The test drives the exact crypto operations the verifier performs — it provisions the roster via
/// `TSS.setAddressBook(...)` (as `VerificationDataProvider` does) and then calls
/// `TSS.verifyTSS(ledgerId, signature, rootHash)` (as `TSSVerifier` does) — so a passing test means
/// the real verifier accepts the proof.
class TssBlockSignerTest {

    /// hVK(1096) + hintsSignature(1632) + genesis Schnorr aggregate abProof(192).
    private static final int EXPECTED_TSS_SIGNATURE_LENGTH = 1096 + 1632 + 192;

    @Test
    void producesSignatureAcceptedByVerifyTss() {
        final TssBlockSigner signer = TssBlockSigner.create();
        final Bytes blockRootHash = sha384("block-root-hash");

        final BlockProof proof = signer.signBlockProof(7L, blockRootHash);

        assertThat(proof.block()).isEqualTo(7L);
        assertThat(proof.hasSignedBlockProof()).isTrue();
        final Bytes signature = proof.signedBlockProof().blockSignature();
        assertThat(signature.length()).isEqualTo(EXPECTED_TSS_SIGNATURE_LENGTH);

        final TssData tssData = signer.verificationMaterial().tssData();
        provisionVerifier(tssData);

        assertThat(TSS.verifyTSS(
                        tssData.ledgerId().toByteArray(), signature.toByteArray(), blockRootHash.toByteArray()))
                .isTrue();
    }

    @Test
    void signatureDoesNotVerifyForADifferentBlockRootHash() {
        final TssBlockSigner signer = TssBlockSigner.create();
        final Bytes signedRootHash = sha384("the-signed-root");
        final Bytes otherRootHash = sha384("a-different-root");

        final BlockProof proof = signer.signBlockProof(1L, signedRootHash);
        final TssData tssData = signer.verificationMaterial().tssData();
        provisionVerifier(tssData);

        assertThat(TSS.verifyTSS(
                        tssData.ledgerId().toByteArray(),
                        proof.signedBlockProof().blockSignature().toByteArray(),
                        otherRootHash.toByteArray()))
                .isFalse();
    }

    /// Mirrors `VerificationDataProvider.updateTssData`: push the roster and WRAPS key into the
    /// library's global state before verifying.
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
