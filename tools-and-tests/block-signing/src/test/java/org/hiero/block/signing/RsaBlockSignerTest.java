// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.signing;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.util.HexFormat;
import org.hiero.block.common.hasher.HashingUtilities;
import org.junit.jupiter.api.Test;

/// Verifies that [RsaBlockSigner] produces version 6 record-file proofs the block-verification
/// `RSAProofVerifier` accepts.
///
/// The test drives the exact crypto operations the verifier performs — it decodes the RSA public key
/// from `NodeAddress.rsaPubKey` (hex DER X.509) and verifies the signature with `SHA384withRSA` over
/// `SHA-384(int32be(6) || record_file_contents)` — so a passing test means the real verifier accepts
/// the proof.
class RsaBlockSignerTest {

    @Test
    void producesRecordFileProofAcceptedBySha384WithRsa() throws GeneralSecurityException {
        final RsaBlockSigner signer = RsaBlockSigner.create();
        final Bytes recordFileContents = Bytes.wrap("verbatim record file contents".getBytes());

        final BlockProof proof = signer.signBlockProof(4L, recordFileContents);

        assertThat(proof.block()).isEqualTo(4L);
        assertThat(proof.hasSignedRecordFileProof()).isTrue();
        assertThat(proof.signedRecordFileProof().version()).isEqualTo(6);
        assertThat(proof.signedRecordFileProof().recordFileSignatures()).hasSize(1);

        final RecordFileSignature signature =
                proof.signedRecordFileProof().recordFileSignatures().getFirst();
        final PublicKey publicKey = publicKeyForNode(signer, signature.nodeId());
        final byte[] payload = HashingUtilities.computeV6SignedPayload(recordFileContents);

        assertThat(verify(publicKey, payload, signature.signaturesBytes().toByteArray()))
                .isTrue();
    }

    @Test
    void signatureDoesNotVerifyForDifferentRecordFileContents() throws GeneralSecurityException {
        final RsaBlockSigner signer = RsaBlockSigner.create();
        final Bytes recordFileContents = Bytes.wrap("the signed contents".getBytes());

        final BlockProof proof = signer.signBlockProof(1L, recordFileContents);
        final RecordFileSignature signature =
                proof.signedRecordFileProof().recordFileSignatures().getFirst();
        final PublicKey publicKey = publicKeyForNode(signer, signature.nodeId());
        final byte[] tamperedPayload = HashingUtilities.computeV6SignedPayload(Bytes.wrap("other contents".getBytes()));

        assertThat(verify(
                        publicKey, tamperedPayload, signature.signaturesBytes().toByteArray()))
                .isFalse();
    }

    private static PublicKey publicKeyForNode(final RsaBlockSigner signer, final long nodeId)
            throws GeneralSecurityException {
        String rsaPubKey = null;
        for (final NodeAddress candidate : signer.verificationMaterial()
                .addressBookHistory()
                .addressBooks()
                .getFirst()
                .addressBook()
                .nodeAddress()) {
            if (candidate.nodeId() == nodeId) {
                rsaPubKey = candidate.rsaPubKey();
                break;
            }
        }
        if (rsaPubKey == null) {
            throw new IllegalStateException("no address book entry for node " + nodeId);
        }
        final byte[] der = HexFormat.of().parseHex(rsaPubKey);
        return KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(der));
    }

    private static boolean verify(final PublicKey publicKey, final byte[] payload, final byte[] signatureBytes)
            throws GeneralSecurityException {
        final Signature engine = Signature.getInstance("SHA384withRSA");
        engine.initVerify(publicKey);
        engine.update(payload);
        return engine.verify(signatureBytes);
    }
}
