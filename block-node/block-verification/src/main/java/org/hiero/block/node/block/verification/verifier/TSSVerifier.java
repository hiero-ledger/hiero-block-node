// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import com.hedera.cryptography.tss.TSS;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Objects;
import org.hiero.block.api.TssData;
import org.hiero.block.common.hasher.HashingUtilities;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.metrics.ProofVerificationMetrics;
import org.hiero.block.node.block.verification.session.SessionFailureType;

/// TSS based proof verifier.
public final class TSSVerifier implements ProofVerifier {
    private static final int HASH_LENGTH = 48;
    private final ProofVerificationMetrics proofVerificationMetrics;
    private final Bytes hashToVerify;
    private final Bytes signature;
    private final VerificationDataProvider verificationDataProvider;

    /// Constructor.
    public TSSVerifier(
            final ProofVerificationMetrics proofVerificationMetrics,
            final Bytes hashToVerify,
            final Bytes signature,
            final VerificationDataProvider verificationDataProvider) {
        this.proofVerificationMetrics = Objects.requireNonNull(proofVerificationMetrics);
        this.hashToVerify = Objects.requireNonNull(hashToVerify);
        this.signature = Objects.requireNonNull(signature);
        this.verificationDataProvider = Objects.requireNonNull(verificationDataProvider);
    }

    /// todo(2528) add documentation
    @Override
    public SessionFailureType verify() {
        final SessionFailureType result;
        if (signature.length() == HASH_LENGTH) {
            // Legacy path: non-TSS blocks carry SHA384(blockHash) as the signature (48 bytes).
            // todo(2528) Remove this path before production — real network block proofs are never hash-of-hash.
            if (!signature.equals(HashingUtilities.noThrowSha384HashOf(hashToVerify))) {
                result = SessionFailureType.SIGNATURE_MISMATCH;
            } else {
                result = null;
            }
        } else {
            final TssData tssData = verificationDataProvider.currentTssData();
            if (tssData == null) {
                result = SessionFailureType.MISSING_VERIFICATION_DATA;
            } else {
                // TSS.verifyTSS() handles both the genesis (Schnorr aggregate) and post-genesis (WRAPS) paths.
                // Signatures without a recognized proof suffix are rejected by the library.
                // todo(2528) check for currentTssData.validFromBlock()? Fail in case?
                try {
                    if (!TSS.verifyTSS(
                            tssData.ledgerId().toByteArray(), signature.toByteArray(), hashToVerify.toByteArray())) {
                        result = SessionFailureType.SIGNATURE_MISMATCH;
                    } else {
                        result = null;
                    }
                } catch (final IllegalStateException e) {
                    // TSS static state (e.g. Schnorr public keys) not yet populated
                    proofVerificationMetrics.tssFailure().increment();
                    return SessionFailureType.MISSING_VERIFICATION_DATA;
                } catch (final IllegalArgumentException e) {
                    proofVerificationMetrics.tssFailure().increment();
                    return SessionFailureType.MALFORMED_PROOF_STRUCTURE;
                }
            }
        }
        if (result != null) {
            proofVerificationMetrics.tssFailure().increment();
        } else {
            proofVerificationMetrics.tssSuccess().increment();
        }
        return result;
    }
}
