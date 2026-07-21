// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import com.hedera.cryptography.tss.TSS;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Objects;
import org.hiero.block.api.TssData;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.metrics.ProofVerificationMetrics;
import org.hiero.block.node.block.verification.session.SessionFailureType;

/// TSS based proof verifier.
public final class TSSVerifier implements ProofVerifier {
    /// Metrics for proof verification results.
    private final ProofVerificationMetrics proofVerificationMetrics;
    /// The hash the signature must verify against, usually the computed block root hash.
    private final Bytes hashToVerify;
    /// The TSS signature to verify.
    private final Bytes signature;
    /// Provider of the TSS data (ledger id) required for verification.
    private final VerificationDataProvider verificationDataProvider;

    /// Constructor.
    ///
    /// @param proofVerificationMetrics metrics for proof verification results, must not be null
    /// @param hashToVerify the hash the signature must verify against, must not be null
    /// @param signature the TSS signature to verify, must not be null
    /// @param verificationDataProvider provider of the TSS data required for verification, must not be null
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

    /// {@inheritDoc}
    /// ---
    /// Verifies the TSS signature against the hash to verify, using the ledger id
    /// from the currently available [TssData].
    ///
    /// `TSS.verifyTSS()` handles both the genesis (Schnorr aggregate) and
    /// post-genesis (WRAPS) paths. Signatures without a recognized proof suffix
    /// are rejected by the library.
    ///
    /// The success or failure TSS series of the proof verification metrics is
    /// incremented accordingly before returning.
    ///
    /// @return `null` if the signature verifies,
    ///     [SessionFailureType#MISSING_VERIFICATION_DATA] when no TSS data is
    ///     available, or [SessionFailureType#BAD_BLOCK_PROOF] when the signature
    ///     does not verify or is malformed
    @Override
    public SessionFailureType verify() {
        final SessionFailureType result;
        final TssData tssData = verificationDataProvider.currentTssData();
        if (tssData == null) {
            result = SessionFailureType.MISSING_VERIFICATION_DATA;
        } else {
            // TSS.verifyTSS() handles both the genesis (Schnorr aggregate) and post-genesis (WRAPS) paths.
            // Signatures without a recognized proof suffix are rejected by the library.
            try {
                if (!TSS.verifyTSS(
                        tssData.ledgerId().toByteArray(), signature.toByteArray(), hashToVerify.toByteArray())) {
                    result = SessionFailureType.BAD_BLOCK_PROOF;
                } else {
                    result = null;
                }
            } catch (final IllegalArgumentException | IllegalStateException e) {
                proofVerificationMetrics.tssFailure().increment();
                return SessionFailureType.BAD_BLOCK_PROOF;
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
