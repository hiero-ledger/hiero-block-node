// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import com.hedera.cryptography.tss.TSS;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Objects;
import org.hiero.block.api.TssData;
import org.hiero.block.node.block.verification.NativeVerificationLibraryLock;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.metrics.ProofVerificationMetrics;
import org.hiero.block.node.block.verification.session.SessionFailureType;

/// TSS based proof verifier.
public final class TSSVerifier implements ProofVerifier {
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

    /// todo(2879) add documentation
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
                final boolean verified;
                // Serialize the native call: the hinTS/WRAPS library is a JVM-wide singleton and is
                // not thread-safe, so it must never run concurrently with another verify or a TSS
                // data update (see NativeVerificationLibraryLock).
                synchronized (NativeVerificationLibraryLock.LOCK) {
                    verified = TSS.verifyTSS(
                            tssData.ledgerId().toByteArray(), signature.toByteArray(), hashToVerify.toByteArray());
                }
                if (!verified) {
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
