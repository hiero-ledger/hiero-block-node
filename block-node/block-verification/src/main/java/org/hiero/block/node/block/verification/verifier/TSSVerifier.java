// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static java.lang.System.Logger.Level.WARNING;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Objects;
import org.hiero.block.api.TssData;
import org.hiero.block.node.block.verification.NativeVerificationLibrary;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.metrics.ProofVerificationMetrics;
import org.hiero.block.node.block.verification.session.SessionFailureType;

/// TSS based proof verifier.
public final class TSSVerifier implements ProofVerifier {
    /// Logger for the verifier.
    private static final System.Logger LOGGER = System.getLogger(TSSVerifier.class.getName());
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
    /// from the currently available [TssData]. The ledger id is the only input the
    /// TSS library needs besides the signature and the hash; it embeds everything
    /// else it verifies against.
    ///
    /// The library rejects a ledger id or signature it considers malformed with an
    /// `IllegalArgumentException`. That is reported as a bad block proof and logged
    /// with the library's reason, since it usually means the TSS data comes from a
    /// network that used an earlier TSS library (see the reset procedure in the
    /// Block Node docs) or the publisher sent a signature of another format.
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
            result = verifyWithLibrary(tssData);
        }
        if (result != null) {
            proofVerificationMetrics.tssFailure().increment();
        } else {
            proofVerificationMetrics.tssSuccess().increment();
        }
        return result;
    }

    /// Calls the TSS library. A malformed input is reported by the library as an
    /// exception rather than a `false` result; it is logged with the library's
    /// reason and reported as a bad block proof like any other signature that does
    /// not verify.
    ///
    /// @param tssData the current TSS data providing the ledger id
    /// @return `null` if the signature verifies, [SessionFailureType#BAD_BLOCK_PROOF] otherwise
    private SessionFailureType verifyWithLibrary(final TssData tssData) {
        SessionFailureType result = SessionFailureType.BAD_BLOCK_PROOF;
        try {
            final boolean verified = NativeVerificationLibrary.verifyTss(
                    tssData.ledgerId().toByteArray(), signature.toByteArray(), hashToVerify.toByteArray());
            if (verified) {
                result = null;
            } else {
                result = SessionFailureType.BAD_BLOCK_PROOF;
            }
        } catch (final IllegalArgumentException | IllegalStateException e) {
            LOGGER.log(
                    WARNING,
                    "The TSS library %s rejected the block signature or the ledger id of the TSS data valid from block %d"
                            .formatted(NativeVerificationLibrary.libraryVersion(), tssData.validFromBlock()),
                    e);
        }
        return result;
    }
}
