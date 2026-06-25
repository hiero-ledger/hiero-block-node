// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.node.block.verification.VerificationHelper.V_0_73_0;
import static org.hiero.block.node.block.verification.VerificationHelper.isVersionGreaterOrEqualTo;

import com.hedera.hapi.block.stream.BlockProof;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.hasher.HashingResult;
import org.hiero.block.node.block.verification.metrics.ProofVerificationMetrics;
import org.hiero.block.node.block.verification.session.SessionFailureType;
import org.hiero.block.node.block.verification.session.VerificationSessionFailedException;

/// Block Verifier.
/// This is the second stage of a [org.hiero.block.node.block.verification.session.CompletableVerificationSession].
/// If the block has passed the hashing stage, we can now verify the block proofs.
public final class BlockVerifier implements Function<HashingResult, BlockVerificationResult> {
    private static final System.Logger LOGGER = System.getLogger(BlockVerifier.class.getName());
    private final ProofVerificationMetrics proofVerificationMetrics;
    private final long sessionStartTime;
    private final VerificationDataProvider verificationDataProvider;

    /// Constructor.
    public BlockVerifier(
            final AtomicBoolean isCanceled, // todo(2528) utilize isCanceled?
            final ProofVerificationMetrics proofVerificationMetrics,
            final long sessionStartTime,
            final VerificationDataProvider verificationDataProvider) {
        this.proofVerificationMetrics = Objects.requireNonNull(proofVerificationMetrics);
        this.sessionStartTime = sessionStartTime;
        this.verificationDataProvider = Objects.requireNonNull(verificationDataProvider);
    }

    /// Accept a valid [HashingResult] and verify the block proofs.
    /// When a block was successfully hashed, we then proceed to gather all proofs and pass verification
    /// on them. If any proof verification fails, the verification is unsuccessful. Otherwise, we pass
    /// verification and can continue forward.
    /// @throws VerificationSessionFailedException when a known failure occurs
    @Override
    public BlockVerificationResult apply(final HashingResult hashingResult) {
        try {
            if (isVersionGreaterOrEqualTo(hashingResult.hapiProtoVersion(), V_0_73_0)) {
                final List<ProofVerifier> verifiers = createVerifiers(hashingResult);
                if (verifiers.isEmpty()) {
                    throw new VerificationSessionFailedException(
                            hashingResult.blockNumber(),
                            SessionFailureType.MISSING_MANDATORY_ITEM,
                            hashingResult.blockSource());
                } else {
                    for (final ProofVerifier verifier : verifiers) {
                        final SessionFailureType result = verifier.verify();
                        if (result != null) {
                            throw new VerificationSessionFailedException(
                                    hashingResult.blockNumber(), result, hashingResult.blockSource());
                        }
                    }
                    // todo(2528) consider the below metric to only calculate verification time, not since
                    //    session start? It now does what the other plugin does
                    final long verificationTimeElapsed = System.nanoTime() - sessionStartTime;
                    proofVerificationMetrics.verificationBlockTime().increment(verificationTimeElapsed);
                    return createVerificationResult(hashingResult);
                }
            } else {
                throw new VerificationSessionFailedException(
                        hashingResult.blockNumber(),
                        SessionFailureType.UNSUPPORTED_HAPI_VERSION,
                        hashingResult.blockSource());
            }
        } catch (final NoSuchAlgorithmException e) {
            throw new VerificationSessionFailedException(
                    hashingResult.blockNumber(),
                    SessionFailureType.MISSING_VERIFICATION_DATA,
                    hashingResult.blockSource(),
                    e);
        }
    }

    /// Create a [ProofVerifier] for each proof in the block.
    /// If an unrecognized proof type is seen, we continue.
    /// @return a [List] of [ProofVerifier]s
    private List<ProofVerifier> createVerifiers(final HashingResult hashingResult) throws NoSuchAlgorithmException {
        final List<ProofVerifier> verifiers = new ArrayList<>();
        // If we have no proofs, or we do not find any recognized, we return an empty verifiers list.
        for (final BlockProof proof : hashingResult.blockProofs()) {
            if (proof.hasSignedBlockProof()) {
                verifiers.add(new TSSVerifier(
                        proofVerificationMetrics,
                        hashingResult.rootHash(),
                        proof.signedBlockProof().blockSignature(),
                        verificationDataProvider));
            } else if (proof.hasSignedRecordFileProof()) {
                final Signature sha384WithRSASig = Signature.getInstance("SHA384withRSA");
                verifiers.add(new RSAProofVerifier(
                        proofVerificationMetrics,
                        hashingResult.blockNumber(),
                        verificationDataProvider.currentRSAPublicKeys(),
                        proof.signedRecordFileProof(),
                        hashingResult.signedWRBPayload(),
                        sha384WithRSASig));
            } else if (proof.hasBlockStateProof()) {
                verifiers.add(new StateProofVerifier(
                        proofVerificationMetrics,
                        hashingResult.blockNumber(),
                        proof.blockStateProof(),
                        hashingResult.rootHash(),
                        verificationDataProvider));
            } else {
                // No recognized proof type found
                LOGGER.log(WARNING, "No recognised proof type in block {0}", hashingResult.blockNumber());
                // todo(2528) increment a metric here
            }
        }
        return verifiers;
    }

    /// Create a [BlockVerificationResult] from the [HashingResult].
    /// @return a [BlockVerificationResult] created from [HashingResult] input
    private BlockVerificationResult createVerificationResult(final HashingResult hashingResult) {
        return new BlockVerificationResult(
                hashingResult.blockNumber(),
                hashingResult.rootHash(),
                hashingResult.block(),
                hashingResult.blockSource());
    }
}
