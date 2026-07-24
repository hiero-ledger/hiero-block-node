// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.block.stream.SignedRecordFileProof;
import java.security.InvalidKeyException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.node.block.verification.metrics.ProofVerificationMetrics;
import org.hiero.block.node.block.verification.session.SessionFailureType;

/// RSA proof verifier.
public final class RSAProofVerifier implements ProofVerifier {
    /// Logger for the verifier.
    private static final System.Logger LOGGER = System.getLogger(RSAProofVerifier.class.getName());
    /// Cancellation flag shared with the owning session.
    private final AtomicBoolean isCanceled;
    /// Metrics for proof verification results.
    private final ProofVerificationMetrics proofVerificationMetrics;
    /// The number of the block being verified, used for logging.
    private final long blockNumber;
    /// Map from `node_id` to RSA [PublicKey], resolved from the address book era covering the block.
    private final Map<Long, PublicKey> rsaKeyByNodeId;
    /// The signed record file proof to verify.
    private final SignedRecordFileProof proof;
    /// The record file format version declared by the proof.
    private final int version;
    /// The precomputed V6 signed payload, `SHA-384(int32(6) || record_file_contents)`,
    /// produced by the hashing stage. Null when the block carried no `RECORD_FILE` item.
    private final byte[] signedWRBPayload;
    /// The `SHA384withRSA` signature engine used to verify each signature.
    private final Signature sha384WithRSA;

    /// Constructor.
    ///
    /// @param isCanceled cancellation flag shared with the owning session, must not be null
    /// @param proofVerificationMetrics metrics for proof verification results, must not be null
    /// @param blockNumber the number of the block being verified
    /// @param rsaKeyByNodeId map from `node_id` to RSA [PublicKey] for the era covering the
    ///     block; an empty map fails verification, must not be null
    /// @param proof the signed record file proof to verify
    /// @param signedWRBPayload the precomputed V6 signed payload from the hashing stage,
    ///     may be null when the block carried no `RECORD_FILE` item
    /// @param sha384WithRSA the `SHA384withRSA` signature engine, must not be null
    public RSAProofVerifier(
            final AtomicBoolean isCanceled,
            final ProofVerificationMetrics proofVerificationMetrics,
            final long blockNumber,
            final Map<Long, PublicKey> rsaKeyByNodeId,
            final SignedRecordFileProof proof,
            final byte[] signedWRBPayload,
            final Signature sha384WithRSA) {
        this.isCanceled = Objects.requireNonNull(isCanceled);
        this.proofVerificationMetrics = Objects.requireNonNull(proofVerificationMetrics);
        this.blockNumber = blockNumber;
        this.rsaKeyByNodeId = Objects.requireNonNull(rsaKeyByNodeId);
        this.proof = proof;
        this.version = proof.version();
        this.signedWRBPayload = signedWRBPayload;
        this.sha384WithRSA = Objects.requireNonNull(sha384WithRSA);
    }

    /// {@inheritDoc}
    /// ---
    /// Verifies a `SignedRecordFileProof` (WRB RSA proof).
    ///
    /// **Algorithm (V6 only for Phase 2a):**
    /// 1. Compute the block root hash for chain continuity (identical to the TSS path).
    /// 2. Extract `record_file_contents` bytes (proto field 2) from the captured `RECORD_FILE` item.
    /// 3. Compute the signed payload: `SHA-384(int32(6) || rawRecordStreamFileBytes)`.
    /// 4. For each `RecordFileSignature` entry:
    ///    - Skip if `node_id` not in `rsaKeyByNodeId` (increment roster-mismatch counter).
    ///    - Skip if signature bytes are all zeros (defensive pre-filter).
    ///    - Verify with `SHA384withRSA`. If verification fails or throws, **reject the block
    ///      immediately** - the CN only includes signatures from nodes that contributed to
    ///      consensus, so any included signature must be cryptographically valid.
    /// 5. Accept if at least one signature verified. Signatures from nodes not in
    ///      the local roster are skipped (see @todo(2808)) and tallied into a single
    ///      batched `rsa_roster_mismatch_total` increment; step 4 already rejects the
    ///      block on any failed signature from a known node, so reaching this step
    ///      means every verifiable signature passed.
    @Override
    public SessionFailureType verify() {
        final SessionFailureType result;
        // Guard: no era in the address book history covers this block number
        if (rsaKeyByNodeId.isEmpty()) {
            LOGGER.log(
                    WARNING,
                    "No address book era covers block {0} - cannot verify RSA WRB proof."
                            + " Ensure rsa-address-book-history.json is loaded and covers this block number.",
                    blockNumber);
            result = SessionFailureType.MISSING_VERIFICATION_DATA;
        } else if (signedWRBPayload == null) {
            // Guard: RECORD_FILE item must be present in the block
            LOGGER.log(WARNING, "WRB block {0} missing signed payload", blockNumber);
            result = SessionFailureType.MISSING_VERIFICATION_DATA;
        } else if (version != 6) {
            // Phase 2a scope: only record file format version 6 is supported
            LOGGER.log(
                    WARNING,
                    "Unsupported SignedRecordFileProof version {0} in block {1} - only V6 is supported",
                    version,
                    blockNumber);
            result = SessionFailureType.MISSING_MANDATORY_FIELD;
        } else {
            // V6 payload: SHA-384(int32(6) || rawRecordStreamFileBytes)
            // Verify each signature and count valid ones.
            // Track which node_id values have already contributed a valid signature to prevent
            // a duplicate entry in the proof from inflating validCount.
            final int rosterSize = rsaKeyByNodeId.size();
            int validCount = 0;
            int mismatchCount = 0;
            final Set<Long> validatedNodes = new HashSet<>();
            for (final RecordFileSignature sig : proof.recordFileSignatures()) {
                if (isCanceled()) {
                    proofVerificationMetrics.rsaFailure().increment();
                    return SessionFailureType.CANCELLED;
                } else {
                    final long nodeId = sig.nodeId();
                    // uses a historical roster keyed by block number so signatures
                    // from nodes that were valid at the time the block was produced are verified correctly
                    // across address-book transitions, skipping unknown node IDs can still occur.
                    final PublicKey publicKey = rsaKeyByNodeId.get(nodeId);
                    if (publicKey == null) {
                        mismatchCount++;
                        LOGGER.log(
                                DEBUG,
                                "Signature from node {0} not in era address book for block {1} - skipped",
                                nodeId,
                                blockNumber);
                        continue;
                    }
                    if (validatedNodes.contains(nodeId)) {
                        LOGGER.log(
                                DEBUG, "Duplicate signature from node {0} in block {1} - skipped", nodeId, blockNumber);
                        continue;
                    }
                    final byte[] sigBytes = sig.signaturesBytes().toByteArray();
                    if (isAllZeros(sigBytes)) {
                        LOGGER.log(DEBUG, "Zeroed signature from node {0} in block {1} - skipped", nodeId, blockNumber);
                        continue;
                    }
                    try {
                        final Signature engine = sha384WithRSA;
                        engine.initVerify(publicKey);
                        engine.update(signedWRBPayload);
                        if (engine.verify(sigBytes)) {
                            validCount++;
                            validatedNodes.add(nodeId);
                        } else {
                            // CN only includes signatures from consensus-contributing nodes, so a failed
                            // cryptographic verification means the block or proof has been tampered with.
                            LOGGER.log(
                                    DEBUG,
                                    "RSA signature from node {0} failed verification in block {1} - rejecting block",
                                    nodeId,
                                    blockNumber);
                            proofVerificationMetrics.rsaFailure().increment();
                            return SessionFailureType.BAD_BLOCK_PROOF;
                        }
                    } catch (final InvalidKeyException | SignatureException e) {
                        LOGGER.log(
                                WARNING,
                                "RSA verification error for node {0} in block {1}: {2} - rejecting block",
                                nodeId,
                                blockNumber,
                                e.getMessage());
                        proofVerificationMetrics.rsaFailure().increment();
                        return SessionFailureType.BAD_BLOCK_PROOF;
                    }
                }
            }
            if (mismatchCount > 0) {
                proofVerificationMetrics.rsaRosterMismatch().increment(mismatchCount);
            }
            // Acceptance threshold: every signature present in the proof passes validation,
            // and at least one such signature exists. Signatures from nodes not in the local
            // roster are skipped (see @todo(2808)).
            // Because we fail fast on any failed verification, reaching this
            // point means all verifiable signatures passed.
            final boolean accepted = validCount > 0;
            if (accepted) {
                LOGGER.log(
                        DEBUG,
                        "RSA WRB proof accepted for block {0}: {1} valid signatures (roster size {2})",
                        blockNumber,
                        validCount,
                        rosterSize);
                result = null;
            } else {
                LOGGER.log(
                        WARNING,
                        "RSA WRB proof rejected for block {0}: {1} valid signatures (roster size {2})",
                        blockNumber,
                        validCount,
                        rosterSize);
                result = SessionFailureType.BAD_BLOCK_PROOF;
            }
        }
        if (result != null) {
            proofVerificationMetrics.rsaFailure().increment();
        } else {
            proofVerificationMetrics.rsaSuccess().increment();
        }
        return result;
    }

    /// Returns `true` if every byte in `bytes` is zero.
    ///
    /// @param bytes the byte array to inspect
    /// @return `true` when the array is all zeros or empty
    private static boolean isAllZeros(final byte[] bytes) {
        for (final byte b : bytes) {
            if (b != 0) return false;
        }
        return true;
    }

    /// Returns `true` if the owning session has been cancelled or the current thread interrupted.
    private boolean isCanceled() {
        return isCanceled.get() || Thread.currentThread().isInterrupted();
    }
}
