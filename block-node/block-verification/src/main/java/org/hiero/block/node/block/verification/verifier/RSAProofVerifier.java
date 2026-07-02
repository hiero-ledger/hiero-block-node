// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
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
import org.hiero.block.node.block.verification.metrics.ProofVerificationMetrics;
import org.hiero.block.node.block.verification.session.SessionFailureType;

/// RSA proof verifier.
public final class RSAProofVerifier implements ProofVerifier {
    private static final System.Logger LOGGER = System.getLogger(RSAProofVerifier.class.getName());
    private final ProofVerificationMetrics proofVerificationMetrics;
    private final long blockNumber;
    private final Map<Long, PublicKey> rsaKeyByNodeId;
    private final SignedRecordFileProof proof;
    private final int version;
    private final byte[] signedWRBPayload;
    private final Signature sha384WithRSA;

    /// Constructor.
    public RSAProofVerifier(
            final ProofVerificationMetrics proofVerificationMetrics,
            final long blockNumber,
            final Map<Long, PublicKey> rsaKeyByNodeId,
            final SignedRecordFileProof proof,
            final byte[] signedWRBPayload,
            final Signature sha384WithRSA) {
        this.proofVerificationMetrics = Objects.requireNonNull(proofVerificationMetrics);
        this.blockNumber = blockNumber;
        this.rsaKeyByNodeId = Objects.requireNonNull(rsaKeyByNodeId);
        this.proof = proof;
        this.version = proof.version();
        this.signedWRBPayload = Objects.requireNonNull(signedWRBPayload);
        this.sha384WithRSA = Objects.requireNonNull(sha384WithRSA);
    }

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
    ///      immediately** — the CN only includes signatures from nodes that contributed to
    ///      consensus, so any included signature must be cryptographically valid.
    /// 5. Accept if all signatures in the proof verified. Signatures from nodes not in
    ///      the local roster are skipped (see @todo(2808));
    ///      step 4 already rejects the block on any failed signature from a
    ///      known node, so reaching this step means every verifiable signature
    ///      passed; we only need to confirm at least one such signature exists.
    @Override
    public SessionFailureType verify() {
        final SessionFailureType result;
        // Guard: no era in the address book history covers this block number
        if (rsaKeyByNodeId.isEmpty()) {
            LOGGER.log(
                    ERROR,
                    "No address book era covers block {0} — cannot verify RSA WRB proof."
                            + " Ensure rsa-address-book-history.json is loaded and covers this block number.",
                    blockNumber);
            result = SessionFailureType.MISSING_VERIFICATION_DATA;
        } else if (signedWRBPayload == null) {
            // Guard: RECORD_FILE item must be present in the block
            LOGGER.log(
                    ERROR, "No RECORD_FILE item found in WRB block {0} — cannot compute signed payload", blockNumber);
            result = SessionFailureType.MISSING_MANDATORY_ITEM;
        } else if (version != 6) {
            // Phase 2a scope: only record file format version 6 is supported
            LOGGER.log(
                    WARNING,
                    "Unsupported SignedRecordFileProof version {0} in block {1} — only V6 is supported",
                    version,
                    blockNumber);
            result = SessionFailureType.MISSING_MANDATORY_ITEM; // todo(2528) consider type to be unsupported version
        } else {
            // V6 payload: SHA-384(int32(6) || rawRecordStreamFileBytes)
            // Verify each signature and count valid ones.
            // Track which node_id values have already contributed a valid signature to prevent
            // a duplicate entry in the proof from inflating validCount.
            final int rosterSize = rsaKeyByNodeId.size();
            int validCount = 0;
            final Set<Long> validatedNodes = new HashSet<>();
            for (final RecordFileSignature sig : proof.recordFileSignatures()) {
                final long nodeId = sig.nodeId();
                final PublicKey publicKey = rsaKeyByNodeId.get(nodeId);
                if (publicKey == null) {
                    LOGGER.log(
                            DEBUG,
                            "Signature from node {0} not in era address book for block {1} — rejecting block",
                            nodeId,
                            blockNumber);
                    proofVerificationMetrics.rsaFailure().increment();
                    return SessionFailureType.BAD_BLOCK_PROOF;
                }
                if (validatedNodes.contains(nodeId)) {
                    LOGGER.log(DEBUG, "Duplicate signature from node {0} in block {1} — skipped", nodeId, blockNumber);
                    continue;
                }
                final byte[] sigBytes = sig.signaturesBytes().toByteArray();
                if (isAllZeros(sigBytes)) {
                    LOGGER.log(DEBUG, "Zeroed signature from node {0} in block {1} — skipped", nodeId, blockNumber);
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
                                "RSA signature from node {0} failed verification in block {1} — rejecting block",
                                nodeId,
                                blockNumber);
                        proofVerificationMetrics.rsaFailure().increment();
                        return SessionFailureType.BAD_BLOCK_PROOF;
                    }
                } catch (final InvalidKeyException | SignatureException e) {
                    LOGGER.log(
                            WARNING,
                            "RSA verification error for node {0} in block {1}: {2} — rejecting block",
                            nodeId,
                            blockNumber,
                            e.getMessage());
                    // todo(2528) consider to simply not catch here at all but propagate, also what is the
                    //    right type of failure here?
                    proofVerificationMetrics.rsaFailure().increment();
                    return SessionFailureType.UNKNOWN_ERROR;
                }
            }
            // Acceptance threshold: every signature present in the proof passes validation,
            // and at least one such signature exists. Because we fail fast on any failed
            // verification, reaching this point means all verifiable signatures passed.
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
}
