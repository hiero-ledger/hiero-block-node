// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.metrics;

import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import org.hiero.metrics.LongCounter;
import org.hiero.metrics.LongCounter.Measurement;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Holder for metrics used by [org.hiero.block.node.block.verification.verifier.BlockVerifier].
///
/// The six success/failure [LongCounter.Measurement] fields are pre-resolved
/// measurements of a single `verification_proof_total` counter with the
/// `proof_type` and `result` dynamic labels, plus the standalone
/// `rsa_roster_mismatch_total` counter.
///
/// @param rsaSuccess         `verification_proof_total{proof_type="rsa",result="success"}`
/// @param rsaFailure         `verification_proof_total{proof_type="rsa",result="failure"}`
/// @param stateProofSuccess  `verification_proof_total{proof_type="state_proof",result="success"}`
/// @param stateProofFailure  `verification_proof_total{proof_type="state_proof",result="failure"}`
/// @param tssSuccess         `verification_proof_total{proof_type="tss",result="success"}`
/// @param tssFailure         `verification_proof_total{proof_type="tss",result="failure"}`
/// @param rsaRosterMismatch  `rsa_roster_mismatch_total` — RSA signatures from node IDs
///                           absent from the loaded address book
/// @param verificationBlockTime time it takes for a block to be completely verified, starts when the session
///    is started, includes time to hash and to complete proof verification. Excludes time waiting in order manager.
public record ProofVerificationMetrics(
        Measurement rsaSuccess,
        Measurement rsaFailure,
        Measurement stateProofSuccess,
        Measurement stateProofFailure,
        Measurement tssSuccess,
        Measurement tssFailure,
        Measurement rsaRosterMismatch,
        Measurement verificationBlockTime) {
    /// Metric for total block proof verifications
    private static final MetricKey<LongCounter> METRIC_VERIFICATION_PROOF_TOTAL =
            MetricKey.of("verification_proof_total", LongCounter.class).addCategory(METRICS_CATEGORY);
    /// Metric key for signatures from `node_id` values not present in the loaded address book.
    private static final MetricKey<LongCounter> METRIC_RSA_ROSTER_MISMATCH =
            MetricKey.of("rsa_roster_mismatch_total", LongCounter.class).addCategory(METRICS_CATEGORY);
    /// Metric key for block verification time
    private static final MetricKey<LongCounter> METRIC_VERIFICATION_BLOCK_TIME =
            MetricKey.of("verification_block_time", LongCounter.class).addCategory(METRICS_CATEGORY);
    /// Dynamic label name identifying the block proof type.
    private static final String LABEL_PROOF_TYPE = "proof_type";
    /// Dynamic label name identifying the verification result.
    private static final String LABEL_RESULT = "result";
    /// Label value for `SignedRecordFileProof` (WRB RSA) verification.
    private static final String PROOF_TYPE_RSA = "rsa";
    /// Label value for `BlockStateProof` (indirect) verification.
    private static final String PROOF_TYPE_STATE_PROOF = "state_proof";
    /// Label value for `SignedBlockProof` (TSS) verification.
    private static final String PROOF_TYPE_TSS = "tss";
    /// Label value for a successful proof verification.
    private static final String RESULT_SUCCESS = "success";
    /// Label value for rejected proof verifications.
    private static final String RESULT_FAILURE = "failure";

    /// Initialize and return a new [ProofVerificationMetrics] instance.
    /// @param metricRegistry used to create and initialize metrics
    /// @return a new [ProofVerificationMetrics] instance fully initialized
    public static ProofVerificationMetrics create(final MetricRegistry metricRegistry) {
        final LongCounter proofResultCounter =
                metricRegistry.register(LongCounter.builder(METRIC_VERIFICATION_PROOF_TOTAL)
                        .setDescription(
                                "Block proof verifications by proof_type (rsa|state_proof|tss) and result (success|failure)")
                        .addDynamicLabelNames(LABEL_PROOF_TYPE, LABEL_RESULT));
        final Measurement rsaRosterMismatch = metricRegistry
                .register(LongCounter.builder(METRIC_RSA_ROSTER_MISMATCH)
                        .setDescription("RSA signatures from node_id values absent from the loaded address book"))
                .getOrCreateNotLabeled();
        final Measurement rsaSuccess =
                proofResultCounter.getOrCreateLabeled(LABEL_PROOF_TYPE, PROOF_TYPE_RSA, LABEL_RESULT, RESULT_SUCCESS);
        final Measurement rssFailure =
                proofResultCounter.getOrCreateLabeled(LABEL_PROOF_TYPE, PROOF_TYPE_RSA, LABEL_RESULT, RESULT_FAILURE);
        final Measurement stateProofSuccess = proofResultCounter.getOrCreateLabeled(
                LABEL_PROOF_TYPE, PROOF_TYPE_STATE_PROOF, LABEL_RESULT, RESULT_SUCCESS);
        final Measurement stateProofFailure = proofResultCounter.getOrCreateLabeled(
                LABEL_PROOF_TYPE, PROOF_TYPE_STATE_PROOF, LABEL_RESULT, RESULT_FAILURE);
        final Measurement tssSuccess =
                proofResultCounter.getOrCreateLabeled(LABEL_PROOF_TYPE, PROOF_TYPE_TSS, LABEL_RESULT, RESULT_SUCCESS);
        final Measurement tssFailure =
                proofResultCounter.getOrCreateLabeled(LABEL_PROOF_TYPE, PROOF_TYPE_TSS, LABEL_RESULT, RESULT_FAILURE);
        final Measurement verificationBlockTime = metricRegistry
                .register(LongCounter.builder(METRIC_VERIFICATION_BLOCK_TIME)
                        .setDescription("Verification time per block (ms)"))
                .getOrCreateNotLabeled();
        return new ProofVerificationMetrics(
                rsaSuccess,
                rssFailure,
                stateProofSuccess,
                stateProofFailure,
                tssSuccess,
                tssFailure,
                rsaRosterMismatch,
                verificationBlockTime);
    }
}
