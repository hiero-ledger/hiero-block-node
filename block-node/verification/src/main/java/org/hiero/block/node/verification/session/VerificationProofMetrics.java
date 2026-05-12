// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.hiero.metrics.LongCounter;

/**
 * Holder for the per-block-proof verification counters used by the verification session.
 *
 * <p>The six success/failure {@link LongCounter.Measurement} fields are pre-resolved
 * measurements of a single {@code verification_proof_total} counter with the
 * {@code proof_type} and {@code result} dynamic labels, plus the standalone
 * {@code rsa_roster_mismatch_total} counter. All fields may be {@code null} when
 * metrics are not configured (e.g. in tests), in which case {@link #NONE} should be used.
 *
 * @param rsaSuccess         {@code verification_proof_total{proof_type="rsa",result="success"}}
 * @param rsaFailure         {@code verification_proof_total{proof_type="rsa",result="failure"}}
 * @param stateProofSuccess  {@code verification_proof_total{proof_type="state_proof",result="success"}}
 * @param stateProofFailure  {@code verification_proof_total{proof_type="state_proof",result="failure"}}
 * @param tssSuccess         {@code verification_proof_total{proof_type="tss",result="success"}}
 * @param tssFailure         {@code verification_proof_total{proof_type="tss",result="failure"}}
 * @param rsaRosterMismatch  {@code rsa_roster_mismatch_total} — RSA signatures from node IDs
 *                           absent from the loaded address book
 */
public record VerificationProofMetrics(
        @Nullable LongCounter.Measurement rsaSuccess,
        @Nullable LongCounter.Measurement rsaFailure,
        @Nullable LongCounter.Measurement stateProofSuccess,
        @Nullable LongCounter.Measurement stateProofFailure,
        @Nullable LongCounter.Measurement tssSuccess,
        @Nullable LongCounter.Measurement tssFailure,
        @Nullable LongCounter.Measurement rsaRosterMismatch) {

    /** All-null instance for use in tests and code paths where metrics are not wired. */
    public static final VerificationProofMetrics NONE =
            new VerificationProofMetrics(null, null, null, null, null, null, null);

    /** Increment {@code measurement} by 1 when non-null. */
    private static void increment(@Nullable final LongCounter.Measurement measurement) {
        if (measurement != null) {
            measurement.increment();
        }
    }

    /** Increment {@code measurement} by {@code value} when non-null. */
    private static void increment(@Nullable final LongCounter.Measurement measurement, final long value) {
        if (measurement != null) {
            measurement.increment(value);
        }
    }

    /** Records a successful RSA WRB proof verification. */
    public void incrementRsaSuccess() {
        increment(rsaSuccess);
    }

    /** Records a failed RSA WRB proof verification. */
    public void incrementRsaFailure() {
        increment(rsaFailure);
    }

    /** Records a successful state-proof verification. */
    public void incrementStateProofSuccess() {
        increment(stateProofSuccess);
    }

    /** Records a failed state-proof verification. */
    public void incrementStateProofFailure() {
        increment(stateProofFailure);
    }

    /** Records a successful TSS-signed block proof verification. */
    public void incrementTssSuccess() {
        increment(tssSuccess);
    }

    /** Records a failed TSS-signed block proof verification. */
    public void incrementTssFailure() {
        increment(tssFailure);
    }

    /** Records {@code count} RSA signatures from node IDs absent from the loaded address book. */
    public void incrementRsaRosterMismatch(final long count) {
        increment(rsaRosterMismatch, count);
    }
}
