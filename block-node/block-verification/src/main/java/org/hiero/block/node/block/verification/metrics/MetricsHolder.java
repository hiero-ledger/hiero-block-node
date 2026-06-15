// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.metrics;

import org.hiero.metrics.core.MetricRegistry;

/// Holder for all metrics used in verification.
/// @param sessionHandlerMetrics metrics used by [org.hiero.block.node.block.verification.session.BlockSessionHandler]
/// @param hashingMetrics metrics used by [org.hiero.block.node.block.verification.hasher.BlockHasher]
/// @param proofVerificationMetrics metrics used by [org.hiero.block.node.block.verification.verifier.BlockVerifier]
/// @param sessionResultMetrics metrics used by [org.hiero.block.node.block.verification.session.SessionResultHandler]
public record MetricsHolder(
        SessionHandlerMetrics sessionHandlerMetrics,
        HashingMetrics hashingMetrics,
        ProofVerificationMetrics proofVerificationMetrics,
        SessionResultMetrics sessionResultMetrics) {
    /// Initialize and return a new [MetricsHolder] instance.
    /// @param metricRegistry used to create and initialize metrics
    /// @return a new [MetricsHolder] instance fully initialized
    public static MetricsHolder create(MetricRegistry metricRegistry) {
        return new MetricsHolder(
                SessionHandlerMetrics.create(metricRegistry),
                HashingMetrics.create(metricRegistry),
                ProofVerificationMetrics.create(metricRegistry),
                SessionResultMetrics.create(metricRegistry));
    }
}
