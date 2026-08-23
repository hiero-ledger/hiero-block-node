// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

/// The fully-resolved runtime admission-control policy for one gRPC method: a plugin's own
/// [PerClientThrottleSettings] merged with the node-wide concurrency ceiling for that method.
///
/// @param ratePerSecond sustained requests (or new session opens, for streaming APIs) per second
///     allowed for one client
/// @param burstTolerance how far ahead of the even-pacing schedule a client's request may arrive
///     and still be admitted, expressed as a multiple of the pacing interval
/// @param maxConcurrentPerClient maximum concurrent in-flight calls/sessions for one client on
///     this method
/// @param maxConcurrentGlobal maximum concurrent in-flight calls/sessions across all clients on
///     this method
public record ThrottlePolicy(
        int ratePerSecond, int burstTolerance, int maxConcurrentPerClient, int maxConcurrentGlobal) {

    /// Merges a plugin's own per-client settings with the node-wide ceiling for the same method.
    ///
    /// @param settings the plugin-owned per-client settings
    /// @param maxConcurrentGlobal the node-wide concurrency ceiling for this method
    /// @return the merged runtime policy
    public static ThrottlePolicy merge(final PerClientThrottleSettings settings, final int maxConcurrentGlobal) {
        return new ThrottlePolicy(
                settings.ratePerSecond(),
                settings.burstTolerance(),
                settings.maxConcurrentPerClient(),
                maxConcurrentGlobal);
    }
}
