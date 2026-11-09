// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

/// Per-client throttle settings a plugin declares for one gRPC method. Excludes the node-wide
/// concurrency ceiling, which is a separate, centrally-owned concern (see
/// [GlobalConcurrencyGate]).
///
/// @param ratePerSecond sustained requests (or, for streaming APIs, new session opens) per second
///     allowed for one client
/// @param burstTolerance how far ahead of the even-pacing schedule a client's request may arrive
///     and still be admitted, expressed as a multiple of the pacing interval
/// @param maxConcurrentPerClient maximum concurrent in-flight calls/sessions for one client on
///     this method
/// @param maxPermitHoldSeconds age after which a still-held concurrency permit is force-released
///     by the periodic sweep, as a backstop for calls whose completion signal never arrives (e.g.
///     a connection that dropped mid-request); `0` means never reap. This only corrects the
///     throttle's accounting, it never terminates the call.
public record PerClientThrottleSettings(
        int ratePerSecond, int burstTolerance, int maxConcurrentPerClient, int maxPermitHoldSeconds) {

    /// Settings with no permit reaping, for methods (e.g. long-lived streams) where a fixed hold
    /// limit would be wrong.
    ///
    /// @param ratePerSecond see the record component
    /// @param burstTolerance see the record component
    /// @param maxConcurrentPerClient see the record component
    public PerClientThrottleSettings(
            final int ratePerSecond, final int burstTolerance, final int maxConcurrentPerClient) {
        this(ratePerSecond, burstTolerance, maxConcurrentPerClient, 0);
    }
}
