// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

/// Implemented by a throttled-service wrapper so a caller (see `ServiceBuilderImpl`) can run the
/// periodic maintenance sweeps uniformly across every throttled service.
public interface StaleClientSweepable {
    /// Removes idle, not-in-flight client-state entries; see the implementing class for the
    /// precise eviction rule.
    ///
    /// @param nowNanos the current time from a monotonic clock (e.g. [System#nanoTime()])
    /// @return the number of entries evicted
    int sweepStaleClients(long nowNanos);

    /// Force-releases concurrency permits held longer than their method's
    /// [PerClientThrottleSettings#maxPermitHoldSeconds], so a call that never signals completion
    /// (e.g. its connection dropped mid-request) cannot hold a permit forever. Releasing is
    /// idempotent: a late real completion after a reap is a no-op. The call itself is untouched.
    ///
    /// @param nowNanos the current time from a monotonic clock (e.g. [System#nanoTime()])
    /// @return the number of permits force-released
    int reapExpiredPermits(long nowNanos);

    /// @return the smallest non-zero `maxPermitHoldSeconds` across this service's methods, or `0`
    ///     if no method reaps; the caller derives how often to run [#reapExpiredPermits] from it
    int smallestPermitHoldSeconds();
}
