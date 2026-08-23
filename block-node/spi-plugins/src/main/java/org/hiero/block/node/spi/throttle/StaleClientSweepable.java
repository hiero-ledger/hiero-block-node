// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

/// Implemented by every throttled-service wrapper so a caller (see `ServiceBuilderImpl`) can run
/// the periodic stale-client-state sweep uniformly, regardless of whether a service has one
/// policy ([ThrottledServiceInterface]) or several, one per [WeightClass]
/// ([WeightedThrottledServiceInterface]).
public interface StaleClientSweepable {
    /// Removes idle, not-in-flight client-state entries — see the implementing class for the
    /// precise eviction rule.
    ///
    /// @param nowNanos the current time from a monotonic clock (e.g. [System#nanoTime()])
    /// @return the number of entries evicted
    int sweepStaleClients(long nowNanos);
}
