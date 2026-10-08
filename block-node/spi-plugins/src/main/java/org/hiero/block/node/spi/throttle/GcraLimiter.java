// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import java.util.concurrent.atomic.AtomicLong;

/// A lock-free rate limiter for one client implementing the Generic Cell Rate Algorithm (GCRA),
/// the leaky-bucket-equivalent algorithm described in `docs/design/apis/api-throttling.md`.
///
/// GCRA tracks a single value, the "theoretical arrival time" (TAT): the earliest instant the
/// next request would be allowed if traffic were perfectly evenly spaced. A request is admitted
/// if it does not arrive more than `burstTolerance` pacing intervals ahead of that schedule.
///
/// This class holds state for exactly one client's rate limit on exactly one method — it is not
/// shared across clients. [ThrottledServiceInterface] holds one instance per (client key, method).
///
/// The clock is deliberately a caller-supplied monotonic nanosecond value (e.g.
/// [System#nanoTime()]), not wall-clock time: wall-clock time can jump backwards or forwards on
/// an NTP adjustment, which would corrupt the TAT comparison and cause false bursts or false
/// rejections.
public final class GcraLimiter {
    /// Spacing interval between admitted requests, in nanoseconds. Zero only if the configured
    /// rate is non-positive, which should be prevented by config validation.
    private final long intervalNanos;

    /// How far ahead of the pacing schedule a request may arrive and still be admitted, in
    /// nanoseconds.
    private final long toleranceNanos;

    /// The theoretical arrival time, in the same units as the caller's monotonic clock. Starts at
    /// zero; the first call on a fresh limiter is always admitted regardless of the clock's actual
    /// origin, since only differences between calls within this limiter's lifetime are compared.
    private final AtomicLong theoreticalArrivalTimeNanos = new AtomicLong(0L);

    /// Creates a new limiter for one client's rate on one method.
    ///
    /// @param ratePerSecond the sustained rate to allow, in requests per second; must be positive
    /// @param burstTolerance how many pacing intervals early a request may arrive and still be
    ///     admitted; zero means no burst tolerance beyond exact pacing
    public GcraLimiter(final int ratePerSecond, final int burstTolerance) {
        if (ratePerSecond <= 0) {
            throw new IllegalArgumentException("ratePerSecond must be positive, was " + ratePerSecond);
        }
        this.intervalNanos = Math.max(1L, 1_000_000_000L / ratePerSecond);
        this.toleranceNanos = Math.max(0, burstTolerance) * intervalNanos;
    }

    /// Attempts to admit a request arriving at the given monotonic time.
    ///
    /// @param nowNanos the current time from a monotonic clock (e.g. [System#nanoTime()])
    /// @return {@code true} if the request conforms to the rate and should be admitted;
    ///     {@code false} if it arrives too far ahead of the pacing schedule and should be rejected
    public boolean tryAcquire(final long nowNanos) {
        while (true) {
            final long tat = theoreticalArrivalTimeNanos.get();
            final long earliestAllowed = tat - toleranceNanos;
            if (nowNanos < earliestAllowed) {
                return false;
            }
            final long newTat = Math.max(nowNanos, tat) + intervalNanos;
            if (theoreticalArrivalTimeNanos.compareAndSet(tat, newTat)) {
                return true;
            }
            // Another thread advanced the TAT concurrently; retry with the new value.
        }
    }
}
