// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class GcraLimiterTest {

    @Test
    @DisplayName("First call on a fresh limiter is always admitted")
    void firstCallAdmitted() {
        final GcraLimiter limiter = new GcraLimiter(10, 0);
        assertTrue(limiter.tryAcquire(System.nanoTime()));
    }

    @Test
    @DisplayName("Calls faster than the configured rate, beyond burst tolerance, are rejected")
    void tooFastRejected() {
        // 1 request/second, no burst tolerance: the second call 1ms later must be rejected.
        final GcraLimiter limiter = new GcraLimiter(1, 0);
        final long now = System.nanoTime();
        assertTrue(limiter.tryAcquire(now));
        assertFalse(limiter.tryAcquire(now + 1_000_000L)); // +1ms, far short of the 1s interval
    }

    @Test
    @DisplayName("A call spaced out beyond the pacing interval is admitted")
    void spacedOutCallAdmitted() {
        final GcraLimiter limiter = new GcraLimiter(10, 0); // 100ms interval
        final long now = System.nanoTime();
        assertTrue(limiter.tryAcquire(now));
        assertTrue(limiter.tryAcquire(now + 200_000_000L)); // +200ms, beyond the 100ms interval
    }

    @Test
    @DisplayName("Burst tolerance allows a bounded number of early arrivals")
    void burstToleranceAllowsEarlyArrivals() {
        // 10/s => 100ms interval; burst tolerance of 2 intervals.
        final GcraLimiter limiter = new GcraLimiter(10, 2);
        final long now = System.nanoTime();
        assertTrue(limiter.tryAcquire(now)); // TAT -> now + 100ms
        // Arriving immediately after (0ms later) is within the 200ms tolerance window.
        assertTrue(limiter.tryAcquire(now)); // TAT -> now + 200ms
        assertTrue(limiter.tryAcquire(now)); // TAT -> now + 300ms; earliest allowed = now + 100ms
        // A fourth immediate arrival is now outside the tolerance window (earliest allowed = now + 200ms).
        assertFalse(limiter.tryAcquire(now));
    }

    @Test
    @DisplayName("Rejects non-positive rates at construction")
    void rejectsNonPositiveRate() {
        assertThrows(IllegalArgumentException.class, () -> new GcraLimiter(0, 0));
        assertThrows(IllegalArgumentException.class, () -> new GcraLimiter(-5, 0));
    }
}
