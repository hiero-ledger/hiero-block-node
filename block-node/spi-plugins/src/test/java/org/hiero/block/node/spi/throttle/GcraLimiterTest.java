// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class GcraLimiterTest {

    @Test
    @DisplayName("A fresh limiter admits its first call and full burst whatever the clock's origin, even negative")
    void freshLimiterAdmitsRegardlessOfClockOrigin() {
        // System.nanoTime() has an arbitrary origin and may be negative; a zero-seeded TAT would reject these.
        for (final long origin : new long[] {System.nanoTime(), 0L, -1_000_000_000L, Long.MIN_VALUE / 2}) {
            final GcraLimiter limiter = new GcraLimiter(10, 2, origin); // burst of 2 => 3 immediate calls
            assertTrue(limiter.tryAcquire(origin), "first call, origin " + origin);
            assertTrue(limiter.tryAcquire(origin), "first burst call, origin " + origin);
            assertTrue(limiter.tryAcquire(origin), "second burst call, origin " + origin);
            assertFalse(limiter.tryAcquire(origin), "beyond the burst, origin " + origin);
        }
    }

    @Test
    @DisplayName("Calls faster than the configured rate, beyond burst tolerance, are rejected")
    void tooFastRejected() {
        final long now = System.nanoTime();
        final GcraLimiter limiter = new GcraLimiter(1, 0, now);
        assertTrue(limiter.tryAcquire(now));
        assertFalse(limiter.tryAcquire(now + 1_000_000L)); // +1ms, far short of the 1s interval
    }

    @Test
    @DisplayName("A call spaced out beyond the pacing interval is admitted")
    void spacedOutCallAdmitted() {
        final long now = System.nanoTime();
        final GcraLimiter limiter = new GcraLimiter(10, 0, now); // 100ms interval
        assertTrue(limiter.tryAcquire(now));
        assertTrue(limiter.tryAcquire(now + 200_000_000L)); // +200ms, beyond the 100ms interval
    }

    @Test
    @DisplayName("Burst tolerance allows a bounded number of early arrivals")
    void burstToleranceAllowsEarlyArrivals() {
        // 10/s => 100ms interval; burst tolerance of 2 intervals.
        final long now = System.nanoTime();
        final GcraLimiter limiter = new GcraLimiter(10, 2, now);
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
        assertThrows(IllegalArgumentException.class, () -> new GcraLimiter(0, 0, 0L));
        assertThrows(IllegalArgumentException.class, () -> new GcraLimiter(-5, 0, 0L));
    }
}
