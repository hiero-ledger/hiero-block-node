// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Verifies the permit reaper: a permit held past `maxPermitHoldSeconds` is force-released exactly
/// once, a young or never-reaped permit is left alone, and a later real completion after a reap is
/// a no-op rather than a double decrement.
class ClientThrottleTest {
    private static final long NOW = 1_000_000_000L;
    private static final long SECOND = 1_000_000_000L;

    private GlobalConcurrencyGate gate;
    private ThrottleMetrics metrics;

    @BeforeEach
    void setUp() {
        gate = new GlobalConcurrencyGate(10);
        metrics = new ThrottleMetrics(MetricRegistry.builder().build());
    }

    private ClientThrottle throttleWithHold(final int maxConcurrentPerClient, final int maxPermitHoldSeconds) {
        return throttleWithHold("method", maxConcurrentPerClient, maxPermitHoldSeconds);
    }

    /// Each `(service, method)` registers its gauges once, so a test needing two throttles uses two names.
    private ClientThrottle throttleWithHold(
            final String method, final int maxConcurrentPerClient, final int maxPermitHoldSeconds) {
        return new ClientThrottle(
                new PerClientThrottleSettings(1_000, 1_000, maxConcurrentPerClient, maxPermitHoldSeconds),
                gate,
                metrics,
                "svc",
                method,
                Duration.ofDays(1));
    }

    @Test
    @DisplayName("A permit held past the hold limit is reaped, freeing both the client and node-wide slots")
    void expiredPermitIsReapedAndSlotsFreed() {
        final ClientThrottle throttle = throttleWithHold(1, 10);
        assertTrue(throttle.tryAdmit("client", NOW).admitted());
        assertFalse(throttle.tryAdmit("client", NOW).admitted(), "the one per-client slot is held");
        assertEquals(1, gate.inFlight());

        assertEquals(1, throttle.reapExpiredPermits(NOW + 11 * SECOND));

        assertEquals(0, gate.inFlight(), "the node-wide slot must be freed");
        assertTrue(throttle.tryAdmit("client", NOW + 11 * SECOND).admitted(), "the client slot must be freed");
    }

    @Test
    @DisplayName("A permit younger than the hold limit is not reaped")
    void youngPermitIsNotReaped() {
        final ClientThrottle throttle = throttleWithHold(1, 10);
        assertTrue(throttle.tryAdmit("client", NOW).admitted());

        assertEquals(0, throttle.reapExpiredPermits(NOW + 5 * SECOND));

        assertEquals(1, gate.inFlight());
        assertFalse(throttle.tryAdmit("client", NOW + 5 * SECOND).admitted());
    }

    @Test
    @DisplayName("A real completion after a reap is a no-op, not a double decrement")
    void lateCompletionAfterReapDoesNotDoubleRelease() {
        final ClientThrottle throttle = throttleWithHold(1, 10);
        final AdmissionResult first = throttle.tryAdmit("client", NOW);
        throttle.reapExpiredPermits(NOW + 11 * SECOND);
        assertTrue(throttle.tryAdmit("client", NOW + 11 * SECOND).admitted(), "a new call takes the freed slot");

        first.releasePermit().run(); // the late, real completion of the reaped call

        assertEquals(1, gate.inFlight(), "the new call's slot must still be held");
        assertFalse(
                throttle.tryAdmit("client", NOW + 11 * SECOND).admitted(),
                "a double release would have wrongly freed the new call's client slot");
    }

    @Test
    @DisplayName("A permit that completes normally leaves the live set and is never reaped")
    void releasedPermitIsNotReaped() {
        final ClientThrottle throttle = throttleWithHold(1, 10);
        throttle.tryAdmit("client", NOW).releasePermit().run();

        assertEquals(0, throttle.reapExpiredPermits(NOW + 60 * SECOND));
        assertEquals(0, gate.inFlight());
    }

    @Test
    @DisplayName("A hold limit of 0 never reaps, and a method's reported hold limit reflects its setting")
    void zeroHoldLimitNeverReaps() {
        final ClientThrottle neverReaps = throttleWithHold(1, 0);
        assertTrue(neverReaps.tryAdmit("client", NOW).admitted());

        assertEquals(0, neverReaps.reapExpiredPermits(NOW + 24 * 3_600 * SECOND));
        assertEquals(1, gate.inFlight());
        assertEquals(0, neverReaps.permitHoldSeconds());
        assertEquals(10, throttleWithHold("other", 1, 10).permitHoldSeconds());
    }
}
