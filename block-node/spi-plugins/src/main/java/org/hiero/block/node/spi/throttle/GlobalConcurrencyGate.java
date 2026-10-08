// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/// The node-wide concurrency ceiling for one throttled service — shared by every method on that
/// service, whether or not that method also has its own per-client gate ([ClientThrottle]). Kept
/// as one pool, deliberately not fragmented per method: it represents one allocation of the
/// node's total shared capacity, so letting each method that opts into per-client settings carry
/// its own independent copy would silently multiply that allocation by however many methods are
/// configured.
///
/// [#tryReserve] atomically checks and commits in one step (a lock-free CAS loop, the same
/// pattern [GcraLimiter] uses), so concurrent callers can never collectively overshoot the
/// ceiling the way a separate check-then-increment would allow. [ClientThrottle] reserves this
/// gate first — before its own per-client and rate checks — and rolls the reservation back via
/// [#release] if either of those later checks goes on to reject the call; this gate does not need
/// to know whether that happens. [#tryAdmit] is a convenience for the simpler case — a method
/// with *no* per-client gate at all, where this is the only check.
final class GlobalConcurrencyGate {
    private final int maxConcurrentGlobal;
    private final AtomicInteger inFlight = new AtomicInteger();

    /// @param maxConcurrentGlobal the node-wide concurrency ceiling this gate enforces
    GlobalConcurrencyGate(final int maxConcurrentGlobal) {
        this.maxConcurrentGlobal = maxConcurrentGlobal;
    }

    /// Atomically reserves a permit if the ceiling has not been reached.
    ///
    /// @return {@code true} if a permit was reserved (the caller must [#release] it exactly once,
    ///     including rolling it back if a later check goes on to reject the call); {@code false}
    ///     if the ceiling was already reached
    boolean tryReserve() {
        while (true) {
            final int current = inFlight.get();
            if (current >= maxConcurrentGlobal) {
                return false;
            }
            if (inFlight.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /// Releases a permit previously committed via [#tryReserve].
    void release() {
        inFlight.decrementAndGet();
    }

    /// Admits a call using only this shared gate — for a method with no per-client settings
    /// configured at all.
    ///
    /// @param description a human-readable label for this call's service/method, used in the
    ///     rejection reason if the ceiling has been reached
    /// @return the admission result — see [AdmissionResult]
    @NonNull
    AdmissionResult tryAdmit(@NonNull final String description) {
        if (!tryReserve()) {
            return AdmissionResult.rejected("node-wide concurrency limit reached for " + description);
        }
        final AtomicBoolean released = new AtomicBoolean(false);
        return AdmissionResult.admitted(() -> {
            if (released.compareAndSet(false, true)) {
                release();
            }
        });
    }
}
