// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/// The node-wide concurrency ceiling for one `(service, weight class)` combination — shared by
/// every method at that weight class, whether or not that method also has its own per-client
/// gate ([SingleWeightThrottle]). Kept as one pool, deliberately not fragmented per method: it
/// represents one allocation of the node's total shared capacity, so letting each method that
/// opts into per-client settings carry its own independent copy would silently multiply that
/// allocation by however many methods are configured.
///
/// Exposes a pure-read check ([#hasCapacity]) separate from the commit ([#acquire]) so
/// [SingleWeightThrottle] can preserve its existing admission-decision order: a call that is
/// going to be rejected by a later check must not mutate this counter first. [#tryAdmit] is a
/// convenience for the simpler case — a method with *no* per-client gate at all, where this is
/// the only check and there is nothing later to roll back for.
final class GlobalConcurrencyGate {
    private final int maxConcurrentGlobal;
    private final AtomicInteger inFlight = new AtomicInteger();

    /// @param maxConcurrentGlobal the node-wide concurrency ceiling this gate enforces
    GlobalConcurrencyGate(final int maxConcurrentGlobal) {
        this.maxConcurrentGlobal = maxConcurrentGlobal;
    }

    /// @return {@code true} if the ceiling has not yet been reached (a pure read)
    boolean hasCapacity() {
        return inFlight.get() < maxConcurrentGlobal;
    }

    /// Commits a permit. Callers must only call this after confirming every other check for the
    /// call has also passed.
    void acquire() {
        inFlight.incrementAndGet();
    }

    /// Releases a permit previously committed via [#acquire].
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
        if (!hasCapacity()) {
            return AdmissionResult.rejected("node-wide concurrency limit reached for " + description);
        }
        acquire();
        final AtomicBoolean released = new AtomicBoolean(false);
        return AdmissionResult.admitted(() -> {
            if (released.compareAndSet(false, true)) {
                release();
            }
        });
    }
}
