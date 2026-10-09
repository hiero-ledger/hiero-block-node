// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import edu.umd.cs.findbugs.annotations.Nullable;

/// The outcome of one [SingleWeightThrottle#tryAdmit] call: either admitted, with the action to
/// run exactly once to release the concurrency permit it acquired, or rejected, with the reason.
record AdmissionResult(
        boolean admitted,
        @Nullable String rejectionReason,
        @Nullable Runnable releasePermit) {
    static AdmissionResult admitted(final Runnable releasePermit) {
        return new AdmissionResult(true, null, releasePermit);
    }

    static AdmissionResult rejected(final String reason) {
        return new AdmissionResult(false, reason, null);
    }
}
