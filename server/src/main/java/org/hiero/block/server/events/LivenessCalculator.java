// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.events;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.InstantSource;

/**
 * Use this class to calculate and refresh the current liveness of a component based on a timeout
 * threshold.
 */
public final class LivenessCalculator {
    private final long timeoutThresholdMillis;
    private final InstantSource clock;
    private long lastMillis;

    /**
     * Once constructed, the liveness calculator will use the given clock and timeout threshold to
     * determine expiry.
     *
     * @param clock the clock to use for time calculations
     * @param timeoutThresholdMillis the timeout threshold in milliseconds
     */
    public LivenessCalculator(@NonNull final InstantSource clock, final long timeoutThresholdMillis) {

        this.clock = clock;
        this.timeoutThresholdMillis = timeoutThresholdMillis;
        this.lastMillis = clock.millis();
    }

    /**
     * Returns true if the timeout has expired based on the configuration window and the current
     * time.
     *
     * @return true if the timeout has expired
     */
    public boolean isTimeoutExpired() {
        return clock.millis() - lastMillis > timeoutThresholdMillis;
    }

    /**
     * Use refresh to reset the liveness calculator to the beginning of the configured threshold
     * window.
     */
    public void refresh() {
        lastMillis = clock.millis();
    }
}
