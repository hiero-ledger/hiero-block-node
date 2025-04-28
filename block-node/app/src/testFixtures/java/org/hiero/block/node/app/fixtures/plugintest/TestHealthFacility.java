// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import java.lang.System.Logger.Level;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.node.spi.health.HealthFacility;

/**
 * A testing {@link HealthFacility} that always returns {@link State#RUNNING} for the block node state. And fails test
 * if the shutdown method is called.
 */
public class TestHealthFacility implements HealthFacility {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** Track if shutdown method was called, for tests that need to know. */
    public final AtomicBoolean shutdownCalled = new AtomicBoolean(false);
    /** If the node is running. Can be set by tests that need it to be false */
    public final AtomicBoolean isRunning = new AtomicBoolean(true);

    /**
     * {@inheritDoc}
     */
    @Override
    public State blockNodeState() {
        return isRunning.get() ? State.RUNNING : State.SHUTTING_DOWN;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(String className, String reason) {
        LOGGER.log(Level.ERROR, "Shutting down " + className + " " + reason);
        shutdownCalled.set(true);
        isRunning.set(false);
    }
}
