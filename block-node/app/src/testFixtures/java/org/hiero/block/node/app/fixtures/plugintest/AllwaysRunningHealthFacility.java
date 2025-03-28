// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import static org.junit.jupiter.api.Assertions.fail;

import org.hiero.block.node.spi.health.HealthFacility;

/**
 * A testing {@link HealthFacility} that always returns {@link State#RUNNING} for the block node state. And fails test
 * if the shutdown method is called.
 */
public class AllwaysRunningHealthFacility implements HealthFacility {
    /**
     * {@inheritDoc}
     */
    @Override
    public State blockNodeState() {
        return State.RUNNING;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(String className, String reason) {
        fail("Shutdown not expected: " + className + " " + reason);
    }
}
