// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit Test for HealthFacility interface and its default methods and its inner enum.
 */
public class HealthFacilityTest {
    private static class TestHealthFacility implements HealthFacility {
        private State state;

        public TestHealthFacility(State state) {
            this.state = state;
        }

        @Override
        public State blockNodeState() {
            return state;
        }

        @Override
        public void shutdown(String className, String reason) {
            state = State.SHUTTING_DOWN;
        }
    }

    @Test
    @DisplayName("Test State enum values")
    void testStateEnumValues() {
        assertEquals(HealthFacility.State.STARTING, HealthFacility.State.valueOf("STARTING"));
        assertEquals(HealthFacility.State.RUNNING, HealthFacility.State.valueOf("RUNNING"));
        assertEquals(HealthFacility.State.SHUTTING_DOWN, HealthFacility.State.valueOf("SHUTTING_DOWN"));
    }

    @Test
    @DisplayName("Test isRunning method when state is RUNNING")
    void testIsRunningWhenRunning() {
        HealthFacility healthFacility = new TestHealthFacility(HealthFacility.State.RUNNING);
        assertTrue(healthFacility.isRunning());
    }

    @Test
    @DisplayName("Test isRunning method when state is not RUNNING")
    void testIsRunningWhenNotRunning() {
        HealthFacility healthFacility = new TestHealthFacility(HealthFacility.State.STARTING);
        assertFalse(healthFacility.isRunning());
    }

    @Test
    @DisplayName("Test shutdown method")
    void testShutdown() {
        TestHealthFacility healthFacility = new TestHealthFacility(HealthFacility.State.RUNNING);
        healthFacility.shutdown("TestClass", "Test reason");
        assertEquals(HealthFacility.State.SHUTTING_DOWN, healthFacility.blockNodeState());
    }
}
