// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.publisher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for the {@link PublisherConfig} class.
 * Tests cover configuration creation, validation, and behavior of the publisher configuration.
 */
@DisplayName("Publisher Configuration Tests")
public class PublisherConfigTest {

    /**
     * Tests the creation of a PublisherConfig with default values.
     * Verifies that the default publisher type is PRODUCTION and timeout is 1500ms.
     */
    @Test
    @DisplayName("Should create configuration with default values")
    void testDefaultConfiguration() {
        // Create config with default values
        final PublisherConfig config = new PublisherConfig(PublisherConfig.PublisherType.PRODUCTION, 1500);

        // Verify default values
        assertEquals(PublisherConfig.PublisherType.PRODUCTION, config.type());
        assertEquals(1500, config.timeoutThresholdMillis());
    }

    /**
     * Tests the creation of PublisherConfig with custom values.
     * Verifies that both publisher type and timeout can be customized.
     */
    @Test
    @DisplayName("Should create configuration with custom values")
    void testCustomConfiguration() {
        // Test NO_OP type
        final PublisherConfig noOpConfig = new PublisherConfig(PublisherConfig.PublisherType.NO_OP, 2000);
        assertEquals(PublisherConfig.PublisherType.NO_OP, noOpConfig.type());
        assertEquals(2000, noOpConfig.timeoutThresholdMillis());

        // Test custom timeout
        final PublisherConfig customTimeoutConfig = new PublisherConfig(PublisherConfig.PublisherType.PRODUCTION, 3000);
        assertEquals(PublisherConfig.PublisherType.PRODUCTION, customTimeoutConfig.type());
        assertEquals(3000, customTimeoutConfig.timeoutThresholdMillis());
    }

    /**
     * Tests the PublisherType enum values and their properties.
     * Verifies the number of enum values, their existence, names, and string representation.
     */
    @Test
    @DisplayName("Should have valid publisher type enum values")
    void testPublisherTypeEnum() {
        // Verify enum has exactly two values
        assertEquals(2, PublisherConfig.PublisherType.values().length);

        // Verify enum values
        assertNotNull(PublisherConfig.PublisherType.PRODUCTION);
        assertNotNull(PublisherConfig.PublisherType.NO_OP);

        // Verify enum names
        assertEquals("PRODUCTION", PublisherConfig.PublisherType.PRODUCTION.name());
        assertEquals("NO_OP", PublisherConfig.PublisherType.NO_OP.name());

        // Verify toString behavior
        assertEquals("PRODUCTION", PublisherConfig.PublisherType.PRODUCTION.toString());
        assertEquals("NO_OP", PublisherConfig.PublisherType.NO_OP.toString());
    }

    /**
     * Tests the record behavior of PublisherConfig including equals, hashCode, and toString methods.
     * Verifies that identical configurations are equal and different configurations are not.
     */
    @Test
    @DisplayName("Should have correct record behavior")
    void testRecordBehavior() {
        // Create two identical configs
        final PublisherConfig config1 = new PublisherConfig(PublisherConfig.PublisherType.PRODUCTION, 1500);
        final PublisherConfig config2 = new PublisherConfig(PublisherConfig.PublisherType.PRODUCTION, 1500);

        // Create a different config
        final PublisherConfig config3 = new PublisherConfig(PublisherConfig.PublisherType.NO_OP, 1500);

        // Test equals
        assertEquals(config1, config2);
        assertNotEquals(config1, config3);

        // Test hashCode
        assertEquals(config1.hashCode(), config2.hashCode());
        assertNotEquals(config1.hashCode(), config3.hashCode());

        // Test toString
        final String toString = config1.toString();
        assertTrue(toString.contains("PRODUCTION"));
        assertTrue(toString.contains("1500"));

        // Test immutability by creating new instances
        final PublisherConfig newConfig = new PublisherConfig(PublisherConfig.PublisherType.NO_OP, 2000);
        assertNotEquals(config1, newConfig);
    }

    /**
     * Tests that PublisherConfig rejects null publisher type.
     * Verifies that attempting to create a configuration with null type throws NullPointerException.
     */
    @Test
    @DisplayName("Should reject null publisher type")
    void testNullType() {
        // Test that null type is not allowed
        assertThrows(NullPointerException.class, () -> new PublisherConfig(null, 1500));
    }

    /**
     * Tests that PublisherConfig rejects invalid timeout threshold values.
     * Verifies that zero or negative timeout values are rejected with IllegalArgumentException.
     *
     * @param invalidTimeout The invalid timeout value to test
     */
    @ParameterizedTest
    @ValueSource(ints = {0, -1, -100})
    @DisplayName("Should reject invalid timeout threshold values")
    void testInvalidTimeoutThreshold(int invalidTimeout) {
        // Test that negative or zero timeout is not allowed
        assertThrows(
                IllegalArgumentException.class,
                () -> new PublisherConfig(PublisherConfig.PublisherType.PRODUCTION, invalidTimeout));
    }

    /**
     * Tests that PublisherConfig remains immutable after creation.
     * Verifies that the configuration values cannot be modified after instantiation.
     */
    @Test
    @DisplayName("Should remain immutable after creation")
    void testConfigImmutability() {
        // Create initial config
        final PublisherConfig config = new PublisherConfig(PublisherConfig.PublisherType.PRODUCTION, 1500);

        // Attempt to modify the config (should not be possible as it's a record)
        // This is a compile-time check, but we can verify the values remain unchanged
        final PublisherConfig modifiedConfig = new PublisherConfig(PublisherConfig.PublisherType.NO_OP, 2000);
        assertNotEquals(config, modifiedConfig);
        assertEquals(PublisherConfig.PublisherType.PRODUCTION, config.type());
        assertEquals(1500, config.timeoutThresholdMillis());
    }
}
