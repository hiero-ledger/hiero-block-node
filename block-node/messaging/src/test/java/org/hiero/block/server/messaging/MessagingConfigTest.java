// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hiero.block.node.messaging.MessagingConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MessagingConfigTest {

    /**
     * Test that a valid MessagingConfig instance is created with default values.
     */
    @Test
    @DisplayName("Test creation of MessagingConfig with default values")
    void testDefaultValues() {
        final MessagingConfig config = new MessagingConfig(1024, 32);
        assertEquals(1024, config.blockItemQueueSize());
        assertEquals(32, config.blockNotificationQueueSize());
    }

    /**
     * Test that an exception is thrown when blockItemQueueSize is not positive.
     */
    @Test
    @DisplayName("Test exception for non-positive blockItemQueueSize")
    void testNonPositiveBlockItemQueueSize() {
        assertThrows(IllegalArgumentException.class, () -> new MessagingConfig(0, 32));
    }

    /**
     * Test that an exception is thrown when blockItemQueueSize is not a power of two.
     */
    @Test
    @DisplayName("Test exception for blockItemQueueSize not being a power of two")
    void testBlockItemQueueSizeNotPowerOfTwo() {
        assertThrows(IllegalArgumentException.class, () -> new MessagingConfig(1000, 32));
    }

    /**
     * Test that an exception is thrown when blockNotificationQueueSize is not positive.
     */
    @Test
    @DisplayName("Test exception for non-positive blockNotificationQueueSize")
    void testNonPositiveBlockNotificationQueueSize() {
        assertThrows(IllegalArgumentException.class, () -> new MessagingConfig(1024, 0));
    }

    /**
     * Test that an exception is thrown when blockNotificationQueueSize is not a power of two.
     */
    @Test
    @DisplayName("Test exception for blockNotificationQueueSize not being a power of two")
    void testBlockNotificationQueueSizeNotPowerOfTwo() {
        assertThrows(IllegalArgumentException.class, () -> new MessagingConfig(1024, 30));
    }

    /**
     * Test edge case with the smallest valid power of two for both queue sizes.
     */
    @Test
    @DisplayName("Test smallest valid power of two for queue sizes")
    void testSmallestValidPowerOfTwo() {
        final MessagingConfig config = new MessagingConfig(2, 2);
        assertEquals(2, config.blockItemQueueSize());
        assertEquals(2, config.blockNotificationQueueSize());
    }

    /**
     * Test edge case with a very large power of two for both queue sizes.
     */
    @Test
    @DisplayName("Test very large power of two for queue sizes")
    void testLargestValidPowerOfTwo() {
        final MessagingConfig config = new MessagingConfig(1 << 30, 1 << 16);
        assertEquals(1 << 30, config.blockItemQueueSize());
        assertEquals(1 << 16, config.blockNotificationQueueSize());
    }
}
