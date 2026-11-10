// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hiero.block.node.messaging.MessagingConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link MessagingConfig}.
 */
class MessagingConfigTest {
    /**
     * This test aims to assert that the unvalidated block queue holds 32 blocks when nothing else is configured.
     */
    @Test
    @DisplayName("The unvalidated block queue size defaults to 32")
    void unvalidatedBlockQueueSizeDefaultsTo32() {
        final MessagingConfig config = TestConfig.getConfig().getConfigData(MessagingConfig.class);
        assertEquals(32, config.unvalidatedBlockQueueSize());
    }

    /**
     * This test aims to assert that an unvalidated block queue size that is not a positive power of two is rejected
     * when the configuration is created, because the ring buffer behind the queue only takes power of two sizes.
     *
     * @param queueSize the rejected queue size
     */
    @ParameterizedTest
    @ValueSource(ints = {0, -32, 6, 33})
    @DisplayName("The unvalidated block queue size must be a positive power of two")
    void unvalidatedBlockQueueSizeMustBeAPositivePowerOfTwo(final int queueSize) {
        assertThrows(IllegalArgumentException.class, () -> new MessagingConfig(512, 32, queueSize, 32));
    }

    /**
     * This test aims to assert that the verified block queue holds 32 blocks when nothing else is configured, the same
     * number of verification results the block notification queue could hold before they had a queue of their own.
     */
    @Test
    @DisplayName("The verified block queue size defaults to 32")
    void verifiedBlockQueueSizeDefaultsTo32() {
        final MessagingConfig config = TestConfig.getConfig().getConfigData(MessagingConfig.class);
        assertEquals(32, config.verifiedBlockQueueSize());
    }

    /**
     * This test aims to assert that a verified block queue size that is not a positive power of two is rejected when
     * the configuration is created, because the ring buffer behind the queue only takes power of two sizes.
     *
     * @param queueSize the rejected queue size
     */
    @ParameterizedTest
    @ValueSource(ints = {0, -32, 6, 33})
    @DisplayName("The verified block queue size must be a positive power of two")
    void verifiedBlockQueueSizeMustBeAPositivePowerOfTwo(final int queueSize) {
        assertThrows(IllegalArgumentException.class, () -> new MessagingConfig(512, 32, 32, queueSize));
    }
}
