// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static org.junit.jupiter.api.Assertions.*;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.api.validation.ConfigViolationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link SubscriberConfig} class.
 */
public class SubscriberConfigTest {

    /**
     * Tests that the default values are loaded correctly when no specific configuration is provided.
     */
    @Test
    @DisplayName("Loads default values correctly")
    void defaultValues() {
        final Configuration configuration = ConfigurationBuilder.create()
                .withConfigDataType(SubscriberConfig.class)
                .build();
        final SubscriberConfig subscriberConfig = configuration.getConfigData(SubscriberConfig.class);

        assertNotNull(subscriberConfig);
        assertEquals(4000, subscriberConfig.liveQueueSize(), "Default liveQueueSize should be 4000");
        assertEquals(4000L, subscriberConfig.maximumFutureRequest(), "Default maximumFutureRequest should be 4000");
        assertEquals(
                400, subscriberConfig.minimumLiveQueueCapacity(), "Default minimumLiveQueueCapacity should be 400");
    }

    /**
     * Tests that custom values provided via configuration override the defaults.
     */
    @Test
    @DisplayName("Loads custom values correctly")
    void customValues() {
        final Configuration configuration = ConfigurationBuilder.create()
                .withValue("subscriber.liveQueueSize", "5000")
                .withValue("subscriber.maximumFutureRequest", "6000")
                .withValue("subscriber.minimumLiveQueueCapacity", "500")
                .withConfigDataType(SubscriberConfig.class)
                .build();
        final SubscriberConfig subscriberConfig = configuration.getConfigData(SubscriberConfig.class);

        assertNotNull(subscriberConfig);
        assertEquals(5000, subscriberConfig.liveQueueSize(), "Custom liveQueueSize should be 5000");
        assertEquals(6000L, subscriberConfig.maximumFutureRequest(), "Custom maximumFutureRequest should be 6000");
        assertEquals(500, subscriberConfig.minimumLiveQueueCapacity(), "Custom minimumLiveQueueCapacity should be 500");
    }

    /**
     * Tests that configuration loading fails if a value below the specified minimum is provided
     * for liveQueueSize.
     */
    @Test
    @DisplayName("Fails validation for liveQueueSize below minimum")
    void validationFailsLiveQueueSizeTooSmall() {
        final ConfigurationBuilder builder = ConfigurationBuilder.create()
                .withValue("subscriber.liveQueueSize", "99") // Below min(100)
                .withConfigDataType(SubscriberConfig.class);

        assertThrows(
                ConfigViolationException.class,
                builder::build,
                "Should throw ConfigViolationException for liveQueueSize below minimum");
    }

    /**
     * Tests that configuration loading fails if a value below the specified minimum is provided
     * for maximumFutureRequest.
     */
    @Test
    @DisplayName("Fails validation for maximumFutureRequest below minimum")
    void validationFailsMaximumFutureRequestTooSmall() {
        final ConfigurationBuilder builder = ConfigurationBuilder.create()
                .withValue("subscriber.maximumFutureRequest", "9") // Below min(10)
                .withConfigDataType(SubscriberConfig.class);

        assertThrows(
                ConfigViolationException.class,
                builder::build,
                "Should throw ConfigViolationException for maximumFutureRequest below minimum");
    }

    /**
     * Tests that configuration loading fails if a value below the specified minimum is provided
     * for minimumLiveQueueCapacity.
     */
    @Test
    @DisplayName("Fails validation for minimumLiveQueueCapacity below minimum")
    void validationFailsMinimumLiveQueueCapacityTooSmall() {
        final ConfigurationBuilder builder = ConfigurationBuilder.create()
                .withValue("subscriber.minimumLiveQueueCapacity", "9") // Below min(10)
                .withConfigDataType(SubscriberConfig.class);

        assertThrows(
                ConfigViolationException.class,
                builder::build,
                "Should throw ConfigViolationException for minimumLiveQueueCapacity below minimum");
    }
}
