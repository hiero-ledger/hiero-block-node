// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.metrics.api.Metrics;
import org.hiero.block.node.messaging.MessagingConfig;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

public class TestConfig {

    /**
     * Helper method to get the configuration for the messaging service. For use in tests.
     *
     * @return the configuration for the messaging service
     * @throws IllegalStateException if no ConfigurationBuilderFactory implementation is found
     */
    public static Configuration getConfig() {
        return ConfigurationBuilder.create()
                .withConfigDataType(MessagingConfig.class)
                .build();
    }

    /**
     * A test context for the BlockNodeContext interface. Only provides a configuration object as that is all that is
     * needed by messaging facility so far.
     */
    public static final BlockNodeContext BLOCK_NODE_CONTEXT = new BlockNodeContext() {
        final Configuration config = getConfig();

        @Override
        public Configuration configuration() {
            return config;
        }

        @Override
        public Metrics metrics() {
            throw new IllegalStateException("Metrics not available in test context");
        }

        @Override
        public HealthFacility serverHealth() {
            throw new IllegalStateException("Metrics not available in test context");
        }

        @Override
        public BlockMessagingFacility blockMessaging() {
            throw new IllegalStateException("Metrics not available in test context");
        }

        @Override
        public HistoricalBlockFacility historicalBlockProvider() {
            throw new IllegalStateException("Metrics not available in test context");
        }
    };
}
