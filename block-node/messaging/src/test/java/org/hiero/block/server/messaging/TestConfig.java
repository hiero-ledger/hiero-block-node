// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import org.hiero.block.node.messaging.MessagingConfig;
import org.hiero.block.node.spi.BlockNodeContext;

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
    public static final BlockNodeContext BLOCK_NODE_CONTEXT =
            new BlockNodeContext(getConfig(), null, null, null, null, null);
}
