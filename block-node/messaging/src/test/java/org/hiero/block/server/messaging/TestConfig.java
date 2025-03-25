// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import org.hiero.block.node.messaging.MessagingConfig;

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
}
