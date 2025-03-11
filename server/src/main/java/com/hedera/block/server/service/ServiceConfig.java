// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.service;

import com.hedera.block.server.config.logging.Loggable;
import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.common.utils.Preconditions;

/**
 * Use this configuration across the service package.
 *
 * <p>ServiceConfig will set the default shutdown delay for the service.
 *
 * @param shutdownDelayMillis the delay in milliseconds for the service
 */
@ConfigData("service")
public record ServiceConfig(@Loggable @ConfigProperty(defaultValue = "500") int shutdownDelayMillis) {

    /**
     * Validate the configuration.
     *
     * @throws IllegalArgumentException if the shutdownDelayMillis is not positive
     */
    public ServiceConfig {
        Preconditions.requirePositive(shutdownDelayMillis);
    }
}
