// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.mediator;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.server.config.logging.Loggable;

/**
 * Constructor to initialize the Mediator configuration.
 *
 * <p>MediatorConfig will set the ring buffer size for the mediator.
 *
 * @param ringBufferSize the number of available "slots" the ring buffer uses internally to store
 *                       events.
 * @param type use a predefined type string to replace the mediator component implementation.
 *  Non-PRODUCTION values should only be used for troubleshooting and development purposes.
 */
@ConfigData("mediator")
public record MediatorConfig(
        @Loggable @ConfigProperty(defaultValue = "4096") int ringBufferSize,
        @Loggable @ConfigProperty(defaultValue = "PRODUCTION") MediatorType type,
        @Loggable @ConfigProperty(defaultValue = "90") int historicTransitionThresholdPercentage) {

    /**
     * Validate the configuration.
     *
     * @throws IllegalArgumentException if the configuration is invalid
     */
    public MediatorConfig {
        Preconditions.requirePositive(ringBufferSize, "Mediator Ring Buffer Size must be positive");
        Preconditions.requirePowerOfTwo(ringBufferSize, "Mediator Ring Buffer Size must be a power of 2");

        Preconditions.requireInRange(
                historicTransitionThresholdPercentage,
                10,
                90,
                "Historic Transition Threshold Percentage must be between 10 and 90");
    }

    /**
     * The type of mediator to use - PRODUCTION or NO_OP.
     */
    public enum MediatorType {
        PRODUCTION,
        NO_OP,
    }
}
