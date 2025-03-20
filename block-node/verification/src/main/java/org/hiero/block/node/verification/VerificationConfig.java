// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.server.config.logging.Loggable;

/**
 * Configuration for the verification module.
 *
 * @param type toggle between production and no-op verification services
 * @param hashCombineBatchSize the size of the batch used to combine hashes
 */
@ConfigData("verification")
public record VerificationConfig(
        @Loggable @ConfigProperty(defaultValue = "PRODUCTION") VerificationServiceType type,
        @Loggable @ConfigProperty(defaultValue = "32") int hashCombineBatchSize) {

    /**
     * Constructs a new instance of {@link VerificationConfig}.
     *
     * @param type toggle between PRODUCTION and NO_OP verification services
     * @param hashCombineBatchSize the size of the batch used to combine hashes
     */
    public VerificationConfig {
        // hashCombineBatchSize must be even and greater than 2
        Preconditions.requirePositive(hashCombineBatchSize, "[VERIFICATION_HASH_COMBINE_BATCH_SIZE] must be positive");
        Preconditions.requireEven(
                hashCombineBatchSize, "[VERIFICATION_HASH_COMBINE_BATCH_SIZE] must be even and greater than 2");
    }

    /**
     * The type of the verification service to use - PRODUCTION or NO_OP.
     */
    public enum VerificationServiceType {
        PRODUCTION,
        NO_OP,
    }
}
