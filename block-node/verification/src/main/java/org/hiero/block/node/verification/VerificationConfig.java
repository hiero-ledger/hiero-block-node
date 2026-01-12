// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.nio.file.Path;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for the verification module.
 * Add here configuration related to TSS when needed.
 */
@ConfigData("verification")
public record VerificationConfig(
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/verification/rootHashOfAllPreviousBlocks.bin")
        Path allBlocksHasherFilePath,

        @Loggable @ConfigProperty(defaultValue = "true") boolean allBlocksHasherEnabled,
        @Loggable @ConfigProperty(defaultValue = "10") int allBlocksHasherPersistenceInterval) {}
