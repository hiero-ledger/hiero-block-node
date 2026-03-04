// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

// spotless:off

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.nio.file.Path;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for the verification module.
 *
 * @param allBlocksHasherFilePath path to the root hash file for all previous blocks
 * @param allBlocksHasherEnabled whether the all-blocks hasher is enabled
 * @param allBlocksHasherPersistenceInterval how often (in blocks) the hasher persists its state
 * @param ledgerId hex-encoded ledger ID for the target network; when non-empty, pre-seeds the
 *     active ledger ID so blocks can be verified before block 0 is processed — useful when
 *     joining a network whose genesis block is not available
 */
@ConfigData("verification")
public record VerificationConfig(
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/verification/rootHashOfAllPreviousBlocks.bin") Path allBlocksHasherFilePath,
        @Loggable @ConfigProperty(defaultValue = "true") boolean allBlocksHasherEnabled,
        @Loggable @ConfigProperty(defaultValue = "10") int allBlocksHasherPersistenceInterval,
        @Loggable @ConfigProperty(defaultValue = "") String ledgerId) {}

// spotless:on
