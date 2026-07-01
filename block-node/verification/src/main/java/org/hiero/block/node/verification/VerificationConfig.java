// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

// spotless:off - long annotations on record components must stay on one line

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
 * @param tssParametersFilePath path where TSS parameters (ledger ID, address book, WRAPS VK)
 *     are persisted across restarts as a serialized {@code LedgerIdPublicationTransactionBody}.
 *     Written when block 0 is processed. Loaded on startup to restore full TSS state.
 * @param dumpEnabled whether failed-block dumping is enabled; off by default, enable in non-prod
 * @param dumpDirectoryPath directory where failing block bytes and metadata are written
 * @param dumpRetentionDays number of days to keep dump files before auto-purge
 */
@ConfigData("verification")
public record VerificationConfig(
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/application-state/rootHashOfAllPreviousBlocks.bin") Path allBlocksHasherFilePath,
        @Loggable @ConfigProperty(defaultValue = "false") boolean allBlocksHasherEnabled,
        @Loggable @ConfigProperty(defaultValue = "10") int allBlocksHasherPersistenceInterval,
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/application-state/tss-parameters.bin") Path tssParametersFilePath,
        @Loggable @ConfigProperty(defaultValue = "false") boolean dumpEnabled,
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/verification/dumps") Path dumpDirectoryPath,
        @Loggable @ConfigProperty(defaultValue = "7") int dumpRetentionDays) {}

// spotless:on
