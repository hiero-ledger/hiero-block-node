// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

// spotless:off

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.nio.file.Path;
import org.hiero.block.node.base.Loggable;

/// Configuration for the verification module.
///
/// @param allBlocksHasherFilePath path to the root hash file for all previous blocks
/// @param allBlocksHasherEnabled whether the all-blocks hasher is enabled
/// @param allBlocksHasherPersistenceInterval how often (in blocks) the hasher persists its state
/// @param tssParametersFilePath path where TSS parameters (ledger ID, address book, WRAPS VK)
///     are persisted across restarts as a serialized `LedgerIdPublicationTransactionBody`.
///     Written when block 0 is processed. Loaded on startup to restore full TSS state.
@ConfigData("verification")
public record VerificationConfig(
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/verification/rootHashOfAllPreviousBlocks.bin") Path allBlocksHasherFilePath,
        @Loggable @ConfigProperty(defaultValue = "true") boolean allBlocksHasherEnabled,
        @Loggable @ConfigProperty(defaultValue = "10") int allBlocksHasherPersistenceInterval,
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/verification/tss-parameters.bin") Path tssParametersFilePath) {}

// spotless:on
