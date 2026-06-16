// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

// spotless:off - long annotations on record components must stay on one line

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.nio.file.Path;
import org.hiero.block.node.base.Loggable;

/// Configuration for the verification module.
///
/// @param allBlocksHasherFilePath path to the root hash file for all previous blocks
/// @param allBlocksHasherEnabled whether the all-blocks hasher is enabled
/// @param rebuildAllBlocksHasherFromStore whether to rebuild the all-blocks hasher from the store in case we are not
///     starting from genesis or we have no available persisted data
/// @param allBlocksHasherPersistenceInterval how often (in blocks) the hasher persists its state
/// @param tssParametersFilePath path where TSS parameters (ledger ID, address book, WRAPS VK)
///     are persisted across restarts as a serialized `LedgerIdPublicationTransactionBody`.
///     Written when block 0 is processed. Loaded on startup to restore full TSS state.
/// @param activeSessionsBufferSize size of maximum allowed active sessions. When full and a new session needs to
///     start, room will be made for it by canceling the longest running one. todo(2528) could be lowest block nubmer
/// @param allSourcesRequireOrdering if true, strict ordering of the next expected block to verify will be enforced
///     for blocks received from any source. If false, only
///     [org.hiero.block.node.spi.blockmessaging.BlockSource#PUBLISHER] will be strictly ordered.
@ConfigData("verification")
public record VerificationConfig(
    @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/application-state/rootHashOfAllPreviousBlocks.bin") Path allBlocksHasherFilePath,
    @Loggable @ConfigProperty(defaultValue = "false") boolean allBlocksHasherEnabled,
    @Loggable @ConfigProperty(defaultValue = "false") boolean rebuildAllBlocksHasherFromStore,
    @Loggable @ConfigProperty(defaultValue = "100") int allBlocksHasherPersistenceInterval,
    @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/application-state/tss-parameters.bin") Path tssParametersFilePath,
    @Loggable @ConfigProperty(defaultValue = "100") int activeSessionsBufferSize,
    @Loggable @ConfigProperty(defaultValue = "true") boolean allSourcesRequireOrdering){}

// spotless:on
