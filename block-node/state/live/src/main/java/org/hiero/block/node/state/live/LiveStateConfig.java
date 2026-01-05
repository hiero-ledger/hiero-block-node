// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.nio.file.Path;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for the live state plugin.
 *
 * @param storagePath the root path for storing live state data (MerkleDB files)
 */
@ConfigData("state.live")
public record LiveStateConfig(
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/state") Path storagePath,
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/state/latest") Path latestStatePath,
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/state/matadata.dat")
                Path stateMetadataPath) {}
