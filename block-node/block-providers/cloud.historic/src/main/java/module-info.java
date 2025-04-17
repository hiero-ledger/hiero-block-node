// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.blocks.cloud.historic.BlocksCloudHistoricPlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.blocks.cloud.historic {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    requires transitive org.hiero.block.node.spi;

    provides org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin with
            BlocksCloudHistoricPlugin;
}
