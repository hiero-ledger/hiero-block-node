// SPDX-License-Identifier: Apache-2.0

import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
import org.hiero.block.node.blocks.cloud.archive.BlocksCloudArchivePlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.blocks.cloud.archive {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.stream;
    requires org.hiero.block.base;
    requires org.hiero.block.node.spi;
    requires com.lmax.disruptor;

    provides BlockProviderPlugin with
            BlocksCloudArchivePlugin;
}
