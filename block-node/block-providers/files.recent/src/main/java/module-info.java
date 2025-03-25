// SPDX-License-Identifier: Apache-2.0

import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
import org.hiero.block.node.blocks.files.recent.BlocksFilesRecentPlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.blocks.files.recent {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.stream;
    requires org.hiero.block.base;
    requires org.hiero.block.node.spi;
    requires com.github.luben.zstd_jni;
    requires com.hedera.pbj.runtime;

    provides BlockProviderPlugin with
            BlocksFilesRecentPlugin;
}
