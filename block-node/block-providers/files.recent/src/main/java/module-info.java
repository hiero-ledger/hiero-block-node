// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.blocks.files.recent.BlocksFilesRecentPlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.blocks.files.recent {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.base;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.stream;
    requires org.hiero.block.common;
    requires com.github.luben.zstd_jni;

    provides org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin with
            BlocksFilesRecentPlugin;
}
