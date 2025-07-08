// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.blocks.files.recent.BlocksFilesRecentPlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.blocks.files.recent {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    // export configuration classes to the config module and app
    exports org.hiero.block.node.blocks.files.recent to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.base;
    requires transitive org.hiero.block.node.spi;
    requires com.hedera.pbj.runtime;
    requires com.swirlds.metrics.api;
    requires org.hiero.block.common;
    requires org.hiero.block.protobuf.pbj;
    requires com.github.luben.zstd_jni;
    requires com.github.spotbugs.annotations;

    provides org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin with
            BlocksFilesRecentPlugin;
}
