// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.blocks.files.historic.BlocksFilesHistoricPlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.blocks.files.historic {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    // export configuration classes to the config module and app
    exports org.hiero.block.node.blocks.files.historic to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.base;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.stream;
    requires org.hiero.block.common;
    requires com.github.luben.zstd_jni;
    requires com.github.spotbugs.annotations;

    provides org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin with
            BlocksFilesHistoricPlugin;
}
