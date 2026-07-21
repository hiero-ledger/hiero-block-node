// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.blocks.files.historic.BlockFileHistoricPlugin;
import org.hiero.block.node.blocks.files.historic.FilesHistoricConfigExtension;

module org.hiero.block.node.blocks.files.historic {
    // export configuration classes to the config module and app
    exports org.hiero.block.node.blocks.files.historic to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.base;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.metrics;
    requires com.hedera.pbj.runtime;
    requires org.hiero.block.common;
    requires org.hiero.block.protobuf.pbj;
    requires com.github.spotbugs.annotations;

    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    provides com.swirlds.config.api.ConfigurationExtension with
            FilesHistoricConfigExtension;
    provides org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin with
            BlockFileHistoricPlugin;
}
