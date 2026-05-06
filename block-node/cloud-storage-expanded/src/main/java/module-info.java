// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.cloud.storage.expanded.ExpandedCloudStoragePlugin;

module org.hiero.block.node.cloud.storage.expanded {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    // export configuration classes to the config module and app
    exports org.hiero.block.node.cloud.storage.expanded to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.protobuf.pbj;
    requires transitive org.hiero.metrics;
    requires com.hedera.bucky;
    requires com.hedera.pbj.runtime;
    requires org.hiero.block.node.base;
    requires com.github.spotbugs.annotations;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            ExpandedCloudStoragePlugin;
}
