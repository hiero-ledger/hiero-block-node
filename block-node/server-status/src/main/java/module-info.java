// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.server.status.ServerStatusConfigExtension;
import org.hiero.block.node.server.status.ServerStatusServicePlugin;

module org.hiero.block.node.server.status {
    // export configuration classes to the config module
    exports org.hiero.block.node.server.status to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.protobuf.pbj;
    requires transitive org.hiero.metrics;
    requires com.hedera.pbj.runtime;
    requires org.hiero.block.node.app.config;
    requires org.hiero.block.node.base;
    requires com.github.spotbugs.annotations;

    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    provides com.swirlds.config.api.ConfigurationExtension with
            ServerStatusConfigExtension;
    provides org.hiero.block.node.spi.BlockNodePlugin with
            ServerStatusServicePlugin;
}
