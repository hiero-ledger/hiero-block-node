// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.access.service.BlockAccessServicePlugin;

module org.hiero.block.node.access.service {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    // export configuration classes to the config module and app
    exports org.hiero.block.node.access.service to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.hedera.pbj.runtime;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.protobuf.pbj;
    requires transitive org.hiero.metrics;
    requires com.swirlds.config.api;
    requires org.hiero.block.node.base;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            BlockAccessServicePlugin;
}
