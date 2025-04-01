// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.publisher.PublisherServicePlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.publisher {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    // export configuration classes to the config module and app
    exports org.hiero.block.node.publisher to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.config.api;
    requires transitive com.swirlds.metrics.api;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.stream;
    requires org.hiero.block.common;
    requires org.hiero.block.node.base;
    requires com.github.spotbugs.annotations;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            PublisherServicePlugin;
}
