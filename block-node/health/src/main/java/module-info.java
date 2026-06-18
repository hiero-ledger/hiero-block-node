// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.health.HealthServicePlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.health {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    // export configuration classes to the config module and app
    exports org.hiero.block.node.health to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires transitive io.helidon.webserver;
    requires com.hedera.pbj.runtime;
    requires org.hiero.block.node.base;
    requires org.hiero.block.protobuf.pbj;
    requires com.github.spotbugs.annotations;
    requires io.helidon.http;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            HealthServicePlugin;
}
