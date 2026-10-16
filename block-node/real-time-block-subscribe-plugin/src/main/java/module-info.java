// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.realtimeblocksubscribe.RealTimeBlockSubscribeConfigExtension;
import org.hiero.block.node.realtimeblocksubscribe.RealTimeBlockSubscribePlugin;

module org.hiero.block.node.realtimeblocksubscribe {
    // export configuration classes to the config module and app
    exports org.hiero.block.node.realtimeblocksubscribe to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.base;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.metrics;
    requires org.hiero.block.node.app.config;
    requires java.logging;
    requires static transitive com.github.spotbugs.annotations;

    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    provides com.swirlds.config.api.ConfigurationExtension with
            RealTimeBlockSubscribeConfigExtension;
    provides org.hiero.block.node.spi.BlockNodePlugin with
            RealTimeBlockSubscribePlugin;
}
