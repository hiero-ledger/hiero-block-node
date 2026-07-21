// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.messaging.BlockMessagingFacilityImpl;
import org.hiero.block.node.messaging.MessagingConfigExtension;

module org.hiero.block.node.messaging {
    // export configuration classes to the config module
    exports org.hiero.block.node.messaging to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.metrics;
    requires org.hiero.block.common;
    requires org.hiero.block.node.base;
    requires com.github.spotbugs.annotations;
    requires com.lmax.disruptor;

    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    provides com.swirlds.config.api.ConfigurationExtension with
            MessagingConfigExtension;
    provides org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility with
            BlockMessagingFacilityImpl;
}
