// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.messaging.BlockMessagingFacilityImpl;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.messaging {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    // export configuration classes to the config module
    exports org.hiero.block.node.messaging to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires org.hiero.block.node.base;
    requires com.github.spotbugs.annotations;
    requires com.lmax.disruptor;

    provides org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility with
            BlockMessagingFacilityImpl;
}
