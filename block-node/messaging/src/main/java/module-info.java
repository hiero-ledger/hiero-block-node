// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.messaging.BlockMessagingFacilityImpl;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.messaging {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires transitive io.helidon.common;
    requires transitive io.helidon.webserver;
    requires org.hiero.block.base;
    requires com.github.spotbugs.annotations;
    requires com.lmax.disruptor;

    provides org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility with
            BlockMessagingFacilityImpl;
}
