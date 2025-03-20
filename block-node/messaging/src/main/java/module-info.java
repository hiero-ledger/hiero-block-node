import org.hiero.block.server.messaging.impl.BlockMessagingFacilityImpl;
import org.hiero.block.server.plugins.blockmessaging.BlockMessagingFacility;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.messaging {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;


    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.stream;
    requires org.hiero.block.base;
    requires org.hiero.block.plugins;
    requires com.lmax.disruptor;

    provides BlockMessagingFacility with
            BlockMessagingFacilityImpl;
}
