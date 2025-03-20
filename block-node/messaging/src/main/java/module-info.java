// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.messaging.BlockMessagingFacilityImpl;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.messaging {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.stream;
    requires org.hiero.block.base;
    requires org.hiero.block.node.spi;
    requires com.lmax.disruptor;

    provides BlockMessagingFacility with
            BlockMessagingFacilityImpl;
}
