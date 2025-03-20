// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.subscriber.SubscriberServicePlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.health {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.stream;
    requires com.hedera.pbj.grpc.helidon;
    requires org.hiero.block.base;
    requires org.hiero.block.node.spi;
    requires com.lmax.disruptor;

    provides BlockNodePlugin with
            SubscriberServicePlugin;
}
