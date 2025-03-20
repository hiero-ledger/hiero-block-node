// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.health.HealthServicePlugin;
import org.hiero.block.node.spi.BlockNodePlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.health {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.stream;
    requires org.hiero.block.base;
    requires org.hiero.block.node.spi;
    requires com.lmax.disruptor;

    provides BlockNodePlugin with
            HealthServicePlugin;
}
