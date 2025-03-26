// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.publisher.PublisherServicePlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.publisher {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.config.api;
    requires transitive com.swirlds.metrics.api;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.stream;
    requires transitive io.helidon.common;
    requires transitive io.helidon.webserver;
    requires com.hedera.pbj.grpc.helidon;
    requires org.hiero.block.base;
    requires com.github.spotbugs.annotations;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            PublisherServicePlugin;
}
