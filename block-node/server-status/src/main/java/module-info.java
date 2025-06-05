// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.server.status.ServerStatusServicePlugin;

module org.hiero.block.node.server.status {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.protobuf.pbj;
    requires com.hedera.pbj.runtime;
    requires com.swirlds.metrics.api;
    requires com.github.spotbugs.annotations;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            ServerStatusServicePlugin;
}
