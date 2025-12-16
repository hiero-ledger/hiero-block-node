// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.access.service.BlockAccessServicePlugin;

module org.hiero.block.node.access.service {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.protobuf.pbj;
    requires com.hedera.pbj.runtime;
    requires com.swirlds.metrics.api;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            BlockAccessServicePlugin;
}
