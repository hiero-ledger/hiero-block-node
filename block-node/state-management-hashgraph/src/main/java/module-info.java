// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.state.management.StateManagementPlugin;

module org.hiero.block.node.state.management {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    exports org.hiero.block.node.state.management to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.protobuf.pbj;
    requires com.hedera.pbj.runtime;
    requires com.swirlds.base;
    requires com.swirlds.merkledb;
    requires com.swirlds.metrics.api;
    requires com.swirlds.state.api;
    requires com.swirlds.state.impl;
    requires com.swirlds.virtualmap;
    requires org.hiero.base.crypto;
    requires org.hiero.base.utility;
    requires org.hiero.consensus.metrics;
    requires org.hiero.consensus.utility;
    requires org.hiero.metrics;
    requires com.github.spotbugs.annotations;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            StateManagementPlugin;
}
