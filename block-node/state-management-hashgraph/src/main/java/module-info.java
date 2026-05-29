// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.state.management.StateManagementPlugin;

module org.hiero.block.node.state.management {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    exports org.hiero.block.node.state.management to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.config.api;
    requires transitive com.swirlds.state.api;
    requires transitive com.swirlds.state.impl;
    requires transitive com.swirlds.virtualmap;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.protobuf.pbj;
    requires transitive org.hiero.metrics;
    requires com.swirlds.base;
    requires com.swirlds.merkledb;
    requires org.hiero.base.utility;
    requires org.hiero.consensus.metrics;
    requires org.hiero.consensus.utility;
    requires com.github.spotbugs.annotations;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            StateManagementPlugin;
}
