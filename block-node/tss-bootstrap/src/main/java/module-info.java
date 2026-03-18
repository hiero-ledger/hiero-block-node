// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.tss.bootstrap.TssBootstrapPlugin;

module org.hiero.block.node.tss.bootstrap {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.protobuf.pbj;
    requires com.hedera.pbj.runtime;
    requires com.swirlds.config.api;
    requires org.hiero.block.node.app.config;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            TssBootstrapPlugin;
}
