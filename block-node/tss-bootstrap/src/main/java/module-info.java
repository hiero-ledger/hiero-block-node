// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.tss.bootstrap.TssBootstrapPlugin;

module org.hiero.block.node.tss.bootstrap {
    // export configuration classes to the config module and app
    exports org.hiero.block.node.tss.bootstrap to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires com.hedera.pbj.runtime;
    requires org.hiero.block.node.base;
    requires org.hiero.block.protobuf.pbj;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            TssBootstrapPlugin;
}
