// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.roster.bootstrap.tss.RosterBootstrapTssPlugin;

module org.hiero.block.node.roster.bootstrap.tss {
    // export configuration classes to the config module and app
    exports org.hiero.block.node.roster.bootstrap.tss to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires org.hiero.block.node.base;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            RosterBootstrapTssPlugin;
}
