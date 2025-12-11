// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.state.live.LiveStatePlugin;

module org.hiero.block.node.state.live {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    // Export configuration to swirlds config
    exports org.hiero.block.node.state.live to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires com.hedera.pbj.runtime;
    requires org.hiero.block.node.base;
    requires static com.github.spotbugs.annotations;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            LiveStatePlugin;
}
