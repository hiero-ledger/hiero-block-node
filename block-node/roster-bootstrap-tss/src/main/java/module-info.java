// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.roster.bootstrap.tss.RosterBootstrapTssPlugin;

module org.hiero.block.node.roster.bootstrap.tss {
    // export configuration classes to the config module and app
    exports org.hiero.block.node.roster.bootstrap.tss to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.protobuf.pbj;
    requires transitive org.hiero.metrics;
    requires com.hedera.pbj.grpc.client.helidon;
    requires org.hiero.block.node.base;
    requires io.helidon.common.tls;
    requires io.helidon.webclient.api;
    requires io.helidon.webclient.grpc;
    requires io.helidon.webclient.http2;
    requires org.antlr.antlr4.runtime;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            RosterBootstrapTssPlugin;
}
