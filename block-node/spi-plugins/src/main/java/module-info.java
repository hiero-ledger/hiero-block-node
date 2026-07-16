// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.spi {
    exports org.hiero.block.node.spi.blockmessaging;
    exports org.hiero.block.node.spi.health;
    exports org.hiero.block.node.spi.historicalblocks;
    exports org.hiero.block.node.spi.module;
    exports org.hiero.block.node.spi.threading;
    exports org.hiero.block.node.spi;

    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.protobuf.pbj;
    requires transitive org.hiero.metrics;
    requires transitive io.helidon.common.socket;
    requires transitive io.helidon.webserver.http2;
    requires transitive io.helidon.webserver;
    requires static transitive com.github.spotbugs.annotations;

    uses com.swirlds.config.api.ConfigurationExtension;
    uses org.hiero.block.node.spi.BlockNodePlugin;
    uses org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
    uses org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
}
