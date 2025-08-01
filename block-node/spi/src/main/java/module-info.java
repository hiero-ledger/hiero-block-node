// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.spi {
    exports org.hiero.block.node.spi;
    exports org.hiero.block.node.spi.blockmessaging;
    exports org.hiero.block.node.spi.historicalblocks;
    exports org.hiero.block.node.spi.health;
    exports org.hiero.block.node.spi.threading;

    uses org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
    uses org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
    uses org.hiero.block.node.spi.BlockNodePlugin;

    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.config.api;
    requires transitive com.swirlds.metrics.api;
    requires transitive org.hiero.block.protobuf.pbj;
    requires transitive io.helidon.webserver;
    requires com.github.luben.zstd_jni;
    requires static transitive com.github.spotbugs.annotations;
    requires static java.compiler; // javax.annotation.processing.Generated
}
