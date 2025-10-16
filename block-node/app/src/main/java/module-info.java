// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

module org.hiero.block.node.app {
    // it is expected the app module will never export any packages, only use others
    uses HistoricalBlockFacility;
    uses BlockMessagingFacility;
    uses BlockNodePlugin;
    uses BlockProviderPlugin;

    requires com.hedera.pbj.grpc.helidon.config;
    requires com.hedera.pbj.grpc.helidon;
    requires com.hedera.pbj.runtime;
    requires com.swirlds.common;
    requires com.swirlds.config.api;
    requires com.swirlds.config.extensions;
    requires com.swirlds.metrics.api;
    requires org.hiero.block.common;
    requires org.hiero.block.node.app.config;
    requires org.hiero.block.node.base;
    requires org.hiero.block.node.spi;
    requires io.helidon.common;
    requires io.helidon.webserver;
    requires io.helidon.webserver.http2;
    requires java.logging; // javax.annotation.processing.Generated
    requires static transitive com.github.spotbugs.annotations;
    requires static java.compiler;
}
