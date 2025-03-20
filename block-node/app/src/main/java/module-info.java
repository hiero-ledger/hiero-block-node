// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.app {
    uses HistoricalBlockFacility;
    uses BlockMessagingFacility;
    uses BlockNodePlugin;

    exports org.hiero.block.node.app;

    requires com.hedera.pbj.grpc.helidon.config;
    requires com.hedera.pbj.grpc.helidon;
    requires com.hedera.pbj.runtime;
    requires com.swirlds.config.api;
    requires com.swirlds.config.extensions;
    requires com.swirlds.metrics.api;
    requires org.hiero.block.base;
    requires org.hiero.block.common;
    requires org.hiero.block.node.spi;
    requires org.hiero.block.stream;
    requires dagger;
    requires io.helidon.common;
    requires io.helidon.webserver;
    requires javax.inject;
    requires static transitive com.github.spotbugs.annotations;
    requires static transitive com.google.auto.service;
    requires static java.compiler; // javax.annotation.processing.Generated
}
