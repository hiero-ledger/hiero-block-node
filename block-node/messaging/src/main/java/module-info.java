// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.messaging {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    exports org.hiero.block.server.messaging;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.stream;
    requires org.hiero.block.base;
    requires com.lmax.disruptor;

    provides org.hiero.block.server.messaging.MessagingService with
            org.hiero.block.server.messaging.impl.MessagingServiceImpl;
}
