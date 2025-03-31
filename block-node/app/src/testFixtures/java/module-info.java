// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.app.test.fixtures {
    // it is expected the app module will never
    exports org.hiero.block.node.app.fixtures.blocks;
    exports org.hiero.block.node.app.fixtures.plugintest;

    requires com.hedera.pbj.runtime;
    requires com.swirlds.common;
    requires com.swirlds.config.api;
    requires com.swirlds.metrics.api;
    requires org.hiero.block.node.spi;
    requires org.hiero.block.stream;
    requires io.helidon.webserver;
    requires java.logging;
    requires org.junit.jupiter.api;
}
