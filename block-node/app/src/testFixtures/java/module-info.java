// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.app.test.fixtures {
    exports org.hiero.block.node.app.fixtures;
    exports org.hiero.block.node.app.fixtures.async;
    exports org.hiero.block.node.app.fixtures.blocks;
    exports org.hiero.block.node.app.fixtures.pipeline;
    exports org.hiero.block.node.app.fixtures.plugintest;
    exports org.hiero.block.node.app.fixtures.server;

    requires com.hedera.pbj.grpc.helidon.config;
    requires com.hedera.pbj.grpc.helidon;
    requires com.hedera.pbj.runtime;
    requires com.swirlds.common;
    requires com.swirlds.config.api;
    requires com.swirlds.metrics.api;
    requires org.hiero.block.node.app.config;
    requires org.hiero.block.node.spi;
    requires org.hiero.block.protobuf.pbj;
    requires com.github.luben.zstd_jni;
    requires io.helidon.webserver;
    requires java.logging;
    requires org.junit.jupiter.api;
}
