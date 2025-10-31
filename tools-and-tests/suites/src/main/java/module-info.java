// SPDX-License-Identifier: Apache-2.0
open module org.hiero.block.node.suites {
    requires com.hedera.pbj.grpc.client.helidon;
    requires com.hedera.pbj.runtime;
    requires com.swirlds.config.api;
    requires com.swirlds.config.extensions;
    requires org.hiero.block.node.app;
    requires org.hiero.block.node.spi;
    requires org.hiero.block.protobuf.protoc;
    requires org.hiero.block.simulator;
    requires com.github.dockerjava.api;
    requires io.grpc.stub;
    requires io.grpc;
    requires io.helidon.webclient.grpc;
    requires java.net.http;
    requires org.assertj.core;
    requires org.junit.jupiter.api;
    requires org.junit.jupiter.params;
    requires org.testcontainers;
    requires static com.github.spotbugs.annotations;
    requires static dagger;
    requires static io.helidon.webclient.api;
    requires static org.junit.platform.suite.api;
}
