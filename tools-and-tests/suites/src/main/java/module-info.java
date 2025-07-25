// SPDX-License-Identifier: Apache-2.0
open module org.hiero.block.node.suites {
    requires com.swirlds.config.api;
    requires com.swirlds.config.extensions;
    requires org.hiero.block.protobuf.protoc;
    requires org.hiero.block.simulator;
    requires io.grpc;
    requires java.net.http;
    requires org.junit.jupiter.api;
    requires org.junit.jupiter.params;
    requires org.junit.platform.suite.api;
    requires org.testcontainers;
    requires static com.github.spotbugs.annotations;
    requires static dagger;
}
