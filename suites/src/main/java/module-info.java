// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.suites {
    requires com.swirlds.config.api;
    requires com.swirlds.config.extensions;
    requires org.hiero.block.simulator;
    requires org.hiero.block.protobuf;
    requires io.grpc;
    requires org.junit.jupiter.api;
    requires org.junit.platform.suite.api;
    requires org.testcontainers;
    requires static com.github.spotbugs.annotations;
    requires static dagger;

    exports org.hiero.block.suites to
            org.junit.platform.commons;
}
