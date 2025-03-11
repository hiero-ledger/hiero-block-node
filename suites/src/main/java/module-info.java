// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.suites {
    requires com.hedera.block.simulator;
    requires com.swirlds.config.api;
    requires com.swirlds.config.extensions;
    requires io.github.cdimascio.dotenv.java;
    requires org.junit.jupiter.api;
    requires org.junit.platform.suite.api;
    requires org.testcontainers;
    requires static dagger;

    exports org.hiero.block.suites to
            org.junit.platform.commons;
}
