/** Runtime module of the suites. */
module com.hedera.block.node.suites {
    // Require testing libraries
    requires com.hedera.block.simulator;
    requires com.swirlds.config.api;
    requires com.swirlds.config.extensions;
    requires io.github.cdimascio.dotenv.java;
    requires org.junit.jupiter.api;
    requires org.junit.platform.suite.api;
    requires org.testcontainers;
    requires static dagger;

    exports com.hedera.block.suites to
            org.junit.platform.commons;
}
