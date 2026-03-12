// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.simulator.test.fixtures {
    exports org.hiero.block.simulator.fixtures;
    exports org.hiero.block.simulator.fixtures.blocks;
    exports org.hiero.block.simulator.fixtures.generator;

    requires com.hedera.pbj.runtime;
    requires com.swirlds.config.extensions;
    requires org.hiero.block.protobuf.protoc;
    requires org.hiero.block.simulator;
    requires org.hiero.metrics;
    requires com.google.protobuf;
    requires org.junit.jupiter.api;
}
