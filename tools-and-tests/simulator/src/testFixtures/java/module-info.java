// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.simulator.test.fixtures {
    exports org.hiero.block.simulator.fixtures;
    exports org.hiero.block.simulator.fixtures.blocks;

    requires com.swirlds.common;
    requires com.swirlds.config.extensions;
    requires org.hiero.block.protobuf.pbj;
    requires org.hiero.block.protobuf.protoc;
    requires com.google.protobuf;
}
