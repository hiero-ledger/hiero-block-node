// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.messaging {
    exports org.hiero.block.server.messaging;

    requires transitive com.lmax.disruptor;
    requires org.hiero.block.stream;
}
