// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.app.config {
    // export configuration classes to the config module
    exports org.hiero.block.node.app.config to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app,
            org.hiero.block.node.app.test.fixtures,
            org.hiero.block.node.stream.publisher,
            org.hiero.block.node.stream.subscriber,
            org.hiero.block.node.access.service,
            org.hiero.block.node.server.status,
            org.hiero.block.node.health;
    // export the node-wide configuration to everything.
    exports org.hiero.block.node.app.config.node;
    exports org.hiero.block.node.app.config.state;

    requires transitive com.swirlds.config.api;
    requires com.swirlds.base;
    requires org.hiero.block.node.base;
    requires java.logging;
    requires static transitive com.github.spotbugs.annotations;
    requires static java.compiler;
}
