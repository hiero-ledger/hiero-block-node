// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.archive.s3.S3ArchivePlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.archive.s3cloud {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    // export configuration classes to the config module and app
    exports org.hiero.block.node.archive.s3 to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires com.hedera.pbj.runtime;
    requires org.hiero.block.common;
    requires org.hiero.block.node.base;
    requires org.hiero.block.protobuf;
    requires com.github.spotbugs.annotations;
    requires java.management;
    requires java.net.http;
    requires java.xml;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            S3ArchivePlugin;
}
