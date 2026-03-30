// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.cloud.archive.ArchiveCloudStoragePlugin;

module org.hiero.block.node.cloud.archive {
    requires transitive com.hedera.bucky;
    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.protobuf.pbj;
    requires com.hedera.pbj.runtime;
    requires org.hiero.block.node.base;

    // export configuration classes to the config module and app
    exports org.hiero.block.node.cloud.archive to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            ArchiveCloudStoragePlugin;
}
