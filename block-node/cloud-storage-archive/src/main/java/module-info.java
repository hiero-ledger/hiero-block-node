// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.cloud.storage.archive.CloudStorageArchiveConfigExtension;
import org.hiero.block.node.cloud.storage.archive.CloudStorageArchivePlugin;

module org.hiero.block.node.cloud.storage.archive {
    exports org.hiero.block.node.cloud.storage.archive to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.hedera.bucky;
    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.metrics;
    requires com.hedera.pbj.runtime;
    requires org.hiero.block.node.base;
    requires org.hiero.block.protobuf.pbj;

    provides com.swirlds.config.api.ConfigurationExtension with
            CloudStorageArchiveConfigExtension;
    provides org.hiero.block.node.spi.BlockNodePlugin with
            CloudStorageArchivePlugin;
}
