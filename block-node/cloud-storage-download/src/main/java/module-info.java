// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.cloud.storage.download.CloudStorageDownloadConfigExtension;
import org.hiero.block.node.cloud.storage.download.CloudStorageDownloadPlugin;

module org.hiero.block.node.cloud.storage.download {
    // export configuration classes to the config module and app
    exports org.hiero.block.node.cloud.storage.download to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires com.hedera.bucky;
    requires com.hedera.pbj.runtime;
    requires org.hiero.block.node.base;
    requires org.hiero.block.protobuf.pbj;
    requires org.hiero.metrics;
    requires com.github.spotbugs.annotations;

    provides com.swirlds.config.api.ConfigurationExtension with
            CloudStorageDownloadConfigExtension;
    provides org.hiero.block.node.spi.BlockNodePlugin with
            CloudStorageDownloadPlugin;
}
