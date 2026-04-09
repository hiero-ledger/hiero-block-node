// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.cloud.archive.ArchiveCloudStoragePlugin;

module org.hiero.block.node.cloud.archive {
    requires transitive org.hiero.block.node.spi;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            ArchiveCloudStoragePlugin;
}
