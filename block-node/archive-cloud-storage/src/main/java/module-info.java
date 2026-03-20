// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.archive.cloud.storage {
    requires transitive org.hiero.block.node.spi;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            org.hiero.block.node.archive.cloud.storage.ArchiveCloudStoragePlugin;
}
