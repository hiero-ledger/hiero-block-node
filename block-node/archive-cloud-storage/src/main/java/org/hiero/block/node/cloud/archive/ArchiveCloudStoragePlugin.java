// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.archive;

import static java.lang.System.Logger.Level.DEBUG;

import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;

/// A block node plugin that stores verified blocks in cloud storage aggregated into large `tar` archives.
///
/// On each [org.hiero.block.node.spi.blockmessaging.VerificationNotification], a
/// `SingleBlockStoreTask` compresses the block with ZStandard and passes it to the active
/// `BlockArchiveTask`, which streams the data directly to the remote `tar` file in block-number
/// order. No local staging occurs. A
/// [org.hiero.block.node.spi.blockmessaging.PersistedNotification] is published per block
/// only after durable remote storage is confirmed.
///
/// When blocks arrive out of order or an operation fails, the plugin initiates recovery via an
/// `ArchiveRecoveryTask` that consolidates any temporary `tar` files and resumes normal archiving.
public class ArchiveCloudStoragePlugin implements BlockNodePlugin, BlockNotificationHandler {

    /// The logger for this class.
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /// {@inheritDoc}
    ///
    /// Starts the archive cloud storage plugin.
    @Override
    public void start() {
        LOGGER.log(DEBUG, "Archive cloud storage plugin started");
    }
}
