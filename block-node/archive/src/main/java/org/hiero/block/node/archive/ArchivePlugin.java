// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.archive;

import org.hiero.block.node.spi.BlockNodePlugin;

/**
 * This a block node plugin that stores verified blocks in a cloud archive for disaster recovery and backup. It will
 * archive in batches as storing billions of small files in the cloud is non-ideal and expensive. Archive style cloud
 * storage has minimum file sizes and per file costs. So batches of compressed blocks will be more optimal. It will
 * watch persisted block notifications for when the next batch of blocks is available to be archived. It will then
 * fetch those blocks from persistence plugins and upload to the archive.
 */
public class ArchivePlugin implements BlockNodePlugin {
    // keep track of the last block number archived
    // have configuration for the number of blocks to archive at a time
    // on init register as a block notification handler, listen to persisted notifications
    // on persisted notification, check if the whole batch size of blocks from last block archived is available
    // if so, fetch them from the persistence plugin, compress and upload to the archive bucket
}
