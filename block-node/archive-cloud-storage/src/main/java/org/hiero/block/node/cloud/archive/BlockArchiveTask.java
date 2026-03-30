// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.archive;

import com.hedera.bucky.S3ClientException;
import java.io.IOException;
import org.hiero.block.internal.BlockUnparsed;

// @todo(1108) Maybe we don't need two implementations of this interface? Let's see whether the "recovery task" is a
// separate implementation of this. If not, we'll just have LiveBlockArchiveTask class
/// Manages the lifecycle of a single tar-archive upload to cloud storage.
///
/// Each task owns one contiguous range of block numbers and streams their serialised bytes
/// directly to a remote tar file via S3 multipart upload. Tasks are single-use: once
/// [ArchiveResult#FINISHED] is returned from [#submit] the task is complete and must not
/// receive further calls.
///
/// Typical call sequence:
/// 1. **[#init]** — opens the S3 client and creates the multipart-upload session.
/// 2. **[Runnable#run]** — submitted to a virtual-thread executor; blocks internally waiting for each block in range to
/// arrive via [#submit].
/// 3. **[#submit]** — called on the notification thread for every verified block; resolves the internal future that
/// unblocks [Runnable#run].
/// 4. **[#abort]** — called when the node is stopping or an unrecoverable error occurs; cancels any pending futures and
// aborts the S3 multipart upload.
interface BlockArchiveTask extends Runnable {

    /// The outcome returned by [#submit] after each block is handed to the task.
    enum ArchiveResult {
        /// The block was accepted and more blocks are still expected.
        SUCCESS,
        /// The block number falls outside this task's range.
        BLOCK_OUT_OF_RANGE,
        /// The block was accepted and this was the last block in the range; the task is done.
        FINISHED
    }

    /// Opens the S3 client and initiates a multipart-upload session for this task's tar key.
    ///
    /// Must be called once before [Runnable#run] is submitted to an executor.
    ///
    /// @throws S3ClientException if the S3 client cannot be created or the multipart-upload
    ///                           initiation request fails
    /// @throws IOException       if an I/O error occurs while establishing the connection
    void init() throws S3ClientException, IOException;

    /// Delivers a verified block to this task.
    ///
    /// If `blockNumber` is within this task's range the block's future is resolved, unblocking
    /// the [Runnable#run] loop. The returned value tells the caller how to proceed:
    ///
    /// - [ArchiveResult#SUCCESS] — block accepted, more blocks expected.
    /// - [ArchiveResult#FINISHED] — block accepted, range complete; discard this task reference.
    /// - [ArchiveResult#BLOCK_OUT_OF_RANGE] — block not accepted; caller must stash it.
    ///
    /// @param block       the unparsed block data to archive
    /// @param blockNumber the block number identifying `block` within the chain
    /// @return the result indicating whether the task accepted the block and whether it is complete
    ArchiveResult submit(final BlockUnparsed block, final long blockNumber);

    /// Aborts the in-progress multipart upload and releases all resources.
    ///
    /// Sets an aborted flag so that the [Runnable#run] loop exits at the next iteration,
    /// cancels any pending block futures, and calls `abortMultipartUpload` on the S3 client.
    /// Safe to call from any thread.
    void abort();
}
