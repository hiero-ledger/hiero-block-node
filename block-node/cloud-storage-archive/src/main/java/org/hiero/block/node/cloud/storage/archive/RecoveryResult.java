// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import java.util.List;

/// The outcome of a [StartupRecoveryTask] run.
///
/// Three cases are encoded:
///
/// - **Fresh start** (`currentGroupStart == -1`): no prior S3 state was found; the plugin will
///   begin uploading normally when the first verified block arrives.
/// - **Completed recovery** (`currentGroupStart >= 0`, `uploadId == null`): the last completed
///   tar group ended at `currentGroupStart`; [CloudStorageArchivePlugin] should start a new
///   [BlockUploadTask] for this group.
/// - **Resume** (`currentGroupStart >= 0`, `uploadId != null`): a hanging multipart upload was
///   found and its clean boundary was located; [BlockUploadTask] should resume the upload via
///   [uploadId], [etags], [nextBlockNumber], and [trailingBytes].
///
/// @param currentGroupStart the `currentGroupStart` value the plugin should use, or `-1` for a
///                          fresh start
/// @param uploadId          non-null when [BlockUploadTask] should resume an existing upload
/// @param etags             ETags of the parts already committed before the boundary part, in
///                          part-number order; non-null when [uploadId] is non-null
/// @param nextBlockNumber   first block number at the recovered boundary; [BlockUploadTask] will
///                          take blocks starting at this number from its queue
/// @param trailingBytes     bytes from the start of the boundary part up to (but not including)
///                          the last block-start marker; these carry-over overflow bytes are used
///                          by [BlockUploadTask] to seed its accumulation buffer so that the
///                          previously-started S3 part is completed correctly on resume;
///                          non-null when [uploadId] is non-null
record RecoveryResult(
        long currentGroupStart, String uploadId, List<String> etags, long nextBlockNumber, byte[] trailingBytes) {}
