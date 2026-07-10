// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import java.util.List;

/// Describes a hanging [TempArchiveUploadTask] multipart upload that [StartupRecoveryTask] has
/// determined is resumable, rather than aborted.
///
/// Unlike [TempArchiveEntry] (which represents only durably completed archives, tracked in
/// [CloudStorageArchivePlugin#tempArchiveTracker]), this record describes a segment whose
/// multipart upload is still open on S3.  Keeping the two types separate means every existing
/// [TempArchiveEntry] consumer: `StartupRecoveryTask#withTempArchives`,
/// `CloudStorageArchivePlugin#hasAnyTempDataForGroup`,
/// `CloudStorageArchivePlugin#isBlockCoveredByAnyTempSegment`, can keep treating every tracker
/// entry as done, without special-casing a still-open upload.
///
/// @param s3Key           the S3 key of the `.tmp` tar object
/// @param firstBlock      the segment's original first block number (its identity / S3 key), used
///                        to key the resumed [TempArchiveUploadTask] the same way a fresh segment
///                        would be
/// @param nextBlockNumber the first block number not yet durably part of the upload; the resumed
///                        [TempArchiveUploadTask] begins consuming its queue from this block
/// @param uploadId        the multipart upload ID of the newly created upload to resume
/// @param etags           ETags of the parts already committed before the boundary part, in
///                        part-number order
/// @param trailingBytes   bytes from the start of the boundary part up to (but not including) the
///                        last block-start marker; prepended to the resumed task's accumulation
///                        buffer so the previously-started S3 part is completed correctly
record TempArchiveResumeState(
        String s3Key,
        long firstBlock,
        long nextBlockNumber,
        String uploadId,
        List<String> etags,
        byte[] trailingBytes) {}
