// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import edu.umd.cs.findbugs.annotations.Nullable;

/// Tracks one temporary S3 archive produced by [TempArchiveUploadTask].
///
/// The entry is the durable unit of the temporary-archive tracker maintained by
/// [CloudStorageArchivePlugin].  It survives restarts via a companion `.meta` object in S3 whose
/// key is [TempArchiveKey#formatMeta(long, String)] and whose content is the decimal string
/// representation of [lastBlock].
///
/// @param s3Key      the S3 key of the `.tmp` tar object
/// @param firstBlock the first block number stored in the archive (inclusive)
/// @param lastBlock  the last block number stored in the archive (inclusive)
/// @param uploadId   non-null while the multipart upload is still open; `null` once the S3 object
///                   has been finalised
record TempArchiveEntry(
        String s3Key,
        long firstBlock,
        long lastBlock,
        @Nullable String uploadId) {}
