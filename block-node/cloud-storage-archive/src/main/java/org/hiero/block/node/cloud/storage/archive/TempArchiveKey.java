// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import edu.umd.cs.findbugs.annotations.NonNull;

/// Encodes and decodes S3 object keys for temporary tar archives.
///
/// Temporary archives live under a `tmp/` virtual directory so that [StartupRecoveryTask]'s
/// [StartupRecoveryTask#findLastKey] traversal can exclude them by skipping the `tmp/` common
/// prefix.  Two companion objects are created per archive segment:
///
/// - **Tar object** — `{prefix}/tmp/{firstBlock:019d}.tmp` — the raw tar content.
/// - **Meta object** — `{prefix}/tmp/{firstBlock:019d}.meta` — a single-line text file whose
///   content is the decimal string representation of `lastBlock`.  Written by
///   [TempArchiveUploadTask] immediately after the multipart upload completes.  Its existence
///   signals that the archive is fully durable.
///
/// At startup, [StartupRecoveryTask] lists `{tmpPrefix}*.meta` files to reconstruct the
/// [TempArchiveEntry] list without downloading any tar content.
final class TempArchiveKey {

    private TempArchiveKey() {}

    /// Returns the virtual directory prefix under which all temporary archives are stored,
    /// including the trailing `/`.
    ///
    /// For an empty `objectKeyPrefix` this is `"tmp/"`.
    /// For a non-empty prefix it is `"{prefix}/tmp/"`.
    static @NonNull String tmpPrefix(@NonNull String objectKeyPrefix) {
        return objectKeyPrefix.isEmpty() ? "tmp/" : objectKeyPrefix + "/tmp/";
    }

    /// Returns the S3 key for the tar content of a temporary archive whose first block is
    /// `firstBlock`.
    static @NonNull String formatTar(long firstBlock, @NonNull String objectKeyPrefix) {
        return tmpPrefix(objectKeyPrefix) + String.format("%019d", firstBlock) + ".tmp";
    }

    /// Returns the S3 key for the meta companion of a temporary archive whose first block is
    /// `firstBlock`.
    static @NonNull String formatMeta(long firstBlock, @NonNull String objectKeyPrefix) {
        return tmpPrefix(objectKeyPrefix) + String.format("%019d", firstBlock) + ".meta";
    }

    /// Parses the first block number from a tar key produced by [#formatTar].
    static long parseFirstBlockFromTar(@NonNull String key, @NonNull String objectKeyPrefix) {
        final String stem = stemFromTmp(key, objectKeyPrefix);
        return Long.parseLong(stem);
    }

    /// Parses the first block number from a meta key produced by [#formatMeta].
    static long parseFirstBlockFromMeta(@NonNull String key, @NonNull String objectKeyPrefix) {
        final String stem = stemFromMeta(key, objectKeyPrefix);
        return Long.parseLong(stem);
    }

    /// Returns `true` when `key` is a temporary-archive tar key under the configured prefix.
    static boolean isTempTarKey(@NonNull String key, @NonNull String objectKeyPrefix) {
        return key.startsWith(tmpPrefix(objectKeyPrefix)) && key.endsWith(".tmp");
    }

    /// Returns `true` when `key` is a temporary-archive meta key under the configured prefix.
    static boolean isTempMetaKey(@NonNull String key, @NonNull String objectKeyPrefix) {
        return key.startsWith(tmpPrefix(objectKeyPrefix)) && key.endsWith(".meta");
    }

    // Strips the tmp-prefix and ".tmp" suffix, leaving just the 19-digit stem.
    private static String stemFromTmp(String key, String objectKeyPrefix) {
        final String withoutPrefix = key.substring(tmpPrefix(objectKeyPrefix).length());
        return withoutPrefix.substring(0, withoutPrefix.length() - 4); // remove ".tmp"
    }

    // Strips the tmp-prefix and ".meta" suffix, leaving just the 19-digit stem.
    private static String stemFromMeta(String key, String objectKeyPrefix) {
        final String withoutPrefix = key.substring(tmpPrefix(objectKeyPrefix).length());
        return withoutPrefix.substring(0, withoutPrefix.length() - 5); // remove ".meta"
    }
}
