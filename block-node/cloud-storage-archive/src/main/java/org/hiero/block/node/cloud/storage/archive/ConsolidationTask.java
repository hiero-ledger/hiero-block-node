// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.util.Objects.requireNonNull;

import com.hedera.bucky.S3Client;
import com.hedera.bucky.S3ClientInitializationException;
import com.hedera.bucky.S3ResponseException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/// Merges a complete set of temporary archives covering one aligned group into a single final
/// `.tar` archive and removes the temporary objects from S3.
///
/// The caller guarantees that the [entries] list, sorted by [TempArchiveEntry#firstBlock()],
/// together cover exactly the block range `[groupStart, groupStart + groupSize)`.  The task:
///
/// 1. Creates a new multipart upload at [ArchiveKey#format(long, int, String)].
/// 2. Downloads each `.tmp` object and pipes its raw bytes directly into the upload (no tar
///    re-encoding — temp archives and final archives share the same tar-entry format).
/// 3. Completes the multipart upload once all bytes are transferred.
/// 4. Deletes each `.tmp` tar object and its `.meta` companion from S3.
///
/// No new [org.hiero.block.node.spi.blockmessaging.PersistedNotification]s are sent because the
/// temporary archives already notified consumers when they were created.
class ConsolidationTask implements Callable<UploadResult> {

    private static final System.Logger LOGGER = System.getLogger(ConsolidationTask.class.getName());

    private final CloudStorageArchiveConfig config;
    private final List<TempArchiveEntry> entries;
    private final long groupStart;
    private final long groupSize;

    ConsolidationTask(
            @NonNull CloudStorageArchiveConfig config,
            @NonNull List<TempArchiveEntry> entries,
            long groupStart,
            long groupSize) {
        this.config = requireNonNull(config);
        this.entries = List.copyOf(requireNonNull(entries));
        this.groupStart = groupStart;
        this.groupSize = groupSize;
    }

    @Override
    public UploadResult call() throws S3ClientInitializationException, S3ResponseException, IOException {
        final String finalKey = ArchiveKey.format(groupStart, config.groupingLevel(), config.objectKeyPrefix());
        LOGGER.log(
                TRACE,
                "Consolidating {0} temp archives into final key {1} for group [{2}, {3})",
                entries.size(),
                finalKey,
                groupStart,
                groupStart + groupSize);

        try (S3Client s3 = S3UploadUtils.createClient(config)) {
            final String uploadId =
                    s3.createMultipartUpload(finalKey, config.storageClass().name(), S3UploadUtils.CONTENT_TYPE);
            final List<String> etags = new ArrayList<>();
            final int partSizeBytes = config.partSizeMb() * 1024 * 1024;
            byte[] buffer = new byte[0];

            try {
                for (final TempArchiveEntry entry : entries) {
                    buffer = downloadAndStreamParts(s3, entry, finalKey, uploadId, partSizeBytes, etags, buffer);
                }
                if (buffer.length > 0) {
                    doUploadPart(buffer, finalKey, s3, uploadId, etags);
                }
                doCompleteMultipartUpload(s3, finalKey, uploadId, etags);
            } catch (S3ResponseException | IOException e) {
                LOGGER.log(INFO, "Consolidation failed for key {0}", finalKey, e);
                S3UploadUtils.abortQuietly(s3, finalKey, uploadId);
                throw e;
            }

            LOGGER.log(TRACE, "Consolidated final archive at key {0}", finalKey);
            deleteTemporaryObjects(s3);
        }
        return UploadResult.SUCCESS;
    }

    /// Downloads [entry]'s bytes in [partSizeBytes]-sized chunks and streams them into the active
    /// multipart upload as complete parts.  Returns the updated accumulation buffer.
    private byte[] downloadAndStreamParts(
            S3Client s3,
            TempArchiveEntry entry,
            String finalKey,
            String uploadId,
            int partSizeBytes,
            List<String> etags,
            byte[] buffer)
            throws S3ResponseException, IOException {
        long offset = 0;
        while (true) {
            final byte[] chunk;
            try {
                chunk = s3.downloadObjectRange(entry.s3Key(), offset, offset + partSizeBytes - 1);
            } catch (S3ResponseException e) {
                if (e.getResponseStatusCode() == 416) {
                    break; // object length is an exact multiple of partSizeBytes
                }
                throw e;
            }
            offset += chunk.length;
            buffer = S3UploadUtils.concat(buffer, chunk);
            while (buffer.length >= partSizeBytes) {
                final S3UploadUtils.SplitBuffer split = S3UploadUtils.splitAtPartSize(buffer, partSizeBytes);
                buffer = split.remainder();
                doUploadPart(split.part(), finalKey, s3, uploadId, etags);
            }
            if (chunk.length < partSizeBytes) {
                break; // reached end of this temp archive
            }
        }
        return buffer;
    }

    /// Delegates to [S3UploadUtils#uploadPart].  Overridable so tests can inject upload failures
    /// without a live S3 endpoint.
    void doUploadPart(byte[] bytes, String key, S3Client s3, String uploadId, List<String> etags)
            throws S3ResponseException, IOException {
        S3UploadUtils.uploadPart(bytes, key, s3, uploadId, etags);
    }

    /// Delegates to [S3Client#completeMultipartUpload].  Overridable so tests can inject completion
    /// failures without a live S3 endpoint.
    void doCompleteMultipartUpload(S3Client s3, String key, String uploadId, List<String> etags)
            throws S3ResponseException, IOException {
        s3.completeMultipartUpload(key, uploadId, etags);
    }

    private void deleteTemporaryObjects(S3Client s3) {
        for (final TempArchiveEntry entry : entries) {
            deleteQuietly(s3, entry.s3Key());
            deleteQuietly(s3, TempArchiveKey.formatMeta(entry.firstBlock(), config.objectKeyPrefix()));
        }
    }

    private void deleteQuietly(S3Client s3, String key) {
        try {
            s3.deleteObject(key);
            LOGGER.log(TRACE, "Deleted temporary object {0}", key);
        } catch (S3ResponseException | IOException e) {
            LOGGER.log(DEBUG, "Failed to delete temporary object {0} after consolidation", key, e);
        }
    }
}
