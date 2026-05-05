// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.bucky.S3Client;
import com.hedera.bucky.S3Client.PartInfo;
import com.hedera.bucky.S3ClientInitializationException;
import com.hedera.bucky.S3ResponseException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/// Determines where [CloudStorageArchivePlugin] should resume uploading after a restart.
///
/// On startup S3 may be in one of three states:
///
/// 1. **No hanging multipart uploads** — find the last completed tar file, derive the group start
///    from its key, and return `groupStart + groupSize` so that the next upload begins with the
///    correct group.
/// 2. **One hanging multipart upload** — the previous run crashed mid-upload.  The already-uploaded
///    parts are preserved on S3.  This task completes the old upload into a temporary readable S3
///    object, creates a new upload for the same key, then scans the parts backwards (last to first)
///    to locate the last intact block-start boundary and its block number.  The new upload is
///    **left open**; [BlockUploadTask] resumes from that block number.
/// 3. **Multiple hanging multipart uploads** — should never happen in normal operation.  All uploads
///    are aborted and the task falls back to Case 1.
///
/// Returns a [RecoveryResult] describing whether to start fresh, resume at a group boundary, or
/// resume an in-progress upload.
class StartupRecoveryTask implements Callable<RecoveryResult> {

    private static final System.Logger LOGGER = System.getLogger(StartupRecoveryTask.class.getName());
    private static final String CONTENT_TYPE = "application/x-tar";
    /// Maximum number of tar objects to list when searching for the last completed tar.
    private static final int MAX_LIST_RESULTS = 1000;

    private final CloudStorageArchiveConfig config;

    StartupRecoveryTask(@NonNull CloudStorageArchiveConfig config) {
        this.config = config;
    }

    /// Connects to S3, inspects any hanging multipart uploads, and returns the [RecoveryResult]
    /// that tells [CloudStorageArchivePlugin] where to resume uploading.
    @Override
    public RecoveryResult call() throws S3ClientInitializationException, S3ResponseException, IOException {
        try (final S3Client s3 = new S3Client(
                config.regionName(),
                config.endpointUrl(),
                config.bucketName(),
                config.accessKey(),
                config.secretKey())) {
            final Map<String, List<String>> uploads = s3.listMultipartUploads();
            final int totalUploads =
                    uploads.values().stream().mapToInt(List::size).sum();
            return switch (totalUploads) {
                case 0 -> recoverFromCompletedObjects(s3);
                case 1 -> {
                    final Map.Entry<String, List<String>> entry =
                            uploads.entrySet().iterator().next();
                    yield recoverFromSingleHangingUpload(
                            s3, entry.getKey(), entry.getValue().getFirst());
                }
                default -> recoverFromMultipleHangingUploads(s3, uploads);
            };
        }
    }

    /// Case 1: no hanging uploads — find the last completed tar and return the start of the next group.
    private RecoveryResult recoverFromCompletedObjects(S3Client s3) throws S3ResponseException, IOException {
        final RecoveryResult recoveryResult;
        final String lastKey = findLastKey(s3);
        if (lastKey == null) {
            LOGGER.log(TRACE, "No prior state found in S3 bucket, starting fresh");
            recoveryResult = new RecoveryResult(-1, null, null, 0, null);
        } else {
            final long groupSize = Math.powExact(10, config.groupingLevel());
            final long groupStart = ArchiveKey.parse(lastKey, config.groupingLevel());
            LOGGER.log(
                    TRACE,
                    "Last completed tar group starts at {0}, resuming from {1}",
                    groupStart,
                    groupStart + groupSize);
            recoveryResult = new RecoveryResult(groupStart + groupSize, null, null, 0, null);
        }
        return recoveryResult;
    }

    /// Right-hand path traversal: descends the virtual S3 directory tree by always following
    /// the alphabetically last common prefix at each level, then pages through the leaf objects
    /// to return the key of the last one, or `null` if the bucket is empty.
    private String findLastKey(S3Client s3) throws S3ResponseException, IOException {
        String currentPrefix = "";
        String lastPrefix = findLastCommonPrefix(s3, currentPrefix);
        while (lastPrefix != null) {
            currentPrefix = lastPrefix;
            lastPrefix = findLastCommonPrefix(s3, currentPrefix);
        }
        return findLastObject(s3, currentPrefix);
    }

    /// Pages through all common prefixes under [prefix] and returns the alphabetically last one,
    /// or `null` if there are no common prefixes at that level.
    private String findLastCommonPrefix(S3Client s3, String prefix) throws S3ResponseException, IOException {
        String lastPrefix = null;
        String token = null;
        boolean hasMore = true;
        while (hasMore) {
            final S3Client.ListPage page = s3.listObjectsPage(prefix, token, "/", MAX_LIST_RESULTS);
            if (!page.keys().isEmpty()) {
                lastPrefix = page.keys().getLast();
            }
            token = page.continuationToken();
            hasMore = token != null;
        }
        return lastPrefix;
    }

    /// Pages through all objects under [prefix] and returns the key of the alphabetically last one,
    /// or `null` if the prefix contains no objects.
    private String findLastObject(S3Client s3, String prefix) throws S3ResponseException, IOException {
        String lastKey = null;
        String token = null;
        boolean hasMore = true;
        while (hasMore) {
            final S3Client.ListPage page = s3.listObjectsPage(prefix, token, null, MAX_LIST_RESULTS);
            if (!page.keys().isEmpty()) {
                lastKey = page.keys().getLast();
            }
            token = page.continuationToken();
            hasMore = token != null;
        }
        return lastKey;
    }

    /// Case 2: one hanging upload — complete it to materialize a readable S3 object, start a fresh
    /// upload for the same key, scan backwards through parts via [rebuildUpload] to find the last
    /// clean block-start boundary, and return a [RecoveryResult] with the new upload ID, ETags,
    /// block number, and trailing bytes — **without** completing the new upload.
    private RecoveryResult recoverFromSingleHangingUpload(S3Client s3, String key, String uploadId)
            throws S3ResponseException, IOException {
        LOGGER.log(TRACE, "Found hanging multipart upload for key {0}, starting recovery", key);
        final List<PartInfo> parts = s3.listParts(key, uploadId);
        final RecoveryResult result;
        if (!parts.isEmpty()) {
            s3.completeMultipartUpload(
                    key, uploadId, parts.stream().map(PartInfo::etag).toList());
            LOGGER.log(TRACE, "Completed hanging upload to create temporary S3 object at key {0}", key);
            final String newUploadId =
                    s3.createMultipartUpload(key, config.storageClass().name(), CONTENT_TYPE);
            final List<String> newEtags = new ArrayList<>();

            final PartScanResult scanResult = rebuildUpload(s3, key, newUploadId, newEtags, parts);
            if (scanResult.trailingBytes().length > 0) {
                final long groupStart = ArchiveKey.parse(key, config.groupingLevel());
                LOGGER.log(
                        TRACE,
                        "Recovery prepared upload {0} for key {1}; resuming from block {2}",
                        newUploadId,
                        key,
                        scanResult.blockNumber());
                result = new RecoveryResult(
                        groupStart, newUploadId, newEtags, scanResult.blockNumber(), scanResult.trailingBytes());
            } else {
                LOGGER.log(DEBUG, "No block start found in any part for key {0}; aborting", key);
                s3.abortMultipartUpload(key, newUploadId);
                s3.deleteObject(key);
                result = recoverFromCompletedObjects(s3);
            }
        } else {
            s3.abortMultipartUpload(key, uploadId);
            LOGGER.log(TRACE, "Hanging upload had no parts, aborted");
            result = recoverFromCompletedObjects(s3);
        }

        return result;
    }

    /// Scans parts newest-first to find the last clean block-start boundary, server-side copying
    /// all preceding clean parts into [newUploadId].
    ///
    /// Parts with no valid UStar header are discarded.  When a boundary is found, the bytes of the
    /// boundary part before the last block-start marker are returned as trailing carry-over bytes
    /// that [BlockUploadTask] prepends to its accumulation buffer on resume.  If no part contains a
    /// block start, an empty [PartScanResult#trailingBytes] signals the caller to fall back to
    /// completed-objects recovery.
    ///
    /// @return a [PartScanResult] with the recovered block number and the bytes of the boundary
    ///         part preceding the last block-start marker, or empty [PartScanResult#trailingBytes]
    ///         if no valid header is found
    private PartScanResult rebuildUpload(
            S3Client s3, String key, String newUploadId, List<String> newEtags, List<PartInfo> parts)
            throws S3ResponseException, IOException {
        long blockNumber = ArchiveKey.parse(key, config.groupingLevel());
        byte[] trailingBytes = new byte[0];

        // Absolute byte offsets within the S3 object, required for range-download and server-side-copy calls.
        final long[] startOffsets = new long[parts.size()];
        long cumulativeOffset = 0;
        for (int i = 0; i < parts.size(); i++) {
            startOffsets[i] = cumulativeOffset;
            cumulativeOffset += parts.get(i).size();
        }

        for (int i = parts.size() - 1; i >= 0; i--) {
            final byte[] partBytes = s3.downloadObjectRange(
                    key, startOffsets[i], startOffsets[i] + parts.get(i).size() - 1);
            final int markerOffset = TarEntries.findLastBlockStart(partBytes, startOffsets[i]);
            if (markerOffset >= 0) {
                try {
                    // The filename starts at offset 0 in the tar header, so markerOffset points directly to it.
                    final long partBlockNumber = Long.parseLong(
                            new String(partBytes, markerOffset, TarEntries.BLOCK_NUMBER_WIDTH, StandardCharsets.UTF_8));
                    // Server-side copy every clean part that precedes the boundary part.
                    for (int j = 0; j < i; j++) {
                        newEtags.add(s3.uploadPartCopy(
                                key,
                                startOffsets[j],
                                startOffsets[j] + parts.get(j).size() - 1,
                                key,
                                newUploadId,
                                newEtags.size() + 1));
                    }
                    blockNumber = partBlockNumber;
                    // Bytes before the block-start marker; BlockUploadTask prepends these to its buffer on resume.
                    trailingBytes = Arrays.copyOfRange(partBytes, 0, markerOffset);
                    break;
                } catch (NumberFormatException e) {
                    // Should not happen, but if it does, log it and continue scanning
                    LOGGER.log(WARNING, "Invalid block number in part %d of key %s".formatted(i, key), e);
                }
            }
        }

        // No block start found in any part.
        return new PartScanResult(blockNumber, trailingBytes);
    }

    /// Carries the result of a backwards parts scan: the recovered block number and the bytes of the
    /// boundary part that precede the last block-start marker (carry-over bytes that [BlockUploadTask]
    /// must prepend to its accumulation buffer on resume).
    private record PartScanResult(long blockNumber, byte[] trailingBytes) {}

    /// Case 3: multiple hanging uploads — abort all and fall back to completed-objects recovery.
    private RecoveryResult recoverFromMultipleHangingUploads(S3Client s3, Map<String, List<String>> uploads)
            throws S3ResponseException, IOException {
        int count = uploads.values().stream().mapToInt(List::size).sum();
        LOGGER.log(TRACE, "Found {0} hanging multipart uploads (expected at most 1); aborting all", count);
        for (final Map.Entry<String, List<String>> entry : uploads.entrySet()) {
            for (final String id : entry.getValue()) {
                try {
                    s3.abortMultipartUpload(entry.getKey(), id);
                } catch (S3ResponseException | IOException e) {
                    LOGGER.log(DEBUG, "Failed to abort upload {0} for key {1}", id, entry.getKey(), e);
                }
            }
        }
        return recoverFromCompletedObjects(s3);
    }
}
