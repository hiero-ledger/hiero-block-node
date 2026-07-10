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
import java.util.stream.Collectors;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;

/// Determines where [CloudStorageArchivePlugin] should resume uploading after a restart.
///
/// On startup S3 may be in one of three states for **regular** archives:
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
/// In addition, the task scans the `tmp/` virtual directory to rebuild the [TempArchiveEntry] list
/// from `.meta` companion objects, and resolves any hanging multipart uploads for `.tmp` keys:
/// each is independently resumed (if durable parts are found) or aborted (if empty or gibberish),
/// mirroring cases 2 and 3 above per key.
///
/// Returns a [RecoveryResult] describing whether to start fresh, resume at a group boundary, or
/// resume an in-progress upload, together with the completed and resumable temporary archives.
class StartupRecoveryTask implements Callable<RecoveryResult> {

    private static final System.Logger LOGGER = System.getLogger(StartupRecoveryTask.class.getName());
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
        try (final S3Client s3 = S3UploadUtils.createClient(config)) {
            final Map<String, List<String>> allUploads = s3.listMultipartUploads();
            final Map<String, List<String>> regularUploads = regularUploadsUnderPrefix(allUploads);
            final int totalRegularUploads =
                    regularUploads.values().stream().mapToInt(List::size).sum();

            final RecoveryResult baseResult =
                    switch (totalRegularUploads) {
                        case 0 -> recoverFromCompletedObjects(s3);
                        case 1 -> {
                            final Map.Entry<String, List<String>> entry =
                                    regularUploads.entrySet().iterator().next();
                            yield recoverFromSingleHangingUpload(
                                    s3, entry.getKey(), entry.getValue().getFirst());
                        }
                        default -> recoverFromMultipleHangingUploads(s3, regularUploads);
                    };

            final TempArchiveRecovery tempRecovery = recoverTempArchives(s3, allUploads);
            return withTempArchives(baseResult, tempRecovery.completed(), tempRecovery.resumable());
        }
    }

    // Returns a new RecoveryResult identical to [result] but with [tempArchives] and
    // [resumableTempArchives] set and [lastHandedOffBlock] computed from the recovery state and
    // any recovered or resumable temp archives.
    private RecoveryResult withTempArchives(
            RecoveryResult result,
            List<TempArchiveEntry> tempArchives,
            List<TempArchiveResumeState> resumableTempArchives) {
        long lastHandedOffBlock;
        if (result.currentGroupStart() == -1) {
            lastHandedOffBlock = -1;
        } else if (result.uploadId() != null) {
            lastHandedOffBlock = result.nextBlockNumber() - 1;
        } else {
            // currentGroupStart is the next group to upload; last handed-off is the block before it.
            lastHandedOffBlock = result.currentGroupStart() - 1;
        }
        for (final TempArchiveEntry entry : tempArchives) {
            if (entry.lastBlock() > lastHandedOffBlock) {
                lastHandedOffBlock = entry.lastBlock();
            }
        }
        for (final TempArchiveResumeState resumed : resumableTempArchives) {
            lastHandedOffBlock = Math.max(lastHandedOffBlock, resumed.nextBlockNumber() - 1);
        }
        return new RecoveryResult(
                result.currentGroupStart(),
                result.uploadId(),
                result.etags(),
                result.nextBlockNumber(),
                result.trailingBytes(),
                tempArchives,
                resumableTempArchives,
                lastHandedOffBlock,
                result.completedRanges());
    }

    /// Filters a raw [S3Client#listMultipartUploads] result to only the **regular** (non-temp)
    /// entries whose key starts with the configured [CloudStorageArchiveConfig#objectKeyPrefix()].
    private Map<String, List<String>> regularUploadsUnderPrefix(Map<String, List<String>> all) {
        final String objectKeyPrefix = config.objectKeyPrefix();
        final String tmpPfx = TempArchiveKey.tmpPrefix(objectKeyPrefix);
        return all.entrySet().stream()
                .filter(e -> {
                    final String key = e.getKey();
                    if (!objectKeyPrefix.isEmpty() && !key.startsWith(objectKeyPrefix + "/")) {
                        return false;
                    }
                    return !key.startsWith(tmpPfx);
                })
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /// Case 1: no hanging uploads — enumerate all completed tar archives and return the start of
    /// the next group after the last one.
    private RecoveryResult recoverFromCompletedObjects(S3Client s3) throws S3ResponseException, IOException {
        final RecoveryResult recoveryResult;
        final List<String> allTarKeys = findAllTarKeys(s3);
        if (allTarKeys.isEmpty()) {
            LOGGER.log(TRACE, "No prior state found in S3 bucket, starting fresh");
            recoveryResult = new RecoveryResult(-1, null, null, 0, null, null, null, -1, new ConcurrentLongRangeSet());
        } else {
            final long groupSize = Math.powExact(10, config.groupingLevel());
            final ConcurrentLongRangeSet completedRanges = new ConcurrentLongRangeSet();
            long maxGroupStart = -1;
            for (final String key : allTarKeys) {
                final long groupStart = ArchiveKey.parse(key, config.groupingLevel(), config.objectKeyPrefix());
                completedRanges.add(groupStart, groupStart + groupSize - 1);
                if (groupStart > maxGroupStart) {
                    maxGroupStart = groupStart;
                }
            }
            LOGGER.log(
                    TRACE,
                    "Last completed tar group starts at {0}, resuming from {1}",
                    maxGroupStart,
                    maxGroupStart + groupSize);
            recoveryResult =
                    new RecoveryResult(maxGroupStart + groupSize, null, null, 0, null, null, null, -1, completedRanges);
        }
        return recoveryResult;
    }

    /// Walks the `/`-delimited directory hierarchy under the configured prefix (excluding `tmp/`)
    /// and returns the list of all final tar keys.
    ///
    /// [ArchiveKey#format] lays out each group as `directoryDepth` path segments followed by a
    /// leaf segment folded into the object name (e.g. `0000/0000/0000/0001/23.tar`). This walks
    /// [directoryDepth] delimiter listings (returning `CommonPrefixes`, one per branch) before
    /// issuing a single flat listing per leaf directory to read the actual tar keys, bounding the
    /// list-call count by O(levels x branches) rather than the total number of completed archives,
    /// with no loss of per-range accuracy since every tar key is still read from a leaf listing.
    private List<String> findAllTarKeys(S3Client s3) throws S3ResponseException, IOException {
        final String objectKeyPrefix = config.objectKeyPrefix();
        final String excludedPrefix = TempArchiveKey.tmpPrefix(objectKeyPrefix);
        final String rootPrefix = objectKeyPrefix.isEmpty() ? "" : objectKeyPrefix + "/";
        final List<String> tarKeys = new ArrayList<>();
        collectTarKeys(s3, rootPrefix, directoryDepth(), excludedPrefix, tarKeys);
        return tarKeys;
    }

    /// Number of `/`-delimited directory levels [ArchiveKey#format] emits before the leaf segment
    /// that is folded into the object name, for the configured [CloudStorageArchiveConfig#groupingLevel()].
    private int directoryDepth() {
        final int digitCount = ArchiveKey.MAX_LONG_DIGITS - config.groupingLevel();
        final int segmentCount = (digitCount + ArchiveKey.PATH_SEGMENT_WIDTH - 1) / ArchiveKey.PATH_SEGMENT_WIDTH;
        return segmentCount - 1;
    }

    /// Recursively descends `remainingDepth` levels of `/`-delimited directories starting at
    /// [prefix], collecting completed tar keys into [out]. At `remainingDepth == 0`, [prefix] is a
    /// leaf directory: a flat (non-delimited) listing reads its objects directly, giving the exact
    /// tar keys without ever walking sibling leaf directories.
    private void collectTarKeys(S3Client s3, String prefix, int remainingDepth, String excludedPrefix, List<String> out)
            throws S3ResponseException, IOException {
        final String delimiter = remainingDepth == 0 ? null : "/";
        String token = null;
        boolean hasMore = true;
        while (hasMore) {
            final S3Client.ListPage page = s3.listObjectsPage(prefix, token, delimiter, MAX_LIST_RESULTS);
            if (remainingDepth == 0) {
                for (final String key : page.keys()) {
                    if (key.endsWith(".tar")) {
                        out.add(key);
                    }
                }
            } else {
                for (final String childPrefix : page.keys()) {
                    if (!childPrefix.equals(excludedPrefix)) {
                        collectTarKeys(s3, childPrefix, remainingDepth - 1, excludedPrefix, out);
                    }
                }
            }
            token = page.continuationToken();
            hasMore = token != null;
        }
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
                    s3.createMultipartUpload(key, config.storageClass().name(), S3UploadUtils.CONTENT_TYPE);
            final List<String> newEtags = new ArrayList<>();
            final long groupStart = ArchiveKey.parse(key, config.groupingLevel(), config.objectKeyPrefix());

            final PartScanResult scanResult = rebuildUpload(s3, key, newUploadId, newEtags, parts, groupStart);
            if (scanResult.found()) {
                LOGGER.log(
                        TRACE,
                        "Recovery prepared upload {0} for key {1}; resuming from block {2}",
                        newUploadId,
                        key,
                        scanResult.blockNumber());
                result = new RecoveryResult(
                        groupStart,
                        newUploadId,
                        newEtags,
                        scanResult.blockNumber(),
                        scanResult.trailingBytes(),
                        null,
                        null,
                        -1,
                        null);
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
    /// that [BlockUploadTask] prepends to its accumulation buffer on resume. [PartScanResult#found]
    /// signals whether a boundary was located; trailingBytes can legitimately be empty even when
    /// found is `true` (marker at offset 0), so it cannot be used as that signal itself.
    ///
    /// @param initialBlockNumber fallback block number if no part contains a valid tar header
    /// @return a [PartScanResult] with the recovered block number, the bytes of the boundary part
    ///         preceding the last block-start marker, and whether a boundary was found
    private PartScanResult rebuildUpload(
            S3Client s3,
            String key,
            String newUploadId,
            List<String> newEtags,
            List<PartInfo> parts,
            long initialBlockNumber)
            throws S3ResponseException, IOException {
        long blockNumber = initialBlockNumber;
        byte[] trailingBytes = new byte[0];
        boolean found = false;

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
                    found = true;
                    break;
                } catch (NumberFormatException e) {
                    // Should not happen, but if it does, log it and continue scanning
                    LOGGER.log(WARNING, "Invalid block number in part %d of key %s".formatted(i, key), e);
                }
            }
        }

        // No block start found in any part.
        return new PartScanResult(blockNumber, trailingBytes, found);
    }

    /// Carries the result of a backwards parts scan: the recovered block number, the trailing
    /// carry-over bytes [BlockUploadTask] must prepend to its buffer on resume, and whether a
    /// boundary was found at all (trailingBytes can be legitimately empty either way).
    private record PartScanResult(long blockNumber, byte[] trailingBytes, boolean found) {}

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

    /// Scans the `tmp/` virtual directory to rebuild the temporary-archive tracker and resolves any
    /// hanging multipart uploads for `.tmp` keys, deciding independently per key whether to resume
    /// or abort (unlike the regular path, several temp uploads can hang at once since
    /// [CloudStorageArchiveConfig#maxConcurrentTempArchives()] allows concurrent segments).
    ///
    /// Only archives with a companion `.meta` file (written atomically after the multipart upload
    /// completes) are considered fully durable and included in the returned completed list.
    /// `.tmp` objects without a `.meta` companion are treated as incomplete; those not claimed by a
    /// resumed upload (see [#resolveHangingTempUpload]) are orphaned and deleted.
    private TempArchiveRecovery recoverTempArchives(S3Client s3, Map<String, List<String>> allUploads)
            throws S3ResponseException, IOException {
        final String objectKeyPrefix = config.objectKeyPrefix();
        final String tmpPfx = TempArchiveKey.tmpPrefix(objectKeyPrefix);

        final List<TempArchiveResumeState> resumableEntries = new ArrayList<>();
        for (final Map.Entry<String, List<String>> entry : allUploads.entrySet()) {
            if (entry.getKey().startsWith(tmpPfx)) {
                for (final String uploadId : entry.getValue()) {
                    try {
                        resolveHangingTempUpload(s3, entry.getKey(), uploadId, objectKeyPrefix, resumableEntries);
                    } catch (S3ResponseException | IOException e) {
                        LOGGER.log(
                                DEBUG,
                                "Failed to resolve hanging temp archive upload {0} for key {1}",
                                uploadId,
                                entry.getKey(),
                                e);
                    }
                }
            }
        }

        // List all objects under the tmp/ prefix — both .tmp tar files and .meta companions.
        final List<String> tmpDirObjects = listAllObjectsUnderPrefix(s3, tmpPfx);
        final List<TempArchiveEntry> completedEntries = new ArrayList<>();

        for (final String key : tmpDirObjects) {
            if (!TempArchiveKey.isTempMetaKey(key, objectKeyPrefix)) {
                continue;
            }
            final long firstBlock = TempArchiveKey.parseFirstBlockFromMeta(key, objectKeyPrefix);
            final String tarKey = TempArchiveKey.formatTar(firstBlock, objectKeyPrefix);
            try {
                final String metaContent = s3.downloadTextFile(key);
                final long lastBlock = Long.parseLong(metaContent.trim());
                completedEntries.add(new TempArchiveEntry(tarKey, firstBlock, lastBlock, null));
                LOGGER.log(TRACE, "Recovered temp archive [{0}, {1}] from meta key {2}", firstBlock, lastBlock, key);
            } catch (S3ResponseException | IOException | NumberFormatException e) {
                LOGGER.log(DEBUG, "Could not read temp archive meta {0}, skipping", key, e);
            }
        }

        // Delete orphaned .tmp objects (no valid .meta companion, and not claimed by a resumed upload).
        for (final String key : tmpDirObjects) {
            if (!TempArchiveKey.isTempTarKey(key, objectKeyPrefix)) {
                continue;
            }
            final long firstBlock = TempArchiveKey.parseFirstBlockFromTar(key, objectKeyPrefix);
            final String metaKey = TempArchiveKey.formatMeta(firstBlock, objectKeyPrefix);
            final boolean hasValidMeta = completedEntries.stream().anyMatch(e -> e.firstBlock() == firstBlock);
            final boolean isResumed = resumableEntries.stream().anyMatch(e -> e.firstBlock() == firstBlock);
            if (!hasValidMeta && !isResumed) {
                LOGGER.log(TRACE, "Deleting orphaned temp archive {0} (no valid .meta companion)", key);
                try {
                    s3.deleteObject(key);
                } catch (S3ResponseException | IOException e) {
                    LOGGER.log(DEBUG, "Failed to delete orphaned temp archive {0}", key, e);
                }
                // Also clean up the meta if it exists but was unreadable.
                try {
                    s3.deleteObject(metaKey);
                } catch (S3ResponseException | IOException e) {
                    LOGGER.log(DEBUG, "Failed to delete orphaned temp archive meta {0}", metaKey, e);
                }
            }
        }

        return new TempArchiveRecovery(completedEntries, resumableEntries);
    }

    /// Resolves one hanging temp-archive multipart upload: aborts it if no parts were ever
    /// uploaded, otherwise completes it into a materialized readable object, starts a fresh upload
    /// for the same key, and scans backwards via [#rebuildUpload] to find the last clean
    /// block-start boundary. Adds a [TempArchiveResumeState] to `resumable` when a boundary is
    /// found; otherwise aborts the new upload and deletes the intermediate object, mirroring
    /// [#recoverFromSingleHangingUpload]'s gibberish-parts branch.
    private void resolveHangingTempUpload(
            S3Client s3, String key, String uploadId, String objectKeyPrefix, List<TempArchiveResumeState> resumable)
            throws S3ResponseException, IOException {
        final List<PartInfo> parts = s3.listParts(key, uploadId);
        if (parts.isEmpty()) {
            s3.abortMultipartUpload(key, uploadId);
            LOGGER.log(TRACE, "Hanging temp archive upload for key {0} had no parts, aborted", key);
            return;
        }

        s3.completeMultipartUpload(
                key, uploadId, parts.stream().map(PartInfo::etag).toList());
        LOGGER.log(TRACE, "Completed hanging temp archive upload to create temporary S3 object at key {0}", key);
        final String newUploadId =
                s3.createMultipartUpload(key, config.storageClass().name(), S3UploadUtils.CONTENT_TYPE);
        final List<String> newEtags = new ArrayList<>();
        final long originalFirstBlock = TempArchiveKey.parseFirstBlockFromTar(key, objectKeyPrefix);

        final PartScanResult scanResult = rebuildUpload(s3, key, newUploadId, newEtags, parts, originalFirstBlock);
        if (scanResult.found()) {
            LOGGER.log(
                    TRACE,
                    "Recovery prepared upload {0} for temp archive key {1}; resuming from block {2}",
                    newUploadId,
                    key,
                    scanResult.blockNumber());
            resumable.add(new TempArchiveResumeState(
                    key,
                    originalFirstBlock,
                    scanResult.blockNumber(),
                    newUploadId,
                    newEtags,
                    scanResult.trailingBytes()));
        } else {
            LOGGER.log(DEBUG, "No block start found in any part for temp archive key {0}; aborting", key);
            s3.abortMultipartUpload(key, newUploadId);
            s3.deleteObject(key);
        }
    }

    /// Carries the two outcomes of [#recoverTempArchives]: durably completed temp archives (from
    /// `.meta` files) and hanging temp uploads found resumable.
    private record TempArchiveRecovery(List<TempArchiveEntry> completed, List<TempArchiveResumeState> resumable) {}

    /// Lists all object keys under [prefix] by paging through [S3Client#listObjectsPage] calls.
    private List<String> listAllObjectsUnderPrefix(S3Client s3, String prefix) throws S3ResponseException, IOException {
        final List<String> keys = new ArrayList<>();
        String token = null;
        boolean hasMore = true;
        while (hasMore) {
            final S3Client.ListPage page = s3.listObjectsPage(prefix, token, null, MAX_LIST_RESULTS);
            keys.addAll(page.keys());
            token = page.continuationToken();
            hasMore = token != null;
        }
        return keys;
    }
}
