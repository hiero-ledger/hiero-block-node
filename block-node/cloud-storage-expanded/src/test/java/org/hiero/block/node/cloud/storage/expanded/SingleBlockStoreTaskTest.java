// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for {@link SingleBlockStoreTask} that verify the {@link SingleBlockStoreTask.UploadStatus}
/// enum value set in the {@link SingleBlockStoreTask.UploadResult} for each outcome.
///
/// These tests exercise `call()` directly without the plugin or any executor, so the
/// status is synchronously available and no drain loop is needed.
class SingleBlockStoreTaskTest {

    /// Fixed epoch used as the first block's start time — keeps generated block hashes deterministic.
    private static final Instant START_TIME =
            ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant();
    /// One-day block duration passed to {@link TestBlockBuilder} — only the block number matters here.
    private static final Duration ONE_DAY = Duration.of(1, ChronoUnit.DAYS);

    // ---- Helpers ------------------------------------------------------------

    /// Generates a single {@link TestBlock} for the given block number using a fixed start time
    /// and one-day duration so the block content is deterministic across test runs.
    private TestBlock testBlock(final long blockNumber) {
        return TestBlockBuilder.generateBlocksInRange(blockNumber, blockNumber, START_TIME, ONE_DAY)
                .getFirst();
    }

    /// Returns an {@link S3UploadClient} whose {@code uploadFile} completes silently —
    /// simulates a fully successful S3 upload without a real endpoint.
    private S3UploadClient successClient() {
        return new S3UploadClient() {
            @Override
            public void uploadFile(String k, String sc, Iterator<byte[]> c, String ct) {}

            @Override
            public void close() {}
        };
    }

    /// Returns an {@link S3UploadClient} that always throws {@link UploadException} —
    /// simulates a transient or permanent S3 service error (as translated by {@link BuckyS3UploadClient}).
    private S3UploadClient throwingS3Client() {
        return new S3UploadClient() {
            @Override
            public void uploadFile(String k, String sc, Iterator<byte[]> c, String ct) throws UploadException {
                throw new UploadException("Simulated S3 failure", null);
            }

            @Override
            public void close() {}
        };
    }

    /// Returns an {@link S3UploadClient} that always throws {@link IOException} —
    /// simulates a network I/O failure during streaming upload.
    private S3UploadClient throwingIoClient() {
        return new S3UploadClient() {
            @Override
            public void uploadFile(String k, String sc, Iterator<byte[]> c, String ct) throws IOException {
                throw new IOException("Simulated I/O failure");
            }

            @Override
            public void close() {}
        };
    }

    // ---- Tests --------------------------------------------------------------

    @Test
    @DisplayName("Successful upload sets UploadStatus.SUCCESS and succeeded() returns true")
    void successSetsSuccessStatus() {
        final SingleBlockStoreTask task = new SingleBlockStoreTask(
                1L,
                testBlock(1L).blockUnparsed(),
                successClient(),
                "blocks/0000/0000/0000/0000/001.blk.zstd",
                "STANDARD",
                BlockSource.UNKNOWN);

        final SingleBlockStoreTask.UploadResult result = task.call();

        assertEquals(
                SingleBlockStoreTask.UploadStatus.SUCCESS,
                result.status(),
                "Successful upload must set status to SUCCESS");
        assertTrue(result.succeeded(), "succeeded() must return true for SUCCESS status");
        assertTrue(result.bytesUploaded() > 0L, "bytesUploaded must be positive on success");
        assertEquals(1L, result.blockNumber());
    }

    @Test
    @DisplayName("UploadException sets UploadStatus.S3_ERROR and succeeded() returns false")
    void uploadExceptionSetsS3ErrorStatus() {
        final SingleBlockStoreTask task = new SingleBlockStoreTask(
                2L,
                testBlock(2L).blockUnparsed(),
                throwingS3Client(),
                "blocks/0000/0000/0000/0000/002.blk.zstd",
                "STANDARD",
                BlockSource.UNKNOWN);

        final SingleBlockStoreTask.UploadResult result = task.call();

        assertEquals(
                SingleBlockStoreTask.UploadStatus.S3_ERROR,
                result.status(),
                "UploadException must set status to S3_ERROR");
        assertFalse(result.succeeded(), "succeeded() must return false for S3_ERROR status");
        assertEquals(0L, result.bytesUploaded(), "bytesUploaded must be 0 on S3 failure");
    }

    @Test
    @DisplayName("IOException sets UploadStatus.IO_ERROR and succeeded() returns false")
    void ioExceptionSetsIoErrorStatus() {
        final SingleBlockStoreTask task = new SingleBlockStoreTask(
                3L,
                testBlock(3L).blockUnparsed(),
                throwingIoClient(),
                "blocks/0000/0000/0000/0000/003.blk.zstd",
                "STANDARD",
                BlockSource.UNKNOWN);

        final SingleBlockStoreTask.UploadResult result = task.call();

        assertEquals(
                SingleBlockStoreTask.UploadStatus.IO_ERROR, result.status(), "IOException must set status to IO_ERROR");
        assertFalse(result.succeeded(), "succeeded() must return false for IO_ERROR status");
        assertEquals(0L, result.bytesUploaded(), "bytesUploaded must be 0 on I/O failure");
    }

    @Test
    @DisplayName("succeeded() convenience method returns true only for SUCCESS status")
    void succeededConvenienceMethodMatchesSuccessStatus() {
        for (final SingleBlockStoreTask.UploadStatus status : SingleBlockStoreTask.UploadStatus.values()) {
            final SingleBlockStoreTask.UploadResult result =
                    new SingleBlockStoreTask.UploadResult(0L, status, 0L, BlockSource.UNKNOWN, 0L);
            if (status == SingleBlockStoreTask.UploadStatus.SUCCESS) {
                assertTrue(result.succeeded(), "succeeded() must be true for SUCCESS");
            } else {
                assertFalse(result.succeeded(), "succeeded() must be false for " + status);
            }
        }
    }

    @Test
    @DisplayName("Successful upload passes the exact objectKey, storageClass, and content-type to S3UploadClient")
    void successPassesCorrectArgumentsToUploadClient() {
        final List<String> capturedKey = new ArrayList<>();
        final List<String> capturedStorageClass = new ArrayList<>();
        final List<String> capturedContentType = new ArrayList<>();
        final S3UploadClient capturingClient = new S3UploadClient() {
            @Override
            public void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType) {
                capturedKey.add(objectKey);
                capturedStorageClass.add(storageClass);
                capturedContentType.add(contentType);
            }

            @Override
            public void close() {}
        };

        final SingleBlockStoreTask task = new SingleBlockStoreTask(
                42L,
                testBlock(42L).blockUnparsed(),
                capturingClient,
                "blocks/0000/0000/0000/0000/042.blk.zstd",
                "INTELLIGENT_TIERING",
                BlockSource.UNKNOWN);
        final SingleBlockStoreTask.UploadResult result = task.call();

        assertEquals(SingleBlockStoreTask.UploadStatus.SUCCESS, result.status());
        assertEquals(1, capturedKey.size(), "Exactly one uploadFile call expected");
        assertEquals("blocks/0000/0000/0000/0000/042.blk.zstd", capturedKey.getFirst(), "objectKey must match");
        assertEquals("INTELLIGENT_TIERING", capturedStorageClass.getFirst(), "storageClass must match");
        assertEquals(
                "application/octet-stream",
                capturedContentType.getFirst(),
                "content-type must be application/octet-stream");
    }

    @Test
    @DisplayName("Bytes delivered to S3UploadClient are ZSTD-compressed and round-trip back to the original block")
    void contentBytesRoundTripToOriginalBlock() throws Exception {
        final List<byte[]> capturedPayload = new ArrayList<>();
        final S3UploadClient capturingClient = new S3UploadClient() {
            @Override
            public void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType) {
                // Drain the PayloadIterator to capture the exact bytes that would be uploaded.
                final java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                while (contentIterable.hasNext()) {
                    final byte[] chunk = contentIterable.next();
                    baos.write(chunk, 0, chunk.length);
                }
                capturedPayload.add(baos.toByteArray());
            }

            @Override
            public void close() {}
        };

        final long blockNumber = 77L;
        final TestBlock block = testBlock(blockNumber);
        new SingleBlockStoreTask(
                        blockNumber,
                        block.blockUnparsed(),
                        capturingClient,
                        "blocks/0000/0000/0000/0000/077.blk.zstd",
                        "STANDARD",
                        BlockSource.UNKNOWN)
                .call();

        assertEquals(1, capturedPayload.size(), "Exactly one chunk expected from PayloadIterator");
        assertTrue(capturedPayload.getFirst().length > 0, "Compressed payload must be non-empty");

        // Decompress and parse — must round-trip back to the original block number
        final byte[] decompressed = CompressionType.ZSTD.decompress(capturedPayload.getFirst());
        final BlockUnparsed parsed = BlockUnparsed.PROTOBUF.parseStrict(Bytes.wrap(decompressed));
        assertEquals(
                blockNumber,
                BlockHeader.PROTOBUF
                        .parse(parsed.blockItems().getFirst().blockHeaderOrThrow())
                        .number(),
                "Block header number must survive ZSTD compress→decompress→parse round-trip");
    }

    @Test
    @DisplayName("UploadResult carries the correct blockNumber and blockSource")
    void uploadResultCarriesBlockNumberAndSource() {
        final SingleBlockStoreTask task = new SingleBlockStoreTask(
                99L, testBlock(99L).blockUnparsed(), successClient(), "blocks/key", "STANDARD", BlockSource.PUBLISHER);

        final SingleBlockStoreTask.UploadResult result = task.call();

        assertEquals(99L, result.blockNumber());
        assertEquals(BlockSource.PUBLISHER, result.blockSource());
        assertTrue(result.uploadDurationNs() >= 0L, "uploadDurationNs must be non-negative");
    }
}
