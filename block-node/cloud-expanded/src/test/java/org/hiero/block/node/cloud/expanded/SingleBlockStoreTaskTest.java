// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.expanded;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.bucky.S3ClientException;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
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
            void uploadFile(String k, String sc, Iterator<byte[]> c, String ct) {}

            @Override
            public void close() {}
        };
    }

    /// Returns an {@link S3UploadClient} that always throws {@link S3ClientException} —
    /// simulates a transient or permanent S3 service error.
    private S3UploadClient throwingS3Client() {
        return new S3UploadClient() {
            @Override
            void uploadFile(String k, String sc, Iterator<byte[]> c, String ct)
                    throws S3ClientException {
                throw new S3ClientException("Simulated S3 failure");
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
            void uploadFile(String k, String sc, Iterator<byte[]> c, String ct)
                    throws IOException {
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

        assertEquals(SingleBlockStoreTask.UploadStatus.SUCCESS, result.status(),
                "Successful upload must set status to SUCCESS");
        assertTrue(result.succeeded(), "succeeded() must return true for SUCCESS status");
        assertTrue(result.bytesUploaded() > 0L, "bytesUploaded must be positive on success");
        assertEquals(1L, result.blockNumber());
    }

    @Test
    @DisplayName("S3ClientException sets UploadStatus.S3_ERROR and succeeded() returns false")
    void s3ExceptionSetsS3ErrorStatus() {
        final SingleBlockStoreTask task = new SingleBlockStoreTask(
                2L,
                testBlock(2L).blockUnparsed(),
                throwingS3Client(),
                "blocks/0000/0000/0000/0000/002.blk.zstd",
                "STANDARD",
                BlockSource.UNKNOWN);

        final SingleBlockStoreTask.UploadResult result = task.call();

        assertEquals(SingleBlockStoreTask.UploadStatus.S3_ERROR, result.status(),
                "S3ClientException must set status to S3_ERROR");
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

        assertEquals(SingleBlockStoreTask.UploadStatus.IO_ERROR, result.status(),
                "IOException must set status to IO_ERROR");
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
    @DisplayName("UploadResult carries the correct blockNumber and blockSource")
    void uploadResultCarriesBlockNumberAndSource() {
        final SingleBlockStoreTask task = new SingleBlockStoreTask(
                99L,
                testBlock(99L).blockUnparsed(),
                successClient(),
                "blocks/key",
                "STANDARD",
                BlockSource.PUBLISHER);

        final SingleBlockStoreTask.UploadResult result = task.call();

        assertEquals(99L, result.blockNumber());
        assertEquals(BlockSource.PUBLISHER, result.blockSource());
        assertTrue(result.uploadDurationNs() >= 0L, "uploadDurationNs must be non-negative");
    }
}
