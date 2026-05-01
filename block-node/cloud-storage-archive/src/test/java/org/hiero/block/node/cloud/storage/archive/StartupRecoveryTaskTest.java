// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.bucky.S3Client;
import com.swirlds.config.api.ConfigurationBuilder;
import io.minio.BucketExistsArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.errors.MinioException;
import io.minio.messages.Item;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/// Direct unit tests for [StartupRecoveryTask] that exercise all three S3 states described in
/// its class-level Javadoc.  Each test uses [S3Client] to arrange MinIO state and then calls
/// [StartupRecoveryTask#call] directly, asserting the returned [RecoveryResult].
@DisplayName("Startup Recovery Task Tests")
class StartupRecoveryTaskTest {

    private static final int MINIO_PORT = 9000;
    private static final String MINIO_USER = "minioadmin";
    private static final String MINIO_PASSWORD = "minioadmin";
    private static final String BUCKET_NAME = "test-bucket";
    private static final String CONTENT_TYPE = "application/x-tar";
    private static final int GROUPING_LEVEL = 1;
    /// Minimum non-final part size required by S3/MinIO.  Non-final parts are zero-padded to this
    /// size before upload.  [TarEntries#findLastBlockStart] skips all-zero 512-byte blocks, so the
    /// padding is transparent to recovery.
    private static final int PART_SIZE = 5 * 1024 * 1024;

    // @todo(2013) we should remove that and use another approach
    /// Shared MinIO container — started once before all tests and stopped after the last test.
    private static final GenericContainer<?> MINIO_CONTAINER = new GenericContainer<>("minio/minio:latest")
            .withCommand("server /data")
            .withExposedPorts(MINIO_PORT)
            .withEnv("MINIO_ROOT_USER", MINIO_USER)
            .withEnv("MINIO_ROOT_PASSWORD", MINIO_PASSWORD);

    private static MinioClient minioClient;
    private static String minioEndpoint;

    private CloudStorageArchiveConfig config;

    @BeforeAll
    static void startMinio() throws GeneralSecurityException, IOException, MinioException {
        MINIO_CONTAINER.start();
        minioEndpoint = "http://" + MINIO_CONTAINER.getHost() + ":" + MINIO_CONTAINER.getMappedPort(MINIO_PORT);
        minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(MINIO_USER, MINIO_PASSWORD)
                .build();
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET_NAME).build());
        assertTrue(minioClient.bucketExists(
                BucketExistsArgs.builder().bucket(BUCKET_NAME).build()));
    }

    @AfterAll
    static void stopMinio() {
        MINIO_CONTAINER.stop();
    }

    @BeforeEach
    void setUp() throws Exception {
        final ConfigurationBuilder builder =
                ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
        pluginConfig().forEach(builder::withValue);
        config = builder.build().getConfigData(CloudStorageArchiveConfig.class);
        clearBucket();
    }

    /// Removes all completed objects and aborts all pending multipart uploads in the test bucket
    /// before each test so tests do not see each other's state.
    private void clearBucket() throws Exception {
        for (final Result<Item> result : minioClient.listObjects(
                ListObjectsArgs.builder().bucket(BUCKET_NAME).recursive(true).build())) {
            minioClient.removeObject(RemoveObjectArgs.builder()
                    .bucket(BUCKET_NAME)
                    .object(result.get().objectName())
                    .build());
        }
        try (S3Client s3 = openS3Client()) {
            for (final Map.Entry<String, List<String>> entry :
                    s3.listMultipartUploads().entrySet()) {
                for (final String uploadId : entry.getValue()) {
                    try {
                        s3.abortMultipartUpload(entry.getKey(), uploadId);
                    } catch (Exception ignored) {
                        // best-effort abort; remaining uploads do not affect other tests
                    }
                }
            }
        }
    }

    private Map<String, String> pluginConfig() {
        return Map.of(
                "cloud.archive.groupingLevel", String.valueOf(StartupRecoveryTaskTest.GROUPING_LEVEL),
                "cloud.archive.partSizeMb", String.valueOf(10),
                "cloud.archive.endpointUrl", minioEndpoint,
                "cloud.archive.regionName", "us-east-1",
                "cloud.archive.bucketName", BUCKET_NAME,
                "cloud.archive.accessKey", MINIO_USER,
                "cloud.archive.secretKey", MINIO_PASSWORD);
    }

    /// Verifies that an empty bucket causes [StartupRecoveryTask] to signal a fresh start by
    /// returning [RecoveryResult#currentGroupStart()] == -1 with no upload context.
    @Test
    @DisplayName("Empty bucket yields a fresh start (currentGroupStart == -1)")
    void freshStartReturnsNegativeGroupStart() throws Exception {
        final RecoveryResult result = new StartupRecoveryTask(config).call();

        assertThat(result.currentGroupStart()).isEqualTo(-1);
        assertThat(result.uploadId()).isNull();
        assertThat(result.trailingBytes()).isNull();
    }

    /// Verifies that a completed tar for the first group causes recovery to return the start of
    /// the next group, with no hanging upload state.
    @Test
    @DisplayName("Completed tar group: recovery returns the start of the next group")
    void completedTarGroupReturnsNextGroupStart() throws Exception {
        final long groupSize = Math.powExact(10, GROUPING_LEVEL);
        final String key = ArchiveKey.format(0, GROUPING_LEVEL);

        final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(0, (int) groupSize - 1);
        try (S3Client s3 = openS3Client()) {
            final String uploadId =
                    s3.createMultipartUpload(key, config.storageClass().name(), CONTENT_TYPE);
            final String etag = s3.multipartUploadPart(key, uploadId, 1, buildTarBytes(blocks));
            s3.completeMultipartUpload(key, uploadId, List.of(etag));
        }

        final RecoveryResult result = new StartupRecoveryTask(config).call();

        assertThat(result.currentGroupStart()).isEqualTo(groupSize);
        assertThat(result.uploadId()).isNull();
        assertThat(result.trailingBytes()).isNull();
    }

    /// Verifies that when three tar groups are already completed, recovery returns the start of
    /// the fourth group (all three tars land in the same leaf S3 "folder").
    @Test
    @DisplayName("Three completed tar groups: recovery returns start of the fourth group")
    void threeCompletedTarGroupsReturnNextGroupStart() throws Exception {
        final long groupSize = Math.powExact(10, GROUPING_LEVEL);
        try (S3Client s3 = openS3Client()) {
            for (int g = 0; g < 3; g++) {
                final long groupStart = (long) g * groupSize;
                final String key = ArchiveKey.format(groupStart, GROUPING_LEVEL);
                final List<TestBlock> blocks =
                        TestBlockBuilder.generateBlocksInRange((int) groupStart, (int) (groupStart + groupSize - 1));
                final String uploadId =
                        s3.createMultipartUpload(key, config.storageClass().name(), CONTENT_TYPE);
                final String etag = s3.multipartUploadPart(key, uploadId, 1, buildTarBytes(blocks));
                s3.completeMultipartUpload(key, uploadId, List.of(etag));
            }
        }

        final RecoveryResult result = new StartupRecoveryTask(config).call();

        assertThat(result.currentGroupStart()).isEqualTo(groupSize * 3);
        assertThat(result.uploadId()).isNull();
        assertThat(result.trailingBytes()).isNull();
    }

    /// Verifies that when completed tars live in two different 4th-level S3 "folders"
    /// (i.e. the key prefix before the last segment differs), [findLastKey] still correctly
    /// navigates to the alphabetically last prefix and returns the right group start.
    ///
    /// With [GROUPING_LEVEL] = 1 and groupSize = 10:
    ///  - `start = 0`    → key `"0000/0000/0000/0000/0.tar"` (4th segment `0000`)
    ///  - `start = 1000` → key `"0000/0000/0000/0001/0.tar"` (4th segment `0001`)
    ///
    /// The right-hand path traversal must descend into `0001/` rather than `0000/` at the
    /// 4th level, yielding `currentGroupStart = 1010`.
    @Test
    @DisplayName("Completed tars in two different leaf folders: traversal crosses prefix boundary correctly")
    void completedTarsAcrossPrefixBoundaryReturnCorrectNextGroupStart() throws Exception {
        final long groupSize = Math.powExact(10, GROUPING_LEVEL);
        final long farGroupStart = 1000L;

        try (S3Client s3 = openS3Client()) {
            // Group 0 (blocks 0–9) → "0000/0000/0000/0000/0.tar"
            final String key0 = ArchiveKey.format(0, GROUPING_LEVEL);
            final List<TestBlock> blocks0 = TestBlockBuilder.generateBlocksInRange(0, (int) groupSize - 1);
            final String uploadId0 =
                    s3.createMultipartUpload(key0, config.storageClass().name(), CONTENT_TYPE);
            final String etag0 = s3.multipartUploadPart(key0, uploadId0, 1, buildTarBytes(blocks0));
            s3.completeMultipartUpload(key0, uploadId0, List.of(etag0));

            // Group at start=1000 (blocks 1000–1009) → "0000/0000/0000/0001/0.tar"
            final String key1 = ArchiveKey.format(farGroupStart, GROUPING_LEVEL);
            final List<TestBlock> blocks1 =
                    TestBlockBuilder.generateBlocksInRange((int) farGroupStart, (int) (farGroupStart + groupSize - 1));
            final String uploadId1 =
                    s3.createMultipartUpload(key1, config.storageClass().name(), CONTENT_TYPE);
            final String etag1 = s3.multipartUploadPart(key1, uploadId1, 1, buildTarBytes(blocks1));
            s3.completeMultipartUpload(key1, uploadId1, List.of(etag1));
        }

        final RecoveryResult result = new StartupRecoveryTask(config).call();

        assertThat(result.currentGroupStart()).isEqualTo(farGroupStart + groupSize);
        assertThat(result.uploadId()).isNull();
        assertThat(result.trailingBytes()).isNull();
    }

    /// Verifies that a single hanging multipart upload causes recovery to locate the last block
    /// start in the boundary part, and return the correct [RecoveryResult#nextBlockNumber()] and
    /// [RecoveryResult#trailingBytes()].
    ///
    /// The part is built with complete tar entries for blocks 0–4.  [TarEntries#findLastBlockStart]
    /// identifies block 4's header as the boundary, so recovery must return
    /// `nextBlockNumber == 4` and `trailingBytes` equal to the complete tar entries for blocks
    /// 0–3 (the carry-over prefix that precedes block 4's header in the part).
    @Test
    @DisplayName("Single hanging upload: nextBlockNumber and trailingBytes are extracted correctly")
    void singleHangingUploadReturnsResumeState() throws Exception {
        final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(0, 4);
        final String key = ArchiveKey.format(0, GROUPING_LEVEL);

        // Build and capture the exact bytes that are uploaded so we can derive the expected
        // trailingBytes without re-encoding (re-encoding would embed a different mtime, producing
        // a different checksum and causing a spurious byte mismatch).
        final byte[] partBytes = buildTarBytes(blocks);
        try (S3Client s3 = openS3Client()) {
            final String uploadId =
                    s3.createMultipartUpload(key, config.storageClass().name(), CONTENT_TYPE);
            s3.multipartUploadPart(key, uploadId, 1, partBytes);
            // deliberately NOT completing — simulate a crash mid-upload
        }

        final RecoveryResult result = new StartupRecoveryTask(config).call();

        assertThat(result.currentGroupStart()).isZero();
        assertThat(result.uploadId()).isNotNull();
        // Part 0 is the boundary part; nothing was server-side copied before it.
        assertThat(result.etags()).isEmpty();
        assertThat(result.nextBlockNumber()).isEqualTo(4L);
        // trailingBytes must be partBytes[0..block4HeaderOffset) — the prefix before block 4's header.
        final int block4HeaderOffset = TarEntries.findLastBlockStart(partBytes);
        assertThat(result.trailingBytes()).isEqualTo(Arrays.copyOfRange(partBytes, 0, block4HeaderOffset));
    }

    /// Verifies that a single hanging multipart upload is correctly recovered even when two
    /// earlier groups are already fully completed in S3, and the hanging upload itself uses
    /// three real S3 parts where one block's tar entry straddles a part boundary and the last
    /// part ends with a partial tar entry.
    ///
    /// The hanging upload covers group 2 (key `"0000/0000/0000/0000/2.tar"`).  Each part holds
    /// exactly 2.5 block entries, exploiting the fact that test blocks are all equal size (`E`):
    ///  - **Part 1**: entries for blocks 21, 22, first `E/2` bytes of block 23; padded to [PART_SIZE].
    ///  - **Part 2**: remaining `E/2` bytes of block 23, entries for blocks 24 and 25; padded to [PART_SIZE].
    ///  - **Part 3** (final, unpadded): entries for blocks 26, 27, first `E/2` bytes of block 28.
    ///    `E/2 ≥ 512`, so block 28's full UStar header is present and [isValidUstarHeader] succeeds.
    ///
    /// Recovery server-side copies parts 1 and 2 into the new upload and returns
    /// `nextBlockNumber = 28` with block 26 and 27 entries as `trailingBytes`.
    @Test
    @DisplayName("Single hanging upload with preceding completed tars: correct group start and resume state")
    void singleHangingUploadWithPrecedingCompletedTarsReturnsResumeState() throws Exception {
        // Build the three parts from individual tar entries before opening the S3 connection,
        // so that the raw part bytes are accessible in the assertion block too.
        final List<TestBlock> hangingBlocks = TestBlockBuilder.generateBlocksInRange(21, 28);
        final byte[][] entries = new byte[hangingBlocks.size()][];
        for (int i = 0; i < hangingBlocks.size(); i++) {
            final TestBlock b = hangingBlocks.get(i);
            entries[i] = TarEntries.toTarEntry(b.blockUnparsed(), b.number());
        }
        // All test blocks are equal size; split each part at the midpoint of one entry.
        final int splitOffset = entries[0].length / 2;
        // Part 1: blocks 21, 22, first half of block 23
        final byte[] part1 = concat(entries[0], entries[1], Arrays.copyOfRange(entries[2], 0, splitOffset));
        // Part 2: second half of block 23, blocks 24, 25
        final byte[] part2 =
                concat(Arrays.copyOfRange(entries[2], splitOffset, entries[2].length), entries[3], entries[4]);
        // Part 3: blocks 26, 27, first half of block 28 (unpadded — final part needs no minimum)
        final byte[] part3 = concat(entries[5], entries[6], Arrays.copyOfRange(entries[7], 0, splitOffset));

        try (S3Client s3 = openS3Client()) {
            // Complete groups 0 (blocks 0–9) and 1 (blocks 10–19)
            final String key0 = ArchiveKey.format(0, 1);
            final String uploadId0 =
                    s3.createMultipartUpload(key0, config.storageClass().name(), CONTENT_TYPE);
            final String etag0 = s3.multipartUploadPart(
                    key0, uploadId0, 1, buildTarBytes(TestBlockBuilder.generateBlocksInRange(0, 9)));
            s3.completeMultipartUpload(key0, uploadId0, List.of(etag0));

            final String key1 = ArchiveKey.format(10, 1);
            final String uploadId1 =
                    s3.createMultipartUpload(key1, config.storageClass().name(), CONTENT_TYPE);
            final String etag1 = s3.multipartUploadPart(
                    key1, uploadId1, 1, buildTarBytes(TestBlockBuilder.generateBlocksInRange(10, 19)));
            s3.completeMultipartUpload(key1, uploadId1, List.of(etag1));

            // Leave group 2 as a hanging 3-part upload — deliberately NOT completing.
            final String key2 = ArchiveKey.format(20, 1);
            final String uploadId2 =
                    s3.createMultipartUpload(key2, config.storageClass().name(), CONTENT_TYPE);
            s3.multipartUploadPart(key2, uploadId2, 1, Arrays.copyOf(part1, PART_SIZE));
            s3.multipartUploadPart(key2, uploadId2, 2, Arrays.copyOf(part2, PART_SIZE));
            s3.multipartUploadPart(key2, uploadId2, 3, Arrays.copyOf(part3, PART_SIZE));
        }

        final RecoveryResult result = new StartupRecoveryTask(config).call();

        // findLastBlockStart scans from offset 0 (part 3 starts at 2*PART_SIZE, a 512 multiple)
        // and finds block 28's header as the last valid one; trailingBytes = block 26 + block 27.
        final int boundaryOffset = TarEntries.findLastBlockStart(part3);
        assertThat(result.currentGroupStart()).isEqualTo(20L);
        assertThat(result.uploadId()).isNotNull();
        assertThat(result.etags()).hasSize(2); // parts 1 and 2 server-side copied into the new upload
        assertThat(result.nextBlockNumber()).isEqualTo(28L);
        assertThat(result.trailingBytes()).isEqualTo(Arrays.copyOfRange(part3, 0, boundaryOffset));
    }

    private static byte[] concat(byte[]... arrays) {
        int total = 0;
        for (final byte[] a : arrays) {
            total += a.length;
        }
        final byte[] result = new byte[total];
        int offset = 0;
        for (final byte[] a : arrays) {
            System.arraycopy(a, 0, result, offset, a.length);
            offset += a.length;
        }
        return result;
    }

    /// Verifies that when a single hanging multipart upload contains only gibberish parts (no valid
    /// tar header in any part), the intermediate S3 object assembled from those parts is deleted,
    /// the new upload is aborted, and recovery falls back to the last completed tar.
    ///
    /// Arrangement:
    /// - Two completed tar groups (blocks 0–9 and 10–19).
    /// - One hanging 3-part upload for group 2's key, all parts filled with `0x42` bytes.
    ///   `0x42` bytes will never match the "ustar" magic, so no valid tar header is found.
    ///
    /// Expected outcome: `currentGroupStart` == `2 * groupSize`, no resume state, no remaining
    /// multipart upload, and no completed S3 object at the gibberish key.
    @Test
    @DisplayName(
            "Hanging upload with all-gibberish parts: intermediate object deleted, recovery falls back to last completed tar")
    void hangingUploadWithGibberishPartsFallsBackToLastCompletedTar() throws Exception {
        final long groupSize = Math.powExact(10, GROUPING_LEVEL);

        final String gibberishKey;
        try (S3Client s3 = openS3Client()) {
            for (int g = 0; g < 2; g++) {
                final long groupStart = (long) g * groupSize;
                final String key = ArchiveKey.format(groupStart, GROUPING_LEVEL);
                final List<TestBlock> blocks =
                        TestBlockBuilder.generateBlocksInRange((int) groupStart, (int) (groupStart + groupSize - 1));
                final String uploadId =
                        s3.createMultipartUpload(key, config.storageClass().name(), CONTENT_TYPE);
                final String etag = s3.multipartUploadPart(key, uploadId, 1, buildTarBytes(blocks));
                s3.completeMultipartUpload(key, uploadId, List.of(etag));
            }

            // Leave a hanging 3-part upload for group 2 filled entirely with gibberish bytes.
            // 0x42 bytes will never match the "ustar" magic (0x75 0x73 0x74 0x61 0x72), so no
            // valid tar header will be found in any part.
            gibberishKey = ArchiveKey.format(2L * groupSize, GROUPING_LEVEL);
            final byte[] gibberishPart = new byte[PART_SIZE];
            Arrays.fill(gibberishPart, (byte) 0x42);
            final byte[] gibberishFinalPart = new byte[1024];
            Arrays.fill(gibberishFinalPart, (byte) 0x42);
            final String uploadId =
                    s3.createMultipartUpload(gibberishKey, config.storageClass().name(), CONTENT_TYPE);
            s3.multipartUploadPart(gibberishKey, uploadId, 1, gibberishPart);
            s3.multipartUploadPart(gibberishKey, uploadId, 2, gibberishPart);
            s3.multipartUploadPart(gibberishKey, uploadId, 3, gibberishFinalPart);
            // deliberately NOT completing — simulate a crash mid-upload
        }

        final RecoveryResult result = new StartupRecoveryTask(config).call();

        assertThat(result.currentGroupStart()).isEqualTo(2L * groupSize);
        assertThat(result.uploadId()).isNull();
        assertThat(result.etags()).isNull();
        assertThat(result.nextBlockNumber()).isZero();
        assertThat(result.trailingBytes()).isNull();
        try (S3Client s3 = openS3Client()) {
            assertThat(s3.listMultipartUploads()).isEmpty();
            assertThat(s3.listObjects(gibberishKey, 1)).doesNotContain(gibberishKey);
        }
    }

    /// Verifies that multiple hanging uploads cause recovery to abort all of them and fall back
    /// to completed-objects recovery (which yields a fresh start when the bucket is otherwise empty).
    @Test
    @DisplayName("Multiple hanging uploads: all are aborted and recovery falls back to fresh start")
    void multipleHangingUploadsAreAbortedAndFallBack() throws Exception {
        final String key0 = ArchiveKey.format(0, GROUPING_LEVEL);
        final String key1 = ArchiveKey.format(10, GROUPING_LEVEL);

        try (S3Client s3 = openS3Client()) {
            s3.createMultipartUpload(key0, config.storageClass().name(), CONTENT_TYPE);
            s3.createMultipartUpload(key1, config.storageClass().name(), CONTENT_TYPE);
        }

        final RecoveryResult result = new StartupRecoveryTask(config).call();

        // All uploads aborted; bucket has no completed objects → fresh start.
        assertThat(result.currentGroupStart()).isEqualTo(-1L);
        assertThat(result.uploadId()).isNull();
        // Confirm MinIO has no remaining multipart uploads.
        try (S3Client s3 = openS3Client()) {
            assertThat(s3.listMultipartUploads()).isEmpty();
        }
    }

    private S3Client openS3Client() throws Exception {
        return new S3Client(
                config.regionName(), config.endpointUrl(), config.bucketName(), config.accessKey(), config.secretKey());
    }

    private static byte[] buildTarBytes(List<TestBlock> blocks) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (final TestBlock block : blocks) {
            baos.write(TarEntries.toTarEntry(block.blockUnparsed(), block.number()));
        }
        return baos.toByteArray();
    }
}
