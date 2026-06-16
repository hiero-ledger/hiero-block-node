// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
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
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/// Direct unit tests for [ConsolidationTask] that exercise the success path and each named error
/// path against a real MinIO container.  Failure injection uses package-private hook methods
/// ([ConsolidationTask#doUploadPart], [ConsolidationTask#doCompleteMultipartUpload]) overridden in
/// local subclasses, following the same pattern as [BlockUploadTaskTest].
@DisplayName("ConsolidationTask Tests")
class ConsolidationTaskTest {

    private static final int MINIO_PORT = 9000;
    private static final String MINIO_USER = "minioadmin";
    private static final String MINIO_PASSWORD = "minioadmin";
    private static final String BUCKET_NAME = "test-bucket";
    private static final int GROUPING_LEVEL = 1;
    private static final int PART_SIZE_MB = 5;

    // @todo(2013) we should remove that and use another approach
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
                    }
                }
            }
        }
    }

    private Map<String, String> pluginConfig() {
        return Map.of(
                "cloud.storage.archive.groupingLevel", String.valueOf(GROUPING_LEVEL),
                "cloud.storage.archive.partSizeMb", String.valueOf(PART_SIZE_MB),
                "cloud.storage.archive.endpointUrl", minioEndpoint,
                "cloud.storage.archive.regionName", "us-east-1",
                "cloud.storage.archive.bucketName", BUCKET_NAME,
                "cloud.storage.archive.accessKey", MINIO_USER,
                "cloud.storage.archive.secretKey", MINIO_PASSWORD);
    }

    private S3Client openS3Client() throws Exception {
        return new S3Client(
                config.regionName(), config.endpointUrl(), config.bucketName(), config.accessKey(), config.secretKey());
    }

    /// Uploads [bytes] as a completed single-part multipart upload at [s3Key], simulating what
    /// [TempArchiveUploadTask] would leave behind.
    private void uploadTempArchive(S3Client s3, String s3Key, byte[] bytes) throws Exception {
        final String uploadId =
                s3.createMultipartUpload(s3Key, config.storageClass().name(), S3UploadUtils.CONTENT_TYPE);
        final String etag = s3.multipartUploadPart(s3Key, uploadId, 1, bytes);
        s3.completeMultipartUpload(s3Key, uploadId, List.of(etag));
    }

    /// Verifies the happy path: two temp archives are downloaded and concatenated into a single
    /// final `.tar`, and all four temp keys (two `.tmp` and two `.meta`) are deleted from S3.
    @Test
    @DisplayName("Success: two temp archives merged into final key; all temp keys deleted")
    void successfulConsolidationMergesAndCleansUp() throws Exception {
        final long groupStart = 0L;
        final long groupSize = Math.powExact(10, GROUPING_LEVEL);
        final String tarKey0 = TempArchiveKey.formatTar(0, "");
        final String metaKey0 = TempArchiveKey.formatMeta(0, "");
        final String tarKey5 = TempArchiveKey.formatTar(5, "");
        final String metaKey5 = TempArchiveKey.formatMeta(5, "");
        final String finalKey = ArchiveKey.format(groupStart, GROUPING_LEVEL, "");

        final byte[] content0 = new byte[1024];
        final byte[] content5 = new byte[2048];
        Arrays.fill(content0, (byte) 0x11);
        Arrays.fill(content5, (byte) 0x22);

        try (S3Client s3 = openS3Client()) {
            uploadTempArchive(s3, tarKey0, content0);
            uploadTempArchive(s3, tarKey5, content5);
            s3.uploadTextFile(metaKey0, config.storageClass().name(), "4");
            s3.uploadTextFile(metaKey5, config.storageClass().name(), "9");
        }

        final List<TempArchiveEntry> entries =
                List.of(new TempArchiveEntry(tarKey0, 0, 4, null), new TempArchiveEntry(tarKey5, 5, 9, null));
        final UploadResult result = new ConsolidationTask(config, entries, groupStart, groupSize).call();

        assertThat(result).isEqualTo(UploadResult.SUCCESS);
        try (S3Client s3 = openS3Client()) {
            assertThat(s3.listObjects(finalKey, 1)).contains(finalKey);
            assertThat(s3.listObjects(tarKey0, 1)).doesNotContain(tarKey0);
            assertThat(s3.listObjects(metaKey0, 1)).doesNotContain(metaKey0);
            assertThat(s3.listObjects(tarKey5, 1)).doesNotContain(tarKey5);
            assertThat(s3.listObjects(metaKey5, 1)).doesNotContain(metaKey5);
        }
    }

    /// Verifies that when the temp archive's S3 key does not exist, the task throws, the multipart
    /// upload for the final key is aborted, and no final key is created.
    @Test
    @DisplayName("Download failure: nonexistent temp archive throws; upload aborted")
    void downloadFailureThrows() throws Exception {
        final long groupStart = 0L;
        final long groupSize = Math.powExact(10, GROUPING_LEVEL);
        final String finalKey = ArchiveKey.format(groupStart, GROUPING_LEVEL, "");

        // Entry references a key that was never uploaded — download will receive a 404.
        final List<TempArchiveEntry> entries = List.of(new TempArchiveEntry("tmp/nonexistent.tmp", 0, 9, null));
        final ConsolidationTask task = new ConsolidationTask(config, entries, groupStart, groupSize);
        assertThatException().isThrownBy(task::call);

        try (S3Client s3 = openS3Client()) {
            assertThat(s3.listObjects(finalKey, 1)).doesNotContain(finalKey);
            assertThat(s3.listMultipartUploads()).doesNotContainKey(finalKey);
        }
    }

    /// Verifies that when [doUploadPart] throws, the task returns FAILED, the in-progress
    /// multipart upload is aborted, and no final key is committed to S3.
    @Test
    @DisplayName("Part upload failure throws; multipart upload aborted")
    void partUploadFailureThrows() throws Exception {
        final long groupStart = 0L;
        final long groupSize = Math.powExact(10, GROUPING_LEVEL);
        final String tarKey = TempArchiveKey.formatTar(0, "");
        final String finalKey = ArchiveKey.format(groupStart, GROUPING_LEVEL, "");

        try (S3Client s3 = openS3Client()) {
            uploadTempArchive(s3, tarKey, new byte[1024]);
        }

        final List<TempArchiveEntry> entries = List.of(new TempArchiveEntry(tarKey, 0, 9, null));
        final ConsolidationTask task = new FailingUploadTask(config, entries, groupStart, groupSize);
        assertThatException().isThrownBy(task::call);

        try (S3Client s3 = openS3Client()) {
            assertThat(s3.listObjects(finalKey, 1)).doesNotContain(finalKey);
            assertThat(s3.listMultipartUploads()).doesNotContainKey(finalKey);
        }
    }

    /// Verifies that when [doCompleteMultipartUpload] throws, the task throws, the multipart upload
    /// is aborted (no orphan left in S3), and no final key is committed.
    @Test
    @DisplayName("completeMultipartUpload failure throws; multipart upload aborted")
    void completeMultipartUploadFailureThrows() throws Exception {
        final long groupStart = 0L;
        final long groupSize = Math.powExact(10, GROUPING_LEVEL);
        final String tarKey = TempArchiveKey.formatTar(0, "");
        final String finalKey = ArchiveKey.format(groupStart, GROUPING_LEVEL, "");

        try (S3Client s3 = openS3Client()) {
            uploadTempArchive(s3, tarKey, new byte[1024]);
        }

        final List<TempArchiveEntry> entries = List.of(new TempArchiveEntry(tarKey, 0, 9, null));
        final ConsolidationTask task = new FailingCompleteTask(config, entries, groupStart, groupSize);
        assertThatException().isThrownBy(task::call);

        try (S3Client s3 = openS3Client()) {
            assertThat(s3.listObjects(finalKey, 1)).doesNotContain(finalKey);
            assertThat(s3.listMultipartUploads()).doesNotContainKey(finalKey);
        }
    }

    /// [ConsolidationTask] subclass that always throws from [doUploadPart] to simulate S3 failures.
    private static final class FailingUploadTask extends ConsolidationTask {

        FailingUploadTask(
                CloudStorageArchiveConfig config, List<TempArchiveEntry> entries, long groupStart, long groupSize) {
            super(config, entries, groupStart, groupSize);
        }

        @Override
        void doUploadPart(byte[] bytes, String key, S3Client s3, String uploadId, List<String> etags)
                throws IOException {
            throw new IOException("Simulated part upload failure");
        }
    }

    /// [ConsolidationTask] subclass that always throws from [doCompleteMultipartUpload] to simulate
    /// S3 completion failures.
    private static final class FailingCompleteTask extends ConsolidationTask {

        FailingCompleteTask(
                CloudStorageArchiveConfig config, List<TempArchiveEntry> entries, long groupStart, long groupSize) {
            super(config, entries, groupStart, groupSize);
        }

        @Override
        void doCompleteMultipartUpload(S3Client s3, String key, String uploadId, List<String> etags)
                throws IOException {
            throw new IOException("Simulated completeMultipartUpload failure");
        }
    }
}
