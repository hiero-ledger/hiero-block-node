// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;

/**
 * Unit tests for the {@link S3ClientTest} class.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class S3ClientTest {
    private static final String BUCKET_NAME = "test-bucket";
    private static final int MINIO_ROOT_PORT = 9000;
    private static final String MINIO_ROOT_USER = "minioadmin";
    private static final String MINIO_ROOT_PASSWORD = "minioadmin";
    private static final String REGION_NAME = "us-east-1";
    private GenericContainer<?> minioContainer;
    private MinioClient minioClient;
    private String endpoint;

    @SuppressWarnings({"resource", "HttpUrlsUsage"})
    @BeforeAll
    void setup() throws Exception {
        // Start MinIO container
        minioContainer = new GenericContainer<>("minio/minio:latest")
                .withCommand("server /data")
                .withExposedPorts(MINIO_ROOT_PORT)
                .withEnv("MINIO_ROOT_USER", MINIO_ROOT_USER)
                .withEnv("MINIO_ROOT_PASSWORD", MINIO_ROOT_PASSWORD);
        minioContainer.start();
        // Initialize MinIO client
        endpoint = "http://" + minioContainer.getHost() + ":" + minioContainer.getMappedPort(MINIO_ROOT_PORT);
        minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)
                .build();
        // Create a bucket
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET_NAME).build());
    }

    @AfterAll
    void teardown() {
        if (minioContainer != null) {
            minioContainer.stop();
        }
    }

    @Test
    @DisplayName("Test listObjects() correctly returns existing objects in a bucket")
    void testList() throws Exception {
        // Setup
        final String content = "Hello, MinIO!";
        final String keyPrefix = "block-";
        final List<String> expected = List.of(
                keyPrefix.concat("0.txt"),
                keyPrefix.concat("1.txt"),
                keyPrefix.concat("2.txt"),
                keyPrefix.concat("3.txt"),
                keyPrefix.concat("4.txt"));
        // verify that the bucket is empty before the test
        final boolean preCheck = minioClient
                .listObjects(ListObjectsArgs.builder()
                        .bucket(BUCKET_NAME)
                        .prefix(keyPrefix)
                        .maxKeys(100)
                        .build())
                .iterator()
                .hasNext();
        assertThat(preCheck).isFalse();
        // upload objects to the bucket
        for (final String object : expected) {
            minioClient.putObject(PutObjectArgs.builder().bucket(BUCKET_NAME).object(object).stream(
                            new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), content.length(), -1)
                    .build());
        }
        try (final S3Client s3Client = client()) {
            // Call
            final List<String> actual = s3Client.listObjects(keyPrefix, 100);
            // Assert
            assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
            // Call filter by max results
            final List<String> actualFilterMaxResults = s3Client.listObjects(keyPrefix, 2);
            // Assert
            assertThat(actualFilterMaxResults)
                    .containsExactlyInAnyOrderElementsOf(List.of(expected.get(0), expected.get(1)));
        }
    }

    /**
     * This test aims to verify that the multipart upload functionality of the
     * S3Client works correctly. We manually build 3 parts of a file and then
     * proceed to upload them using the multipart upload API of the S3Client.
     * Then we verify that the data exists in the bucket by downloading it
     * via the MinIO client and checking that the content matches what we
     * uploaded.
     */
    @Test
    @DisplayName("Test multipart upload")
    void testMultipartUpload() throws Exception {
        // Setup
        final String key = "testMultipartUploadSuccess.txt";
        // check that the object does not exist before the test
        final boolean preCheck = minioClient
                .listObjects(ListObjectsArgs.builder()
                        .bucket(BUCKET_NAME)
                        .prefix(key)
                        .maxKeys(100)
                        .build())
                .iterator()
                .hasNext();
        assertThat(preCheck).isFalse();
        final Random random = new Random(23131535653443L);
        final byte[] part1 = new byte[5 * 1024 * 1024];
        final byte[] part2 = new byte[5 * 1024 * 1024];
        final byte[] part3 = new byte[1024];
        random.nextBytes(part1);
        random.nextBytes(part2);
        random.nextBytes(part3);
        try (final S3Client s3Client = client()) {
            // Call
            final String uploadId = s3Client.createMultipartUpload(key, "STANDARD", "plain/text");
            final List<String> eTags = new ArrayList<>();
            eTags.add(s3Client.multipartUploadPart(key, uploadId, 1, part1));
            eTags.add(s3Client.multipartUploadPart(key, uploadId, 2, part2));
            eTags.add(s3Client.multipartUploadPart(key, uploadId, 3, part3));
            s3Client.completeMultipartUpload(key, uploadId, eTags);
        }
        // Assert
        // download with a minio client
        byte[] actual = minioClient
                .getObject(
                        GetObjectArgs.builder().bucket(BUCKET_NAME).object(key).build())
                .readAllBytes();
        // Verify the content
        byte[] expected = new byte[part1.length + part2.length + part3.length];
        System.arraycopy(part1, 0, expected, 0, part1.length);
        System.arraycopy(part2, 0, expected, part1.length, part2.length);
        System.arraycopy(part3, 0, expected, part1.length + part2.length, part3.length);
        assertThat(actual).hasSameSizeAs(expected).isEqualTo(expected).containsExactly(expected);
    }

    @Test
    @DisplayName("Test upload of a large file")
    void testUploadFile() throws Exception {
        // Setup
        final int testContentSize = 8 * 1024 * 1024 + 826;
        final String key = "uploadOfLargeFileSuccessful.txt";
        // check that the object does not exist before the test
        final boolean preCheck = minioClient
                .listObjects(ListObjectsArgs.builder()
                        .bucket(BUCKET_NAME)
                        .prefix(key)
                        .maxKeys(100)
                        .build())
                .iterator()
                .hasNext();
        assertThat(preCheck).isFalse();
        // create sample string data
        final StringBuilder contentBuilder = new StringBuilder();
        while (contentBuilder.length() < testContentSize) {
            contentBuilder.append("foo bar baz");
        }
        final String content = contentBuilder.toString();
        byte[] expected = content.getBytes(StandardCharsets.UTF_8);
        // split content in random size parts
        final Random random = new Random(23131535653443L);
        final List<byte[]> parts = new ArrayList<>();
        int offset = 0;
        while (offset < expected.length) {
            int partSize = random.nextInt(1, 1024 * 1024);
            if (offset + partSize > expected.length) {
                partSize = expected.length - offset;
            }
            final byte[] part = new byte[partSize];
            System.arraycopy(expected, offset, part, 0, partSize);
            parts.add(part);
            offset += partSize;
        }
        // upload parts
        try (final S3Client s3Client = client()) {
            s3Client.uploadFile(key, "STANDARD", parts.iterator(), "plain/text");
        }
        // download with a minio client
        final byte[] actual = minioClient
                .getObject(
                        GetObjectArgs.builder().bucket(BUCKET_NAME).object(key).build())
                .readAllBytes();
        // Verify the content
        assertThat(actual)
                .hasSameSizeAs(expected)
                .isEqualTo(expected)
                .containsExactly(expected)
                .asString()
                .isEqualTo(content);
    }

    @Test
    @DisplayName("Test upload and download of a text file")
    void testTextFileUploadAndDownload() throws Exception {
        // Setup
        final String key = "uploadSimpleTextFile.txt";
        final String expected = "Hello, MinIO!";
        // verify that the file does not exist in the bucket before the test
        final boolean preCheck = minioClient
                .listObjects(ListObjectsArgs.builder()
                        .bucket(BUCKET_NAME)
                        .prefix(key)
                        .maxKeys(100)
                        .build())
                .iterator()
                .hasNext();
        assertThat(preCheck).isFalse();
        try (final S3Client s3Client = client()) {
            // upload text file via the client
            assertDoesNotThrow(() -> s3Client.uploadTextFile(key, "STANDARD", expected));
            // check download with minio client
            assertEquals(
                    expected,
                    new String(
                            minioClient
                                    .getObject(GetObjectArgs.builder()
                                            .bucket(BUCKET_NAME)
                                            .object(key)
                                            .build())
                                    .readAllBytes(),
                            StandardCharsets.UTF_8),
                    "Downloaded content does not match expected content");
            // check download with s3 client
            assertEquals(
                    expected, s3Client.downloadTextFile(key), "Downloaded content does not match expected content");
        }
    }

    @Test
    @DisplayName("Test fetching a non-existent object")
    void testFetchNonExistentObject() throws Exception {
        try (final S3Client s3Client = client()) {
            assertNull(s3Client.downloadTextFile("non-existent-object.txt"));
        }
    }

    private S3Client client() throws S3ClientInitializationException {
        return new S3Client(REGION_NAME, endpoint, BUCKET_NAME, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD);
    }
}
