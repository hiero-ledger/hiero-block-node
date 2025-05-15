// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.s3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.minio.GetObjectArgs;
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
    void testList() throws Exception {
        // Upload a file
        String content = "Hello, MinIO!";
        for (int i = 0; i < 5; i++) {
            minioClient.putObject(PutObjectArgs.builder().bucket(BUCKET_NAME).object("block-" + i + ".txt").stream(
                            new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), content.length(), -1)
                    .build());
        }

        try (S3Client s3Client =
                new S3Client(REGION_NAME, endpoint, BUCKET_NAME, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)) {
            final List<String> keys = s3Client.listObjects("", 100);
            List<String> expectedKeys =
                    List.of("block-0.txt", "block-1.txt", "block-2.txt", "block-3.txt", "block-4.txt");
            assertEquals(
                    expectedKeys,
                    keys.stream().filter(name -> name.startsWith("block-")).toList(),
                    "Downloaded content does not match expected content");
        }
    }

    @Test
    @DisplayName("Test multipart upload")
    void testMultipartUpload() throws Exception {
        final String key = "foo.txt";
        // create sample data
        Random random = new Random(23131535653443L);
        byte[] part1 = new byte[5 * 1024 * 1024];
        byte[] part2 = new byte[5 * 1024 * 1024];
        byte[] part3 = new byte[1024];
        random.nextBytes(part1);
        random.nextBytes(part2);
        random.nextBytes(part3);

        try (S3Client s3Client =
                new S3Client(REGION_NAME, endpoint, BUCKET_NAME, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)) {
            final String uploadId = s3Client.createMultipartUpload(key, "STANDARD", "plain/text");
            List<String> eTags = new ArrayList<>();
            eTags.add(s3Client.multipartUploadPart(key, uploadId, 1, part1));
            eTags.add(s3Client.multipartUploadPart(key, uploadId, 2, part2));
            eTags.add(s3Client.multipartUploadPart(key, uploadId, 3, part3));
            s3Client.completeMultipartUpload(key, uploadId, eTags);
        }

        // download with a minio client
        byte[] downloadedContent = minioClient
                .getObject(
                        GetObjectArgs.builder().bucket(BUCKET_NAME).object(key).build())
                .readAllBytes();
        // Verify the content
        byte[] expectedContent = new byte[part1.length + part2.length + part3.length];
        System.arraycopy(part1, 0, expectedContent, 0, part1.length);
        System.arraycopy(part2, 0, expectedContent, part1.length, part2.length);
        System.arraycopy(part3, 0, expectedContent, part1.length + part2.length, part3.length);
        assertEquals(
                expectedContent.length,
                downloadedContent.length,
                "Downloaded content length does not match expected content length");
        assertArrayEquals(expectedContent, downloadedContent, "Downloaded content does not match expected content");
    }

    @Test
    @DisplayName("Test upload of a large file")
    void testUploadFile() throws Exception {
        final int testContentSize = 8 * 1024 * 1024 + 826;
        final String key = "foo.txt";
        // create sample string data
        StringBuilder contentBuilder = new StringBuilder();
        while (contentBuilder.length() < testContentSize) {
            contentBuilder.append("foo bar baz");
        }
        final String content = contentBuilder.toString();
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        // split content in random size parts
        Random random = new Random(23131535653443L);
        List<byte[]> parts = new ArrayList<>();
        int offset = 0;
        while (offset < contentBytes.length) {
            int partSize = random.nextInt(1, 1024 * 1024);
            if (offset + partSize > contentBytes.length) {
                partSize = contentBytes.length - offset;
            }
            byte[] part = new byte[partSize];
            System.arraycopy(contentBytes, offset, part, 0, partSize);
            parts.add(part);
            offset += partSize;
        }
        // upload parts
        try (S3Client s3Client =
                new S3Client(REGION_NAME, endpoint, BUCKET_NAME, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)) {
            s3Client.uploadFile(key, "STANDARD", parts.iterator(), "plain/text");
        }
        // download with a minio client
        byte[] downloadedContent = minioClient
                .getObject(
                        GetObjectArgs.builder().bucket(BUCKET_NAME).object(key).build())
                .readAllBytes();
        // Verify the content
        assertEquals(
                contentBytes.length,
                downloadedContent.length,
                "Downloaded content length does not match expected content length");
        assertEquals(content, new String(downloadedContent), "Downloaded content does not match expected content");
        assertArrayEquals(
                contentBytes, downloadedContent, "Downloaded content bytes does not match expected content bytes");
    }

    @Test
    @DisplayName("Test upload and download of a text file")
    void testTextFileUploadAndDownload() throws Exception {
        final String key = "test-text-file.txt";
        final String text = "Hello, MinIO!";

        try (S3Client s3Client =
                new S3Client(REGION_NAME, endpoint, BUCKET_NAME, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)) {
            assertDoesNotThrow(() -> s3Client.uploadTextFile(key, "STANDARD", text));
            // check download with minio client
            assertEquals(
                    text,
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
            assertEquals(text, s3Client.downloadTextFile(key), "Downloaded content does not match expected content");
        }
    }

    @Test
    @DisplayName("Test fetching a non-existent object")
    void testFetchNonExistentObject() throws Exception {
        try (S3Client s3Client =
                new S3Client(REGION_NAME, endpoint, BUCKET_NAME, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)) {
            assertNull(s3Client.downloadTextFile("non-existent-object.txt"));
        }
    }
}
