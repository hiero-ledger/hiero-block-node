package org.hiero.block.node.archive;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.GetObjectArgs;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class S3UploadTest {
    private static final String BUCKET_NAME = "test-bucket";
    private GenericContainer<?> minioContainer;
    private MinioClient minioClient;

    @BeforeAll
    void setup() throws Exception {
        // Start MinIO container
        minioContainer = new GenericContainer<>("minio/minio:latest")
                .withCommand("server /data")
                .withExposedPorts(9000)
                .withEnv("MINIO_ROOT_USER", "minioadmin")
                .withEnv("MINIO_ROOT_PASSWORD", "minioadmin");
        minioContainer.start();

        // Initialize MinIO client
        String endpoint = "http://" + minioContainer.getHost() + ":" + minioContainer.getMappedPort(9000);
        minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials("minioadmin", "minioadmin")
                .build();

        // Create a bucket
        minioClient.makeBucket(b -> b.bucket(BUCKET_NAME));
    }

    @AfterAll
    void teardown() {
        if (minioContainer != null) {
            minioContainer.stop();
        }
    }

    @Test
    void testS3UploadAndDownload() throws Exception {
        // Upload a file
        String content = "Hello, MinIO!";
        minioClient.putObject(PutObjectArgs.builder()
                .bucket(BUCKET_NAME)
                .object("test.txt")
                .stream(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), content.length(), -1)
                .build());

        // Download the file
        String downloadedContent = new String(minioClient.getObject(GetObjectArgs.builder()
                .bucket(BUCKET_NAME)
                .object("test.txt")
                .build()).readAllBytes(), StandardCharsets.UTF_8);

        // Verify the content
        assertEquals(content, downloadedContent);
    }

    @Test
    @DisplayName("S3 upload test")
    public void testBasicFileUpload() {

    }
}
