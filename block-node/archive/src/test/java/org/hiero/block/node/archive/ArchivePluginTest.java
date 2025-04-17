// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.archive;

import static org.junit.jupiter.api.Assertions.*;

import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import java.util.Map;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * Unit tests for the {@link ArchivePlugin} class.
 */
class ArchivePluginTest extends PluginTestBase<ArchivePlugin> {
    private static final String BUCKET_NAME = "test-bucket";
    private static final int MINIO_ROOT_PORT = 9000;
    private static final String MINIO_ROOT_USER = "minioadmin";
    private static final String MINIO_ROOT_PASSWORD = "minioadmin";
    private GenericContainer<?> minioContainer;
    private MinioClient minioClient;
    private String endpoint;

    public ArchivePluginTest(ArchivePlugin plugin, HistoricalBlockFacility historicalBlockFacility) throws Exception {
        // Start MinIO container
        minioContainer = new GenericContainer<>("minio/minio:latest")
                .withCommand("server /data")
                .withExposedPorts(MINIO_ROOT_PORT)
                .withEnv("MINIO_ROOT_USER", MINIO_ROOT_USER)
                .withEnv("MINIO_ROOT_PASSWORD", MINIO_ROOT_PASSWORD);
        minioContainer.start();
        // Initialize MinIO client
        endpoint = "http://" + minioContainer.getHost() + ":" + minioContainer.getMappedPort(MINIO_ROOT_PORT);
        System.out.println("endpoint = " + endpoint); // 63137
        minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)
                .build();
        // Create a bucket
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET_NAME).build());
        // Initialize the plugin and set any required configuration
        start(
                plugin,
                historicalBlockFacility,
                Map.of(
                        "endpointUrl", endpoint,
                        "accessKey", MINIO_ROOT_USER,
                        "secretKey", MINIO_ROOT_PASSWORD));
    }

    @Test
    @DisplayName("ArchivePlugin should implement BlockNodePlugin interface")
    void shouldImplementBlockNodePlugin() {
        // Given
        final ArchivePlugin archivePlugin = new ArchivePlugin();

        // Then
        assertTrue(archivePlugin instanceof BlockNodePlugin, "ArchivePlugin should implement BlockNodePlugin");
    }

    @Test
    @DisplayName("ArchivePlugin should initialize without exceptions")
    void shouldInitializeWithoutExceptions() {
        // Given
        final ArchivePlugin archivePlugin = new ArchivePlugin();

        // When & Then
        assertDoesNotThrow(archivePlugin::toString, "ArchivePlugin should initialize without throwing exceptions");
    }

    @Test
    @DisplayName("ArchivePlugin should have a non-null name")
    void shouldHaveName() {
        final ArchivePlugin archivePlugin = new ArchivePlugin();
        assertNotNull(archivePlugin.name(), "name should not be null");
        assertEquals(ArchivePlugin.class.getSimpleName(), archivePlugin.name(), "Name should be class name");
    }

    @Test
    @DisplayName("ArchivePlugin should have empty config data types")
    void shouldHaveEmptyConfigDataTypes() {
        final ArchivePlugin archivePlugin = new ArchivePlugin();
        assertTrue(archivePlugin.configDataTypes().isEmpty(), "configDataTypes should be empty");
    }
}
