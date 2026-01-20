// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConcurrentDownloadManagerVirtualThreadsV3}.
 *
 * Uses Google's LocalStorageHelper for in-memory GCS testing without external mocking libraries.
 */
class ConcurrentDownloadManagerVirtualThreadsV3Test {

    private Storage localStorage;
    private ConcurrentDownloadManagerVirtualThreadsV3 manager;

    private static final String TEST_BUCKET = "test-bucket";

    @BeforeEach
    void setUp() {
        // Use Google's in-memory storage helper for testing
        localStorage = LocalStorageHelper.getOptions().getService();
    }

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.close();
        }
    }

    /**
     * Helper to create a test blob in local storage.
     */
    private void createTestBlob(String objectName, String content) {
        localStorage.create(
                com.google.cloud.storage.BlobInfo.newBuilder(TEST_BUCKET, objectName)
                        .build(),
                content.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void testBuilderDefaults() {
        manager = ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(localStorage)
                .build();

        assertNotNull(manager);
        assertEquals(64, manager.getCurrentConcurrency()); // default initial concurrency
        assertEquals(2048, manager.getMaxConcurrency()); // default max concurrency
        assertEquals(0, manager.getBytesDownloaded());
        assertEquals(0, manager.getObjectsCompleted());
    }

    @Test
    void testBuilderCustomValues() {
        manager = ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(localStorage)
                .setInitialConcurrency(128)
                .setMinConcurrency(16)
                .setMaxConcurrency(512)
                .setRampUpInterval(Duration.ofSeconds(5))
                .setRampUpStep(32)
                .setErrorRateForCut(0.05)
                .setCutMultiplier(0.75)
                .setGlobalCooldown(Duration.ofSeconds(20))
                .setMaxRetryAttempts(5)
                .setInitialBackoff(Duration.ofMillis(500))
                .setMaxBackoff(Duration.ofSeconds(30))
                .setErrorWindowSeconds(60)
                .setThreadNamePrefix("test-dl")
                .build();

        assertNotNull(manager);
        assertEquals(128, manager.getCurrentConcurrency());
        assertEquals(512, manager.getMaxConcurrency());
    }

    @Test
    void testBuilderClampsInitialConcurrency() {
        // Initial concurrency higher than max should be clamped
        manager = ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(localStorage)
                .setInitialConcurrency(1000)
                .setMaxConcurrency(100)
                .build();

        assertEquals(100, manager.getCurrentConcurrency());
    }

    @Test
    void testBuilderClampsInitialConcurrencyToMin() {
        // Initial concurrency lower than min should be clamped
        manager = ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(localStorage)
                .setInitialConcurrency(2)
                .setMinConcurrency(10)
                .setMaxConcurrency(100)
                .build();

        assertEquals(10, manager.getCurrentConcurrency());
    }

    @Test
    void testBuilderNullStorageThrows() {
        assertThrows(NullPointerException.class, () -> {
            ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(null).build();
        });
    }

    @Test
    void testDownloadAfterCloseThrows() {
        manager = ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(localStorage)
                .setInitialConcurrency(4)
                .build();

        manager.close();

        assertThrows(IllegalStateException.class, () -> {
            manager.downloadAsync("bucket", "object");
        });
    }

    @Test
    void testStatisticsStartAtZero() {
        manager = ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(localStorage)
                .setInitialConcurrency(4)
                .build();

        assertEquals(0, manager.getBytesDownloaded());
        assertEquals(0, manager.getObjectsCompleted());
    }

    @Test
    void testGetCurrentConcurrencyReturnsConfigured() {
        manager = ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(localStorage)
                .setInitialConcurrency(42)
                .setMinConcurrency(10)
                .setMaxConcurrency(100)
                .build();

        assertEquals(42, manager.getCurrentConcurrency());
    }

    @Test
    void testGetMaxConcurrencyReturnsConfigured() {
        manager = ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(localStorage)
                .setMaxConcurrency(256)
                .build();

        assertEquals(256, manager.getMaxConcurrency());
    }

    @Test
    void testMultipleClosesAreSafe() {
        manager = ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(localStorage)
                .setInitialConcurrency(4)
                .build();

        // Should not throw
        manager.close();
        manager.close();
        manager.close();
    }

    @Test
    void testDownloadAsyncReturnsCompletableFuture() {
        createTestBlob("test-object.txt", "test content");

        manager = ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(localStorage)
                .setInitialConcurrency(4)
                .setMaxConcurrency(4)
                .build();

        var future = manager.downloadAsync(TEST_BUCKET, "test-object.txt");

        assertNotNull(future);
        assertTrue(future instanceof CompletableFuture);
    }

    @Test
    void testConcurrencyGateLimitsBehavior() throws Exception {
        // Create some test blobs
        for (int i = 0; i < 10; i++) {
            createTestBlob("object-" + i, "data-" + i);
        }

        int maxConcurrency = 3;
        manager = ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(localStorage)
                .setInitialConcurrency(maxConcurrency)
                .setMaxConcurrency(maxConcurrency)
                .build();

        // Start multiple downloads
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add(manager.downloadAsync(TEST_BUCKET, "object-" + i));
        }

        // Wait for all to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);

        // All should complete
        assertEquals(10, manager.getObjectsCompleted());
        assertTrue(manager.getBytesDownloaded() > 0);
    }

    @Test
    void testStatisticsAccumulateAfterDownloads() throws Exception {
        String content1 = "short";
        String content2 = "much longer content here for testing";

        createTestBlob("obj1.txt", content1);
        createTestBlob("obj2.txt", content2);

        manager = ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(localStorage)
                .setInitialConcurrency(4)
                .build();

        manager.downloadAsync(TEST_BUCKET, "obj1.txt").get(5, TimeUnit.SECONDS);
        manager.downloadAsync(TEST_BUCKET, "obj2.txt").get(5, TimeUnit.SECONDS);

        assertEquals(2, manager.getObjectsCompleted());
        assertEquals(content1.length() + content2.length(), manager.getBytesDownloaded());
    }

    @Test
    void testDownloadReturnsCorrectData() throws Exception {
        String testContent = "Hello, this is test content!";
        createTestBlob("hello.txt", testContent);

        manager = ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(localStorage)
                .setInitialConcurrency(4)
                .build();

        var result = manager.downloadAsync(TEST_BUCKET, "hello.txt").get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertEquals("hello.txt", result.path().toString());
        assertEquals(testContent, new String(result.data(), StandardCharsets.UTF_8));
    }

    @Test
    void testDownloadReturnsCorrectPath() throws Exception {
        createTestBlob("path/to/nested/file.txt", "nested content");

        manager = ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(localStorage)
                .setInitialConcurrency(4)
                .build();

        var result =
                manager.downloadAsync(TEST_BUCKET, "path/to/nested/file.txt").get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertEquals("path/to/nested/file.txt", result.path().toString());
    }

    @Test
    void testMultipleConcurrentDownloadsComplete() throws Exception {
        int numDownloads = 20;
        for (int i = 0; i < numDownloads; i++) {
            createTestBlob("concurrent-" + i, "data for " + i);
        }

        manager = ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(localStorage)
                .setInitialConcurrency(10)
                .setMaxConcurrency(10)
                .build();

        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < numDownloads; i++) {
            futures.add(manager.downloadAsync(TEST_BUCKET, "concurrent-" + i));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);

        assertEquals(numDownloads, manager.getObjectsCompleted());
    }
}
