// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Contract tests for {@link NoOpS3Client}. Verifies that no exceptions are thrown and that
 * return values match the documented no-op behaviour.
 */
class NoOpS3ClientTest {

    private NoOpS3Client client;

    @BeforeEach
    void setUp() {
        client = new NoOpS3Client();
    }

    @Test
    @DisplayName("uploadFile does not throw and accepts any arguments")
    void uploadFileDoesNotThrow() {
        assertDoesNotThrow(() -> client.uploadFile(
                "blocks/0000000000000000001.blk.zstd",
                "STANDARD",
                Collections.singletonList(new byte[]{1, 2, 3}).iterator(),
                "application/octet-stream"));
    }

    @Test
    @DisplayName("uploadTextFile does not throw and accepts any arguments")
    void uploadTextFileDoesNotThrow() {
        assertDoesNotThrow(() -> client.uploadTextFile("some/key.txt", "STANDARD", "hello world"));
    }

    @Test
    @DisplayName("downloadTextFile returns null without throwing")
    void downloadTextFileReturnsNull() {
        assertDoesNotThrow(() -> {
            final String result = client.downloadTextFile("some/key.txt");
            assertNull(result, "NoOpS3Client.downloadTextFile must return null");
        });
    }

    @Test
    @DisplayName("listObjects returns empty list without throwing")
    void listObjectsReturnsEmptyList() {
        assertDoesNotThrow(() -> {
            final List<String> result = client.listObjects("blocks/", 10);
            assertTrue(result.isEmpty(), "NoOpS3Client.listObjects must return an empty list");
        });
    }

    @Test
    @DisplayName("listMultipartUploads returns empty map without throwing")
    void listMultipartUploadsReturnsEmptyMap() {
        assertDoesNotThrow(() -> {
            final var result = client.listMultipartUploads();
            assertTrue(result.isEmpty(), "NoOpS3Client.listMultipartUploads must return an empty map");
        });
    }

    @Test
    @DisplayName("abortMultipartUpload does not throw")
    void abortMultipartUploadDoesNotThrow() {
        assertDoesNotThrow(() -> client.abortMultipartUpload("some/key", "upload-id-123"));
    }

    @Test
    @DisplayName("close does not throw")
    void closeDoesNotThrow() {
        assertDoesNotThrow(() -> client.close());
    }
}
