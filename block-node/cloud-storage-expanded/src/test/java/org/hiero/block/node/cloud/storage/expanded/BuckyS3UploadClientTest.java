// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Iterator;
import org.hiero.block.node.cloud.storage.expanded.ExpandedCloudStorageConfig.StorageClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for {@link BuckyS3UploadClient}.
///
/// `com.hedera.bucky.S3Client` is `final` with no test seam for mocking, so these tests drive
/// the real bucky client against a closed local port (deterministic connection-refused I/O
/// error) and a minimal `com.sun.net.httpserver.HttpServer` fake endpoint — no Docker or mocking
/// framework required.
class BuckyS3UploadClientTest {

    private HttpServer fakeServer;

    @AfterEach
    void tearDown() {
        if (fakeServer != null) {
            fakeServer.stop(0);
        }
    }

    private ExpandedCloudStorageConfig configFor(final String endpointUrl) {
        return new ExpandedCloudStorageConfig(
                endpointUrl, "test-bucket", "prefix", StorageClass.STANDARD, "us-east-1", "key", "secret", 60);
    }

    /// Opens and immediately closes a local socket to obtain a port number that nothing is
    /// listening on, so a subsequent connection attempt deterministically fails fast with
    /// "connection refused" rather than timing out.
    private int closedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private Iterator<byte[]> singleChunkPayload() {
        return Collections.singletonList(new byte[] {1, 2, 3}).iterator();
    }

    @Test
    @DisplayName("Constructor wraps a blank required field as UploadException")
    void constructorWrapsBlankFieldAsUploadException() {
        assertThrows(UploadException.class, () -> new BuckyS3UploadClient(configFor("")));
    }

    @Test
    @DisplayName("uploadFile throws a checked IOException when the connection is refused")
    void uploadFileThrowsCheckedIOExceptionOnConnectionFailure() throws Exception {
        final BuckyS3UploadClient client = new BuckyS3UploadClient(configFor("http://127.0.0.1:" + closedPort() + "/"));

        assertThrows(
                IOException.class,
                () -> client.uploadFile("key", "STANDARD", singleChunkPayload(), "application/octet-stream"));
    }

    @Test
    @DisplayName("uploadFile wraps a non-200 S3 response as UploadException")
    void uploadFileWrapsS3ErrorResponseAsUploadException() throws Exception {
        fakeServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fakeServer.createContext("/", exchange -> {
            final byte[] body = "<Error/>".getBytes();
            exchange.sendResponseHeaders(500, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        fakeServer.start();

        final BuckyS3UploadClient client = new BuckyS3UploadClient(
                configFor("http://127.0.0.1:" + fakeServer.getAddress().getPort() + "/"));

        assertThrows(
                UploadException.class,
                () -> client.uploadFile("key", "STANDARD", singleChunkPayload(), "application/octet-stream"));
    }

    @Test
    @DisplayName("close() releases the underlying bucky client without throwing")
    void closeDoesNotThrow() throws Exception {
        final BuckyS3UploadClient client = new BuckyS3UploadClient(configFor("http://127.0.0.1:" + closedPort() + "/"));

        assertDoesNotThrow(client::close);
    }
}
