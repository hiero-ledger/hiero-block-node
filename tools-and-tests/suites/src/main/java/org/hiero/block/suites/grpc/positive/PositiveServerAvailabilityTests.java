// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.grpc.positive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.HttpURLConnection;
import org.hiero.block.suites.BaseSuite;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test class for verifying the positive scenarios for server availability, specifically related to
 * the gRPC server. This class contains tests to check that the gRPC server starts successfully and
 * listens on the correct port.
 *
 * <p>Inherits from {@link BaseSuite} to reuse the container setup and teardown logic for the Block
 * Node.
 */
@DisplayName("Positive Server Availability Tests")
public class PositiveServerAvailabilityTests extends BaseSuite {

    /** Default constructor for the {@link PositiveServerAvailabilityTests} class. */
    public PositiveServerAvailabilityTests() {}

    /**
     * Test to verify that the gRPC server starts successfully.
     *
     * <p>The test checks if the Block Node container is  running and marked as healthy.
     */
    @Test
    @DisplayName("Should start gRPC server successfully")
    public void verifyGrpcServerStartsSuccessfully() throws IOException {
        assertTrue(blockNodeContainer.isRunning(), "Block Node container should be running.");
        // Replacing docker-specific HEALTHCHECK command with runtime-agnostic equivalent:
        // container.isHealthy() relies on the image HEALTHCHECK, which is dropped under Podman/OCI,
        // so assert health directly over HTTP against the dedicated health port instead.
        assertEquals(200, healthGet("/healthz/livez"), "Block Node should report healthy via /healthz/livez.");
    }

    /**
     * Test to verify that the gRPC server is listening on the correct port.
     *
     * <p>The test asserts that the container is running and exposes both the gRPC server port and the
     * dedicated health server port.
     */
    @Test
    @DisplayName("Should listen on correct gRPC port")
    public void verifyGrpcServerListeningOnCorrectPort() {
        assertTrue(blockNodeContainer.isRunning(), "Block Node container should be running.");
        assertTrue(
                blockNodeContainer.getExposedPorts().contains(blockNodePort),
                "The gRPC server port should be exposed.");
        assertTrue(
                blockNodeContainer.getExposedPorts().contains(blockNodeHealthPort),
                "The health server port should be exposed.");
    }

    /**
     * Test should verify the healthz endpoints of the REST API.
     *
     * <p>The test asserts that /healthz/readyz REST endpoint returns HTTP 200 Status</p>
     * <p>The test asserts that /healthz/livez REST endpoint returns HTTP 200 Status</p>
     *
     */
    @Test
    @DisplayName("Verify /healthz endpoints")
    public void verifyHealthzEndpoints() throws IOException {
        assertEquals(200, healthGet("/healthz/readyz"), "Expected HTTP 200 for /healthz/readyz endpoint.");
        assertEquals(200, healthGet("/healthz/livez"), "Expected HTTP 200 for /healthz/livez endpoint.");
    }

    /**
     * Sends a GET to the given path on the block node's dedicated health port (the health and statusz
     * endpoints run on their own web server) and returns the HTTP status code.
     *
     * @param path the request path (e.g. {@code /healthz/livez})
     * @return the HTTP response code
     */
    private int healthGet(final String path) throws IOException {
        final HttpURLConnection connection = (HttpURLConnection)
                new java.net.URL(String.format("http://localhost:%d%s", blockNodeHealthPort, path)).openConnection();
        connection.setRequestMethod("GET");
        return connection.getResponseCode();
    }

    /**
     * Test should verify the metrics endpoints of the REST API.
     *
     */
    @Test
    @DisplayName("Verify /metrics endpoint")
    public void verifyMetricsEndpoint() throws IOException {
        final String host = "localhost";
        final int port = 16007;
        final String baseUrl = String.format("http://%s:%d", host, port);

        // Test /metrics endpoint
        final HttpURLConnection metricsConnection =
                (HttpURLConnection) new java.net.URL(baseUrl + "/metrics").openConnection();
        metricsConnection.setRequestMethod("GET");
        final int metricsResponseCode = metricsConnection.getResponseCode();
        assertEquals(200, metricsResponseCode, "Expected HTTP 200 for /metrics endpoint.");
    }
}
