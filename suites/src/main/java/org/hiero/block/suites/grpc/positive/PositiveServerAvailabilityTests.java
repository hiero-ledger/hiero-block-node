// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.grpc.positive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    public void verifyGrpcServerStartsSuccessfully() {
        assertTrue(blockNodeContainer.isRunning(), "Block Node container should be running.");
        assertTrue(blockNodeContainer.isHealthy(), "Block Node container should be healthy.");
    }

    /**
     * Test to verify that the gRPC server is listening on the correct port.
     *
     * <p>The test asserts that the container is running, exposes exactly one port, and that the
     * exposed port matches the expected gRPC server port.
     */
    @Test
    @DisplayName("Should listen on correct gRPC port")
    public void verifyGrpcServerListeningOnCorrectPort() {
        assertTrue(blockNodeContainer.isRunning(), "Block Node container should be running.");
        assertEquals(1, blockNodeContainer.getExposedPorts().size(), "There should be exactly one exposed port.");
        assertEquals(
                blockNodePort,
                blockNodeContainer.getExposedPorts().getFirst(),
                "The exposed port should match the expected gRPC server port.");
    }

    /**
     * Test should verify the healthz endpoints of the REST API.
     *
     * <p>The test asserts that /healthz/readyz REST endpoint returns HTTP 200 Status</p>
     * <p>The test asserts that /healthz/livez REST endpoint returns HTTP 200 Status</p>
     *
     * @throws Exception catch all exceptions
     */
    @Test
    @DisplayName("Verify /healthz endpoints")
    public void verifyHealthzEndpoints() throws Exception {
        final String host = "localhost";
        final int port = blockNodeContainer.getExposedPorts().getFirst();
        final String baseUrl = String.format("http://%s:%d", host, port);

        // Test /healthz/readyz endpoint
        final HttpURLConnection readyzConnection =
                (HttpURLConnection) new java.net.URL(baseUrl + "/healthz/readyz").openConnection();
        readyzConnection.setRequestMethod("GET");
        final int readyzResponseCode = readyzConnection.getResponseCode();
        assertEquals(200, readyzResponseCode, "Expected HTTP 200 for /healthz/readyz endpoint.");

        // Test /healthz/livez endpoint
        final HttpURLConnection livezConnection =
                (HttpURLConnection) new java.net.URL(baseUrl + "/healthz/livez").openConnection();
        livezConnection.setRequestMethod("GET");
        final int livezResponseCode = livezConnection.getResponseCode();
        assertEquals(200, livezResponseCode, "Expected HTTP 200 for /healthz/livez endpoint.");
    }

    /**
     * Test should verify the metrics endpoints of the REST API.
     *
     * @throws Exception catch all exceptions
     */
    @Test
    @DisplayName("Verify /metrics endpoint")
    public void verifyMetricsEndpoint() throws Exception {
        final String host = "localhost";
        final int port = 9999;
        final String baseUrl = String.format("http://%s:%d", host, port);

        // Test /metrics endpoint
        final HttpURLConnection metricsConnection =
                (HttpURLConnection) new java.net.URL(baseUrl + "/metrics").openConnection();
        metricsConnection.setRequestMethod("GET");
        final int metricsResponseCode = metricsConnection.getResponseCode();
        assertEquals(200, metricsResponseCode, "Expected HTTP 200 for /metrics endpoint.");
    }
}
