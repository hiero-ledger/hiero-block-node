// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.io.IOException;
import org.hiero.block.internal.BlockNodeSourceConfig;
import org.hiero.block.internal.GrpcWebClientTuning;
import org.hiero.block.node.base.client.BlockNodeClient.IntConfigSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class BlockNodeClientTest {

    private static final int GLOBAL_TIMEOUT_MS = 10_000;
    private static final int MAX_INCOMING_BUFFER_SIZE = 10_485_760;
    private static final int MAX_PROTOBUF_MESSAGE_SIZE = 10_485_760;

    private static BlockNodeClient clientFor(final int port, final int subscribePort, final int statusPort) {
        final BlockNodeSourceConfig source = BlockNodeSourceConfig.newBuilder()
                .address("localhost")
                .port(port)
                .subscribePort(subscribePort)
                .statusPort(statusPort)
                .build();
        return new BlockNodeClient(
                source, GLOBAL_TIMEOUT_MS, false, MAX_INCOMING_BUFFER_SIZE, MAX_PROTOBUF_MESSAGE_SIZE, null);
    }

    private static int subscribePortOf(final BlockNodeClient client) {
        return client.webClient.prototype().baseUri().orElseThrow().port();
    }

    private static int statusPortOf(final BlockNodeClient client) {
        return client.statusWebClient.prototype().baseUri().orElseThrow().port();
    }

    @Test
    @DisplayName("Subscribe and status RPCs dial their dedicated ports when configured")
    void dedicatedPortsRouteToTheirOwnChannels() throws IOException {
        try (BlockNodeClient client = clientFor(40840, 40980, 40982)) {
            assertNotSame(client.webClient, client.statusWebClient);
            assertEquals(40980, subscribePortOf(client));
            assertEquals(40982, statusPortOf(client));
        }
    }

    @Test
    @DisplayName("Subscribe and status RPCs fall back to `port` on their own channels when unset")
    void portsFallBackToPortWhenUnset() throws IOException {
        try (BlockNodeClient client = clientFor(40840, 0, 0)) {
            assertNotSame(client.webClient, client.statusWebClient);
            assertEquals(40840, subscribePortOf(client));
            assertEquals(40840, statusPortOf(client));
        }
    }

    @Test
    @DisplayName("Status falls back to `port` independently of a configured subscribe port")
    void statusFallsBackToPortNotSubscribePort() throws IOException {
        try (BlockNodeClient client = clientFor(40840, 40980, 0)) {
            assertEquals(40980, subscribePortOf(client));
            assertEquals(40840, statusPortOf(client));
        }
    }

    GrpcWebClientTuning tuning = GrpcWebClientTuning.newBuilder()
            .connectTimeout(1)
            .flowControlTimeout(2)
            .initialWindowSize(5)
            .maxFrameSize(8)
            .pingTimeout(11)
            .readTimeout(0)
            .build();

    @Test
    @DisplayName("WebClients do not share Helidon's JVM-wide HTTP/2 connection cache")
    void connectionCacheIsNotShared() throws IOException {
        // Helidon's HttpClientConfigBlueprint defaults shareConnectionCache to true, which would let a
        // freshly-constructed client (e.g. after getNodeClient() evicts an unreachable peer) reuse a
        // stale connection handler for the same host:port from the JVM-wide Http2ConnectionCache.SHARED
        // singleton instead of dialing a new connection — surfacing as a persistent
        // "SocketException: Socket closed" after the peer restarts. Each client must use its own
        // private cache so eviction actually forces a fresh connection.
        try (BlockNodeClient client = clientFor(40840, 0, 0)) {
            assertFalse(client.webClient.prototype().shareConnectionCache());
            assertFalse(client.statusWebClient.prototype().shareConnectionCache());
        }
    }

    @Test
    @DisplayName("Test null GrpcWebClientTuning")
    void nullTest() {
        final IntConfigSpec testConfig = new IntConfigSpec("null test", 7, 5, 10, GrpcWebClientTuning::connectTimeout);

        assertEquals(7, testConfig.getValidOrDefault(null));
    }

    @Test
    @DisplayName("Test connectTimeout")
    void connectTimeoutTest() {
        final IntConfigSpec testConfig =
                new IntConfigSpec("connectTimeout test", 7, 5, 10, GrpcWebClientTuning::connectTimeout);

        assertEquals(7, testConfig.getValidOrDefault(tuning));
    }

    @Test
    @DisplayName("Test flowControlTimeout")
    void flowControlTimeout() {
        final IntConfigSpec testConfig =
                new IntConfigSpec("null test", 7, 5, 10, GrpcWebClientTuning::flowControlTimeout);

        assertEquals(7, testConfig.getValidOrDefault(tuning));
    }

    @Test
    @DisplayName("Test initialWindowSize")
    void initialWindowSize() {
        final IntConfigSpec testConfig =
                new IntConfigSpec("initialWindowSize test", 7, 5, 10, GrpcWebClientTuning::initialWindowSize);

        assertEquals(5, testConfig.getValidOrDefault(tuning));
    }

    @Test
    @DisplayName("Test pingTimeout")
    void pingTimeout() {
        final IntConfigSpec testConfig =
                new IntConfigSpec("pingTimeout test", 7, 5, 10, GrpcWebClientTuning::pingTimeout);

        assertEquals(7, testConfig.getValidOrDefault(tuning));
    }

    @Test
    @DisplayName("Test maxFrameSize")
    void maxFrameSize() {
        final IntConfigSpec testConfig =
                new IntConfigSpec("maxFrameSize test", 7, 5, 10, GrpcWebClientTuning::maxFrameSize);

        assertEquals(8, testConfig.getValidOrDefault(tuning));
    }

    @Test
    @DisplayName("Test readTimeout")
    void readTimeoutTest() {
        final IntConfigSpec testConfig =
                new IntConfigSpec("readTimeout test", 7, 5, 10, GrpcWebClientTuning::readTimeout);

        assertEquals(7, testConfig.getValidOrDefault(tuning));
    }
}
