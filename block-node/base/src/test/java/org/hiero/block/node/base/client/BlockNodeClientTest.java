// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.hiero.block.internal.BlockNodeSourceConfig;
import org.hiero.block.internal.GrpcWebClientTuning;
import org.hiero.block.node.base.client.BlockNodeClient.IntConfigSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class BlockNodeClientTest {

    private static final int GLOBAL_TIMEOUT_MS = 10_000;
    private static final int MAX_INCOMING_BUFFER_SIZE = 10_485_760;
    private static final int MAX_PROTOBUF_MESSAGE_SIZE = 10_485_760;

    private static BlockNodeClient clientFor(final int port, final int statusPort) {
        final BlockNodeSourceConfig source = BlockNodeSourceConfig.newBuilder()
                .address("localhost")
                .port(port)
                .statusPort(statusPort)
                .build();
        return new BlockNodeClient(
                source, GLOBAL_TIMEOUT_MS, false, MAX_INCOMING_BUFFER_SIZE, MAX_PROTOBUF_MESSAGE_SIZE, null);
    }

    @Test
    @DisplayName("Status RPC dials the dedicated status port when configured")
    void statusPortRoutesToDedicatedChannel() {
        final BlockNodeClient client = clientFor(40980, 40982);

        assertNotSame(client.webClient, client.statusWebClient);
        assertEquals(40980, client.webClient.prototype().baseUri().orElseThrow().port());
        assertEquals(
                40982,
                client.statusWebClient.prototype().baseUri().orElseThrow().port());
    }

    @Test
    @DisplayName("Status RPC uses its own channel on the subscribe port when no status port is configured")
    void statusPortDefaultsToSubscribePort() {
        final BlockNodeClient client = clientFor(40840, 0);

        assertNotSame(client.webClient, client.statusWebClient);
        assertEquals(40840, client.webClient.prototype().baseUri().orElseThrow().port());
        assertEquals(
                40840,
                client.statusWebClient.prototype().baseUri().orElseThrow().port());
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
