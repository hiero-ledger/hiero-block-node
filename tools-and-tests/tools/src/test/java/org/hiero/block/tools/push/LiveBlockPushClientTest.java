// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.push;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.Block;
import org.hiero.block.tools.config.HelidonWebClientConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link LiveBlockPushClient}. These cover the safe-to-use API contract
 * (lifecycle invariants, metric defaults, default-config loading, and the error path of
 * {@link LiveBlockPushClient#queryLastAvailableBlock()} when the BN is unreachable).
 *
 * <p>End-to-end push/ACK/reconnect behavior requires either a real Block Node or a stub gRPC
 * server and belongs in the integration test suites, not here.
 */
class LiveBlockPushClientTest {

    /** Port 1 is reserved and effectively always refuses TCP connections — used as an unreachable target. */
    private static final int UNREACHABLE_PORT = 1;

    private static LiveBlockPushClient newClient(final int queueCapacity) {
        return new LiveBlockPushClient(
                "127.0.0.1", UNREACHABLE_PORT, queueCapacity, LiveBlockPushClient.loadDefaultWebConfig());
    }

    @Nested
    @DisplayName("Default config")
    class DefaultConfig {

        @Test
        @DisplayName("loadDefaultWebConfig() loads the bundled JSON without error")
        void loadsBundledConfig() {
            final HelidonWebClientConfig cfg = LiveBlockPushClient.loadDefaultWebConfig();
            assertNotNull(cfg);
            // serverMaxMessageSizeBytes from clientDefaultConfig.json is 131_072_000 (~125 MiB)
            assertEquals(131_072_000, cfg.serverMaxMessageSizeBytes());
            assertTrue(cfg.readTimeoutMillis() > 0);
        }
    }

    @Nested
    @DisplayName("Lifecycle")
    class Lifecycle {

        @Test
        @DisplayName("pushBlock before start() throws IllegalStateException")
        void pushBeforeStartThrows() {
            try (final LiveBlockPushClient client = newClient(8)) {
                assertThrows(IllegalStateException.class, () -> client.pushBlock(0L, Block.DEFAULT));
            }
        }

        @Test
        @DisplayName("shutdown() before start() is a no-op")
        void shutdownBeforeStartIsNoOp() {
            final LiveBlockPushClient client = newClient(8);
            assertDoesNotThrow(client::shutdown);
        }

        @Test
        @DisplayName("close() is an alias for shutdown() and is idempotent")
        @Timeout(10)
        void closeIsIdempotent() {
            final LiveBlockPushClient client = newClient(8);
            assertDoesNotThrow(client::close);
            assertDoesNotThrow(client::close); // second close must not throw
        }
    }

    @Nested
    @DisplayName("Metrics defaults")
    class MetricsDefaults {

        @Test
        @DisplayName("Counters and last-acked start at expected initial values")
        void initialMetrics() {
            try (final LiveBlockPushClient client = newClient(8)) {
                assertEquals(0L, client.submitted());
                assertEquals(0L, client.acked());
                assertEquals(-1L, client.lastAcked());
                assertEquals(0L, client.reconnects());
                assertEquals(0, client.queueDepth());
            }
        }
    }

    @Nested
    @DisplayName("Unreachable BN error path")
    class UnreachableBn {

        @Test
        @DisplayName("queryLastAvailableBlock() returns -1 when the BN is unreachable")
        @Timeout(60)
        void queryReturnsMinusOneOnUnreachable() {
            try (final LiveBlockPushClient client = newClient(8)) {
                final long result = client.queryLastAvailableBlock();
                assertEquals(-1L, result, "Expected sentinel -1 when BN is unreachable");
            }
        }
    }
}
