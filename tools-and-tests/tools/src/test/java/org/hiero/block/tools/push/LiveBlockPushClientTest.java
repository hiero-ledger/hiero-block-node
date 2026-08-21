// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.push;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.Block;
import org.hiero.block.tools.config.HelidonWebClientConfig;
import org.hiero.block.tools.push.LiveBlockPushClient.QueryFailedException;
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

    /**
     * A second reserved/unreachable port, distinct from {@link #UNREACHABLE_PORT}, so a split-port
     * test can tell which of the two client URIs the code actually contacted.
     */
    private static final int UNREACHABLE_STATUS_PORT = 7;

    private static LiveBlockPushClient newClient(final int queueCapacity) {
        return new LiveBlockPushClient(
                "127.0.0.1", UNREACHABLE_PORT, queueCapacity, LiveBlockPushClient.loadDefaultWebConfig());
    }

    private static LiveBlockPushClient newSplitPortClient(final int queueCapacity) {
        return new LiveBlockPushClient(
                "127.0.0.1",
                UNREACHABLE_PORT,
                UNREACHABLE_STATUS_PORT,
                queueCapacity,
                LiveBlockPushClient.loadDefaultWebConfig());
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
        @DisplayName("queryLastAvailableBlock() throws QueryFailedException when the BN is unreachable")
        @Timeout(60)
        void queryThrowsOnUnreachable() {
            try (final LiveBlockPushClient client = newClient(8)) {
                final QueryFailedException thrown = assertThrows(
                        QueryFailedException.class,
                        client::queryLastAvailableBlock,
                        "Expected QueryFailedException when BN is unreachable (silent -1 return caused #3374)");
                final String msg = thrown.getMessage();
                assertTrue(
                        msg.contains("127.0.0.1") && msg.contains(":1"),
                        "Message must name the host:port that was tried so a port mismatch is diagnosable; was: "
                                + msg);
            }
        }

        @Test
        @DisplayName("queryLastAvailableBlock() uses the status port, not the publish port, in split-port mode")
        @Timeout(60)
        void queryUsesStatusPortInSplitPortMode() {
            // Publish port = 1, status port = 7 — both unreachable, distinct so the message
            // uniquely identifies which URI the query actually contacted. Guards against a
            // regression in buildStatusWebClient()/statusPort routing that would otherwise
            // silently query the publish port and only surface in production against real BNs.
            try (final LiveBlockPushClient client = newSplitPortClient(8)) {
                final QueryFailedException thrown =
                        assertThrows(QueryFailedException.class, client::queryLastAvailableBlock);
                final String msg = thrown.getMessage();
                assertTrue(
                        msg.contains(":" + UNREACHABLE_STATUS_PORT),
                        "Message must name the STATUS port (" + UNREACHABLE_STATUS_PORT
                                + ") the query targets — otherwise the split-port routing is broken; was: "
                                + msg);
                assertFalse(
                        msg.contains(":" + UNREACHABLE_PORT + ")") || msg.contains(":" + UNREACHABLE_PORT + " "),
                        "Message must NOT name the publish port (" + UNREACHABLE_PORT
                                + ") — that would mean the query went to the wrong port; was: " + msg);
            }
        }
    }
}
