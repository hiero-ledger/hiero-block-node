// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.subscribeclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import java.util.Map;
import org.hiero.block.node.subscribeclient.SubscribeClientConfiguration.DeliveryMode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for {@link SubscribeClientConfiguration}. Covers the config record's defaults,
 * override parsing, and validation boundaries called out in the design doc (section:
 * Configuration).
 */
@DisplayName("SubscribeClientConfiguration")
class SubscribeClientConfigurationTest {

    private static SubscribeClientConfiguration load(final Map<String, String> overrides) {
        ConfigurationBuilder builder = ConfigurationBuilder.create()
                .autoDiscoverExtensions()
                .withConfigDataType(SubscribeClientConfiguration.class);
        for (Map.Entry<String, String> e : overrides.entrySet()) {
            builder = builder.withValue(e.getKey(), e.getValue());
        }
        final Configuration config = builder.build();
        return config.getConfigData(SubscribeClientConfiguration.class);
    }

    @Nested
    @DisplayName("defaults")
    class Defaults {

        @Test
        @DisplayName("match the design-doc Configuration table")
        void defaultsMatchDesign() {
            final SubscribeClientConfiguration c = load(Map.of());
            assertEquals(DeliveryMode.FULL_BLOCK, c.deliveryMode());
            assertEquals("", c.blockNodeSourcesPath());
            assertEquals(3000L, c.staleThresholdMs());
            assertEquals(500L, c.initialRetryDelayMs());
            assertEquals(60_000L, c.maxBackoffMs());
            assertEquals(250L, c.reconnectMinDelayMs());
            assertEquals(30_000, c.grpcOverallTimeout());
            assertEquals(30_000L, c.peerTipPollInterval());
            assertEquals(50L, c.peerTipLagThresholdBlocks());
            assertFalse(c.enableTLS());
            assertEquals(4_194_304, c.maxIncomingBufferSize());
        }
    }

    @Nested
    @DisplayName("override parsing")
    class Overrides {

        @Test
        @DisplayName("delivery mode flips to IMMEDIATE when configured")
        void deliveryModeOverride() {
            final SubscribeClientConfiguration c = load(Map.of("subscribe.client.deliveryMode", "IMMEDIATE"));
            assertEquals(DeliveryMode.IMMEDIATE, c.deliveryMode());
        }

        @Test
        @DisplayName("numeric fields parse operator overrides")
        void numericOverrides() {
            final SubscribeClientConfiguration c = load(Map.of(
                    "subscribe.client.staleThresholdMs", "5000",
                    "subscribe.client.peerTipLagThresholdBlocks", "200"));
            assertEquals(5000L, c.staleThresholdMs());
            assertEquals(200L, c.peerTipLagThresholdBlocks());
        }

        @Test
        @DisplayName("blockNodeSourcesPath passes through verbatim")
        void sourcesPathOverride() {
            final SubscribeClientConfiguration c =
                    load(Map.of("subscribe.client.blockNodeSourcesPath", "/opt/hiero/peers.json"));
            assertEquals("/opt/hiero/peers.json", c.blockNodeSourcesPath());
        }
    }

    @Nested
    @DisplayName("validation")
    class Validation {

        @Test
        @DisplayName("deliveryMode must be a known enum value")
        void unknownDeliveryModeRejected() {
            assertThrows(RuntimeException.class, () -> load(Map.of("subscribe.client.deliveryMode", "BOGUS")));
        }

        @Test
        @DisplayName("staleThresholdMs below @Min(100) is rejected")
        void staleThresholdFloorEnforced() {
            assertThrows(RuntimeException.class, () -> load(Map.of("subscribe.client.staleThresholdMs", "50")));
        }

        @Test
        @DisplayName("peerTipLagThresholdBlocks above @Max(10_000) is rejected")
        void peerTipLagCeilingEnforced() {
            assertThrows(
                    RuntimeException.class, () -> load(Map.of("subscribe.client.peerTipLagThresholdBlocks", "100000")));
        }
    }
}
