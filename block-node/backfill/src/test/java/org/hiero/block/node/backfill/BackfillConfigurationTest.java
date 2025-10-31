// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.extensions.sources.SimpleConfigSource;
import com.swirlds.config.extensions.test.fixtures.TestConfigBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for BackfillConfiguration to verify gRPC client configuration overrides.
 */
class BackfillConfigurationTest {

    @Test
    @DisplayName("Test default configuration values")
    void testDefaultConfiguration() {
        Configuration config = new TestConfigBuilder()
                .withSource(new SimpleConfigSource())
                .getOrCreateConfig();

        BackfillConfiguration backfillConfig = config.getConfigData(BackfillConfiguration.class);

        assertNotNull(backfillConfig);
        assertEquals(0, backfillConfig.startBlock());
        assertEquals(-1, backfillConfig.endBlock());
        assertEquals("", backfillConfig.blockNodeSourcesPath());
        assertEquals(60000, backfillConfig.scanInterval());
        assertEquals(3, backfillConfig.maxRetries());
        assertEquals(5000, backfillConfig.initialRetryDelay());
        assertEquals(10, backfillConfig.fetchBatchSize());
        assertEquals(1000, backfillConfig.delayBetweenBatches());
        assertEquals(15000, backfillConfig.initialDelay());
        assertEquals(1000, backfillConfig.perBlockProcessingTimeout());
        assertEquals(60000, backfillConfig.grpcOverallTimeout());
        assertEquals(false, backfillConfig.enableTLS());
        // New gRPC override fields should default to -1
        assertEquals(-1, backfillConfig.grpcConnectTimeout());
        assertEquals(-1, backfillConfig.grpcReadTimeout());
        assertEquals(-1, backfillConfig.grpcPollWaitTime());
    }

    @Test
    @DisplayName("Test gRPC client configuration overrides")
    void testGrpcClientOverrides() {
        Configuration config = new TestConfigBuilder()
                .withSource(new SimpleConfigSource()
                        .withValue("backfill.grpcConnectTimeout", "30000")
                        .withValue("backfill.grpcReadTimeout", "45000")
                        .withValue("backfill.grpcPollWaitTime", "20000"))
                .getOrCreateConfig();

        BackfillConfiguration backfillConfig = config.getConfigData(BackfillConfiguration.class);

        assertNotNull(backfillConfig);
        assertEquals(30000, backfillConfig.grpcConnectTimeout());
        assertEquals(45000, backfillConfig.grpcReadTimeout());
        assertEquals(20000, backfillConfig.grpcPollWaitTime());
        // Overall timeout should remain at default
        assertEquals(60000, backfillConfig.grpcOverallTimeout());
    }

    @Test
    @DisplayName("Test partial gRPC client configuration overrides")
    void testPartialGrpcClientOverrides() {
        Configuration config = new TestConfigBuilder()
                .withSource(new SimpleConfigSource()
                        .withValue("backfill.grpcConnectTimeout", "25000")
                        .withValue("backfill.grpcOverallTimeout", "50000"))
                .getOrCreateConfig();

        BackfillConfiguration backfillConfig = config.getConfigData(BackfillConfiguration.class);

        assertNotNull(backfillConfig);
        assertEquals(25000, backfillConfig.grpcConnectTimeout());
        assertEquals(-1, backfillConfig.grpcReadTimeout()); // Should use default -1
        assertEquals(-1, backfillConfig.grpcPollWaitTime()); // Should use default -1
        assertEquals(50000, backfillConfig.grpcOverallTimeout());
    }

    @Test
    @DisplayName("Test all configuration values with overrides")
    void testAllConfigurationWithOverrides() {
        Configuration config = new TestConfigBuilder()
                .withSource(new SimpleConfigSource()
                        .withValue("backfill.startBlock", "100")
                        .withValue("backfill.endBlock", "1000")
                        .withValue("backfill.blockNodeSourcesPath", "/path/to/sources.json")
                        .withValue("backfill.scanInterval", "30000")
                        .withValue("backfill.maxRetries", "5")
                        .withValue("backfill.initialRetryDelay", "2000")
                        .withValue("backfill.fetchBatchSize", "50")
                        .withValue("backfill.delayBetweenBatches", "500")
                        .withValue("backfill.initialDelay", "10000")
                        .withValue("backfill.perBlockProcessingTimeout", "2000")
                        .withValue("backfill.grpcOverallTimeout", "40000")
                        .withValue("backfill.enableTLS", "true")
                        .withValue("backfill.grpcConnectTimeout", "15000")
                        .withValue("backfill.grpcReadTimeout", "35000")
                        .withValue("backfill.grpcPollWaitTime", "25000"))
                .getOrCreateConfig();

        BackfillConfiguration backfillConfig = config.getConfigData(BackfillConfiguration.class);

        assertNotNull(backfillConfig);
        assertEquals(100, backfillConfig.startBlock());
        assertEquals(1000, backfillConfig.endBlock());
        assertEquals("/path/to/sources.json", backfillConfig.blockNodeSourcesPath());
        assertEquals(30000, backfillConfig.scanInterval());
        assertEquals(5, backfillConfig.maxRetries());
        assertEquals(2000, backfillConfig.initialRetryDelay());
        assertEquals(50, backfillConfig.fetchBatchSize());
        assertEquals(500, backfillConfig.delayBetweenBatches());
        assertEquals(10000, backfillConfig.initialDelay());
        assertEquals(2000, backfillConfig.perBlockProcessingTimeout());
        assertEquals(40000, backfillConfig.grpcOverallTimeout());
        assertEquals(true, backfillConfig.enableTLS());
        assertEquals(15000, backfillConfig.grpcConnectTimeout());
        assertEquals(35000, backfillConfig.grpcReadTimeout());
        assertEquals(25000, backfillConfig.grpcPollWaitTime());
    }

    @Test
    @DisplayName("Test zero value for gRPC overrides uses overall timeout")
    void testZeroValueGrpcOverrides() {
        Configuration config = new TestConfigBuilder()
                .withSource(new SimpleConfigSource()
                        .withValue("backfill.grpcConnectTimeout", "0")
                        .withValue("backfill.grpcReadTimeout", "0")
                        .withValue("backfill.grpcPollWaitTime", "0")
                        .withValue("backfill.grpcOverallTimeout", "50000"))
                .getOrCreateConfig();

        BackfillConfiguration backfillConfig = config.getConfigData(BackfillConfiguration.class);

        assertNotNull(backfillConfig);
        // Zero values should be treated as "not set" and fall back to overall timeout
        assertEquals(0, backfillConfig.grpcConnectTimeout());
        assertEquals(0, backfillConfig.grpcReadTimeout());
        assertEquals(0, backfillConfig.grpcPollWaitTime());
        assertEquals(50000, backfillConfig.grpcOverallTimeout());
    }
}
