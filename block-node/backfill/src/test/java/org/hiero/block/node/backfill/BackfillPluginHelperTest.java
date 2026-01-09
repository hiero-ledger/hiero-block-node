// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.backfill.client.BackfillSourceConfig;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.NewestBlockKnownToNetworkNotification;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link BackfillPlugin} helper methods and notification handlers.
 */
@Timeout(value = 5, unit = TimeUnit.SECONDS)
class BackfillPluginHelperTest {

    private BackfillSourceConfig node(String host, int port) {
        return BackfillSourceConfig.newBuilder()
                .address(host)
                .port(port)
                .priority(1)
                .build();
    }

    @Nested
    @DisplayName("computeChunk")
    class ComputeChunkTests {

        @Test
        @DisplayName("Returns null when requested start is outside every available range")
        void computeChunkReturnsNullWhenStartOutsideAvailability() {
            final BackfillPlugin plugin = new BackfillPlugin();
            final BackfillSourceConfig source = node("localhost", 1);
            final Map<BackfillSourceConfig, List<LongRange>> availability =
                    Map.of(source, List.of(new LongRange(20, 30)));

            // Start is below the only available range, so no chunk should be produced.
            LongRange result =
                    plugin.computeChunk(new NodeSelectionStrategy.NodeSelection(source, 10), availability, 50, 5);

            assertNull(result, "Should return null when start is not within any available range");
        }

        @Test
        @DisplayName("Clamps chunk end to both available range and gap end")
        void computeChunkClampsToRangeAndGap() {
            final BackfillPlugin plugin = new BackfillPlugin();
            final BackfillSourceConfig source = node("localhost", 1);
            final Map<BackfillSourceConfig, List<LongRange>> availability =
                    Map.of(source, List.of(new LongRange(10, 15)));

            // Batch size would request past range end, and gapEnd is 12, so end should clamp at 12.
            LongRange result =
                    plugin.computeChunk(new NodeSelectionStrategy.NodeSelection(source, 10), availability, 12, 10);

            assertEquals(new LongRange(10, 12), result);
        }

        @Test
        @DisplayName("Returns full batch when range and gap allow it")
        void computeChunkReturnsFullBatch() {
            final BackfillPlugin plugin = new BackfillPlugin();
            final BackfillSourceConfig source = node("localhost", 1);
            final Map<BackfillSourceConfig, List<LongRange>> availability =
                    Map.of(source, List.of(new LongRange(0, 100)));

            // Request batch of 10 starting at 5, gap ends at 50, range goes to 100
            LongRange result =
                    plugin.computeChunk(new NodeSelectionStrategy.NodeSelection(source, 5), availability, 50, 10);

            assertEquals(new LongRange(5, 14), result, "Should return full batch size when possible");
        }

        @Test
        @DisplayName("Returns null when source has empty availability")
        void computeChunkReturnsNullForEmptyAvailability() {
            final BackfillPlugin plugin = new BackfillPlugin();
            final BackfillSourceConfig source = node("localhost", 1);
            final Map<BackfillSourceConfig, List<LongRange>> availability = Map.of(source, List.of());

            LongRange result =
                    plugin.computeChunk(new NodeSelectionStrategy.NodeSelection(source, 10), availability, 50, 5);

            assertNull(result, "Should return null when source has no available ranges");
        }
    }

    @Nested
    @DisplayName("configDataTypes")
    class ConfigDataTypesTests {

        @Test
        @DisplayName("Returns list containing BackfillConfiguration class")
        void configDataTypesReturnsBackfillConfiguration() {
            final BackfillPlugin plugin = new BackfillPlugin();
            List<Class<? extends Record>> configTypes = plugin.configDataTypes();

            assertEquals(1, configTypes.size(), "Should return exactly one config type");
            assertTrue(configTypes.contains(BackfillConfiguration.class), "Should contain BackfillConfiguration.class");
        }
    }

    @Nested
    @DisplayName("Notification Handlers")
    class NotificationHandlerTests {

        @Test
        @DisplayName("handlePersisted ignores non-BACKFILL notifications without error")
        void handlePersistedIgnoresNonBackfillSource() {
            final BackfillPlugin plugin = new BackfillPlugin();
            // Create a notification with PUBLISHER source (not BACKFILL)
            PersistedNotification notification = new PersistedNotification(100L, true, 10, BlockSource.PUBLISHER);

            // Should not throw - just logs and returns
            assertDoesNotThrow(() -> plugin.handlePersisted(notification));
        }

        @Test
        @DisplayName("handleVerification ignores non-BACKFILL notifications without error")
        void handleVerificationIgnoresNonBackfillSource() {
            final BackfillPlugin plugin = new BackfillPlugin();
            // Create a verification notification with PUBLISHER source
            VerificationNotification notification =
                    new VerificationNotification(true, 100L, Bytes.wrap("hash"), null, BlockSource.PUBLISHER);

            // Should not throw - does nothing for non-BACKFILL source
            assertDoesNotThrow(() -> plugin.handleVerification(notification));
        }

        @Test
        @DisplayName("handleNewestBlockKnownToNetwork returns early when no sources configured")
        void handleNewestBlockReturnsEarlyWhenNoSources() {
            final BackfillPlugin plugin = new BackfillPlugin();
            // Plugin not initialized, so hasBNSourcesPath is false
            NewestBlockKnownToNetworkNotification notification = new NewestBlockKnownToNetworkNotification(500L);

            // Should return early without error
            assertDoesNotThrow(() -> plugin.handleNewestBlockKnownToNetwork(notification));
        }
    }
}
