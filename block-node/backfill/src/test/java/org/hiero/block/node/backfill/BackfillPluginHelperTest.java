// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.backfill.client.BackfillSourceConfig;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link BackfillPlugin} helper methods.
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

    @Test
    @DisplayName("Returns null when requested start is outside every available range")
    void computeChunkReturnsNullWhenStartOutsideAvailability() {
        final BackfillPlugin plugin = new BackfillPlugin();
        final BackfillSourceConfig source = node("localhost", 1);
        final Map<BackfillSourceConfig, List<LongRange>> availability = Map.of(source, List.of(new LongRange(20, 30)));

        // Start is below the only available range, so no chunk should be produced.
        LongRange result = plugin.computeChunk(new NodeSelectionStrategy.NodeSelection(source, 10), availability, 50, 5);

        assertNull(result, "Should return null when start is not within any available range");
    }

    @Test
    @DisplayName("Clamps chunk end to both available range and gap end")
    void computeChunkClampsToRangeAndGap() {
        final BackfillPlugin plugin = new BackfillPlugin();
        final BackfillSourceConfig source = node("localhost", 1);
        final Map<BackfillSourceConfig, List<LongRange>> availability = Map.of(source, List.of(new LongRange(10, 15)));

        // Batch size would request past range end, and gapEnd is 12, so end should clamp at 12.
        LongRange result = plugin.computeChunk(new NodeSelectionStrategy.NodeSelection(source, 10), availability, 12, 10);

        assertEquals(new LongRange(10, 12), result);
    }
}
