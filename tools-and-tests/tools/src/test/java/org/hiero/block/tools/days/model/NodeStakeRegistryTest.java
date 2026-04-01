// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.transaction.NodeStake;
import com.hedera.hapi.node.transaction.NodeStakeUpdateTransactionBody;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("NodeStakeRegistry")
class NodeStakeRegistryTest {

    private static NodeStakeUpdateTransactionBody createStakeUpdate(long... nodeIdAndStakePairs) {
        NodeStakeUpdateTransactionBody.Builder builder = NodeStakeUpdateTransactionBody.newBuilder();
        List<NodeStake> stakes = new java.util.ArrayList<>();
        for (int i = 0; i < nodeIdAndStakePairs.length; i += 2) {
            stakes.add(NodeStake.newBuilder()
                    .nodeId(nodeIdAndStakePairs[i])
                    .stake(nodeIdAndStakePairs[i + 1])
                    .build());
        }
        return builder.nodeStake(stakes).build();
    }

    @Nested
    @DisplayName("empty registry")
    class EmptyRegistry {

        @Test
        @DisplayName("returns null stake map when no data exists")
        void returnsNullStakeMap() {
            NodeStakeRegistry registry = new NodeStakeRegistry();
            assertNull(registry.getStakeMapForBlock(Instant.now()));
        }

        @Test
        @DisplayName("reports no stake data")
        void hasNoStakeData() {
            NodeStakeRegistry registry = new NodeStakeRegistry();
            assertFalse(registry.hasStakeData());
            assertEquals(0, registry.getSnapshotCount());
        }
    }

    @Nested
    @DisplayName("updateStakes")
    class UpdateStakes {

        @Test
        @DisplayName("records a snapshot and returns change description")
        void recordsSnapshot() {
            NodeStakeRegistry registry = new NodeStakeRegistry();
            String result =
                    registry.updateStakes(Instant.ofEpochSecond(100), createStakeUpdate(0, 1000, 1, 2000, 2, 3000));
            assertNotNull(result);
            assertTrue(result.contains("3 nodes"));
            assertTrue(result.contains("totalStake=6000"));
            assertTrue(result.contains("node 0: stake=1000"));
            assertTrue(result.contains("node 2: stake=3000"));
            assertTrue(registry.hasStakeData());
            assertEquals(1, registry.getSnapshotCount());
        }

        @Test
        @DisplayName("does not duplicate identical consecutive snapshots")
        void doesNotDuplicateIdenticalSnapshots() {
            NodeStakeRegistry registry = new NodeStakeRegistry();
            NodeStakeUpdateTransactionBody body = createStakeUpdate(0, 1000, 1, 2000);
            registry.updateStakes(Instant.ofEpochSecond(100), body);
            String result = registry.updateStakes(Instant.ofEpochSecond(200), body);
            assertNull(result);
            assertEquals(1, registry.getSnapshotCount());
        }

        @Test
        @DisplayName("appends when stake data changes")
        void appendsOnChange() {
            NodeStakeRegistry registry = new NodeStakeRegistry();
            registry.updateStakes(Instant.ofEpochSecond(100), createStakeUpdate(0, 1000, 1, 2000));
            String result = registry.updateStakes(Instant.ofEpochSecond(200), createStakeUpdate(0, 1500, 1, 2500));
            assertNotNull(result);
            assertEquals(2, registry.getSnapshotCount());
        }
    }

    @Nested
    @DisplayName("getStakeMapForBlock")
    class GetStakeMapForBlock {

        @Test
        @DisplayName("returns correct snapshot by block time")
        void returnsCorrectSnapshotByTime() {
            NodeStakeRegistry registry = new NodeStakeRegistry();
            registry.updateStakes(Instant.ofEpochSecond(100), createStakeUpdate(0, 1000, 1, 2000));
            registry.updateStakes(Instant.ofEpochSecond(200), createStakeUpdate(0, 3000, 1, 4000));

            // Before first snapshot — no data
            assertNull(registry.getStakeMapForBlock(Instant.ofEpochSecond(50)));

            // After first, before second — returns first snapshot
            Map<Long, Long> map = registry.getStakeMapForBlock(Instant.ofEpochSecond(150));
            assertNotNull(map);
            assertEquals(1000L, map.get(0L));
            assertEquals(2000L, map.get(1L));

            // After second — returns second snapshot
            map = registry.getStakeMapForBlock(Instant.ofEpochSecond(250));
            assertNotNull(map);
            assertEquals(3000L, map.get(0L));
            assertEquals(4000L, map.get(1L));
        }

        @Test
        @DisplayName("returns snapshot at exact timestamp")
        void returnsSnapshotAtExactTimestamp() {
            NodeStakeRegistry registry = new NodeStakeRegistry();
            registry.updateStakes(Instant.ofEpochSecond(100), createStakeUpdate(0, 1000));

            Map<Long, Long> map = registry.getStakeMapForBlock(Instant.ofEpochSecond(100));
            assertNotNull(map);
            assertEquals(1000L, map.get(0L));
        }
    }

    @Nested
    @DisplayName("persistence")
    class Persistence {

        @Test
        @DisplayName("save and reload preserves data")
        void saveAndReloadPreservesData(@TempDir Path tempDir) {
            NodeStakeRegistry registry = new NodeStakeRegistry();
            registry.updateStakes(Instant.ofEpochSecond(100), createStakeUpdate(0, 1000, 1, 2000));
            registry.updateStakes(Instant.ofEpochSecond(200), createStakeUpdate(0, 3000, 1, 4000));

            Path file = tempDir.resolve("nodeStakeHistory.json");
            registry.saveToJsonFile(file);

            NodeStakeRegistry loaded = new NodeStakeRegistry(file);
            assertEquals(2, loaded.getSnapshotCount());

            Map<Long, Long> map = loaded.getStakeMapForBlock(Instant.ofEpochSecond(250));
            assertNotNull(map);
            assertEquals(3000L, map.get(0L));
            assertEquals(4000L, map.get(1L));
        }

        @Test
        @DisplayName("reloadFromFile replaces existing data")
        void reloadFromFileReplacesData(@TempDir Path tempDir) {
            NodeStakeRegistry original = new NodeStakeRegistry();
            original.updateStakes(Instant.ofEpochSecond(100), createStakeUpdate(0, 1000));
            Path file = tempDir.resolve("nodeStakeHistory.json");
            original.saveToJsonFile(file);

            NodeStakeRegistry registry = new NodeStakeRegistry();
            registry.updateStakes(Instant.ofEpochSecond(50), createStakeUpdate(0, 500));
            assertEquals(1, registry.getSnapshotCount());

            registry.reloadFromFile(file);
            assertEquals(1, registry.getSnapshotCount());
            Map<Long, Long> map = registry.getStakeMapForBlock(Instant.ofEpochSecond(150));
            assertNotNull(map);
            assertEquals(1000L, map.get(0L));
        }
    }
}
