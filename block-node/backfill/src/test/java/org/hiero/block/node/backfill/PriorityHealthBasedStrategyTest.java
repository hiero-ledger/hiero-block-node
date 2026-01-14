// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.backfill.NodeSelectionStrategy.NodeSelection;
import org.hiero.block.node.backfill.client.BackfillSourceConfig;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link PriorityHealthBasedStrategy}.
 */
@Timeout(value = 5, unit = TimeUnit.SECONDS)
class PriorityHealthBasedStrategyTest {

    private TestHealthProvider healthProvider;
    private PriorityHealthBasedStrategy strategy;

    @BeforeEach
    void setUp() {
        healthProvider = new TestHealthProvider();
        strategy = new PriorityHealthBasedStrategy(healthProvider);
    }

    private BackfillSourceConfig createNode(String address, int port, int priority) {
        return BackfillSourceConfig.newBuilder()
                .address(address)
                .port(port)
                .priority(priority)
                .build();
    }

    @Nested
    @DisplayName("Empty availability scenarios")
    class EmptyAvailabilityTests {

        @Test
        @DisplayName("should return empty when availability map is empty")
        void shouldReturnEmptyWhenNoAvailability() {
            Map<BackfillSourceConfig, List<LongRange>> availability = Collections.emptyMap();

            Optional<NodeSelection> result = strategy.select(0, 100, availability);

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("should return empty when startBlock exceeds gapEnd")
        void shouldReturnEmptyWhenStartExceedsEnd() {
            BackfillSourceConfig node = createNode("localhost", 8080, 1);
            // Availability covers the requested range, but startBlock (200) > gapEnd (100)
            Map<BackfillSourceConfig, List<LongRange>> availability = Map.of(node, List.of(new LongRange(0, 300)));

            Optional<NodeSelection> result = strategy.select(200, 100, availability);

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("should return empty when no ranges intersect with requested range")
        void shouldReturnEmptyWhenNoIntersection() {
            BackfillSourceConfig node = createNode("localhost", 8080, 1);
            // Available range is 200-300, but we need 0-100
            Map<BackfillSourceConfig, List<LongRange>> availability = Map.of(node, List.of(new LongRange(200, 300)));

            Optional<NodeSelection> result = strategy.select(0, 100, availability);

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("should return empty when nodes have empty range lists")
        void shouldReturnEmptyWhenEmptyRangeLists() {
            BackfillSourceConfig node = createNode("localhost", 8080, 1);
            Map<BackfillSourceConfig, List<LongRange>> availability = Map.of(node, Collections.emptyList());

            Optional<NodeSelection> result = strategy.select(0, 100, availability);

            assertTrue(result.isEmpty());
        }
    }

    @Nested
    @DisplayName("Single node scenarios")
    class SingleNodeTests {

        @Test
        @DisplayName("should select single available node")
        void shouldSelectSingleNode() {
            BackfillSourceConfig node = createNode("localhost", 8080, 1);
            Map<BackfillSourceConfig, List<LongRange>> availability = Map.of(node, List.of(new LongRange(0, 100)));

            Optional<NodeSelection> result = strategy.select(0, 100, availability);

            assertTrue(result.isPresent());
            assertEquals(node, result.get().nodeConfig());
            assertEquals(0, result.get().startBlock());
        }

        @Test
        @DisplayName("should adjust start block to available range start")
        void shouldAdjustStartToAvailableRange() {
            BackfillSourceConfig node = createNode("localhost", 8080, 1);
            // Available from block 50, but we need from 0
            Map<BackfillSourceConfig, List<LongRange>> availability = Map.of(node, List.of(new LongRange(50, 100)));

            Optional<NodeSelection> result = strategy.select(0, 100, availability);

            assertTrue(result.isPresent());
            assertEquals(50, result.get().startBlock());
        }

        @Test
        @DisplayName("should return empty when single node is in backoff")
        void shouldReturnEmptyWhenNodeInBackoff() {
            BackfillSourceConfig node = createNode("localhost", 8080, 1);
            healthProvider.setInBackoff(node, true);
            Map<BackfillSourceConfig, List<LongRange>> availability = Map.of(node, List.of(new LongRange(0, 100)));

            Optional<NodeSelection> result = strategy.select(0, 100, availability);

            assertTrue(result.isEmpty());
        }
    }

    @Nested
    @DisplayName("Priority-based selection")
    class PrioritySelectionTests {

        @Test
        @DisplayName("should select node with highest priority")
        void shouldSelectLowestPriorityNumber() {
            BackfillSourceConfig lowPriority = createNode("low", 8080, 10);
            BackfillSourceConfig highPriority = createNode("high", 8081, 1);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(lowPriority, List.of(new LongRange(0, 100)));
            availability.put(highPriority, List.of(new LongRange(0, 100)));

            Optional<NodeSelection> result = strategy.select(0, 100, availability);

            assertTrue(result.isPresent());
            assertEquals(highPriority, result.get().nodeConfig());
        }

        @Test
        @DisplayName("should return empty when best priority node is in backoff (no fallback to lower priority)")
        void shouldReturnEmptyWhenBestPriorityInBackoff() {
            // Note: The algorithm filters by best priority FIRST, then removes backoff nodes.
            // It does NOT fall back to lower priority nodes if best priority is in backoff.
            BackfillSourceConfig highPriority = createNode("high", 8080, 1);
            BackfillSourceConfig lowPriority = createNode("low", 8081, 10);
            healthProvider.setInBackoff(highPriority, true);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(highPriority, List.of(new LongRange(0, 100)));
            availability.put(lowPriority, List.of(new LongRange(0, 100)));

            Optional<NodeSelection> result = strategy.select(0, 100, availability);

            // Returns empty because only priority-1 node exists, and it's in backoff
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("should select available node among same priority when one is in backoff")
        void shouldSelectAvailableNodeWhenSamePriorityPeerInBackoff() {
            BackfillSourceConfig backoffNode = createNode("backoff", 8080, 1);
            BackfillSourceConfig availableNode = createNode("available", 8081, 1);
            healthProvider.setInBackoff(backoffNode, true);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(backoffNode, List.of(new LongRange(0, 100)));
            availability.put(availableNode, List.of(new LongRange(0, 100)));

            Optional<NodeSelection> result = strategy.select(0, 100, availability);

            assertTrue(result.isPresent());
            assertEquals(availableNode, result.get().nodeConfig());
        }
    }

    @Nested
    @DisplayName("Health-based selection")
    class HealthSelectionTests {

        @Test
        @DisplayName("should select healthier node when priorities are equal")
        void shouldSelectHealthierNodeWhenSamePriority() {
            // Create nodes with same priority but different health scores
            // Using distinct scores (1.0 vs 10.0) to ensure selection is by health, not insertion order
            BackfillSourceConfig unhealthy = createNode("unhealthy", 8080, 1);
            BackfillSourceConfig healthy = createNode("healthy", 8081, 1);
            healthProvider.setHealthScore(unhealthy, 10.0); // Higher score = less healthy
            healthProvider.setHealthScore(healthy, 1.0); // Lower score = more healthy
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            // Insert unhealthy first to verify selection is by health, not insertion order
            availability.put(unhealthy, List.of(new LongRange(0, 100)));
            availability.put(healthy, List.of(new LongRange(0, 100)));

            Optional<NodeSelection> result = strategy.select(0, 100, availability);

            assertTrue(result.isPresent());
            // Verify by reference that we got the healthy node, not just any node
            assertEquals(healthy, result.get().nodeConfig());
        }
    }

    @Nested
    @DisplayName("Earliest block selection")
    class EarliestBlockTests {

        @Test
        @DisplayName("should prefer node that can serve earlier blocks")
        void shouldPreferNodeWithEarlierBlocks() {
            BackfillSourceConfig laterNode = createNode("later", 8080, 1);
            BackfillSourceConfig earlierNode = createNode("earlier", 8081, 1);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            // laterNode can only serve from block 50
            availability.put(laterNode, List.of(new LongRange(50, 100)));
            // earlierNode can serve from block 10
            availability.put(earlierNode, List.of(new LongRange(10, 100)));

            Optional<NodeSelection> result = strategy.select(0, 100, availability);

            assertTrue(result.isPresent());
            assertEquals(earlierNode, result.get().nodeConfig());
            assertEquals(10, result.get().startBlock());
        }
    }

    @Nested
    @DisplayName("All nodes in backoff")
    class AllNodesBackoffTests {

        @Test
        @DisplayName("should return empty when all candidate nodes are in backoff")
        void shouldReturnEmptyWhenAllNodesInBackoff() {
            BackfillSourceConfig node1 = createNode("node1", 8080, 1);
            BackfillSourceConfig node2 = createNode("node2", 8081, 1);
            healthProvider.setInBackoff(node1, true);
            healthProvider.setInBackoff(node2, true);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(node1, List.of(new LongRange(0, 100)));
            availability.put(node2, List.of(new LongRange(0, 100)));

            Optional<NodeSelection> result = strategy.select(0, 100, availability);

            assertTrue(result.isEmpty());
        }
    }

    /**
     * Test implementation of NodeHealthProvider for controlled testing.
     */
    private static class TestHealthProvider implements PriorityHealthBasedStrategy.NodeHealthProvider {
        private final Map<BackfillSourceConfig, Boolean> backoffStatus = new HashMap<>();
        private final Map<BackfillSourceConfig, Double> healthScores = new HashMap<>();

        void setInBackoff(BackfillSourceConfig node, boolean inBackoff) {
            backoffStatus.put(node, inBackoff);
        }

        void setHealthScore(BackfillSourceConfig node, double score) {
            healthScores.put(node, score);
        }

        @Override
        public boolean isInBackoff(BackfillSourceConfig node) {
            return backoffStatus.getOrDefault(node, false);
        }

        @Override
        public double healthScore(BackfillSourceConfig node) {
            return healthScores.getOrDefault(node, 0.0);
        }
    }
}
