// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.swirlds.metrics.api.Metrics;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.backfill.client.BackfillSource;
import org.hiero.block.node.backfill.client.BackfillSourceConfig;
import org.hiero.block.node.backfill.client.BlockNodeClient;
import org.hiero.block.node.backfill.client.BlockStreamSubscribeUnparsedClient;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link BackfillFetcher}.
 */
@Timeout(value = 5, unit = TimeUnit.SECONDS)
class BackfillFetcherTest {

    private static Metrics testMetrics;

    private static BackfillSourceConfig node(String host, int port, int priority) {
        return BackfillSourceConfig.newBuilder()
                .address(host)
                .port(port)
                .priority(priority)
                .build();
    }

    private static BlockUnparsed createTestBlock(long blockNumber) {
        return BlockUnparsed.newBuilder()
                .blockItems(Arrays.asList(SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(blockNumber)))
                .build();
    }

    private static BackfillFetcher newClient(BackfillSourceConfig... nodes) throws Exception {
        final BackfillSource source =
                BackfillSource.newBuilder().nodes(List.of(nodes)).build();
        return new BackfillFetcher(source, createTestConfig(1, 0, 0, 0, 300_000L, 1000.0), createTestMetricsHolder());
    }

    private static BackfillConfiguration createTestConfig(
            int maxRetries,
            int initialRetryDelay,
            int grpcOverallTimeout,
            int perBlockProcessingTimeout,
            long maxBackoffMs,
            double healthPenaltyPerFailure) {
        return new BackfillConfiguration(
                0L, // startBlock
                -1L, // endBlock
                "", // blockNodeSourcesPath
                60_000, // scanInterval
                maxRetries,
                initialRetryDelay,
                10, // fetchBatchSize
                1_000, // delayBetweenBatches
                15_000, // initialDelay
                perBlockProcessingTimeout,
                grpcOverallTimeout,
                false, // enableTLS
                false, // greedy
                20, // historicalQueueCapacity
                10, // liveTailQueueCapacity
                healthPenaltyPerFailure,
                maxBackoffMs);
    }

    private static BackfillPlugin.MetricsHolder createTestMetricsHolder() {
        if (testMetrics == null) {
            testMetrics = TestUtils.createMetrics();
        }
        return BackfillPlugin.MetricsHolder.createMetrics(testMetrics);
    }

    @Nested
    @DisplayName("selectNextChunk")
    class SelectNextChunkTests {

        @Test
        @DisplayName("selects node with earliest start block")
        void selectsEarliestStart() throws Exception {
            final BackfillSourceConfig lowPriority = node("localhost", 1, 1);
            final BackfillSourceConfig highPriority = node("localhost", 2, 2);
            final BackfillFetcher client = newClient(lowPriority, highPriority);

            Optional<NodeSelectionStrategy.NodeSelection> selection = client.selectNextChunk(
                    10,
                    30,
                    Map.of(lowPriority, List.of(new LongRange(12, 30)), highPriority, List.of(new LongRange(10, 20))));
            assertTrue(selection.isPresent());
            assertEquals(highPriority, selection.get().nodeConfig());
            assertEquals(10, selection.get().startBlock());
        }

        @Test
        @DisplayName("breaks ties by lower priority value")
        void breaksTiesByPriority() throws Exception {
            final BackfillSourceConfig lowPriority = node("localhost", 1, 1);
            final BackfillSourceConfig highPriority = node("localhost", 2, 2);
            final BackfillFetcher client = newClient(lowPriority, highPriority);

            Optional<NodeSelectionStrategy.NodeSelection> selection = client.selectNextChunk(
                    15,
                    30,
                    Map.of(lowPriority, List.of(new LongRange(15, 25)), highPriority, List.of(new LongRange(15, 20))));
            assertTrue(selection.isPresent());
            assertEquals(lowPriority, selection.get().nodeConfig());
        }

        @Test
        @DisplayName("returns empty when no range covers start")
        void returnsEmptyWhenNoRangeCoversStart() throws Exception {
            final BackfillSourceConfig lowPriority = node("localhost", 1, 1);
            final BackfillFetcher client = newClient(lowPriority);

            Optional<NodeSelectionStrategy.NodeSelection> selection =
                    client.selectNextChunk(25, 30, Map.of(lowPriority, List.of(new LongRange(10, 20))));
            assertTrue(selection.isEmpty());
        }

        @Test
        @DisplayName("returns empty for empty availability")
        void returnsEmptyForEmptyAvailability() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);
            final BackfillFetcher client = newClient(nodeConfig);

            Optional<NodeSelectionStrategy.NodeSelection> selection =
                    client.selectNextChunk(10, 30, Collections.emptyMap());
            assertTrue(selection.isEmpty());
        }
    }

    @Nested
    @DisplayName("fetchBlocksFromNode")
    class FetchBlocksFromNodeTests {

        @Test
        @Timeout(value = 10, unit = TimeUnit.SECONDS)
        @DisplayName("returns blocks on success")
        void returnsBlocksOnSuccess() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);
            final BackfillPlugin.MetricsHolder metrics = createTestMetricsHolder();

            long retriesBefore = metrics.backfillRetries().get();
            BlockNodeClient successClient = mockClientReturning(List.of(createTestBlock(0L), createTestBlock(1L)));
            BackfillFetcher fetcher = createFetcherWithClient(nodeConfig, 3, metrics, successClient);
            assertEquals(
                    2,
                    fetcher.fetchBlocksFromNode(nodeConfig, new LongRange(0, 1)).size());
            assertEquals(retriesBefore, metrics.backfillRetries().get());
        }

        @Test
        @Timeout(value = 10, unit = TimeUnit.SECONDS)
        @DisplayName("returns empty on block count mismatch")
        void returnsEmptyOnMismatch() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);
            final BackfillPlugin.MetricsHolder metrics = createTestMetricsHolder();

            BlockNodeClient mismatchClient = mockClientReturning(List.of(createTestBlock(0L)));
            BackfillFetcher fetcher = createFetcherWithClient(nodeConfig, 1, metrics, mismatchClient);
            assertTrue(
                    fetcher.fetchBlocksFromNode(nodeConfig, new LongRange(0, 1)).isEmpty());
        }

        @Test
        @Timeout(value = 10, unit = TimeUnit.SECONDS)
        @DisplayName("retries on failure and increments retry metric")
        void retriesOnFailure() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);
            final BackfillPlugin.MetricsHolder metrics = createTestMetricsHolder();

            long retriesBefore = metrics.backfillRetries().get();
            BlockNodeClient failingClient = mockClientThrowing(new RuntimeException("fail"));
            BackfillFetcher fetcher = createFetcherWithClient(nodeConfig, 2, metrics, failingClient);
            assertTrue(
                    fetcher.fetchBlocksFromNode(nodeConfig, new LongRange(0, 1)).isEmpty());
            assertEquals(retriesBefore + 1, metrics.backfillRetries().get());
        }

        private BlockNodeClient mockClientReturning(List<BlockUnparsed> blocks) throws Exception {
            BlockStreamSubscribeUnparsedClient subscribeClient = mock(BlockStreamSubscribeUnparsedClient.class);
            when(subscribeClient.getBatchOfBlocks(any(Long.class), any(Long.class)))
                    .thenReturn(blocks);
            BlockNodeClient client = mock(BlockNodeClient.class);
            when(client.getBlockstreamSubscribeUnparsedClient()).thenReturn(subscribeClient);
            return client;
        }

        private BlockNodeClient mockClientThrowing(Exception e) throws Exception {
            BlockStreamSubscribeUnparsedClient subscribeClient = mock(BlockStreamSubscribeUnparsedClient.class);
            when(subscribeClient.getBatchOfBlocks(any(Long.class), any(Long.class)))
                    .thenThrow(e);
            BlockNodeClient client = mock(BlockNodeClient.class);
            when(client.getBlockstreamSubscribeUnparsedClient()).thenReturn(subscribeClient);
            return client;
        }
    }

    @Nested
    @DisplayName("getNewAvailableRange")
    class GetNewAvailableRangeTests {

        @Test
        @DisplayName("returns null when node is unreachable")
        void returnsNullWhenUnreachable() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);

            BlockNodeClient unreachable = mock(BlockNodeClient.class);
            when(unreachable.isNodeReachable()).thenReturn(false);
            BackfillFetcher fetcher = createFetcherWithClient(nodeConfig, 1, createTestMetricsHolder(), unreachable);
            assertNull(fetcher.getNewAvailableRange(0L));
        }

        @Test
        @DisplayName("returns range when node is reachable")
        void returnsRangeWhenReachable() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);

            BlockNodeClient reachable = mockReachableClientWithStatus(0L, 100L);
            BackfillFetcher fetcher = createFetcherWithClient(nodeConfig, 1, createTestMetricsHolder(), reachable);
            LongRange range = fetcher.getNewAvailableRange(10L);
            assertNotNull(range);
            assertEquals(11L, range.start());
            assertEquals(100L, range.end());
        }

        @Test
        @DisplayName("returns null when already ahead of peers")
        void returnsNullWhenAheadOfPeers() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);

            BlockNodeClient reachable = mockReachableClientWithStatus(0L, 100L);
            BackfillFetcher fetcher = createFetcherWithClient(nodeConfig, 1, createTestMetricsHolder(), reachable);
            LongRange range = fetcher.getNewAvailableRange(100L);
            assertNull(range);
        }

        private BlockNodeClient mockReachableClientWithStatus(long first, long last) {
            BlockNodeServiceInterface.BlockNodeServiceClient serviceClient =
                    mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(serviceClient.serverStatus(any()))
                    .thenReturn(ServerStatusResponse.newBuilder()
                            .firstAvailableBlock(first)
                            .lastAvailableBlock(last)
                            .build());
            BlockNodeClient client = mock(BlockNodeClient.class);
            when(client.isNodeReachable()).thenReturn(true);
            when(client.getBlockNodeServiceClient()).thenReturn(serviceClient);
            return client;
        }

        @Test
        @DisplayName("returns null when serverStatus throws exception for all nodes")
        void shouldReturnNullWhenServerStatusThrowsForAllNodes() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);

            // Mock a reachable client that throws on serverStatus()
            BlockNodeServiceInterface.BlockNodeServiceClient serviceClient =
                    mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(serviceClient.serverStatus(any())).thenThrow(new RuntimeException("Connection timeout"));
            BlockNodeClient client = mock(BlockNodeClient.class);
            when(client.isNodeReachable()).thenReturn(true);
            when(client.getBlockNodeServiceClient()).thenReturn(serviceClient);

            BackfillFetcher fetcher = createFetcherWithClient(nodeConfig, 1, createTestMetricsHolder(), client);

            // Should return null when all nodes fail (doesn't crash)
            assertNull(fetcher.getNewAvailableRange(10L));
        }

        @Test
        @DisplayName("returns range from healthy nodes when some nodes timeout on serverStatus")
        void shouldReturnRangeFromHealthyNodesWhenSomeTimeout() throws Exception {
            final BackfillSourceConfig failingNode = node("localhost", 1, 1);
            final BackfillSourceConfig healthyNode = node("localhost", 2, 2);

            // Failing node throws on serverStatus
            BlockNodeServiceInterface.BlockNodeServiceClient failingServiceClient =
                    mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(failingServiceClient.serverStatus(any())).thenThrow(new RuntimeException("Connection timeout"));
            BlockNodeClient failingClient = mock(BlockNodeClient.class);
            when(failingClient.isNodeReachable()).thenReturn(true);
            when(failingClient.getBlockNodeServiceClient()).thenReturn(failingServiceClient);

            // Healthy node returns valid status
            BlockNodeServiceInterface.BlockNodeServiceClient healthyServiceClient =
                    mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(healthyServiceClient.serverStatus(any()))
                    .thenReturn(ServerStatusResponse.newBuilder()
                            .firstAvailableBlock(0L)
                            .lastAvailableBlock(100L)
                            .build());
            BlockNodeClient healthyClient = mock(BlockNodeClient.class);
            when(healthyClient.isNodeReachable()).thenReturn(true);
            when(healthyClient.getBlockNodeServiceClient()).thenReturn(healthyServiceClient);

            // Create fetcher that returns different clients for different nodes
            final BackfillSource source = createSource(failingNode, healthyNode);
            final BackfillConfiguration config = createTestConfig(1, 100, 1000, 1000, 300_000L, 1000.0);
            BackfillFetcher fetcher = new BackfillFetcher(source, config, createTestMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig node) {
                    return node.equals(failingNode) ? failingClient : healthyClient;
                }
            };

            // Should return range from healthy node
            LongRange range = fetcher.getNewAvailableRange(10L);
            assertNotNull(range);
            assertEquals(11L, range.start());
            assertEquals(100L, range.end());
        }
    }

    @Nested
    @DisplayName("getAvailabilityForRange")
    class GetAvailabilityForRangeTests {

        @Test
        @DisplayName("returns intersection for reachable nodes with overlap")
        void returnsIntersectionForReachableNodes() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);
            final BackfillSource source = createSource(nodeConfig);
            final BackfillConfiguration config = createTestConfig(1, 100, 1000, 1000, 300_000L, 1000.0);

            BlockNodeClient reachable = mock(BlockNodeClient.class);
            when(reachable.isNodeReachable()).thenReturn(true);

            BackfillFetcher fetcher = new BackfillFetcher(source, config, createTestMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig ignored) {
                    return reachable;
                }

                @Override
                protected List<LongRange> resolveAvailableRanges(BlockNodeClient node) {
                    return List.of(new LongRange(0, 100));
                }
            };

            Map<BackfillSourceConfig, List<LongRange>> availability =
                    fetcher.getAvailabilityForRange(new LongRange(10, 50));
            assertFalse(availability.isEmpty());
            assertEquals(new LongRange(10, 50), availability.get(nodeConfig).get(0));
        }

        @Test
        @DisplayName("returns empty when no overlap with requested range")
        void returnsEmptyWhenNoOverlap() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);
            final BackfillSource source = createSource(nodeConfig);
            final BackfillConfiguration config = createTestConfig(1, 100, 1000, 1000, 300_000L, 1000.0);

            BlockNodeClient reachable = mock(BlockNodeClient.class);
            when(reachable.isNodeReachable()).thenReturn(true);

            BackfillFetcher fetcher = new BackfillFetcher(source, config, createTestMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig ignored) {
                    return reachable;
                }

                @Override
                protected List<LongRange> resolveAvailableRanges(BlockNodeClient node) {
                    return List.of(new LongRange(0, 100));
                }
            };

            Map<BackfillSourceConfig, List<LongRange>> availability =
                    fetcher.getAvailabilityForRange(new LongRange(200, 300));
            assertTrue(availability.isEmpty());
        }

        @Test
        @DisplayName("returns empty when node is unreachable")
        void returnsEmptyWhenUnreachable() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);

            BlockNodeClient unreachable = mock(BlockNodeClient.class);
            when(unreachable.isNodeReachable()).thenReturn(false);
            BackfillFetcher unreachableFetcher =
                    createFetcherWithClient(nodeConfig, 1, createTestMetricsHolder(), unreachable);
            Map<BackfillSourceConfig, List<LongRange>> availability =
                    unreachableFetcher.getAvailabilityForRange(new LongRange(0, 100));
            assertTrue(availability.isEmpty());
        }
    }

    @Nested
    @DisplayName("Health and Backoff")
    class HealthAndBackoffTests {

        @Test
        @DisplayName("tracks health score and backoff after failures")
        void healthAndBackoffBehaviors() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);

            // Initially no backoff, zero health score
            BackfillFetcher fetcher = newClient(nodeConfig);
            assertFalse(fetcher.isInBackoff(nodeConfig));
            assertEquals(0.0, fetcher.healthScore(nodeConfig));

            // After failure: in backoff, health score increases
            BlockNodeClient failingClient = mock(BlockNodeClient.class);
            BlockStreamSubscribeUnparsedClient subscribeClient = mock(BlockStreamSubscribeUnparsedClient.class);
            when(subscribeClient.getBatchOfBlocks(any(Long.class), any(Long.class)))
                    .thenThrow(new RuntimeException("fail"));
            when(failingClient.getBlockstreamSubscribeUnparsedClient()).thenReturn(subscribeClient);

            double healthPenalty = 1000.0;
            final BackfillSource source = createSource(nodeConfig);
            final BackfillConfiguration config = createTestConfig(1, 10000, 1, 1, 300_000L, healthPenalty);
            final BackfillPlugin.MetricsHolder metrics = createTestMetricsHolder();
            fetcher = new BackfillFetcher(source, config, metrics) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig ignored) {
                    return failingClient;
                }
            };

            fetcher.fetchBlocksFromNode(nodeConfig, new LongRange(0, 0));
            assertTrue(fetcher.isInBackoff(nodeConfig));
            assertTrue(fetcher.healthScore(nodeConfig) >= healthPenalty);
        }
    }

    @Nested
    @DisplayName("mergeContiguousRanges (via getAvailabilityForRange)")
    class MergeRangesTests {

        @Test
        @DisplayName("merges overlapping and contiguous ranges into single range")
        void mergesOverlappingRanges() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);
            final BackfillSource source = createSource(nodeConfig);
            final BackfillConfiguration config = createTestConfig(1, 100, 1000, 1000, 300_000L, 1000.0);

            BlockNodeClient reachable = mock(BlockNodeClient.class);
            when(reachable.isNodeReachable()).thenReturn(true);

            BackfillFetcher fetcher = new BackfillFetcher(source, config, createTestMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig ignored) {
                    return reachable;
                }

                @Override
                protected List<LongRange> resolveAvailableRanges(BlockNodeClient node) {
                    return List.of(new LongRange(0, 10), new LongRange(5, 15), new LongRange(16, 20));
                }
            };

            Map<BackfillSourceConfig, List<LongRange>> availability =
                    fetcher.getAvailabilityForRange(new LongRange(0, 20));
            assertEquals(1, availability.get(nodeConfig).size());
            assertEquals(new LongRange(0, 20), availability.get(nodeConfig).get(0));
        }

        @Test
        @DisplayName("keeps disjoint ranges separate")
        void keepsDisjointRangesSeparate() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);
            final BackfillSource source = createSource(nodeConfig);
            final BackfillConfiguration config = createTestConfig(1, 100, 1000, 1000, 300_000L, 1000.0);

            BlockNodeClient reachable = mock(BlockNodeClient.class);
            when(reachable.isNodeReachable()).thenReturn(true);

            BackfillFetcher fetcher = new BackfillFetcher(source, config, createTestMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig ignored) {
                    return reachable;
                }

                @Override
                protected List<LongRange> resolveAvailableRanges(BlockNodeClient node) {
                    return List.of(new LongRange(0, 10), new LongRange(50, 60));
                }
            };

            Map<BackfillSourceConfig, List<LongRange>> availability =
                    fetcher.getAvailabilityForRange(new LongRange(0, 60));
            assertEquals(2, availability.get(nodeConfig).size());
        }
    }

    @Nested
    @DisplayName("Stale Connection Recovery")
    class StaleConnectionRecoveryTests {

        /**
         * Verifies that markFailure() evicts the cached client so a fresh one
         * is created after the backoff period expires.
         */
        @Test
        @Timeout(value = 10, unit = TimeUnit.SECONDS)
        @DisplayName("should evict client on failure and create fresh one after backoff")
        void shouldEvictClientOnFailureAndCreateFreshAfterBackoff() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);
            final BackfillSource source = createSource(nodeConfig);
            final BackfillConfiguration config = createTestConfig(1, 10, 100, 100, 50L, 100.0);

            AtomicBoolean shouldFail = new AtomicBoolean(false);

            BackfillFetcher fetcher = new BackfillFetcher(source, config, createTestMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig node) {
                    return nodeClientMap.computeIfAbsent(node, n -> createToggleableMockClient(shouldFail));
                }
            };

            // Step 1: Initial call - client gets cached
            assertNotNull(fetcher.getNewAvailableRange(0L));
            BlockNodeClient firstClient = fetcher.nodeClientMap.get(nodeConfig);
            assertNotNull(firstClient, "Client should be cached");

            // Step 2: Failure - markFailure() evicts the client
            shouldFail.set(true);
            fetcher.getNewAvailableRange(0L);
            assertTrue(fetcher.nodeClientMap.isEmpty(), "Client should be evicted after failure");
            assertTrue(fetcher.isInBackoff(nodeConfig));

            // Step 3: After backoff expires, fresh client is created
            Thread.sleep(15);
            shouldFail.set(false);
            assertNotNull(fetcher.getNewAvailableRange(0L));
            BlockNodeClient newClient = fetcher.nodeClientMap.get(nodeConfig);
            assertNotNull(newClient, "New client should be cached");
            assertNotSame(firstClient, newClient, "Should be a fresh client instance");
        }

        private BlockNodeClient createToggleableMockClient(AtomicBoolean shouldFail) {
            BlockNodeServiceInterface.BlockNodeServiceClient serviceClient =
                    mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(serviceClient.serverStatus(any())).thenAnswer(invocation -> {
                if (shouldFail.get()) {
                    throw new UncheckedIOException(new IOException("Socket closed"));
                }
                return ServerStatusResponse.newBuilder()
                        .firstAvailableBlock(0L)
                        .lastAvailableBlock(100L)
                        .build();
            });

            BlockNodeClient client = mock(BlockNodeClient.class);
            when(client.isNodeReachable()).thenReturn(true);
            when(client.getBlockNodeServiceClient()).thenReturn(serviceClient);
            return client;
        }
    }

    @Nested
    @DisplayName("Backoff Retry Behavior")
    class BackoffRetryBehaviorTests {

        @Test
        @DisplayName("getAvailabilityForRange should skip nodes in backoff")
        void shouldSkipNodesInBackoffForAvailability() throws Exception {
            BackfillSourceConfig node1 = node("localhost", 1, 1);
            BackfillSourceConfig node2 = node("localhost", 2, 1);
            BackfillSourceConfig node3 = node("localhost", 3, 1);

            BackfillSource source = createSource(node1, node2, node3);
            BackfillConfiguration config = createTestConfig(1, 5_000, 1000, 1000, 300_000L, 1000.0);

            AtomicInteger clientCreationCount = new AtomicInteger(0);

            BackfillFetcher fetcher = new BackfillFetcher(source, config, createTestMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig node) {
                    clientCreationCount.incrementAndGet();
                    BlockNodeClient client = mock(BlockNodeClient.class);
                    when(client.isNodeReachable()).thenReturn(true);
                    return client;
                }

                @Override
                protected List<LongRange> resolveAvailableRanges(BlockNodeClient node) {
                    return List.of(new LongRange(0, 100));
                }
            };

            // First call - all 3 nodes should be queried
            fetcher.getAvailabilityForRange(new LongRange(0, 50));
            assertEquals(3, clientCreationCount.get(), "All 3 nodes should be queried initially");

            // Now put node1 and node2 in backoff by marking failures
            fetcher.healthMap.compute(
                    node1, (n, h) -> new BackfillFetcher.SourceHealth(1, System.currentTimeMillis() + 60_000, 0, 0));
            fetcher.healthMap.compute(
                    node2, (n, h) -> new BackfillFetcher.SourceHealth(1, System.currentTimeMillis() + 60_000, 0, 0));

            // Reset counter
            clientCreationCount.set(0);

            // Second call - only node3 should be queried (node1, node2 in backoff)
            Map<BackfillSourceConfig, List<LongRange>> availability =
                    fetcher.getAvailabilityForRange(new LongRange(0, 50));
            assertEquals(1, clientCreationCount.get(), "Only 1 node should be queried when 2 are in backoff");
            assertTrue(availability.containsKey(node3), "node3 should be in availability");
            assertFalse(availability.containsKey(node1), "node1 should be skipped (in backoff)");
            assertFalse(availability.containsKey(node2), "node2 should be skipped (in backoff)");
        }

        @Test
        @DisplayName("should track connection attempts for multiple down sources")
        void shouldTrackConnectionAttemptsForMultipleDownSources() throws Exception {
            int numberOfNodes = 5;
            List<BackfillSourceConfig> nodes = new ArrayList<>();
            for (int i = 0; i < numberOfNodes; i++) {
                nodes.add(node("localhost", i + 1, 1));
            }

            BackfillSource source = BackfillSource.newBuilder().nodes(nodes).build();
            // Very short initial retry delay for testing
            BackfillConfiguration config = createTestConfig(1, 100, 1000, 1000, 300_000L, 1000.0);

            AtomicInteger connectionAttempts = new AtomicInteger(0);

            BackfillFetcher fetcher = new BackfillFetcher(source, config, createTestMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig node) {
                    connectionAttempts.incrementAndGet();
                    // All nodes fail to connect
                    BlockNodeClient client = mock(BlockNodeClient.class);
                    when(client.isNodeReachable()).thenReturn(true);
                    when(client.getBlockNodeServiceClient()).thenThrow(new RuntimeException("Connection refused"));
                    return client;
                }
            };

            // First scan - should attempt all 5 nodes
            fetcher.getNewAvailableRange(0L);
            assertEquals(numberOfNodes, connectionAttempts.get(), "Should attempt all nodes on first scan");

            // All nodes should now be in backoff
            for (BackfillSourceConfig node : nodes) {
                assertTrue(fetcher.isInBackoff(node), "Node " + node.port() + " should be in backoff after failure");
            }

            // Reset counter
            connectionAttempts.set(0);

            // Second scan immediately - all nodes are in backoff, so NONE should be attempted
            fetcher.getNewAvailableRange(0L);
            assertEquals(
                    0,
                    connectionAttempts.get(),
                    "getNewAvailableRange should skip all nodes in backoff - no excessive retries");
        }

        @Test
        @DisplayName("should apply exponential backoff on failure with max cap")
        void shouldApplyExponentialBackoffOnFailure() throws Exception {
            BackfillSourceConfig nodeConfig = node("localhost", 1, 1);
            BackfillSource source = createSource(nodeConfig);
            // Initial delay 100ms, max backoff 1000ms for fast testing
            long initialDelayMs = 100L;
            long maxBackoffMs = 1_000L;
            BackfillConfiguration config = createTestConfig(1, (int) initialDelayMs, 1000, 1000, maxBackoffMs, 1000.0);

            BackfillFetcher fetcher = new BackfillFetcher(source, config, createTestMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig node) {
                    BlockNodeClient client = mock(BlockNodeClient.class);
                    when(client.isNodeReachable()).thenReturn(true);
                    when(client.getBlockNodeServiceClient()).thenThrow(new RuntimeException("Connection refused"));
                    return client;
                }
            };

            // First failure - should set initial backoff
            fetcher.getNewAvailableRange(0L);
            BackfillFetcher.SourceHealth health1 = fetcher.healthMap.get(nodeConfig);
            assertNotNull(health1, "Health record should exist after first failure");
            assertEquals(1, health1.failures(), "Should have 1 failure");
            assertTrue(fetcher.isInBackoff(nodeConfig), "Should be in backoff after failure");

            // Simulate backoff expiring and second failure
            fetcher.healthMap.compute(
                    nodeConfig,
                    (n, h) -> new BackfillFetcher.SourceHealth(h.failures(), System.currentTimeMillis() - 1, 0, 0));
            assertFalse(fetcher.isInBackoff(nodeConfig), "Backoff should have expired");

            // Second failure
            fetcher.getNewAvailableRange(0L);
            BackfillFetcher.SourceHealth health2 = fetcher.healthMap.get(nodeConfig);
            assertEquals(2, health2.failures(), "Should have 2 failures");

            // Continue until we hit max backoff (simulate 10 failures by manually updating)
            // Expected progression: 100, 200, 400, 800, 1000 (capped)
            for (int i = 3; i <= 10; i++) {
                fetcher.healthMap.compute(
                        nodeConfig,
                        (n, h) -> new BackfillFetcher.SourceHealth(
                                h.failures(), System.currentTimeMillis() - 1, h.successes(), h.totalLatencyNanos()));
                fetcher.getNewAvailableRange(0L);
            }

            BackfillFetcher.SourceHealth finalHealth = fetcher.healthMap.get(nodeConfig);
            assertEquals(10, finalHealth.failures(), "Should have recorded 10 failures");

            // Verify backoff is capped at maxBackoffMs
            long now = System.currentTimeMillis();
            long actualBackoff = finalHealth.nextAllowedMillis() - now;
            assertTrue(
                    actualBackoff <= maxBackoffMs + 100, // 100ms tolerance for timing
                    "Backoff should be capped at maxBackoffMs, actual: " + actualBackoff);
        }

        @Test
        @DisplayName("getNewAvailableRange should respect backoff and skip nodes in backoff")
        void shouldRespectBackoffInGetNewAvailableRange() throws Exception {
            BackfillSourceConfig node1 = node("localhost", 1, 1);
            BackfillSourceConfig node2 = node("localhost", 2, 1);

            BackfillSource source = createSource(node1, node2);
            BackfillConfiguration config = createTestConfig(1, 60_000, 1000, 1000, 300_000L, 1000.0);

            AtomicInteger node1Attempts = new AtomicInteger(0);
            AtomicInteger node2Attempts = new AtomicInteger(0);

            BackfillFetcher fetcher = new BackfillFetcher(source, config, createTestMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig node) {
                    if (node.equals(node1)) {
                        node1Attempts.incrementAndGet();
                    } else {
                        node2Attempts.incrementAndGet();
                    }

                    BlockNodeClient client = mock(BlockNodeClient.class);
                    when(client.isNodeReachable()).thenReturn(true);
                    // node1 always fails, node2 succeeds
                    if (node.equals(node1)) {
                        when(client.getBlockNodeServiceClient()).thenThrow(new RuntimeException("Connection refused"));
                    } else {
                        BlockNodeServiceInterface.BlockNodeServiceClient serviceClient =
                                mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
                        when(serviceClient.serverStatus(any()))
                                .thenReturn(ServerStatusResponse.newBuilder()
                                        .firstAvailableBlock(0L)
                                        .lastAvailableBlock(100L)
                                        .build());
                        when(client.getBlockNodeServiceClient()).thenReturn(serviceClient);
                    }
                    return client;
                }
            };

            // First call
            LongRange range = fetcher.getNewAvailableRange(0L);
            assertNotNull(range, "Should get range from node2");
            assertEquals(1, node1Attempts.get(), "node1 should be attempted once");
            assertEquals(1, node2Attempts.get(), "node2 should be attempted once");
            assertTrue(fetcher.isInBackoff(node1), "node1 should be in backoff");

            // Second call - node1 is in backoff and should be SKIPPED
            range = fetcher.getNewAvailableRange(0L);
            assertNotNull(range, "Should still get range from node2");
            assertEquals(
                    1,
                    node1Attempts.get(),
                    "node1 should NOT be attempted again - getNewAvailableRange respects backoff");
            assertEquals(2, node2Attempts.get(), "node2 should be attempted again (not in backoff)");
        }

        @Test
        @DisplayName("both getAvailabilityForRange and getNewAvailableRange should respect backoff")
        void bothMethodsShouldRespectBackoff() throws Exception {
            BackfillSourceConfig node1 = node("localhost", 1, 1);

            BackfillSource source = createSource(node1);
            BackfillConfiguration config = createTestConfig(1, 60_000, 1000, 1000, 300_000L, 1000.0);

            AtomicInteger getAvailabilityAttempts = new AtomicInteger(0);
            AtomicInteger getNewRangeAttempts = new AtomicInteger(0);

            // Track attempts in getAvailabilityForRange
            BackfillFetcher availabilityFetcher = new BackfillFetcher(source, config, createTestMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig node) {
                    getAvailabilityAttempts.incrementAndGet();
                    BlockNodeClient client = mock(BlockNodeClient.class);
                    when(client.isNodeReachable()).thenReturn(true);
                    return client;
                }

                @Override
                protected List<LongRange> resolveAvailableRanges(BlockNodeClient node) {
                    return List.of(new LongRange(0, 100));
                }
            };

            // Put node in backoff
            availabilityFetcher.healthMap.compute(
                    node1, (n, h) -> new BackfillFetcher.SourceHealth(5, System.currentTimeMillis() + 300_000, 0, 0));

            // getAvailabilityForRange respects backoff - should NOT attempt
            availabilityFetcher.getAvailabilityForRange(new LongRange(0, 50));
            assertEquals(
                    0,
                    getAvailabilityAttempts.get(),
                    "getAvailabilityForRange should skip nodes in backoff - 0 attempts");

            // Track attempts in getNewAvailableRange
            BackfillFetcher newRangeFetcher = new BackfillFetcher(source, config, createTestMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig node) {
                    getNewRangeAttempts.incrementAndGet();
                    BlockNodeClient client = mock(BlockNodeClient.class);
                    when(client.isNodeReachable()).thenReturn(true);
                    BlockNodeServiceInterface.BlockNodeServiceClient serviceClient =
                            mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
                    when(serviceClient.serverStatus(any()))
                            .thenReturn(ServerStatusResponse.newBuilder()
                                    .firstAvailableBlock(0L)
                                    .lastAvailableBlock(100L)
                                    .build());
                    when(client.getBlockNodeServiceClient()).thenReturn(serviceClient);
                    return client;
                }
            };

            // Put node in backoff
            newRangeFetcher.healthMap.compute(
                    node1, (n, h) -> new BackfillFetcher.SourceHealth(5, System.currentTimeMillis() + 300_000, 0, 0));

            // getNewAvailableRange also respects backoff now - should NOT attempt
            newRangeFetcher.getNewAvailableRange(0L);
            assertEquals(
                    0,
                    getNewRangeAttempts.get(),
                    "getNewAvailableRange should also skip nodes in backoff - 0 attempts");
        }
    }

    // Helper methods
    private static BackfillSource createSource(BackfillSourceConfig... nodes) {
        return BackfillSource.newBuilder().nodes(List.of(nodes)).build();
    }

    private static BackfillFetcher createFetcherWithClient(
            BackfillSourceConfig nodeConfig,
            int maxRetries,
            BackfillPlugin.MetricsHolder metrics,
            BlockNodeClient client)
            throws Exception {
        final BackfillSource source = createSource(nodeConfig);
        final BackfillConfiguration config = createTestConfig(maxRetries, 1, 1000, 1000, 300_000L, 1000.0);
        return new BackfillFetcher(source, config, metrics) {
            @Override
            protected BlockNodeClient getNodeClient(BackfillSourceConfig ignored) {
                return client;
            }
        };
    }
}
