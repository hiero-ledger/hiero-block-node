// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.stream.Block;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
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

    private static BackfillSourceConfig node(String host, int port, int priority) {
        return BackfillSourceConfig.newBuilder()
                .address(host)
                .port(port)
                .priority(priority)
                .build();
    }

    private static BlockUnparsed createTestBlock(long blockNumber) {
        Block block = new Block(Arrays.asList(SimpleTestBlockItemBuilder.createSimpleBlockWithNumber(blockNumber)));
        return BlockUtils.toBlockUnparsed(block);
    }

    private static BackfillFetcher newClient(BackfillSourceConfig... nodes) throws Exception {
        final BackfillSource source =
                BackfillSource.newBuilder().nodes(List.of(nodes)).build();
        return new BackfillFetcher(source, createTestConfig(1, 0, 0, 0, 300_000L, 1000.0), createMockMetricsHolder());
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

    private static BackfillPlugin.MetricsHolder createMockMetricsHolder() {
        return new BackfillPlugin.MetricsHolder(
                mock(Counter.class), // backfillGapsDetected
                mock(Counter.class), // backfillFetchedBlocks
                mock(Counter.class), // backfillBlocksBackfilled
                mock(Counter.class), // backfillFetchErrors
                mock(Counter.class), // backfillRetries
                mock(LongGauge.class), // backfillStatus
                mock(LongGauge.class), // backfillPendingBlocksGauge
                mock(LongGauge.class)); // backfillInFlightGauge
    }

    @Nested
    @DisplayName("selectNextChunk")
    class SelectNextChunkTests {

        @Test
        @DisplayName("selects earliest start, breaks ties by priority, returns empty when no match")
        void selectNextChunkBehaviors() throws Exception {
            final BackfillSourceConfig lowPriority = node("localhost", 1, 1);
            final BackfillSourceConfig highPriority = node("localhost", 2, 2);
            final BackfillFetcher client = newClient(lowPriority, highPriority);

            // Selects earliest start even if higher priority
            var selection = client.selectNextChunk(
                    10,
                    30,
                    Map.of(lowPriority, List.of(new LongRange(12, 30)), highPriority, List.of(new LongRange(10, 20))));
            assertTrue(selection.isPresent());
            assertEquals(highPriority, selection.get().nodeConfig());
            assertEquals(10, selection.get().startBlock());

            // Breaks ties by lower priority
            selection = client.selectNextChunk(
                    15,
                    30,
                    Map.of(lowPriority, List.of(new LongRange(15, 25)), highPriority, List.of(new LongRange(15, 20))));
            assertTrue(selection.isPresent());
            assertEquals(lowPriority, selection.get().nodeConfig());

            // Returns empty when no range covers start
            selection = client.selectNextChunk(25, 30, Map.of(lowPriority, List.of(new LongRange(10, 20))));
            assertTrue(selection.isEmpty());

            // Returns empty for empty availability
            selection = client.selectNextChunk(10, 30, Collections.emptyMap());
            assertTrue(selection.isEmpty());
        }
    }

    @Nested
    @DisplayName("fetchBlocksFromNode")
    class FetchBlocksFromNodeTests {

        @Test
        @Timeout(value = 10, unit = TimeUnit.SECONDS)
        @DisplayName("returns blocks on success, retries on failure, returns empty on mismatch")
        void fetchBehaviors() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);
            final BackfillPlugin.MetricsHolder metrics = createMockMetricsHolder();

            // Success case
            var successClient = mockClientReturning(List.of(createTestBlock(0L), createTestBlock(1L)));
            var fetcher = createFetcherWithClient(nodeConfig, 3, metrics, successClient);
            assertEquals(
                    2,
                    fetcher.fetchBlocksFromNode(nodeConfig, new LongRange(0, 1)).size());
            verify(metrics.backfillRetries(), never()).increment();

            // Mismatch case - returns fewer blocks than expected
            var mismatchClient = mockClientReturning(List.of(createTestBlock(0L)));
            fetcher = createFetcherWithClient(nodeConfig, 1, metrics, mismatchClient);
            assertTrue(
                    fetcher.fetchBlocksFromNode(nodeConfig, new LongRange(0, 1)).isEmpty());

            // Failure with retry case
            var failingClient = mockClientThrowing(new RuntimeException("fail"));
            fetcher = createFetcherWithClient(nodeConfig, 2, metrics, failingClient);
            assertTrue(
                    fetcher.fetchBlocksFromNode(nodeConfig, new LongRange(0, 1)).isEmpty());
            verify(metrics.backfillRetries(), times(1)).increment();
        }

        private BlockNodeClient mockClientReturning(List<BlockUnparsed> blocks) throws Exception {
            var subscribeClient = mock(BlockStreamSubscribeUnparsedClient.class);
            when(subscribeClient.getBatchOfBlocks(any(Long.class), any(Long.class)))
                    .thenReturn(blocks);
            var client = mock(BlockNodeClient.class);
            when(client.getBlockstreamSubscribeUnparsedClient()).thenReturn(subscribeClient);
            return client;
        }

        private BlockNodeClient mockClientThrowing(Exception e) throws Exception {
            var subscribeClient = mock(BlockStreamSubscribeUnparsedClient.class);
            when(subscribeClient.getBatchOfBlocks(any(Long.class), any(Long.class)))
                    .thenThrow(e);
            var client = mock(BlockNodeClient.class);
            when(client.getBlockstreamSubscribeUnparsedClient()).thenReturn(subscribeClient);
            return client;
        }
    }

    @Nested
    @DisplayName("getNewAvailableRange")
    class GetNewAvailableRangeTests {

        @Test
        @DisplayName("returns null when unreachable, returns range when reachable, null when ahead of peers")
        void getNewAvailableRangeBehaviors() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);

            // Unreachable node
            var unreachable = mock(BlockNodeClient.class);
            when(unreachable.isNodeReachable()).thenReturn(false);
            var fetcher = createFetcherWithClient(nodeConfig, 1, createMockMetricsHolder(), unreachable);
            assertNull(fetcher.getNewAvailableRange(0L));

            // Reachable node with range
            var reachable = mockReachableClientWithStatus(0L, 100L);
            fetcher = createFetcherWithClient(nodeConfig, 1, createMockMetricsHolder(), reachable);
            var range = fetcher.getNewAvailableRange(10L);
            assertNotNull(range);
            assertEquals(11L, range.start());
            assertEquals(100L, range.end());

            // Already ahead of peers
            range = fetcher.getNewAvailableRange(100L);
            assertNull(range);
        }

        private BlockNodeClient mockReachableClientWithStatus(long first, long last) {
            var serviceClient = mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(serviceClient.serverStatus(any()))
                    .thenReturn(ServerStatusResponse.newBuilder()
                            .firstAvailableBlock(first)
                            .lastAvailableBlock(last)
                            .build());
            var client = mock(BlockNodeClient.class);
            when(client.isNodeReachable()).thenReturn(true);
            when(client.getBlockNodeServiceClient()).thenReturn(serviceClient);
            return client;
        }

        @Test
        @DisplayName("returns null when serverStatus throws exception for all nodes")
        void shouldReturnNullWhenServerStatusThrowsForAllNodes() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);

            // Mock a reachable client that throws on serverStatus()
            var serviceClient = mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(serviceClient.serverStatus(any())).thenThrow(new RuntimeException("Connection timeout"));
            var client = mock(BlockNodeClient.class);
            when(client.isNodeReachable()).thenReturn(true);
            when(client.getBlockNodeServiceClient()).thenReturn(serviceClient);

            var fetcher = createFetcherWithClient(nodeConfig, 1, createMockMetricsHolder(), client);

            // Should return null when all nodes fail (doesn't crash)
            assertNull(fetcher.getNewAvailableRange(10L));
        }

        @Test
        @DisplayName("returns range from healthy nodes when some nodes timeout on serverStatus")
        void shouldReturnRangeFromHealthyNodesWhenSomeTimeout() throws Exception {
            final BackfillSourceConfig failingNode = node("localhost", 1, 1);
            final BackfillSourceConfig healthyNode = node("localhost", 2, 2);

            // Failing node throws on serverStatus
            var failingServiceClient = mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(failingServiceClient.serverStatus(any())).thenThrow(new RuntimeException("Connection timeout"));
            var failingClient = mock(BlockNodeClient.class);
            when(failingClient.isNodeReachable()).thenReturn(true);
            when(failingClient.getBlockNodeServiceClient()).thenReturn(failingServiceClient);

            // Healthy node returns valid status
            var healthyServiceClient = mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(healthyServiceClient.serverStatus(any()))
                    .thenReturn(ServerStatusResponse.newBuilder()
                            .firstAvailableBlock(0L)
                            .lastAvailableBlock(100L)
                            .build());
            var healthyClient = mock(BlockNodeClient.class);
            when(healthyClient.isNodeReachable()).thenReturn(true);
            when(healthyClient.getBlockNodeServiceClient()).thenReturn(healthyServiceClient);

            // Create fetcher that returns different clients for different nodes
            final BackfillSource source = createSource(failingNode, healthyNode);
            final BackfillConfiguration config = createTestConfig(1, 100, 1000, 1000, 300_000L, 1000.0);
            var fetcher = new BackfillFetcher(source, config, createMockMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig node) {
                    return node.equals(failingNode) ? failingClient : healthyClient;
                }
            };

            // Should return range from healthy node
            var range = fetcher.getNewAvailableRange(10L);
            assertNotNull(range);
            assertEquals(11L, range.start());
            assertEquals(100L, range.end());
        }
    }

    @Nested
    @DisplayName("getAvailabilityForRange")
    class GetAvailabilityForRangeTests {

        @Test
        @DisplayName("returns intersection for reachable nodes, empty when no overlap or unreachable")
        void getAvailabilityBehaviors() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);
            final BackfillSource source = createSource(nodeConfig);
            final BackfillConfiguration config = createTestConfig(1, 100, 1000, 1000, 300_000L, 1000.0);

            // Reachable with overlap - use custom fetcher to control resolveAvailableRanges
            var reachable = mock(BlockNodeClient.class);
            when(reachable.isNodeReachable()).thenReturn(true);

            var fetcher = new BackfillFetcher(source, config, createMockMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig ignored) {
                    return reachable;
                }

                @Override
                protected List<LongRange> resolveAvailableRanges(BlockNodeClient node) {
                    return List.of(new LongRange(0, 100));
                }
            };

            var availability = fetcher.getAvailabilityForRange(new LongRange(10, 50));
            assertFalse(availability.isEmpty());
            assertEquals(new LongRange(10, 50), availability.get(nodeConfig).get(0));

            // No overlap
            availability = fetcher.getAvailabilityForRange(new LongRange(200, 300));
            assertTrue(availability.isEmpty());

            // Unreachable
            var unreachable = mock(BlockNodeClient.class);
            when(unreachable.isNodeReachable()).thenReturn(false);
            var unreachableFetcher = createFetcherWithClient(nodeConfig, 1, createMockMetricsHolder(), unreachable);
            availability = unreachableFetcher.getAvailabilityForRange(new LongRange(0, 100));
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
            var fetcher = newClient(nodeConfig);
            assertFalse(fetcher.isInBackoff(nodeConfig));
            assertEquals(0.0, fetcher.healthScore(nodeConfig));

            // After failure: in backoff, health score increases
            var failingClient = mock(BlockNodeClient.class);
            var subscribeClient = mock(BlockStreamSubscribeUnparsedClient.class);
            when(subscribeClient.getBatchOfBlocks(any(Long.class), any(Long.class)))
                    .thenThrow(new RuntimeException("fail"));
            when(failingClient.getBlockstreamSubscribeUnparsedClient()).thenReturn(subscribeClient);

            double healthPenalty = 1000.0;
            final BackfillSource source = createSource(nodeConfig);
            final BackfillConfiguration config = createTestConfig(1, 10000, 1, 1, 300_000L, healthPenalty);
            final BackfillPlugin.MetricsHolder metrics = createMockMetricsHolder();
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
        @DisplayName("merges overlapping/contiguous ranges, keeps disjoint separate")
        void mergeBehaviors() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);
            final BackfillSource source = createSource(nodeConfig);
            final BackfillConfiguration config = createTestConfig(1, 100, 1000, 1000, 300_000L, 1000.0);

            var reachable = mock(BlockNodeClient.class);
            when(reachable.isNodeReachable()).thenReturn(true);

            // Overlapping ranges merge to single range
            var fetcher1 = new BackfillFetcher(source, config, createMockMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig ignored) {
                    return reachable;
                }

                @Override
                protected List<LongRange> resolveAvailableRanges(BlockNodeClient node) {
                    return List.of(new LongRange(0, 10), new LongRange(5, 15), new LongRange(16, 20));
                }
            };

            var availability = fetcher1.getAvailabilityForRange(new LongRange(0, 20));
            assertEquals(1, availability.get(nodeConfig).size());
            assertEquals(new LongRange(0, 20), availability.get(nodeConfig).get(0));

            // Disjoint ranges stay separate
            var fetcher2 = new BackfillFetcher(source, config, createMockMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig ignored) {
                    return reachable;
                }

                @Override
                protected List<LongRange> resolveAvailableRanges(BlockNodeClient node) {
                    return List.of(new LongRange(0, 10), new LongRange(50, 60));
                }
            };

            availability = fetcher2.getAvailabilityForRange(new LongRange(0, 60));
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

            BackfillFetcher fetcher = new BackfillFetcher(source, config, createMockMetricsHolder()) {
                @Override
                protected BlockNodeClient getNodeClient(BackfillSourceConfig node) {
                    return nodeClientMap.computeIfAbsent(node, n -> createToggleableMockClient(shouldFail));
                }
            };

            // Step 1: Initial call - client gets cached
            assertNotNull(fetcher.getNewAvailableRange(0L));
            assertNotNull(fetcher.nodeClientMap.get(nodeConfig), "Client should be cached");

            // Step 2: Failure - markFailure() evicts the client
            shouldFail.set(true);
            fetcher.getNewAvailableRange(0L);
            assertTrue(fetcher.nodeClientMap.isEmpty(), "Client should be evicted after failure");
            assertTrue(fetcher.isInBackoff(nodeConfig));

            // Step 3: After backoff expires, fresh client is created
            Thread.sleep(15);
            shouldFail.set(false);
            assertNotNull(fetcher.getNewAvailableRange(0L));
            assertNotNull(fetcher.nodeClientMap.get(nodeConfig), "New client should be cached");
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
