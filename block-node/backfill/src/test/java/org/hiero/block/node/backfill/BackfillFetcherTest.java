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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
        final Path tempFile = Files.createTempFile("bn-sources", ".json");
        Files.write(tempFile, BackfillSource.JSON.toBytes(source).toByteArray());
        return new BackfillFetcher(tempFile, 1, null, 0, 0, 0, false, 300_000L, 1000.0);
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
            final Counter retryCounter = mock(Counter.class);
            final Path tempFile = createTempSourceFile(nodeConfig);

            // Success case
            var successClient = mockClientReturning(List.of(createTestBlock(0L), createTestBlock(1L)));
            var fetcher = createFetcherWithClient(tempFile, 3, retryCounter, successClient);
            assertEquals(
                    2,
                    fetcher.fetchBlocksFromNode(nodeConfig, new LongRange(0, 1)).size());
            verify(retryCounter, never()).increment();

            // Mismatch case - returns fewer blocks than expected
            var mismatchClient = mockClientReturning(List.of(createTestBlock(0L)));
            fetcher = createFetcherWithClient(tempFile, 1, retryCounter, mismatchClient);
            assertTrue(
                    fetcher.fetchBlocksFromNode(nodeConfig, new LongRange(0, 1)).isEmpty());

            // Failure with retry case
            var failingClient = mockClientThrowing(new RuntimeException("fail"));
            fetcher = createFetcherWithClient(tempFile, 2, retryCounter, failingClient);
            assertTrue(
                    fetcher.fetchBlocksFromNode(nodeConfig, new LongRange(0, 1)).isEmpty());
            verify(retryCounter, times(1)).increment();
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
            final Path tempFile = createTempSourceFile(nodeConfig);

            // Unreachable node
            var unreachable = mock(BlockNodeClient.class);
            when(unreachable.isNodeReachable()).thenReturn(false);
            var fetcher = createFetcherWithClient(tempFile, 1, null, unreachable);
            assertNull(fetcher.getNewAvailableRange(0L));

            // Reachable node with range
            var reachable = mockReachableClientWithStatus(0L, 100L);
            fetcher = createFetcherWithClient(tempFile, 1, null, reachable);
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
    }

    @Nested
    @DisplayName("getAvailabilityForRange")
    class GetAvailabilityForRangeTests {

        @Test
        @DisplayName("returns intersection for reachable nodes, empty when no overlap or unreachable")
        void getAvailabilityBehaviors() throws Exception {
            final BackfillSourceConfig nodeConfig = node("localhost", 1, 1);
            final Path tempFile = createTempSourceFile(nodeConfig);

            // Reachable with overlap - use custom fetcher to control resolveAvailableRanges
            var reachable = mock(BlockNodeClient.class);
            when(reachable.isNodeReachable()).thenReturn(true);

            var fetcher = new BackfillFetcher(tempFile, 1, null, 100, 1000, 1000, false, 300_000L, 1000.0) {
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
            var unreachableFetcher = createFetcherWithClient(tempFile, 1, null, unreachable);
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
            final Counter retryCounter = mock(Counter.class);
            final Path tempFile = createTempSourceFile(nodeConfig);

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
            fetcher = new BackfillFetcher(tempFile, 1, retryCounter, 10000, 1, 1, false, 300_000L, healthPenalty) {
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
            final Path tempFile = createTempSourceFile(nodeConfig);

            var reachable = mock(BlockNodeClient.class);
            when(reachable.isNodeReachable()).thenReturn(true);

            // Overlapping ranges merge to single range
            var fetcher1 = new BackfillFetcher(tempFile, 1, null, 100, 1000, 1000, false, 300_000L, 1000.0) {
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
            var fetcher2 = new BackfillFetcher(tempFile, 1, null, 100, 1000, 1000, false, 300_000L, 1000.0) {
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

    // Helper methods
    private static Path createTempSourceFile(BackfillSourceConfig... nodes) throws Exception {
        final BackfillSource source =
                BackfillSource.newBuilder().nodes(List.of(nodes)).build();
        final Path tempFile = Files.createTempFile("bn-sources", ".json");
        Files.write(tempFile, BackfillSource.JSON.toBytes(source).toByteArray());
        return tempFile;
    }

    private static BackfillFetcher createFetcherWithClient(
            Path tempFile, int maxRetries, Counter retryCounter, BlockNodeClient client) throws Exception {
        return new BackfillFetcher(tempFile, maxRetries, retryCounter, 1, 1000, 1000, false, 300_000L, 1000.0) {
            @Override
            protected BlockNodeClient getNodeClient(BackfillSourceConfig ignored) {
                return client;
            }
        };
    }
}
