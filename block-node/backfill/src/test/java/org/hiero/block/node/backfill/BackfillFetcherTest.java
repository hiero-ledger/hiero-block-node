// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.swirlds.metrics.api.Counter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.backfill.client.BackfillSource;
import org.hiero.block.node.backfill.client.BackfillSourceConfig;
import org.hiero.block.node.backfill.client.BlockNodeClient;
import org.hiero.block.node.backfill.client.BlockStreamSubscribeUnparsedClient;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

/**
 * Unit tests for {@link BackfillFetcher}.
 */
@Timeout(value = 5, unit = TimeUnit.SECONDS)
class BackfillFetcherTest {

    private BackfillSourceConfig node(String host, int port, int priority) {
        return BackfillSourceConfig.newBuilder()
                .address(host)
                .port(port)
                .priority(priority)
                .build();
    }

    @Test
    @DisplayName("Selects earliest start even when that node is higher priority")
    void selectsEarliestStartEvenIfPriorityIsHigher() throws Exception {
        final BackfillSourceConfig lowPriority = node("localhost", 1, 1);
        final BackfillSourceConfig highPriority = node("localhost", 2, 2);

        // Second node covers an earlier block but has higher priority (worse cost).
        final Map<BackfillSourceConfig, List<LongRange>> availability = Map.of(
                lowPriority, List.of(new LongRange(12, 30)),
                highPriority, List.of(new LongRange(10, 20)));

        final BackfillFetcher client = newClient(lowPriority, highPriority);
        final Optional<NodeSelectionStrategy.NodeSelection> selection = client.selectNextChunk(10, 30, availability);

        assertTrue(selection.isPresent(), "Expected a candidate");
        assertEquals(
                highPriority, selection.get().nodeConfig(), "Should choose earliest start even if higher priority");
        assertEquals(10, selection.get().startBlock());
    }

    @Test
    @DisplayName("Breaks ties on start by picking lower-priority node")
    void selectsLowerPriorityWhenStartsTie() throws Exception {
        final BackfillSourceConfig priorityOne = node("localhost", 1, 1);
        final BackfillSourceConfig priorityTwo = node("localhost", 2, 2);

        // Both nodes start at 15; expect the cheaper (lower priority number) node to win.
        final Map<BackfillSourceConfig, List<LongRange>> availability =
                Map.of(priorityOne, List.of(new LongRange(15, 25)), priorityTwo, List.of(new LongRange(15, 20)));

        final BackfillFetcher client = newClient(priorityOne, priorityTwo);
        final Optional<NodeSelectionStrategy.NodeSelection> selection = client.selectNextChunk(15, 30, availability);

        assertTrue(selection.isPresent(), "Expected a candidate");
        assertEquals(priorityOne, selection.get().nodeConfig(), "Should prefer lower priority when starts match");
        assertEquals(15, selection.get().startBlock());
    }

    @Test
    @DisplayName("Returns empty when no range can satisfy requested start")
    void returnsEmptyWhenNoRangeCoversStart() throws Exception {
        final BackfillSourceConfig node = node("localhost", 1, 1);
        final Map<BackfillSourceConfig, List<LongRange>> availability = Map.of(node, List.of(new LongRange(10, 20)));

        final BackfillFetcher client = newClient(node);
        final Optional<NodeSelectionStrategy.NodeSelection> selection = client.selectNextChunk(25, 30, availability);

        assertTrue(selection.isEmpty(), "Selection should be empty when start is outside all ranges");
    }

    @Test
    @DisplayName("Chooses randomly among equal priority/start candidates")
    void choosesRandomWhenStartsAndPriorityTie() throws Exception {
        final BackfillSourceConfig nodeA = node("localhost", 1, 1);
        final BackfillSourceConfig nodeB = node("localhost", 2, 1);
        final Map<BackfillSourceConfig, List<LongRange>> availability = Map.of(
                nodeA, List.of(new LongRange(10, 20)),
                nodeB, List.of(new LongRange(10, 25)));

        final BackfillFetcher client = newClient(nodeA, nodeB);
        final Optional<NodeSelectionStrategy.NodeSelection> selection = client.selectNextChunk(10, 30, availability);

        assertTrue(selection.isPresent(), "Expected one of the tied candidates");
        assertEquals(10, selection.get().startBlock());
        assertTrue(
                selection.get().nodeConfig().equals(nodeA)
                        || selection.get().nodeConfig().equals(nodeB),
                "Selection should be one of the tied nodes");
    }

    @Test
    @DisplayName("Retries once, increments retry counter, then marks node unavailable on repeated failure")
    void retriesAndMarksUnavailableOnFailure() throws Exception {
        final BackfillSourceConfig node = node("localhost", 1, 1);
        final Counter retryCounter = Mockito.mock(Counter.class);
        final BackfillSource source =
                BackfillSource.newBuilder().nodes(List.of(node)).build();
        final Path tempFile = Files.createTempFile("bn-sources", ".json");
        Files.write(tempFile, BackfillSource.JSON.toBytes(source).toByteArray());

        final BlockStreamSubscribeUnparsedClient subscribeClient =
                Mockito.mock(BlockStreamSubscribeUnparsedClient.class);
        Mockito.when(subscribeClient.getBatchOfBlocks(Mockito.anyLong(), Mockito.anyLong()))
                .thenThrow(new RuntimeException("fail"));
        final BlockNodeClient failingClient = Mockito.mock(BlockNodeClient.class);
        Mockito.when(failingClient.getBlockstreamSubscribeUnparsedClient()).thenReturn(subscribeClient);

        BackfillFetcher client = new BackfillFetcher(tempFile, 2, retryCounter, 1, 1, 1, false, 300_000L, 1000.0) {
            @Override
            protected BlockNodeClient getNodeClient(BackfillSourceConfig ignored) {
                return failingClient;
            }
        };

        List<BlockUnparsed> result = client.fetchBlocksFromNode(node, new LongRange(0, 1));

        assertTrue(result.isEmpty(), "Result should be empty when all retries fail");
        Mockito.verify(retryCounter, Mockito.times(1)).increment();
    }

    /** Build a real BackfillFetcher backed by a temp source file using the provided nodes. */
    private BackfillFetcher newClient(BackfillSourceConfig... nodes) throws Exception {
        final BackfillSource source =
                BackfillSource.newBuilder().nodes(List.of(nodes)).build();
        final Path tempFile = Files.createTempFile("bn-sources", ".json");
        Files.write(tempFile, BackfillSource.JSON.toBytes(source).toByteArray());
        return new BackfillFetcher(tempFile, 1, null, 0, 0, 0, false, 300_000L, 1000.0);
    }
}
