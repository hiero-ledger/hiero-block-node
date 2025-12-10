// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hiero.block.node.backfill.client.BackfillSource;
import org.hiero.block.node.backfill.client.BackfillSourceConfig;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BackfillGrpcClientTest {

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
        BackfillSourceConfig lowPriority = node("localhost", 1, 1);
        BackfillSourceConfig highPriority = node("localhost", 2, 2);

        // Second node covers an earlier block but has higher priority (worse cost).
        Map<BackfillSourceConfig, List<LongRange>> availability = Map.of(
                lowPriority, List.of(new LongRange(12, 30)),
                highPriority, List.of(new LongRange(10, 20)));

        BackfillGrpcClient client = newClient(lowPriority, highPriority);
        Optional<BackfillGrpcClient.NodeSelection> selection = client.selectNextChunk(10, 30, availability);

        assertTrue(selection.isPresent(), "Expected a candidate");
        assertEquals(
                highPriority, selection.get().nodeConfig(), "Should choose earliest start even if higher priority");
        assertEquals(10, selection.get().startBlock());
    }

    @Test
    @DisplayName("Breaks ties on start by picking lower-priority node")
    void selectsLowerPriorityWhenStartsTie() throws Exception {
        BackfillSourceConfig priorityOne = node("localhost", 1, 1);
        BackfillSourceConfig priorityTwo = node("localhost", 2, 2);

        // Both nodes start at 15; expect the cheaper (lower priority number) node to win.
        Map<BackfillSourceConfig, List<LongRange>> availability =
                Map.of(priorityOne, List.of(new LongRange(15, 25)), priorityTwo, List.of(new LongRange(15, 20)));

        BackfillGrpcClient client = newClient(priorityOne, priorityTwo);
        Optional<BackfillGrpcClient.NodeSelection> selection = client.selectNextChunk(15, 30, availability);

        assertTrue(selection.isPresent(), "Expected a candidate");
        assertEquals(priorityOne, selection.get().nodeConfig(), "Should prefer lower priority when starts match");
        assertEquals(15, selection.get().startBlock());
    }

    private BackfillGrpcClient newClient(BackfillSourceConfig... nodes) throws Exception {
        BackfillSource source =
                BackfillSource.newBuilder().nodes(List.of(nodes)).build();
        Path tempFile = Files.createTempFile("bn-sources", ".json");
        Files.write(tempFile, BackfillSource.JSON.toBytes(source).toByteArray());
        return new BackfillGrpcClient(tempFile, 1, null, 0, 0, false);
    }
}
