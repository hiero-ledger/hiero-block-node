// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.metrics;

import static org.hiero.block.suites.utils.BlockAccessUtils.getBlock;
import static org.hiero.block.suites.utils.BlockAccessUtils.getLatestBlock;
import static org.hiero.block.suites.utils.BlockSimulatorUtils.createBlockSimulator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.Future;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.hiero.block.suites.BaseSuite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * This class contains common tests for verifying metrics collection in the Block-Node application.
 * It uses a simulator to publish blocks and checks various metrics related to block access, messaging,
 * publisher, verification, and file storage.
 */
@DisplayName("Metrics Common Tests")
public class MetricsCommonTests extends BaseSuite {

    // Simulator instance to be used for testing
    private BlockStreamSimulatorApp blockStreamSimulatorApp;

    // Thread to run the simulator
    private Future<?> simulatorThread;

    private MetricsAccessor metricsAccessor;

    @BeforeEach
    void publishSomeBlocks() throws IOException, InterruptedException {

        // Initialize the metrics endpoint
        String metricsEndpoint = "http://" + blockNodeContainer.getHost() + ":" + blockNodeMetricsPort + "/metrics";
        metricsAccessor = new MetricsAccessor(metricsEndpoint);
        // Use the simulator to publish some blocks
        blockStreamSimulatorApp = createBlockSimulator();
        simulatorThread = startSimulatorInThread(blockStreamSimulatorApp);
        Thread.sleep(5000);
        blockStreamSimulatorApp.stop();
        // get a single block using block access api
        getLatestBlock(blockAccessStub);
        getBlock(blockAccessStub, 100);
        // sleep to allow metrics to be updated
        Thread.sleep(2000);
    }

    @AfterEach
    void teardownEnvironment() {
        if (simulatorThread != null && !simulatorThread.isCancelled()) {
            simulatorThread.cancel(true);
        }
        metricsAccessor.close();
    }

    @Test
    @DisplayName("Verify that metrics are being collected - reported in host:16007/metrics")
    void verifyMetricsAreCollected() throws IOException, InterruptedException {
        long publishedBlocks = blockStreamSimulatorApp.getStreamStatus().publishedBlocks();

        // Verify that we have published some blocks
        assertTrue(publishedBlocks > 0, "Should have published at least one block");

        // Verify app metrics
        long appNewestBlock =
                metricsAccessor.getMetricValue("app_historical_newest_block", MetricsAccessor.MetricType.GAUGE);
        long appOldestBlock =
                metricsAccessor.getMetricValue("app_historical_oldest_block", MetricsAccessor.MetricType.GAUGE);
        long appState = metricsAccessor.getMetricValue("app_state_status", MetricsAccessor.MetricType.GAUGE);

        assertEquals(publishedBlocks - 1, appNewestBlock, "Newest block should match the published blocks");
        assertEquals(0, appOldestBlock, "Oldest block should be 0 for a new simulator run");
        assertEquals(1, appState, "App state should be 1 (running) after publishing blocks");

        Thread.sleep(2_000); // Allow some time for metrics to be updated

        // Verify block access metrics
        long getBlockRequests =
                metricsAccessor.getMetricValue("get_block_requests", MetricsAccessor.MetricType.COUNTER);
        long getBlockRequestsSuccess =
                metricsAccessor.getMetricValue("get_block_requests_success", MetricsAccessor.MetricType.COUNTER);
        long getBlockRequestsNotAvailable =
                metricsAccessor.getMetricValue("get_block_requests_not_available", MetricsAccessor.MetricType.COUNTER);
        long getBlockRequestsFailed =
                metricsAccessor.getMetricValue("get_block_requests_not_found", MetricsAccessor.MetricType.COUNTER);

        assertEquals(
                2,
                getBlockRequests,
                "There should be 2 block requests made (one for latest and one for non-existing block)");
        assertEquals(1, getBlockRequestsSuccess, "There should be 1 successful block request (for latest block)");
        assertEquals(
                1,
                getBlockRequestsNotAvailable,
                "There should be 1 not available block request (for non-existing block)");
        assertEquals(0, getBlockRequestsFailed, "There should be no failed block requests");

        // Verify messaging metrics
        long blockItemsReceived =
                metricsAccessor.getMetricValue("messaging_block_items_received", MetricsAccessor.MetricType.COUNTER);
        long verificationNotifications = metricsAccessor.getMetricValue(
                "messaging_block_verification_notifications", MetricsAccessor.MetricType.COUNTER);
        long persistedNotifications = metricsAccessor.getMetricValue(
                "messaging_block_persisted_notifications", MetricsAccessor.MetricType.COUNTER);

        assertTrue(blockItemsReceived >= 0, "Block items received should be a non-negative number");
        assertTrue(verificationNotifications >= 0, "Verification notifications should be a non-negative number");
        assertTrue(persistedNotifications >= 0, "Persisted notifications should be a non-negative number");

        // Verify publisher metrics
        long publisherBlocksReceived =
                metricsAccessor.getMetricValue("publisher_block_items_received", MetricsAccessor.MetricType.COUNTER);
        long publisherOpenConnections =
                metricsAccessor.getMetricValue("publisher_open_connections", MetricsAccessor.MetricType.GAUGE);

        assertTrue(publisherBlocksReceived >= 0, "Publisher blocks received should be a non-negative number");
        assertTrue(publisherOpenConnections >= 0, "Publisher open connections should be a non-negative number");

        // Verify verification metrics
        long blocksReceived =
                metricsAccessor.getMetricValue("verification_blocks_received", MetricsAccessor.MetricType.COUNTER);
        long blocksVerified =
                metricsAccessor.getMetricValue("verification_blocks_verified", MetricsAccessor.MetricType.COUNTER);

        assertTrue(blocksReceived >= 0, "Blocks received for verification should be a non-negative number");
        assertTrue(blocksVerified >= 0, "Blocks verified should be a non-negative number");
        assertTrue(blocksVerified <= blocksReceived, "Verified blocks should be <= received blocks");

        // Verify files.recent metrics
        long recentBlocksWritten =
                metricsAccessor.getMetricValue("files_recent_blocks_written", MetricsAccessor.MetricType.COUNTER);
        long recentBlocksStored =
                metricsAccessor.getMetricValue("files_recent_blocks_stored", MetricsAccessor.MetricType.GAUGE);

        assertTrue(recentBlocksWritten >= 0, "Recent blocks written should be a non-negative number");
        assertTrue(recentBlocksStored >= 0, "Recent blocks stored should be a non-negative number");

        // Additional verification: blocks published should match metrics
        assertTrue(
                publisherBlocksReceived >= publishedBlocks, "Number of blocks received should be >= published blocks");
    }
}
