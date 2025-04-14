// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.block.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.protoc.SingleBlockRequest;
import com.hedera.hapi.block.protoc.SingleBlockResponse;
import com.hedera.hapi.block.protoc.SingleBlockResponseCode;
import java.io.IOException;
import java.util.concurrent.Future;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.hiero.block.suites.BaseSuite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 *
 * Test class for verifying the functionality of the GetSingleBlock API in the Block Node
 * application.
 *
 * <p>This class is part of the Block Node suite and aims to test the behavior of the GetSingleBlock
 * API under various conditions, including both positive and negative scenarios.
 *
 * <p>Inherits from {@link BaseSuite} to reuse the container setup and teardown logic for the Block
 * Node.
 *
 */
@DisplayName("GetSingleBlockApiTests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GetSingleBlockApiTests extends BaseSuite {

    // Simulator instance to be used for testing
    private BlockStreamSimulatorApp blockStreamSimulatorApp;

    // Thread to run the simulator
    private Future<?> simulatorThread;

    @AfterEach
    void teardownEnvironment() {
        if (simulatorThread != null && !simulatorThread.isCancelled()) {
            simulatorThread.cancel(true);
        }
    }

    @BeforeAll
    void publishSomeBlocks() throws IOException, InterruptedException {
        // Use the simulator to publish some blocks
        blockStreamSimulatorApp = createBlockSimulator();
        simulatorThread = startSimulatorInThread(blockStreamSimulatorApp);
        Thread.sleep(5000);
        blockStreamSimulatorApp.stop();
    }

    @Test
    @DisplayName("Get a Single Block using API - Happy Path")
    void requestExistingBlockUsingSingleBlockAPI() {
        // Request block number 1 (which should have been published by the simulator)
        final long blockNumber = 1;
        final SingleBlockResponse response = getSingleBlock(blockNumber, false);

        // Verify the response
        assertNotNull(response, "Response should not be null");
        assertEquals(
                SingleBlockResponseCode.READ_BLOCK_SUCCESS,
                response.getStatus(),
                "Block retrieval should be successful");

        // Verify the block content
        assertTrue(response.hasBlock(), "Response should contain a block");
        assertEquals(
                blockNumber,
                response.getBlock().getItemsList().getFirst().getBlockHeader().getNumber(),
                "Block number should match the requested block number");
    }

    @Test
    @DisplayName("Get a Single Block using API - Negative Test - Non-existing Block")
    void requestNonExistingBlockUsingSingleBlockAPI() {
        // Request a non-existing block number
        final long blockNumber = 1000;
        final SingleBlockResponse response = getSingleBlock(blockNumber, false);

        // Verify the response
        assertNotNull(response, "Response should not be null");
        assertEquals(
                SingleBlockResponseCode.READ_BLOCK_NOT_AVAILABLE,
                response.getStatus(),
                "Block retrieval should fail for non-existing block");

        // Verify that the block is null
        assertFalse(response.hasBlock(), "Response should not contain a block");
    }

    @Test
    @DisplayName("Get a Single Block using API - Request Latest Block")
    void requestLatestBlockUsingSingleBlockAPI() {
        // Request the latest block
        final SingleBlockResponse response = getSingleBlock(-1, true);

        // Verify the response
        assertNotNull(response, "Response should not be null");
        assertEquals(
                SingleBlockResponseCode.READ_BLOCK_SUCCESS,
                response.getStatus(),
                "Block retrieval should be successful");

        // Verify the block content
        final long latestPublishedBlock =
                blockStreamSimulatorApp.getStreamStatus().publishedBlocks() - 1;
        assertTrue(response.hasBlock(), "Response should contain a block");
        assertEquals(
                latestPublishedBlock,
                response.getBlock().getItemsList().getFirst().getBlockHeader().getNumber(),
                "Block number should match the latest block number");
    }

    @Test
    @DisplayName("Get a Single Block using API - Request Latest and Specific Block - should fail with NOT_FOUND")
    void requestLatestBlockAndSpecificBlockUsingSingleBlockAPI() {
        // Request the latest block and a specific block number
        final long blockNumber = 1;
        final SingleBlockRequest request = createSingleBlockRequest(blockNumber, true);
        final SingleBlockResponse response = blockAccessStub.singleBlock(request);

        // Verify the response
        assertNotNull(response, "Response should not be null");
        assertEquals(
                SingleBlockResponseCode.READ_BLOCK_NOT_FOUND,
                response.getStatus(),
                "Block retrieval should fail for non-existing block");

        // Verify that the block is null
        assertFalse(response.hasBlock(), "Response should not contain a block");
    }

    @Test
    @DisplayName(
            "Get a Single Block using API - block_number to -1 and retrieve_latest to false - should return NOT_FOUND")
    void requestWithoutBlockNumberAndRetrieveLatestFalse() {
        // Request the latest block and a specific block number
        final SingleBlockRequest request = createSingleBlockRequest(-1, false);
        final SingleBlockResponse response = blockAccessStub.singleBlock(request);

        // Verify the response
        assertNotNull(response, "Response should not be null");
        assertEquals(
                SingleBlockResponseCode.READ_BLOCK_NOT_FOUND,
                response.getStatus(),
                "Block retrieval should fail for non-existing block");

        // Verify that the block is null
        assertFalse(response.hasBlock(), "Response should not contain a block");
    }
}
