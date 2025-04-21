// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.block.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.Future;
import org.hiero.block.api.protoc.BlockRequest;
import org.hiero.block.api.protoc.BlockResponse;
import org.hiero.block.api.protoc.BlockResponse.BlockResponseCode;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.hiero.block.suites.BaseSuite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 *
 * Test class for verifying the functionality of the GetBlock API in the Block Node
 * application.
 *
 * <p>This class is part of the Block Node suite and aims to test the behavior of the GetBlock
 * API under various conditions, including both positive and negative scenarios.
 *
 * <p>Inherits from {@link BaseSuite} to reuse the container setup and teardown logic for the Block
 * Node.
 *
 */
@DisplayName("GetBlockApiTests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GetBlockApiTests extends BaseSuite {

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

    @BeforeEach
    void publishSomeBlocks() throws IOException, InterruptedException {
        // Use the simulator to publish some blocks
        blockStreamSimulatorApp = createBlockSimulator();
        simulatorThread = startSimulatorInThread(blockStreamSimulatorApp);
        Thread.sleep(5000);
        blockStreamSimulatorApp.stop();
    }

    @Test
    @DisplayName("Get a Single Block using API - Happy Path")
    void requestExistingBlockUsingBlockAPI() {
        // Request block number 1 (which should have been published by the simulator)
        final long blockNumber = 1;
        final BlockResponse response = getBlock(blockNumber, false);

        // Verify the response
        assertNotNull(response, "Response should not be null");
        assertEquals(
                BlockResponseCode.READ_BLOCK_SUCCESS, response.getStatus(), "Block retrieval should be successful");

        // Verify the block content
        assertTrue(response.hasBlock(), "Response should contain a block");
        assertEquals(
                blockNumber,
                response.getBlock().getItemsList().getFirst().getBlockHeader().getNumber(),
                "Block number should match the requested block number");
    }

    @Test
    @DisplayName("Get a Single Block using API - Negative Test - Non-existing Block")
    void requestNonExistingBlockUsingBlockAPI() {
        // Request a non-existing block number
        final long blockNumber = 1000;
        final BlockResponse response = getBlock(blockNumber, false);

        // Verify the response
        assertNotNull(response, "Response should not be null");
        assertEquals(
                BlockResponseCode.READ_BLOCK_NOT_AVAILABLE,
                response.getStatus(),
                "Block retrieval should fail for non-existing block");

        // Verify that the block is null
        assertFalse(response.hasBlock(), "Response should not contain a block");
    }

    @Test
    @DisplayName("Get a Single Block using API - Request Latest Block")
    void requestLatestBlockUsingBlockAPI() {
        // Request the latest block
        final BlockResponse response = getLatestBlock(false);

        // Verify the response
        assertNotNull(response, "Response should not be null");
        assertEquals(
                BlockResponseCode.READ_BLOCK_SUCCESS, response.getStatus(), "Block retrieval should be successful");

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
    void requestLatestBlockAndSpecificBlockUsingBlockAPI() {
        // Request the latest block and a specific block number
        final long blockNumber = 1;
        final BlockRequest request = createBlockRequest(blockNumber, true);
        final BlockResponse response = blockAccessStub.getBlock(request);

        // Verify the response
        assertNotNull(response, "Response should not be null");
        assertEquals(
                BlockResponseCode.READ_BLOCK_NOT_FOUND,
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
        final BlockRequest request = createBlockRequest(-1, false);
        final BlockResponse response = blockAccessStub.getBlock(request);

        // Verify the response
        assertNotNull(response, "Response should not be null");
        assertEquals(
                BlockResponseCode.READ_BLOCK_NOT_FOUND,
                response.getStatus(),
                "Block retrieval should fail for non-existing block");

        // Verify that the block is null
        assertFalse(response.hasBlock(), "Response should not contain a block");
    }
}
