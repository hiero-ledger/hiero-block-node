// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.block.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.protoc.BlockAccessServiceGrpc;
import com.hedera.hapi.block.protoc.SingleBlockRequest;
import com.hedera.hapi.block.protoc.SingleBlockResponse;
import com.hedera.hapi.block.protoc.SingleBlockResponseCode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.hiero.block.suites.BaseSuite;
import org.junit.jupiter.api.AfterAll;
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

    /**
     * Default constructor for the {@link GetSingleBlockApiTests} class. This constructor does not
     * require any specific initialization.
     */
    public GetSingleBlockApiTests() {}

    // Simulator instance to be used for testing
    private BlockStreamSimulatorApp blockStreamSimulatorApp;

    // Thread to run the simulator
    private Future<?> simulatorThread;

    // gRPC channel for connecting to the Block Node
    private ManagedChannel channel;

    // gRPC client stub for BlockAccessService
    private BlockAccessServiceGrpc.BlockAccessServiceBlockingStub blockAccessStub;

    @AfterEach
    void teardownEnvironment() {
        if (simulatorThread != null && !simulatorThread.isCancelled()) {
            simulatorThread.cancel(true);
        }
    }

    @BeforeAll
    void publishSomeBlocks() throws IOException, InterruptedException {
        // Initialize the gRPC client
        initializeGrpcClient();

        // Use the simulator to publish some blocks
        blockStreamSimulatorApp = createBlockSimulator();
        simulatorThread = startSimulatorInThread(blockStreamSimulatorApp);
        Thread.sleep(5000);
        blockStreamSimulatorApp.stop();
    }

    @AfterAll
    void shutdown() throws InterruptedException {
        // Shutdown the gRPC channel
        if (channel != null) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Initializes the gRPC client for connecting to the Block Node.
     */
    private void initializeGrpcClient() {
        String host = blockNodeContainer.getHost();
        int port = blockNodePort;

        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // For testing only
                .build();

        blockAccessStub = BlockAccessServiceGrpc.newBlockingStub(channel);
    }

    /**
     * Creates a SingleBlockRequest to retrieve a specific block.
     *
     * @param blockNumber The block number to retrieve
     * @param latest Whether to retrieve the latest block
     * @return A SingleBlockRequest object
     */
    private SingleBlockRequest createSingleBlockRequest(long blockNumber, boolean latest) {
        return SingleBlockRequest.newBuilder()
                .setBlockNumber(blockNumber)
                .setRetrieveLatest(latest)
                .setAllowUnverified(true)
                .build();
    }

    /**
     * Retrieves a single block using the Block Node API.
     *
     * @param blockNumber The block number to retrieve
     * @param latest Whether to retrieve the latest block
     * @return The SingleBlockResponse from the API
     */
    private SingleBlockResponse getSingleBlock(long blockNumber, boolean latest) {
        SingleBlockRequest request = createSingleBlockRequest(blockNumber, latest);
        return blockAccessStub.singleBlock(request);
    }

    @Test
    @DisplayName("Get a Single Block using API - Happy Path")
    void requestExistingBlockUsingSingleBlockAPI() {
        // Request block number 1 (which should have been published by the simulator)
        long blockNumber = 1;
        SingleBlockResponse response = getSingleBlock(blockNumber, false);

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
        long blockNumber = 1000;
        SingleBlockResponse response = getSingleBlock(blockNumber, false);

        // Verify the response
        assertNotNull(response, "Response should not be null");
        assertEquals(
                SingleBlockResponseCode.READ_BLOCK_NOT_AVAILABLE,
                response.getStatus(),
                "Block retrieval should fail for non-existing block");

        // Verify that the block is null
        assertTrue(!response.hasBlock(), "Response should not contain a block");
    }

    @Test
    @DisplayName("Get a Single Block using API - Request Latest Block")
    void requestLatestBlockUsingSingleBlockAPI() {
        // Request the latest block
        SingleBlockResponse response = getSingleBlock(-1, true);

        // Verify the response
        assertNotNull(response, "Response should not be null");
        assertEquals(
                SingleBlockResponseCode.READ_BLOCK_SUCCESS,
                response.getStatus(),
                "Block retrieval should be successful");

        // Verify the block content
        long latestPublishedBlock = blockStreamSimulatorApp.getStreamStatus().publishedBlocks() - 1;
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
        long blockNumber = 1;
        SingleBlockRequest request = createSingleBlockRequest(blockNumber, true);
        SingleBlockResponse response = blockAccessStub.singleBlock(request);

        // Verify the response
        assertNotNull(response, "Response should not be null");
        assertEquals(
                SingleBlockResponseCode.READ_BLOCK_NOT_FOUND,
                response.getStatus(),
                "Block retrieval should fail for non-existing block");

        // Verify that the block is null
        assertTrue(!response.hasBlock(), "Response should not contain a block");
    }

    @Test
    @DisplayName(
            "Get a Single Block using API - block_number to -1 and retrieve_latest to false - should return NOT_FOUND")
    void requestWithoutBlockNumberAndRetrieveLatestFalse() {
        // Request the latest block and a specific block number
        SingleBlockRequest request = createSingleBlockRequest(-1, false);
        SingleBlockResponse response = blockAccessStub.singleBlock(request);

        // Verify the response
        assertNotNull(response, "Response should not be null");
        assertEquals(
                SingleBlockResponseCode.READ_BLOCK_NOT_FOUND,
                response.getStatus(),
                "Block retrieval should fail for non-existing block");

        // Verify that the block is null
        assertTrue(!response.hasBlock(), "Response should not contain a block");
    }
}
