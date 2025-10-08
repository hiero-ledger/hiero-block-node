// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.INFO;

import com.hedera.hapi.block.stream.Block;
import com.swirlds.metrics.api.Counter;
import org.hiero.block.api.BlockAccessServiceInterface;
import org.hiero.block.api.BlockRequest;
import org.hiero.block.api.BlockResponse;
import org.hiero.block.api.BlockResponse.Code;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

/**
 * Plugin that implements the BlockAccessService and provides the 'block' RPC.
 */
public class BlockAccessServicePlugin implements BlockNodePlugin, BlockAccessServiceInterface {

    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The block provider */
    private HistoricalBlockFacility blockProvider;
    /** Counter for the number of requests */
    private Counter requestCounter;
    /** Counter for the number of responses Success */
    private Counter responseCounterSuccess;
    /** Counter for the number of responses not available */
    private Counter responseCounterNotAvailable;
    /** Counter for the number of responses not found */
    private Counter responseCounterNotFound;

    // ==== BlockAccessServiceInterface Methods ========================================================================

    /**
     * Handle a request for a single block
     *
     * @param request the request containing the block number or latest flag
     * @return the response containing the block or an error status
     */
    public BlockResponse getBlock(BlockRequest request) {
        LOGGER.log(TRACE, "Received BlockRequest for block number: {0}", request.blockNumber());
        requestCounter.increment();

        try {
            long blockNumberToRetrieve;
            // The block number and retrieve latest are mutually exclusive in
            // the proto definition, so no need to check for that here.

            // if retrieveLatest is set, or the request is for the largest possible block number, get the latest block.
            if (request.hasBlockNumber() && request.blockNumber() >= 0) {
                blockNumberToRetrieve = request.blockNumber();
                LOGGER.log(TRACE, "Received `block_number` BlockRequest, retrieving block: {0}", blockNumberToRetrieve);
            } else if ((request.hasRetrieveLatest() && request.retrieveLatest()) ||
                       (request.hasBlockNumber() && request.blockNumber() == -1)) {
                blockNumberToRetrieve = blockProvider.availableBlocks().max();
                LOGGER.log(TRACE, "Received 'retrieveLatest' BlockRequest, retrieving block: {0}", blockNumberToRetrieve);
            } else {
                LOGGER.log(INFO, "Invalid request, 'retrieve_latest' or a valid 'block number' is required.");
                return new BlockResponse(Code.INVALID_REQUEST, null);
            }

            // Check if block is within the available range
            if (!blockProvider.availableBlocks().contains(blockNumberToRetrieve)) {
                long lowestBlockNumber = blockProvider.availableBlocks().min();
                long highestBlockNumber = blockProvider.availableBlocks().max();
                LOGGER.log(
                        TRACE,
                        "Requested block {0} is outside available range [{1}, {2}]",
                        blockNumberToRetrieve,
                        lowestBlockNumber,
                        highestBlockNumber);
                responseCounterNotAvailable.increment();
                return new BlockResponse(Code.NOT_AVAILABLE, null);
            }

            // Retrieve the block
            Block block = blockProvider.block(blockNumberToRetrieve).block();
            responseCounterSuccess.increment();
            return new BlockResponse(Code.SUCCESS, block);

        } catch (RuntimeException e) {
            String message = "Failed to retrieve block number %d.".formatted(request.blockNumber());
            LOGGER.log(ERROR, message, e);
            responseCounterNotFound.increment(); // Should this be a failure counter?
            return new BlockResponse(Code.NOT_FOUND, null);
        }
    }

    // ==== BlockNodePlugin Methods ====================================================================================

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return "BlockAccessServicePlugin";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        // Create the metrics
        requestCounter = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "get_block_requests")
                        .withDescription("Number of get block requests"));
        responseCounterSuccess = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "get_block_requests_success")
                        .withDescription("Successful single block requests"));
        responseCounterNotAvailable = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "get_block_requests_not_available")
                        .withDescription("Requests for blocks that were not available"));
        responseCounterNotFound = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "get_block_requests_not_found")
                        .withDescription("Requests for blocks that were not found"));
        // Get the block provider
        this.blockProvider = context.historicalBlockProvider();
        // Register this service
        serviceBuilder.registerGrpcService(this);
    }
}
