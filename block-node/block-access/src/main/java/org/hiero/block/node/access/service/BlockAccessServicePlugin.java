// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

import com.hedera.hapi.block.stream.Block;
import com.swirlds.metrics.api.Counter;
import org.hiero.block.api.BlockAccessServiceInterface;
import org.hiero.block.api.BlockRequest;
import org.hiero.block.api.BlockResponse;
import org.hiero.block.api.BlockResponse.Code;
// PBJ doesn't generate GRPC stubs for some reason, also the proto file is broken when PBJ compiles it...
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
        LOGGER.log(DEBUG, "Received BlockRequest for block number: {0}", request.blockNumber());
        requestCounter.increment();

        try {
            // Log in case both block_number and retrieve_latest are set, this should not happen
            if (request.retrieveLatest() && request.blockNumber() != -1) {
                LOGGER.log(
                        INFO,
                        "Both block_number and retrieve_latest set. Using retrieve_latest instead of block_number: {0}",
                        request.blockNumber());
                responseCounterNotFound.increment();
                return new BlockResponse(Code.READ_BLOCK_NOT_FOUND, null);
            }
            // when block_number is -1 and retrieve_latest is false, return an NOT_FOUND error
            if (request.blockNumber() == -1 && !request.retrieveLatest()) {
                LOGGER.log(INFO, "Block number is -1 and retrieve_latest is false");
                responseCounterNotFound.increment();
                return new BlockResponse(Code.READ_BLOCK_NOT_FOUND, null);
            }

            long blockNumberToRetrieve;

            // if retrieveLatest is set, get the latest block number
            if (request.retrieveLatest()) {
                blockNumberToRetrieve = blockProvider.availableBlocks().max();
                if (blockNumberToRetrieve < 0) {
                    LOGGER.log(INFO, "Latest block number not available");
                    responseCounterNotAvailable.increment();
                    return new BlockResponse(Code.READ_BLOCK_NOT_AVAILABLE, null);
                }
            } else {
                blockNumberToRetrieve = request.blockNumber();
            }

            // Check if block is within the available range
            if (!blockProvider.availableBlocks().contains(blockNumberToRetrieve)) {
                long lowestBlockNumber = blockProvider.availableBlocks().min();
                long highestBlockNumber = blockProvider.availableBlocks().max();
                LOGGER.log(
                        DEBUG,
                        "Requested block {0} is outside available range [{1}, {2}]",
                        blockNumberToRetrieve,
                        lowestBlockNumber,
                        highestBlockNumber);
                responseCounterNotAvailable.increment();
                return new BlockResponse(Code.READ_BLOCK_NOT_AVAILABLE, null);
            }

            // Retrieve the block
            Block block = blockProvider.block(blockNumberToRetrieve).block();
            responseCounterSuccess.increment();
            return new BlockResponse(Code.READ_BLOCK_SUCCESS, block);

        } catch (RuntimeException e) {
            LOGGER.log(ERROR, "Failed to retrieve block number: {0}", request.blockNumber());
            responseCounterNotFound.increment();
            return new BlockResponse(Code.READ_BLOCK_NOT_FOUND, null);
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
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "single-block-requests")
                        .withDescription("Number of single block requests"));
        responseCounterSuccess = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "single-block-requests-success")
                        .withDescription("Number of successful single block requests"));
        responseCounterNotAvailable = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "single-block-requests-not-available")
                        .withDescription("Number of single block requests that were not available"));
        responseCounterNotFound = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "single-block-requests-not-found")
                        .withDescription("Number of single block requests that were not found"));
        // Get the block provider
        this.blockProvider = context.historicalBlockProvider();
        // Register this service
        serviceBuilder.registerGrpcService(this);
    }
}
