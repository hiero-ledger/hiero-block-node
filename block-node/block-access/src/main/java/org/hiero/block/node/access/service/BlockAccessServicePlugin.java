// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;
import java.util.List;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.hapi.block.node.SingleBlockRequest;
import org.hiero.hapi.block.node.SingleBlockResponse;
import org.hiero.hapi.block.node.SingleBlockResponseCode;

/**
 * Plugin that implements the BlockAccessService and provides the 'singleBlock' RPC.
 */
public class BlockAccessServicePlugin implements BlockNodePlugin, ServiceInterface {

    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The block provider */
    private HistoricalBlockFacility blockProvider;
    /** Counter for the number of requests */
    private Counter requestCounter;
    /** Counter for the number of requests Success */
    private Counter requestCounterSuccess;
    /** Counter for the number of requests */
    private Counter requestCounterNotAvailable;
    /** Counter for the number of requests */
    private Counter requestCounterNotFound;

    /**
     * Handle a request for a single block
     *
     * @param request the request containing the block number or latest flag
     * @return the response containing the block or an error status
     */
    private SingleBlockResponse handleSingleBlockRequest(SingleBlockRequest request) {
        LOGGER.log(
                System.Logger.Level.DEBUG, "Received SingleBlockRequest for block number: {0}", request.blockNumber());
        requestCounter.increment();

        try {
            // Validate request
            if (request.retrieveLatest() && request.blockNumber() != 0) {
                //LOGGER.log(System.Logger.Level.WARNING, "Invalid request: both block_number and retrieve_latest set");
                //requestCounterNotFound.increment();
                //return new SingleBlockResponse(SingleBlockResponseCode.READ_BLOCK_NOT_FOUND, null);

                LOGGER.log(
                        System.Logger.Level.WARNING,
                        "Both block_number and retrieve_latest set. Using retrieve_latest instead of block_number: {0}",
                        request.blockNumber());
            }
            long blockNumberToRetrieve;

            // if retrieveLatest is set, get the latest block number
            if (request.retrieveLatest()) {
                blockNumberToRetrieve = blockProvider.latestBlockNumber();
                if (blockNumberToRetrieve < 0) {
                    LOGGER.log(System.Logger.Level.WARNING, "Latest block number not available");
                    requestCounterNotAvailable.increment();
                    return new SingleBlockResponse(SingleBlockResponseCode.READ_BLOCK_NOT_AVAILABLE, null);
                }
            } else {
                blockNumberToRetrieve = request.blockNumber();
            }

            // Check if block is within available range
            long lowestBlockNumber = blockProvider.oldestBlockNumber();
            long highestBlockNumber = blockProvider.latestBlockNumber();

            if (blockNumberToRetrieve < lowestBlockNumber || blockNumberToRetrieve > highestBlockNumber) {
                LOGGER.log(
                        System.Logger.Level.WARNING,
                        "Requested block {0} is outside available range [{1}, {2}]",
                        blockNumberToRetrieve,
                        lowestBlockNumber,
                        highestBlockNumber);
                requestCounterNotAvailable.increment();
                return new SingleBlockResponse(SingleBlockResponseCode.READ_BLOCK_NOT_AVAILABLE, null);
            }

            // Retrieve the block
            Block block = blockProvider.block(blockNumberToRetrieve).block();
            requestCounterSuccess.increment();
            return new SingleBlockResponse(SingleBlockResponseCode.READ_BLOCK_SUCCESS, block);

        } catch (Exception e) {
            LOGGER.log(System.Logger.Level.ERROR, "Failed to retrieve block number: {0}", request.blockNumber());
            requestCounterNotFound.increment();
            return new SingleBlockResponse(SingleBlockResponseCode.READ_BLOCK_NOT_FOUND, null);
        }
    }


    //<editor-fold desc="BlockNodePlugin Methods">
    // ==== BlockNodePlugin Methods ====================================================================================
    @Override
    public String name() {
        return "BlockAccessServicePlugin";
    }

    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        // Create the metrics
        requestCounter = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "single-block-requests")
                        .withDescription("Number of single block requests"));
        requestCounterSuccess = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "single-block-requests-success")
                        .withDescription("Number of successful single block requests"));
        requestCounterNotAvailable = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "single-block-requests-not-available")
                        .withDescription("Number of single block requests that were not available"));
        requestCounterNotFound = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "single-block-requests-not-found")
                        .withDescription("Number of single block requests that were not found"));
        // Get the block provider
        this.blockProvider = context.historicalBlockProvider();
        // Register this service
        serviceBuilder.registerGrpcService(this);
    }

    //</editor-fold>

    //<editor-fold desc="ServiceInterface Methods">
    // ==== ServiceInterface Methods ===================================================================================

    /**
     * BlockAccessService methods define the gRPC methods available on the BlockAccessService.
     */
    enum BlockAccessServiceMethod implements Method {
        /**
         * The singleBlock method retrieves a single block from the block node.
         */
        singleBlock
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public String serviceName() {
        return "BlockAccessService";
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public String fullName() {
        return "com.hedera.hapi.block." + serviceName();
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public List<Method> methods() {
        return Arrays.asList(BlockAccessServiceMethod.values());
    }

    /**
     * {@inheritDoc}
     *
     * This is called each time a new request is received.
     */
    @NonNull
    @Override
    public Pipeline<? super Bytes> open(
            @NonNull Method method, @NonNull RequestOptions requestOptions, @NonNull Pipeline<? super Bytes> pipeline)
            throws GrpcException {
        final BlockAccessServiceMethod blockAccessServiceMethod = (BlockAccessServiceMethod) method;
        return switch (blockAccessServiceMethod) {
            case singleBlock:
                yield Pipelines.<SingleBlockRequest, SingleBlockResponse>unary()
                        .mapRequest(SingleBlockRequest.PROTOBUF::parse)
                        .method(this::handleSingleBlockRequest)
                        .mapResponse(SingleBlockResponse.PROTOBUF::toBytes)
                        .respondTo(pipeline)
                        .build();
        };
    }

    //</editor-fold>
}
