// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

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
import org.hiero.block.api.BlockRequest;
import org.hiero.block.api.BlockResponse;
import org.hiero.block.api.BlockResponse.Code;
// PBJ doesn't generate GRPC stubs for some reason, also the proto file is broken when PBJ compiles it...
import org.hiero.block.api.protoc.BlockAccessServiceGrpc;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

/**
 * Plugin that implements the BlockAccessService and provides the 'block' RPC.
 */
public class BlockAccessServicePlugin implements BlockNodePlugin, ServiceInterface {

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

    /**
     * Handle a request for a single block
     *
     * @param request the request containing the block number or latest flag
     * @return the response containing the block or an error status
     */
    private BlockResponse handleBlockRequest(BlockRequest request) {
        LOGGER.log(DEBUG, "Received BlockRequest for block number: {0}", request.blockNumber());
        requestCounter.increment();

        try {
            // Log and return an error if both block_number and retrieve_latest are set.
            // blockNumber is considered set if it is different from 0 (default)
            if (request.retrieveLatest() && request.blockNumber() != 0) {
                LOGGER.log(
                        INFO,
                        "Both block_number and retrieve_latest set. Using retrieve_latest instead of block_number: {0}",
                        request.blockNumber());
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

            // Check if block is within available range
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

    // ==== ServiceInterface Methods ===================================================================================
    /**
     * BlockAccessService methods define the gRPC methods available on the BlockAccessService.
     */
    enum BlockAccessServiceMethod implements Method {
        /**
         * The getBlock method retrieves a single block from the block node.
         */
        getBlock
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public String serviceName() {
        String[] parts = fullName().split("\\.");
        return parts[parts.length - 1];
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public String fullName() {
        return BlockAccessServiceGrpc.SERVICE_NAME;
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
            case getBlock:
                yield Pipelines.<BlockRequest, BlockResponse>unary()
                        .mapRequest(BlockRequest.PROTOBUF::parse)
                        .method(this::handleBlockRequest)
                        .mapResponse(BlockResponse.PROTOBUF::toBytes)
                        .respondTo(pipeline)
                        .build();
        };
    }
}
