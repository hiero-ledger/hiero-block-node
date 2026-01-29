// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.api.BlockAccessServiceInterface;
import org.hiero.block.api.BlockRequest;
import org.hiero.block.api.BlockResponse;
import org.hiero.block.api.BlockResponse.Code;
import org.hiero.block.internal.BlockResponseUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
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
     * {@inheritDoc}
     * <p>
     * Override to use unparsed response type, avoiding full block parsing.
     * This prevents parse failures when proto versions differ between CN and BN,
     * and improves performance by not parsing block items unnecessarily.
     */
    @Override
    @NonNull
    public Pipeline<? super Bytes> open(
            @NonNull final Method method,
            @NonNull final RequestOptions options,
            @NonNull final Pipeline<? super Bytes> replies) {
        final BlockAccessServiceMethod blockAccessServiceMethod = (BlockAccessServiceMethod) method;
        return switch (blockAccessServiceMethod) {
            case getBlock ->
                Pipelines.<BlockRequest, BlockResponseUnparsed>unary()
                        .mapRequest(bytes -> BlockRequest.PROTOBUF.parse(bytes.toReadableSequentialData()))
                        .method(this::getBlockUnparsed)
                        .mapResponse(BlockResponseUnparsed.PROTOBUF::toBytes)
                        .respondTo(replies)
                        .build();
        };
    }

    /**
     * Handle a request for a single block, returning unparsed block data.
     *
     * @param request the request containing the block number or latest flag
     * @return the response containing the unparsed block or an error status
     */
    private BlockResponseUnparsed getBlockUnparsed(BlockRequest request) {
        requestCounter.increment();
        try {
            long blockNumberToRetrieve;
            // The block number and retrieve latest are mutually exclusive in
            // the proto definition, so no need to check for that here.
            // if retrieveLatest is set, or the request is for the largest possible block number, get the latest block.
            if (request.hasBlockNumber() && request.blockNumber() >= 0) {
                blockNumberToRetrieve = request.blockNumber();
                LOGGER.log(TRACE, "Received `block_number` BlockRequest, retrieving block: {0}", blockNumberToRetrieve);
            } else if ((request.hasRetrieveLatest() && request.retrieveLatest())
                    || (request.hasBlockNumber() && request.blockNumber() == -1)) {
                blockNumberToRetrieve = blockProvider.availableBlocks().max();
                LOGGER.log(
                        TRACE, "Received 'retrieveLatest' BlockRequest, retrieving block={0}", blockNumberToRetrieve);
            } else {
                LOGGER.log(INFO, "Invalid request, 'retrieve_latest' or a valid 'block number' is required.");
                return new BlockResponseUnparsed(Code.INVALID_REQUEST, null);
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
                return new BlockResponseUnparsed(Code.NOT_AVAILABLE, null);
            }
            // Retrieve the block
            try (final BlockAccessor accessor = blockProvider.block(blockNumberToRetrieve)) {
                if (accessor != null) {
                    // Use blockUnparsed() to avoid full parsing of block items
                    final BlockUnparsed block = accessor.blockUnparsed();
                    if (block != null) {
                        responseCounterSuccess.increment();
                        return new BlockResponseUnparsed(Code.SUCCESS, block);
                    }
                }
                responseCounterNotFound.increment();
                return new BlockResponseUnparsed(Code.NOT_FOUND, null);
            }
        } catch (final RuntimeException e) {
            final String message = "Failed to retrieve block number %d.".formatted(request.blockNumber());
            LOGGER.log(ERROR, message, e);
            responseCounterNotFound.increment();
            return new BlockResponseUnparsed(Code.NOT_FOUND, null);
        }
    }

    // ==== "dead" method required by the interface ====
    @Override
    public BlockResponse getBlock(BlockRequest request) {
        // do nothing; in order to use unparsed alternatives we must override
        // open instead of this method.
        return null;
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
