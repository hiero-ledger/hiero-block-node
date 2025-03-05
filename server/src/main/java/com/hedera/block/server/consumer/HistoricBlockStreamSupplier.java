// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.consumer;

import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Counter.ClosedRangeHistoricBlocksRetrieved;
import static com.hedera.block.server.pbj.PbjBlockStreamServiceProxy.READ_STREAM_NOT_AVAILABLE;
import static com.hedera.block.server.pbj.PbjBlockStreamServiceProxy.READ_STREAM_SUCCESS_RESPONSE;
import static java.lang.System.Logger.Level.ERROR;

import com.hedera.block.common.utils.ChunkUtils;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.persistence.storage.read.BlockReader;
import com.hedera.hapi.block.BlockItemSetUnparsed;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.SubscribeStreamResponseUnparsed;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.config.api.Configuration;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Use this supplier to send historic blocks to the consumer.
 */
class HistoricBlockStreamSupplier implements Runnable {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final long startBlockNumber;
    private final long endBlockNumber;
    private final BlockReader<BlockUnparsed> blockReader;
    private final int maxBlockItemBatchSize;
    private final Pipeline<? super SubscribeStreamResponseUnparsed> helidonConsumerObserver;
    private final MetricsService metricsService;

    /**
     * Create a new instance of HistoricBlockStreamSupplier.
     *
     * @param startBlockNumber - the start of the requested range of blocks
     * @param endBlockNumber - the end of the requested range of blocks
     * @param blockReader - the block reader to query for blocks
     * @param helidonConsumerObserver - the consumer stream response observer to send the blocks
     * @param metricsService - the service responsible for handling metrics
     * @param configuration - the configuration settings for the block node
     */
    public HistoricBlockStreamSupplier(
            long startBlockNumber,
            long endBlockNumber,
            @NonNull final BlockReader<BlockUnparsed> blockReader,
            @NonNull final Pipeline<? super SubscribeStreamResponseUnparsed> helidonConsumerObserver,
            @NonNull final MetricsService metricsService,
            @NonNull final Configuration configuration) {
        this.startBlockNumber = startBlockNumber;
        this.endBlockNumber = endBlockNumber;
        this.blockReader = Objects.requireNonNull(blockReader);

        this.metricsService = Objects.requireNonNull(metricsService);
        final ConsumerConfig consumerConfig =
                Objects.requireNonNull(configuration).getConfigData(ConsumerConfig.class);
        this.maxBlockItemBatchSize = Objects.requireNonNull(consumerConfig).maxBlockItemBatchSize();
        this.helidonConsumerObserver = Objects.requireNonNull(helidonConsumerObserver);
    }

    /**
     * Run the supplier to send the historic blocks to the consumer.
     */
    @Override
    public void run() {
        for (long i = startBlockNumber; i <= endBlockNumber; i++) {
            try {
                if (!send(i)) {
                    LOGGER.log(ERROR, "Block was not found: " + i);
                    sendReadStreamNotAvailable();
                    return;
                }
            } catch (Exception e) {
                LOGGER.log(ERROR, "Exception thrown attempting to send blocks: " + e.getMessage(), e);
                sendReadStreamNotAvailable();
                return;
            }
        }

        // Send a success response to the client
        // to close the stream
        sendSuccessResponse();
    }

    private boolean send(final long currentIndex) throws Exception {

        final Optional<BlockUnparsed> blockOpt = blockReader.read(currentIndex);
        if (blockOpt.isPresent()) {
            metricsService.get(ClosedRangeHistoricBlocksRetrieved).increment();
            List<List<BlockItemUnparsed>> blockItems =
                    ChunkUtils.chunkify(blockOpt.get().blockItems(), maxBlockItemBatchSize);
            sendInBatches(blockItems);
        } else {
            return false;
        }

        return true;
    }

    void sendInBatches(final List<List<BlockItemUnparsed>> blockItems) {
        for (List<BlockItemUnparsed> blockItemsBatch : blockItems) {
            // Prepare the response
            final var subscribeStreamResponse = SubscribeStreamResponseUnparsed.newBuilder()
                    .blockItems(BlockItemSetUnparsed.newBuilder()
                            .blockItems(blockItemsBatch)
                            .build())
                    .build();

            helidonConsumerObserver.onNext(subscribeStreamResponse);
        }
    }

    private void sendReadStreamNotAvailable() {
        try {
            helidonConsumerObserver.onNext(READ_STREAM_NOT_AVAILABLE);
        } catch (Exception e) {
            LOGGER.log(
                    ERROR,
                    "Exception thrown attempting to send READ_STREAM_NOT_AVAILABLE response: " + e.getMessage(),
                    e);
        }
    }

    private void sendSuccessResponse() {
        try {
            // End of stream success
            helidonConsumerObserver.onNext(READ_STREAM_SUCCESS_RESPONSE);
        } catch (Exception e) {
            LOGGER.log(
                    ERROR,
                    "Exception thrown attempting to send READ_STREAM_SUCCESS_RESPONSE response: " + e.getMessage(),
                    e);
        }
    }
}
