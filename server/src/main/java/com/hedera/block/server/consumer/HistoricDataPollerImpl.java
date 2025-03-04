// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.consumer;

import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Counter.ClosedRangeHistoricBlocksRetrieved;
import static java.lang.System.Logger.Level.TRACE;

import com.hedera.block.common.utils.ChunkUtils;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.persistence.storage.read.BlockReader;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.swirlds.config.api.Configuration;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HistoricDataPollerImpl is responsible for polling historic data from the block reader.
 */
public class HistoricDataPollerImpl implements HistoricDataPoller<List<BlockItemUnparsed>> {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final AtomicLong currentIndex = new AtomicLong(0);

    private final BlockReader<BlockUnparsed> blockReader;
    private final MetricsService metricsService;
    private final int maxBlockItemBatchSize;

    private final Queue<List<BlockItemUnparsed>> blockItemBatches = new LinkedList<>();

    public HistoricDataPollerImpl(
            @NonNull final BlockReader<BlockUnparsed> blockReader,
            @NonNull final MetricsService metricsService,
            @NonNull final Configuration configuration) {

        this.blockReader = Objects.requireNonNull(blockReader);
        this.metricsService = Objects.requireNonNull(metricsService);
        this.maxBlockItemBatchSize =
                configuration.getConfigData(ConsumerConfig.class).maxBlockItemBatchSize();
    }

    /**
     * Initializes the poller with the block number to start polling from.
     *
     * @param blockNumber the block number to start polling from
     */
    @Override
    public void init(long blockNumber) {
        currentIndex.set(blockNumber);
        blockItemBatches.clear();
    }

    /**
     * Polls the block reader for historic data.
     *
     * @return an optional of block item batches. An empty optional is returned if no data is available.
     * @throws Exception if an error occurs while polling
     */
    @Override
    public Optional<List<BlockItemUnparsed>> poll() throws Exception {

        if (blockItemBatches.isEmpty()) {
            fetchData();
        }

        final List<BlockItemUnparsed> batch = blockItemBatches.poll();
        if (batch == null) {
            LOGGER.log(TRACE, "Returning block item batch of size: 0");
            return Optional.empty();
        }

        LOGGER.log(TRACE, "Returning block item batch of size: {0}", batch.size());
        return Optional.of(batch);
    }

    private void fetchData() throws Exception {
        LOGGER.log(TRACE, "Fetching historic data for block number: {0}", currentIndex.get());
        final Optional<BlockUnparsed> blockOpt = blockReader.read(currentIndex.get());
        if (blockOpt.isPresent()) {
            metricsService.get(ClosedRangeHistoricBlocksRetrieved).increment();
            List<List<BlockItemUnparsed>> blockItems =
                    ChunkUtils.chunkify(blockOpt.get().blockItems(), maxBlockItemBatchSize);
            LOGGER.log(
                    TRACE, "Found {0} block item batches for block number: {1}", blockItems.size(), currentIndex.get());
            blockItemBatches.addAll(blockItems);

            // Only increment once we've successfully fetched the data
            currentIndex.incrementAndGet();
        }

        LOGGER.log(TRACE, "No historic data found for block number: {0}", currentIndex.get());
    }
}
