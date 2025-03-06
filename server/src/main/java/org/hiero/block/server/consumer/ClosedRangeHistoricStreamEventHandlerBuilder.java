// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.consumer;

import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.SubscribeStreamResponseUnparsed;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.config.api.Configuration;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.persistence.storage.read.BlockReader;

/**
 * Use this builder to create a closed range historic stream event handler.
 */
public final class ClosedRangeHistoricStreamEventHandlerBuilder {
    private ClosedRangeHistoricStreamEventHandlerBuilder() {}

    /**
     * Create a new instance of a closed range historic stream event handler.
     *
     * @param startBlockNumber - the start of the requested range of blocks
     * @param endBlockNumber - the end of the requested range of blocks
     * @param blockReader - the block reader to query for blocks
     * @param helidonConsumerObserver - the consumer observer used to send data to the consumer
     * @param metricsService - the service responsible for handling metrics
     * @param configuration - the configuration settings for the block node
     * @return a new instance of a closed range historic stream event handler
     */
    @NonNull
    public static Runnable build(
            long startBlockNumber,
            long endBlockNumber,
            @NonNull final BlockReader<BlockUnparsed> blockReader,
            @NonNull final Pipeline<? super SubscribeStreamResponseUnparsed> helidonConsumerObserver,
            @NonNull final MetricsService metricsService,
            @NonNull final Configuration configuration) {

        return new HistoricBlockStreamSupplier(
                startBlockNumber, endBlockNumber, blockReader, helidonConsumerObserver, metricsService, configuration);
    }
}
