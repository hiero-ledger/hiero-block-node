// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.mode.impl;

import static java.lang.System.Logger.Level.INFO;
import static java.util.Objects.requireNonNull;
import static org.hiero.block.simulator.Constants.NANOS_PER_MILLI;
import static org.hiero.block.simulator.metrics.SimulatorMetricTypes.Counter.LiveBlockItemsSent;
import static org.hiero.block.simulator.metrics.SimulatorMetricTypes.Counter.LiveBlocksSent;

import com.hedera.hapi.block.stream.protoc.Block;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import org.hiero.block.api.protoc.PublishStreamResponse;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.types.EndStreamMode;
import org.hiero.block.simulator.config.types.StreamingMode;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.hiero.block.simulator.generator.BlockStreamManager;
import org.hiero.block.simulator.grpc.PublishStreamGrpcClient;
import org.hiero.block.simulator.metrics.MetricsService;
import org.hiero.block.simulator.mode.SimulatorModeHandler;

/**
 * The {@code PublisherModeHandler} class implements the
 * {@link SimulatorModeHandler} interface
 * and provides the behavior for a mode where simulator is working using PublishBlockStream and acts as a client.
 *
 * <p>
 * This mode handles single operation in the block streaming process, utilizing
 * the
 * {@link BlockStreamConfig} for configuration settings. It is designed for
 * scenarios where
 * the simulator needs to handle publication of blocks.
 */
public class PublisherClientModeHandler implements SimulatorModeHandler {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    // Configuration fields
    private final BlockStreamConfig blockStreamConfig;
    private final StreamingMode streamingMode;
    private final int delayBetweenBlockItems;
    private final int millisecondsPerBlock;

    // Service dependencies
    private final BlockStreamManager blockStreamManager;
    private final PublishStreamGrpcClient publishStreamGrpcClient;
    private final MetricsService metricsService;

    // State fields
    private final AtomicBoolean shouldPublish;

    private PublishClientManager publishClientManager;

    /**
     * Constructs a new {@code PublisherModeHandler} with the specified dependencies.
     *
     * @param blockStreamConfig       The configuration for block streaming parameters
     * @param publishStreamGrpcClient The client for publishing blocks via gRPC
     * @param blockStreamManager      The manager responsible for block generation
     * @param metricsService          The service for recording metrics
     * @throws NullPointerException if any parameter is null
     */
    @Inject
    public PublisherClientModeHandler(
            @NonNull final BlockStreamConfig blockStreamConfig,
            @NonNull final PublishStreamGrpcClient publishStreamGrpcClient,
            @NonNull final BlockStreamManager blockStreamManager,
            @NonNull final MetricsService metricsService) {
        this.blockStreamConfig = requireNonNull(blockStreamConfig);
        this.publishStreamGrpcClient = requireNonNull(publishStreamGrpcClient);
        this.blockStreamManager = requireNonNull(blockStreamManager);
        this.metricsService = requireNonNull(metricsService);

        streamingMode = blockStreamConfig.streamingMode();
        delayBetweenBlockItems = blockStreamConfig.delayBetweenBlockItems();
        millisecondsPerBlock = blockStreamConfig.millisecondsPerBlock();
        shouldPublish = new AtomicBoolean(true);
    }

    public void init() {
        blockStreamManager.init();
        publishStreamGrpcClient.init();
        LOGGER.log(INFO, "gRPC Channel initialized for publishing blocks.");
    }

    /**
     * Starts the simulator and initiate streaming, depending on the working mode.
     *
     * @throws BlockSimulatorParsingException if an error occurs while parsing
     *                                        blocks
     * @throws IOException                    if an I/O error occurs during block
     *                                        streaming
     * @throws InterruptedException           if the thread running the simulator is
     *                                        interrupted
     */
    @Override
    public void start() throws BlockSimulatorParsingException, IOException, InterruptedException {
        LOGGER.log(INFO, "Block Stream Simulator is starting in publisher mode.");
        try {
            if (streamingMode == StreamingMode.MILLIS_PER_BLOCK) {
                millisPerBlockStreaming();
            } else {
                constantRateStreaming();
            }
        } finally {
            publishStreamGrpcClient.shutdown();
        }
        LOGGER.log(INFO, "Block Stream Simulator has stopped streaming.");
    }

    private void millisPerBlockStreaming() throws IOException, InterruptedException, BlockSimulatorParsingException {
        final long secondsPerBlockNanos = (long) millisecondsPerBlock * NANOS_PER_MILLI;
        PublishStreamResponse[] responseHolder = new PublishStreamResponse[1];
        long blockCount = 0;
        long endStreamInterval = blockStreamConfig.endStreamFrequency();

        Block nextBlock = blockStreamManager.getNextBlock();
        while (nextBlock != null && shouldPublish.get()) {
            blockCount++;
            if (blockStreamConfig.endStreamMode() != EndStreamMode.NONE
                    && endStreamInterval > 0
                    && blockCount % endStreamInterval == 0) {
                publishClientManager.sendEndStream(blockStreamConfig.endStreamMode());
                break;
            }
            long startTime = System.nanoTime();
            if (!publishStreamGrpcClient.streamBlock(nextBlock, response -> responseHolder[0] = response)) {
                PublishStreamResponse publishStreamResponse = responseHolder[0];
                if (publishStreamResponse != null) {
                    publishClientManager.handleResponse(nextBlock, publishStreamResponse);
                }
                break;
            }

            long elapsedTime = System.nanoTime() - startTime;
            long timeToDelay = secondsPerBlockNanos - elapsedTime;
            if (timeToDelay > 0) {
                Thread.sleep(timeToDelay / NANOS_PER_MILLI, (int) (timeToDelay % NANOS_PER_MILLI));
            } else {
                LOGGER.log(
                        System.Logger.Level.WARNING,
                        "Block Server is running behind. Streaming took: "
                                + (elapsedTime / 1_000_000)
                                + "ms - Longer than max expected of: "
                                + millisecondsPerBlock
                                + " milliseconds");
            }
            nextBlock = blockStreamManager.getNextBlock();
        }
        LOGGER.log(INFO, "Block Stream Simulator has stopped");
        LOGGER.log(
                INFO,
                "Number of BlockItems sent by the Block Stream Simulator: "
                        + metricsService.get(LiveBlockItemsSent).get());
        LOGGER.log(
                INFO,
                "Number of Blocks sent by the Block Stream Simulator: "
                        + metricsService.get(LiveBlocksSent).get());
    }

    private void constantRateStreaming() throws InterruptedException, IOException, BlockSimulatorParsingException {
        int delayMSBetweenBlockItems = delayBetweenBlockItems / NANOS_PER_MILLI;
        int delayNSBetweenBlockItems = delayBetweenBlockItems % NANOS_PER_MILLI;
        int blockItemsStreamed = 0;
        PublishStreamResponse[] responseHolder = new PublishStreamResponse[1];
        long blockCount = 0;
        long endStreamInterval = blockStreamConfig.endStreamFrequency();

        while (shouldPublish.get()) {
            // get block
            Block block = blockStreamManager.getNextBlock();

            if (block == null) {
                LOGGER.log(INFO, "Block Stream Simulator has reached the end of the block items");
                break;
            }
            blockCount++;
            if (blockStreamConfig.endStreamMode() != EndStreamMode.NONE
                    && endStreamInterval > 0
                    && blockCount % endStreamInterval == 0) {
                publishClientManager.sendEndStream(blockStreamConfig.endStreamMode());
                break;
            }
            if (!publishStreamGrpcClient.streamBlock(block, response -> responseHolder[0] = response)) {
                PublishStreamResponse publishStreamResponse = responseHolder[0];
                if (publishStreamResponse != null) {
                    publishClientManager.handleResponse(block, publishStreamResponse);
                }
                break;
            }

            blockItemsStreamed += block.getItemsList().size();

            Thread.sleep(delayMSBetweenBlockItems, delayNSBetweenBlockItems);

            if (blockItemsStreamed >= blockStreamConfig.maxBlockItemsToStream()) {
                LOGGER.log(INFO, "Block Stream Simulator has reached the maximum number of block items to" + " stream");
                shouldPublish.set(false);
            }
        }
    }

    /**
     * Stops the handler and manager from streaming.
     */
    @Override
    public void stop() throws InterruptedException {
        shouldPublish.set(false);
        publishStreamGrpcClient.shutdown();
    }

    /**
     * Sets the {@link PublishClientManager} instance to be used by this handler.
     *
     * @param publishClientManager The {@link PublishClientManager} instance to set.
     */
    public void setPublishClientManager(PublishClientManager publishClientManager) {
        this.publishClientManager = publishClientManager;
    }
}
