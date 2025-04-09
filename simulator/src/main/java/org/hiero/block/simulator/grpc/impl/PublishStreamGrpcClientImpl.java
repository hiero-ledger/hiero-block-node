// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc.impl;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.util.Objects.requireNonNull;
import static org.hiero.block.simulator.metrics.SimulatorMetricTypes.Counter.LiveBlockItemsSent;
import static org.hiero.block.simulator.metrics.SimulatorMetricTypes.Counter.LiveBlocksSent;

import com.hedera.hapi.block.protoc.BlockItemSet;
import com.hedera.hapi.block.protoc.BlockStreamServiceGrpc;
import com.hedera.hapi.block.protoc.PublishStreamRequest;
import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import org.hiero.block.common.utils.ChunkUtils;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.hiero.block.simulator.generator.BlockStreamManager;
import org.hiero.block.simulator.grpc.PublishStreamGrpcClient;
import org.hiero.block.simulator.metrics.MetricsService;
import org.hiero.block.simulator.startup.SimulatorStartupData;

/**
 * Implementation of {@link PublishStreamGrpcClient} that handles the publication of blocks
 * via gRPC streaming. This implementation manages the connection to the server, handles
 * block chunking, and tracks metrics related to block publication.
 */
public class PublishStreamGrpcClientImpl implements PublishStreamGrpcClient {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    // Configuration
    private final BlockStreamConfig blockStreamConfig;
    private final GrpcConfig grpcConfig;

    // Service dependencies
    private final MetricsService metricsService;

    // gRPC components
    private ManagedChannel channel;
    private StreamObserver<PublishStreamRequest> requestStreamObserver;
    private BlockStreamServiceGrpc.BlockStreamServiceStub stub;

    // State
    private final AtomicBoolean streamEnabled;
    private final int lastKnownStatusesCapacity;
    private final Deque<String> lastKnownStatuses;
    private final SimulatorStartupData startupData;
    private final BlockStreamManager blockStreamManager;

    /**
     * Creates a new PublishStreamGrpcClientImpl with the specified dependencies.
     *
     * @param grpcConfig The configuration for gRPC connection settings
     * @param blockStreamConfig The configuration for block streaming parameters
     * @param metricsService The service for recording publication metrics
     * @param streamEnabled Flag controlling stream state
     * @param startupData The startup data for the simulator
     * @throws NullPointerException if any parameter is null
     */
    @Inject
    public PublishStreamGrpcClientImpl(
            @NonNull final GrpcConfig grpcConfig,
            @NonNull final BlockStreamConfig blockStreamConfig,
            @NonNull final MetricsService metricsService,
            @NonNull final AtomicBoolean streamEnabled,
            @NonNull final SimulatorStartupData startupData,
            @NonNull final BlockStreamManager blockStreamManager) {
        this.grpcConfig = requireNonNull(grpcConfig);
        this.blockStreamConfig = requireNonNull(blockStreamConfig);
        this.metricsService = requireNonNull(metricsService);
        this.streamEnabled = requireNonNull(streamEnabled);
        this.lastKnownStatusesCapacity = blockStreamConfig.lastKnownStatusesCapacity();
        this.blockStreamManager = blockStreamManager;
        this.lastKnownStatuses = new ArrayDeque<>(this.lastKnownStatusesCapacity);
        this.startupData = requireNonNull(startupData);
    }

    /**
     * Initializes the gRPC channel and creates the publishing stream.
     */
    @Override
    public void init() {
        channel = ManagedChannelBuilder.forAddress(grpcConfig.serverAddress(), grpcConfig.port())
                .usePlaintext()
                .build();
        stub = BlockStreamServiceGrpc.newStub(channel);
        createNewStreamObserver();
        lastKnownStatuses.clear();
    }

    private void createNewStreamObserver() {
        final PublishStreamObserver publishStreamObserver = new PublishStreamObserver(
                startupData, streamEnabled, lastKnownStatuses, lastKnownStatusesCapacity, this);
        requestStreamObserver = stub.publishBlockStream(publishStreamObserver);
    }

    @Override
    public void recoverStream() {
        int maxRetries = 5;
        long baseBackoff = 2000;

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                long waitMillis = Math.min(60000, (long) Math.pow(2, attempt) * baseBackoff);
                LOGGER.log(
                        INFO, "Attempting to recover stream (attempt " + (attempt + 1) + ") in " + waitMillis + " ms");
                Thread.sleep(waitMillis);

                resetBlockPointer();
                createNewStreamObserver();
                streamEnabled.set(true);
                LOGGER.log(INFO, "Stream recovered successfully on attempt " + (attempt + 1));
                return;
            } catch (Exception e) {
                LOGGER.log(ERROR, "Stream recovery failed on attempt " + (attempt + 1), e);
            }
        }

        LOGGER.log(ERROR, "Max retries reached. Stream could not be recovered.");
        streamEnabled.set(false);
    }

    private void resetBlockPointer() throws BlockSimulatorParsingException, IOException {
        long lastAckedBlock = startupData.getLatestAckBlockNumber();
        LOGGER.log(INFO, "Resetting block pointer. Last acknowledged block: " + lastAckedBlock);

        switch (blockStreamConfig.recoveryMode()) {
            case RESEND_LAST -> {
                LOGGER.log(INFO, "Recovery strategy: RESEND_LAST");
                Block block = blockStreamManager.getLastBlock();
                if (block != null) {
                    streamBlock(block);
                } else {
                    LOGGER.log(INFO, "Block #" + lastAckedBlock + " not found in stream. Skipping resend.");
                }
            }

            case ROLLBACK -> {
                LOGGER.log(INFO, "Recovery strategy: ROLLBACK");
                long rollbackBlockNumber = Math.max(0, lastAckedBlock - grpcConfig.rollbackDistance());
                LOGGER.log(INFO, "Rolling back to block #" + rollbackBlockNumber);

                Block block = blockStreamManager.getBlockByNumber(rollbackBlockNumber);
                while (block != null && rollbackBlockNumber <= lastAckedBlock) {
                    streamBlock(block);
                    rollbackBlockNumber++;
                    block = blockStreamManager.getBlockByNumber(rollbackBlockNumber);
                }
            }

            case SKIP_AHEAD -> {
                LOGGER.log(INFO, "Recovery strategy: SKIP_AHEAD");
                Block block = blockStreamManager.getNextBlock();
                if (block != null) {
                    streamBlock(block);
                }
            }
        }
    }

    /**
     * Streams a list of block items to the server.
     *
     * @param blockItems The list of block items to stream
     * @return true if streaming should continue, false if streaming should stop
     */
    @Override
    public boolean streamBlockItem(List<BlockItem> blockItems) {
        if (streamEnabled.get()) {
            requestStreamObserver.onNext(PublishStreamRequest.newBuilder()
                    .setBlockItems(BlockItemSet.newBuilder()
                            .addAllBlockItems(blockItems)
                            .build())
                    .build());

            metricsService.get(LiveBlockItemsSent).add(blockItems.size());
            LOGGER.log(
                    INFO,
                    "Number of block items sent: "
                            + metricsService.get(LiveBlockItemsSent).get());
        } else {
            LOGGER.log(ERROR, "Not allowed to send next batch of block items");
        }

        return streamEnabled.get();
    }

    /**
     * Streams a complete block to the server, chunking it if necessary based on configuration.
     *
     * @param block The block to stream
     * @return true if streaming should continue, false if streaming should stop
     */
    @Override
    public boolean streamBlock(Block block) {
        List<List<BlockItem>> streamingBatches =
                ChunkUtils.chunkify(block.getItemsList(), blockStreamConfig.blockItemsBatchSize());
        for (List<BlockItem> streamingBatch : streamingBatches) {
            if (streamEnabled.get()) {
                requestStreamObserver.onNext(PublishStreamRequest.newBuilder()
                        .setBlockItems(BlockItemSet.newBuilder()
                                .addAllBlockItems(streamingBatch)
                                .build())
                        .build());
                metricsService.get(LiveBlockItemsSent).add(streamingBatch.size());
                LOGGER.log(
                        DEBUG,
                        "Number of block items sent: "
                                + metricsService.get(LiveBlockItemsSent).get());
            } else {
                LOGGER.log(ERROR, "Not allowed to send next batch of block items");
                break;
            }
        }
        metricsService.get(LiveBlocksSent).increment();
        return streamEnabled.get();
    }

    /**
     * Sends a onCompleted message to the server and waits for a short period of
     * time to ensure the message is sent.
     */
    @Override
    public void completeStreaming() {
        requestStreamObserver.onCompleted();
    }

    /**
     * Gets the number of published blocks.
     *
     * @return the number of published blocks
     */
    @Override
    public long getPublishedBlocks() {
        return metricsService.get(LiveBlocksSent).get();
    }

    /**
     * Gets the last known statuses.
     *
     * @return the last known statuses
     */
    @Override
    public List<String> getLastKnownStatuses() {
        return List.copyOf(lastKnownStatuses);
    }

    /**
     * Shutdowns the channel.
     *
     * @throws InterruptedException if the thread is interrupted
     */
    @Override
    public void shutdown() throws InterruptedException {
        completeStreaming();
        channel.shutdown();
    }
}
