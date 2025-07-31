// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc.impl;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.util.Objects.requireNonNull;
import static org.hiero.block.simulator.metrics.SimulatorMetricTypes.Counter.LiveBlockItemsSent;
import static org.hiero.block.simulator.metrics.SimulatorMetricTypes.Counter.LiveBlocksSent;

import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.inject.Inject;
import org.hiero.block.api.protoc.BlockItemSet;
import org.hiero.block.api.protoc.BlockStreamPublishServiceGrpc;
import org.hiero.block.api.protoc.PublishStreamRequest;
import org.hiero.block.api.protoc.PublishStreamRequest.EndStream;
import org.hiero.block.api.protoc.PublishStreamRequest.EndStream.Code;
import org.hiero.block.api.protoc.PublishStreamResponse;
import org.hiero.block.common.utils.ChunkUtils;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.hiero.block.simulator.config.types.EndStreamMode;
import org.hiero.block.simulator.config.types.MidBlockFailType;
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

    // State
    private final AtomicBoolean streamEnabled;
    private final int lastKnownStatusesCapacity;
    private final Deque<String> lastKnownStatuses;
    private final SimulatorStartupData startupData;

    private final PublishStreamObserver publishStreamObserver;

    /**
     * Creates a new PublishStreamGrpcClientImpl with the specified dependencies.
     *
     * @param grpcConfig        The configuration for gRPC connection settings
     * @param blockStreamConfig The configuration for block streaming parameters
     * @param metricsService    The service for recording publication metrics
     * @param streamEnabled     Flag controlling stream state
     * @param startupData       The startup data for the simulator
     * @throws NullPointerException if any parameter is null
     */
    @Inject
    public PublishStreamGrpcClientImpl(
            @NonNull final GrpcConfig grpcConfig,
            @NonNull final BlockStreamConfig blockStreamConfig,
            @NonNull final MetricsService metricsService,
            @NonNull final AtomicBoolean streamEnabled,
            @NonNull final SimulatorStartupData startupData) {
        this.grpcConfig = requireNonNull(grpcConfig);
        this.blockStreamConfig = requireNonNull(blockStreamConfig);
        this.metricsService = requireNonNull(metricsService);
        this.streamEnabled = requireNonNull(streamEnabled);
        this.lastKnownStatusesCapacity = blockStreamConfig.lastKnownStatusesCapacity();
        this.lastKnownStatuses = new ArrayDeque<>(this.lastKnownStatusesCapacity);
        this.startupData = requireNonNull(startupData);
        this.publishStreamObserver =
                new PublishStreamObserver(startupData, streamEnabled, lastKnownStatuses, lastKnownStatusesCapacity);
    }

    /**
     * Initializes the gRPC channel and creates the publishing stream.
     */
    @Override
    public void init() {
        channel = ManagedChannelBuilder.forAddress(grpcConfig.serverAddress(), grpcConfig.port())
                .usePlaintext()
                .build();
        final BlockStreamPublishServiceGrpc.BlockStreamPublishServiceStub stub =
                BlockStreamPublishServiceGrpc.newStub(channel);
        requestStreamObserver = stub.publishBlockStream(publishStreamObserver);
        lastKnownStatuses.clear();
    }

    /**
     * Streams a complete block to the server, chunking it if necessary based on configuration.
     *
     * @param block The block to stream
     * @return true if streaming should continue, false if streaming should stop
     */
    @Override
    public boolean streamBlock(
            Block block, @NonNull final Consumer<PublishStreamResponse> publishStreamResponseConsumer) {
        List<List<BlockItem>> streamingBatches =
                ChunkUtils.chunkify(block.getItemsList(), blockStreamConfig.blockItemsBatchSize());
        for (List<BlockItem> streamingBatch : streamingBatches) {
            if (streamEnabled.get()) {
                handleMidBlockFailIfSet(streamingBatch);
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
                publishStreamResponseConsumer.accept(publishStreamObserver.getPublishStreamResponse());
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

    private void handleMidBlockFailIfSet(@NonNull final List<BlockItem> streamingBatch) {
        requireNonNull(streamingBatch);
        if (blockStreamConfig.midBlockFailType() == MidBlockFailType.NONE
                || streamingBatch.size() < 3
                || blockStreamConfig.midBlockFailOffset()
                        != metricsService.get(LiveBlocksSent).get()) {
            return;
        }
        final int failIndex = new Random().nextInt(1, streamingBatch.size() - 1);
        final List<BlockItem> streamingBatchBeforeFail = streamingBatch.subList(0, failIndex);
        requestStreamObserver.onNext(PublishStreamRequest.newBuilder()
                .setBlockItems(BlockItemSet.newBuilder()
                        .addAllBlockItems(streamingBatchBeforeFail)
                        .build())
                .build());
        if (blockStreamConfig.midBlockFailType() == MidBlockFailType.ABRUPT) {
            throw new RuntimeException("Configured abrupt disconnection occurred");
        }
        requestStreamObserver.onError(new Exception("Configured failure occurred, calling onError()"));
    }

    public void handleEndStreamModeIfSet() {
        if (blockStreamConfig.endStreamMode() == EndStreamMode.TOO_FAR_BEHIND) {
            requestStreamObserver.onNext(PublishStreamRequest.newBuilder()
                    .setEndStream(EndStream.newBuilder()
                            .setEndCode(Code.TOO_FAR_BEHIND)
                            .build())
                    .build());
        }
    }
}
