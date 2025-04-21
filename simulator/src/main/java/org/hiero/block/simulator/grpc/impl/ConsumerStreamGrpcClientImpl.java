// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc.impl;

import static java.util.Objects.requireNonNull;
import static org.hiero.block.simulator.metrics.SimulatorMetricTypes.Counter.LiveBlocksConsumed;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import javax.inject.Inject;
import org.hiero.block.api.protoc.BlockStreamSubscribeServiceGrpc;
import org.hiero.block.api.protoc.SubscribeStreamRequest;
import org.hiero.block.api.protoc.SubscribeStreamResponse;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.data.ConsumerConfig;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.hiero.block.simulator.grpc.ConsumerStreamGrpcClient;
import org.hiero.block.simulator.metrics.MetricsService;

/**
 * Implementation of {@link ConsumerStreamGrpcClient} that handles the consumption of blocks
 * via gRPC streaming. This implementation manages the connection to the server and tracks
 * metrics related to block consumption.
 */
public class ConsumerStreamGrpcClientImpl implements ConsumerStreamGrpcClient {
    // Configuration
    private final GrpcConfig grpcConfig;
    private final ConsumerConfig consumerConfig;

    // Service dependencies
    private final MetricsService metricsService;

    // gRPC components
    private ManagedChannel channel;
    private BlockStreamSubscribeServiceGrpc.BlockStreamSubscribeServiceStub stub;
    private StreamObserver<SubscribeStreamResponse> consumerStreamObserver;

    // State
    private final int lastKnownStatusesCapacity;
    private final Deque<String> lastKnownStatuses;
    private CountDownLatch streamLatch;

    /**
     * Constructs a new ConsumerStreamGrpcClientImpl with the specified configuration and metrics service.
     *
     * @param grpcConfig The configuration for gRPC connection settings
     * @param blockStreamConfig The configuration for the block stream
     * @param metricsService The service for recording consumption metrics
     * @throws NullPointerException if any parameter is null
     */
    @Inject
    public ConsumerStreamGrpcClientImpl(
            @NonNull final GrpcConfig grpcConfig,
            @NonNull final BlockStreamConfig blockStreamConfig,
            @NonNull final ConsumerConfig consumerConfig,
            @NonNull final MetricsService metricsService) {
        this.grpcConfig = requireNonNull(grpcConfig);
        this.metricsService = requireNonNull(metricsService);
        this.consumerConfig = requireNonNull(consumerConfig);
        this.lastKnownStatusesCapacity = blockStreamConfig.lastKnownStatusesCapacity();
        this.lastKnownStatuses = new ArrayDeque<>(lastKnownStatusesCapacity);
    }

    @Override
    public void init() {
        channel = ManagedChannelBuilder.forAddress(grpcConfig.serverAddress(), grpcConfig.port())
                .usePlaintext()
                .build();
        stub = BlockStreamSubscribeServiceGrpc.newStub(channel);
        lastKnownStatuses.clear();
        streamLatch = new CountDownLatch(1);
    }

    @Override
    public void requestBlocks(long startBlock, long endBlock) throws InterruptedException {
        Preconditions.requireWhole(startBlock);
        Preconditions.requireWhole(endBlock);
        Preconditions.requireGreaterOrEqual(endBlock, startBlock);

        consumerStreamObserver =
                new ConsumerStreamObserver(metricsService, streamLatch, lastKnownStatuses, lastKnownStatusesCapacity);

        SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                .setStartBlockNumber(startBlock)
                .setEndBlockNumber(endBlock)
                .setAllowUnverified(true)
                .build();
        stub.subscribeBlockStream(request, consumerStreamObserver);

        streamLatch.await();
    }

    @Override
    public void requestBlocks() throws InterruptedException {
        consumerStreamObserver =
                new ConsumerStreamObserver(metricsService, streamLatch, lastKnownStatuses, lastKnownStatusesCapacity);

        SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                .setStartBlockNumber(consumerConfig.startBlockNumber())
                .setEndBlockNumber(consumerConfig.endBlockNumber())
                .setAllowUnverified(true)
                .build();
        stub.subscribeBlockStream(request, consumerStreamObserver);

        streamLatch.await();
    }

    @Override
    public void completeStreaming() {
        streamLatch.countDown();
        channel.shutdown();
    }

    @Override
    public long getConsumedBlocks() {
        return metricsService.get(LiveBlocksConsumed).get();
    }

    @Override
    public List<String> getLastKnownStatuses() {
        return List.copyOf(lastKnownStatuses);
    }
}
