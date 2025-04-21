// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc.impl;

import static java.lang.System.Logger.Level.ERROR;
import static java.util.Objects.requireNonNull;
import static org.hiero.block.simulator.metrics.SimulatorMetricTypes.Counter.LiveBlocksProcessed;

import org.hiero.block.api.protoc.BlockStreamPublishServiceGrpc;
import org.hiero.block.api.protoc.PublishStreamRequest;
import org.hiero.block.api.protoc.PublishStreamResponse;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import javax.inject.Inject;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.hiero.block.simulator.grpc.PublishStreamGrpcServer;
import org.hiero.block.simulator.metrics.MetricsService;

/**
 * Implementation of {@link PublishStreamGrpcServer} that handles incoming block stream publications
 * via gRPC streaming. This implementation manages the server setup, handles incoming block streams,
 * tracks processed blocks, and maintains a history of stream statuses. It provides functionality
 * to start, monitor, and shutdown the gRPC server.
 */
public class PublishStreamGrpcServerImpl implements PublishStreamGrpcServer {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    // gRPC Components
    private Server server;

    // Configuration
    private final GrpcConfig grpcConfig;

    // Service dependencies
    private final MetricsService metricsService;

    // State
    private final int lastKnownStatusesCapacity;
    private final Deque<String> lastKnownStatuses;

    /**
     * Constructs a new PublishStreamGrpcServerImpl.
     *
     * @param grpcConfig Configuration for the gRPC server settings
     * @param blockStreamConfig Configuration for the block stream settings
     * @param metricsService Service for tracking metrics
     * @throws NullPointerException if any of the parameters are null
     */
    @Inject
    public PublishStreamGrpcServerImpl(
            @NonNull final GrpcConfig grpcConfig,
            @NonNull final BlockStreamConfig blockStreamConfig,
            @NonNull final MetricsService metricsService) {
        this.grpcConfig = requireNonNull(grpcConfig);
        this.metricsService = requireNonNull(metricsService);

        this.lastKnownStatusesCapacity = blockStreamConfig.lastKnownStatusesCapacity();
        lastKnownStatuses = new ArrayDeque<>(this.lastKnownStatusesCapacity);
    }

    /**
     * Initialize, opens a gRPC channel and creates the needed services with the passed configuration.
     */
    @Override
    public void init() {
        server = ServerBuilder.forPort(grpcConfig.port())
                .addService(new BlockStreamPublishServiceGrpc.BlockStreamPublishServiceImplBase() {
                    @Override
                    public StreamObserver<PublishStreamRequest> publishBlockStream(
                            StreamObserver<PublishStreamResponse> responseObserver) {
                        return new PublishStreamServerObserver(
                                responseObserver, metricsService, lastKnownStatuses, lastKnownStatusesCapacity);
                    }
                })
                .build();
    }

    /**
     * Starts the gRPC server.
     */
    @Override
    public void start() {
        try {
            server.start();
        } catch (IOException e) {
            LOGGER.log(ERROR, "Something went wrong, while trying to start the gRPC server. Error: %s".formatted(e));
        }
    }

    /**
     * Gets the number of processed blocks.
     *
     * @return the number of processed blocks
     */
    @Override
    public long getProcessedBlocks() {
        return metricsService.get(LiveBlocksProcessed).get();
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
     * Shutdowns the server.
     *
     * @throws InterruptedException if the thread is interrupted
     */
    @Override
    public void shutdown() throws InterruptedException {
        server.shutdown();
    }
}
