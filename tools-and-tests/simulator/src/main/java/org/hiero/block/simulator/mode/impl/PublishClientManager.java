package org.hiero.block.simulator.mode.impl;

import com.hedera.hapi.block.stream.protoc.Block;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import org.hiero.block.api.protoc.PublishStreamResponse;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.hiero.block.simulator.generator.BlockStreamManager;
import org.hiero.block.simulator.grpc.PublishStreamGrpcClient;
import org.hiero.block.simulator.grpc.impl.PublishStreamGrpcClientImpl;
import org.hiero.block.simulator.metrics.MetricsService;
import org.hiero.block.simulator.startup.SimulatorStartupData;

public class PublishClientManager {
    private final BlockStreamConfig blockStreamConfig;
    private final BlockStreamManager blockStreamManager;
    private final MetricsService metricsService;
    private final GrpcConfig grpcConfig;
    private final SimulatorStartupData startupData;
    private final AtomicBoolean streamEnabled;


    private PublishStreamGrpcClient currentClient;
    private PublisherClientModeHandler currentHandler;

    @Inject
    public PublishClientManager(
            @NonNull final GrpcConfig grpcConfig,
            @NonNull final BlockStreamConfig blockStreamConfig,
            @NonNull final BlockStreamManager blockStreamManager,
            @NonNull final MetricsService metricsService,
            @NonNull final SimulatorStartupData startupData,
            @NonNull final AtomicBoolean streamEnabled,
            @NonNull final PublishStreamGrpcClient publishStreamGrpcClient) {
        this.grpcConfig = grpcConfig;
        this.blockStreamConfig = blockStreamConfig;
        this.blockStreamManager = blockStreamManager;
        this.metricsService = metricsService;
        this.startupData = startupData;
        this.streamEnabled = streamEnabled;
        this.currentClient = publishStreamGrpcClient;
    }

    public void init() {
        blockStreamManager.init();
        currentClient.init();
    }

    private void initializeNewClientAndHandler(Block nextBlock, PublishStreamResponse publishStreamResponse) {
        currentClient = new PublishStreamGrpcClientImpl(grpcConfig, blockStreamConfig, metricsService, streamEnabled, startupData);

        currentHandler = new PublisherClientModeHandler(
                blockStreamConfig, blockStreamManager, metricsService, this);
        currentHandler.init();
        if (nextBlock != null) {
            blockStreamManager.resetToBlock(publishStreamResponse.getEndStream().getBlockNumber());
        }
    }

    public void handleEndStream(Block nextBlock, PublishStreamResponse publishStreamResponse)  {
        initializeNewClientAndHandler(nextBlock, publishStreamResponse);

        try {
            currentHandler.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to restart handler", e);
        }
    }

    public PublisherClientModeHandler getCurrentHandler() {
        return currentHandler;
    }

    public PublishStreamGrpcClient getCurrentClient() {
        return currentClient;
    }
}
