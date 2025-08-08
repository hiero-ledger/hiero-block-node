// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.mode.impl;

import com.hedera.hapi.block.stream.protoc.Block;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import org.hiero.block.api.protoc.PublishStreamResponse;
import org.hiero.block.api.protoc.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.hiero.block.simulator.generator.BlockStreamManager;
import org.hiero.block.simulator.grpc.PublishStreamGrpcClient;
import org.hiero.block.simulator.grpc.impl.PublishStreamGrpcClientImpl;
import org.hiero.block.simulator.metrics.MetricsService;
import org.hiero.block.simulator.mode.SimulatorModeHandler;
import org.hiero.block.simulator.startup.SimulatorStartupData;

public class PublishClientManager implements SimulatorModeHandler {
    private final BlockStreamConfig blockStreamConfig;
    private final BlockStreamManager blockStreamManager;
    private final MetricsService metricsService;
    private final GrpcConfig grpcConfig;
    private final SimulatorStartupData startupData;
    private AtomicBoolean streamEnabled;

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
            @NonNull final PublishStreamGrpcClient publishStreamGrpcClient,
            @NonNull final PublisherClientModeHandler publisherClientModeHandler) {
        this.grpcConfig = grpcConfig;
        this.blockStreamConfig = blockStreamConfig;
        this.blockStreamManager = blockStreamManager;
        this.metricsService = metricsService;
        this.startupData = startupData;
        this.streamEnabled = streamEnabled;
        this.currentClient = publishStreamGrpcClient;
        this.currentHandler = publisherClientModeHandler;
        currentHandler.setPublishClientManager(this);
    }

    public void init() {
        blockStreamManager.init();
        currentClient.init();
    }

    @Override
    public void start() throws BlockSimulatorParsingException, IOException, InterruptedException {
        currentHandler.start();
    }

    @Override
    public void stop() throws InterruptedException {
        currentHandler.stop();
    }

    public void handleResponse(Block nextBlock, PublishStreamResponse publishStreamResponse)
            throws BlockSimulatorParsingException, IOException, InterruptedException {
        if (publishStreamResponse.hasEndStream()) {
            handleEndStream(nextBlock, publishStreamResponse);
        } else if (publishStreamResponse.hasResendBlock()) {
            handleResendBlock(publishStreamResponse);
        } else if (publishStreamResponse.hasSkipBlock()) {
            handleSkipBlock(publishStreamResponse);
        }
    }

    private void handleEndStream(Block nextBlock, PublishStreamResponse publishStreamResponse)
            throws InterruptedException, BlockSimulatorParsingException, IOException {
        if (publishStreamResponse.getEndStream().getStatus() == Code.BEHIND) {
            currentClient.handleEndStreamModeIfSet();
        }

        stop();
        adjustStreamManager(nextBlock, publishStreamResponse);
        initializeNewClientAndHandler();

        start();
    }

    private void handleResendBlock(PublishStreamResponse publishStreamResponse)
            throws BlockSimulatorParsingException, IOException, InterruptedException {
        blockStreamManager.resetToBlock(publishStreamResponse.getResendBlock().getBlockNumber());
        start();
    }

    private void handleSkipBlock(PublishStreamResponse publishStreamResponse)
            throws BlockSimulatorParsingException, IOException, InterruptedException {
        blockStreamManager.resetToBlock(publishStreamResponse.getSkipBlock().getBlockNumber() + 1);
        start();
    }

    private void adjustStreamManager(Block nextBlock, PublishStreamResponse publishStreamResponse) {
        if (nextBlock != null) {
            long blockNumber = publishStreamResponse.getEndStream().getBlockNumber();
            blockStreamManager.resetToBlock(blockNumber + 1);
        }
    }

    private void initializeNewClientAndHandler() {
        streamEnabled = new AtomicBoolean(true);
        currentClient = new PublishStreamGrpcClientImpl(
                grpcConfig, blockStreamConfig, metricsService, streamEnabled, startupData);
        currentHandler =
                new PublisherClientModeHandler(blockStreamConfig, currentClient, blockStreamManager, metricsService);

        currentHandler.setPublishClientManager(this);
        currentHandler.init();
    }

    public PublisherClientModeHandler getCurrentHandler() {
        return currentHandler;
    }

    public PublishStreamGrpcClient getCurrentClient() {
        return currentClient;
    }
}
