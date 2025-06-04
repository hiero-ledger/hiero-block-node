// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.mode;

import com.swirlds.config.api.Configuration;
import dagger.Module;
import dagger.Provides;
import edu.umd.cs.findbugs.annotations.NonNull;
import javax.inject.Singleton;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.types.SimulatorMode;
import org.hiero.block.simulator.generator.BlockStreamManager;
import org.hiero.block.simulator.grpc.ConsumerStreamGrpcClient;
import org.hiero.block.simulator.grpc.PublishStreamGrpcClient;
import org.hiero.block.simulator.grpc.PublishStreamGrpcServer;
import org.hiero.block.simulator.metrics.MetricsService;
import org.hiero.block.simulator.mode.impl.ConsumerModeHandler;
import org.hiero.block.simulator.mode.impl.PublisherClientModeHandler;
import org.hiero.block.simulator.mode.impl.PublisherServerModeHandler;

@Module
public interface SimulatorModeInjectionModule {

    @Singleton
    @Provides
    static SimulatorModeHandler providesSimulatorModeHandler(
            @NonNull Configuration configuration,
            @NonNull BlockStreamManager blockStreamManager,
            @NonNull PublishStreamGrpcClient publishStreamGrpcClient,
            @NonNull PublishStreamGrpcServer publishStreamGrpcServer,
            @NonNull ConsumerStreamGrpcClient consumerStreamGrpcClient,
            @NonNull MetricsService metricsService) {

        final BlockStreamConfig blockStreamConfig = configuration.getConfigData(BlockStreamConfig.class);
        final SimulatorMode simulatorMode = blockStreamConfig.simulatorMode();
        return switch (simulatorMode) {
            case PUBLISHER_CLIENT ->
                new PublisherClientModeHandler(
                        blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);
            case PUBLISHER_SERVER -> new PublisherServerModeHandler(publishStreamGrpcServer);
            case CONSUMER -> new ConsumerModeHandler(consumerStreamGrpcClient);
        };
    }
}
