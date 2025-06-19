// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.mode;

import com.swirlds.config.api.Configuration;
import dagger.Module;
import dagger.Provides;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Singleton;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.hiero.block.simulator.config.types.SimulatorMode;
import org.hiero.block.simulator.generator.BlockStreamManager;
import org.hiero.block.simulator.grpc.ConsumerStreamGrpcClient;
import org.hiero.block.simulator.grpc.PublishStreamGrpcServer;
import org.hiero.block.simulator.metrics.MetricsService;
import org.hiero.block.simulator.mode.impl.ConsumerModeHandler;
import org.hiero.block.simulator.mode.impl.PublishClientManager;
import org.hiero.block.simulator.mode.impl.PublisherServerModeHandler;
import org.hiero.block.simulator.startup.SimulatorStartupData;

@Module
public interface SimulatorModeInjectionModule {

    @Singleton
    @Provides
    static SimulatorModeHandler providesSimulatorModeHandler(
            @NonNull Configuration configuration,
            @NonNull BlockStreamManager blockStreamManager,
            @NonNull PublishStreamGrpcServer publishStreamGrpcServer,
            @NonNull ConsumerStreamGrpcClient consumerStreamGrpcClient,
            @NonNull MetricsService metricsService,
            @NonNull PublishClientManager publishClientManager,
            @NonNull SimulatorStartupData startupData,
            @NonNull AtomicBoolean streamEnabled) {

        final BlockStreamConfig blockStreamConfig = configuration.getConfigData(BlockStreamConfig.class);
        final GrpcConfig grpcConfig = configuration.getConfigData(GrpcConfig.class);
        final SimulatorMode simulatorMode = blockStreamConfig.simulatorMode();
        return switch (simulatorMode) {
            case PUBLISHER_CLIENT ->
                new PublishClientManager(
                        grpcConfig,
                        blockStreamConfig,
                        blockStreamManager,
                        metricsService,
                        startupData,
                        streamEnabled,
                        publishClientManager.getCurrentClient(),
                        publishClientManager.getCurrentHandler());
            case PUBLISHER_SERVER -> new PublisherServerModeHandler(publishStreamGrpcServer);
            case CONSUMER -> new ConsumerModeHandler(consumerStreamGrpcClient);
        };
    }
}
