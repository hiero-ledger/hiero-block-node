// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.mediator;

import static com.hedera.block.server.mediator.MediatorConfig.MediatorType.NO_OP;

import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.notifier.Notifiable;
import com.hedera.block.server.service.ServiceStatus;
import com.hedera.hapi.block.BlockItemUnparsed;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import javax.inject.Singleton;

/** A Dagger module for providing dependencies for Mediator Module.` */
@Module
public interface MediatorInjectionModule {

    /**
     * Provides the stream mediator.
     *
     * @param mediatorConfig the mediator configuration
     * @param metricsService the metrics service
     * @param serviceStatus the service status
     * @return the stream mediator
     */
    @Provides
    @Singleton
    static LiveStreamMediator providesLiveStreamMediator(
            @NonNull MediatorConfig mediatorConfig,
            @NonNull MetricsService metricsService,
            @NonNull ServiceStatus serviceStatus) {
        final MediatorConfig.MediatorType mediatorType = mediatorConfig.type();
        if (mediatorType == NO_OP) {
            return new NoOpLiveStreamMediator(metricsService);
        }

        return LiveStreamMediatorBuilder.newBuilder(metricsService, mediatorConfig, serviceStatus)
                .build();
    }

    /**
     * Binds the subscription handler to the live stream mediator.
     *
     * @param liveStreamMediator the live stream mediator
     * @return the subscription handler
     */
    @Binds
    @Singleton
    SubscriptionHandler<List<BlockItemUnparsed>> bindSubscriptionHandler(
            @NonNull final LiveStreamMediator liveStreamMediator);

    /**
     * Binds the mediator to the notifiable interface.
     *
     * @param liveStreamMediator the live stream mediator
     * @return the notifiable interface
     */
    @Binds
    @Singleton
    Notifiable bindMediator(@NonNull final LiveStreamMediator liveStreamMediator);
}
