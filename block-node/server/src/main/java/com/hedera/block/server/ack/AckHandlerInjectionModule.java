// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.ack;

import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.notifier.Notifier;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.remove.BlockRemover;
import com.hedera.block.server.service.ServiceStatus;
import com.hedera.block.server.verification.VerificationConfig;
import dagger.Module;
import dagger.Provides;
import edu.umd.cs.findbugs.annotations.NonNull;
import javax.inject.Singleton;

@Module
public interface AckHandlerInjectionModule {

    /**
     * Provides a {@link AckHandler} instance.
     *
     * @param notifier the {@link Notifier} instance
     * @param persistenceStorageConfig the {@link PersistenceStorageConfig} instance
     * @param verificationConfig the {@link VerificationConfig} instance
     * @param serviceStatus the {@link ServiceStatus} instance
     * @param blockRemover the {@link BlockRemover} instance
     * @param metricsService the {@link MetricsService} instance
     * @return a {@link AckHandler} instance
     */
    @Provides
    @Singleton
    static AckHandler provideBlockManager(
            @NonNull final Notifier notifier,
            @NonNull final PersistenceStorageConfig persistenceStorageConfig,
            @NonNull final VerificationConfig verificationConfig,
            @NonNull final ServiceStatus serviceStatus,
            @NonNull final BlockRemover blockRemover,
            @NonNull final MetricsService metricsService) {

        boolean skipPersistence = persistenceStorageConfig.type().equals(PersistenceStorageConfig.StorageType.NO_OP);
        boolean skipVerification = verificationConfig.type().equals(VerificationConfig.VerificationServiceType.NO_OP);

        return new AckHandlerImpl(
                notifier, skipPersistence | skipVerification, serviceStatus, blockRemover, metricsService);
    }
}
