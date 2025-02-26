// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hedera.block.server.ack.AckHandler;
import com.hedera.block.server.ack.AckHandlerImpl;
import com.hedera.block.server.ack.AckHandlerInjectionModule;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.notifier.Notifier;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.remove.BlockRemover;
import com.hedera.block.server.service.ServiceStatus;
import com.hedera.block.server.verification.VerificationConfig;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AckHandlerInjectionModuleTest {

    @Mock
    private MetricsService metricsService;

    @Test
    void testProvideBlockManager() {
        // given
        final Notifier notifier = mock(Notifier.class);
        final ServiceStatus serviceStatus = mock(ServiceStatus.class);
        final BlockRemover blockRemover = mock(BlockRemover.class);
        final PersistenceStorageConfig persistenceStorageConfig = new PersistenceStorageConfig(
                Path.of(""),
                Path.of(""),
                PersistenceStorageConfig.StorageType.BLOCK_AS_LOCAL_FILE,
                PersistenceStorageConfig.CompressionType.NONE,
                0,
                10);
        final VerificationConfig verificationConfig = mock(VerificationConfig.class);
        when(verificationConfig.type()).thenReturn(VerificationConfig.VerificationServiceType.PRODUCTION);

        // when
        final AckHandler ackHandler = AckHandlerInjectionModule.provideBlockManager(
                notifier, persistenceStorageConfig, verificationConfig, serviceStatus, blockRemover, metricsService);

        // then
        // AckHandlerImpl is the default and only implementation
        // so we should get an instance of it
        assertEquals(AckHandlerImpl.class, ackHandler.getClass());
    }
}
