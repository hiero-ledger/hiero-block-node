// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence;

import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.StreamPersistenceHandlerError;
import static org.hiero.block.server.util.PersistTestUtils.*;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY;
import static org.hiero.block.server.util.PersistTestUtils.generateBlockItemsUnparsed;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executor;
import org.hiero.block.server.ack.AckHandler;
import org.hiero.block.server.config.BlockNodeContext;
import org.hiero.block.server.events.ObjectEvent;
import org.hiero.block.server.mediator.SubscriptionHandler;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.notifier.Notifier;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig;
import org.hiero.block.server.persistence.storage.archive.LocalBlockArchiver;
import org.hiero.block.server.persistence.storage.write.AsyncBlockWriterFactory;
import org.hiero.block.server.service.ServiceStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StreamPersistenceHandlerImplTest {
    @Mock
    private SubscriptionHandler<List<BlockItemUnparsed>> subscriptionHandler;

    @Mock
    private Notifier notifier;

    @Mock
    private BlockNodeContext blockNodeContext;

    @Mock
    private ServiceStatus serviceStatus;

    @Mock
    private MetricsService metricsService;

    @Mock
    private AckHandler ackHandlerMock;

    @Mock
    private AsyncBlockWriterFactory asyncBlockWriterFactoryMock;

    @Mock
    private LocalBlockArchiver archiverMock;

    @Mock
    private Executor executorMock;

    @TempDir
    private Path testTempDir;

    private PersistenceStorageConfig persistenceStorageConfig;

    @BeforeEach
    void setUp() {
        final ConfigurationBuilder configBuilder = ConfigurationBuilder.create().autoDiscoverExtensions();
        configBuilder.withValue(PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY, testTempDir.toString());
        configBuilder.withValue(PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH_KEY, testTempDir.toString());
        final Configuration config = configBuilder.build();
        persistenceStorageConfig = config.getConfigData(PersistenceStorageConfig.class);
    }

    @Test
    void testOnEventWhenServiceIsNotRunning() throws IOException {
        when(blockNodeContext.metricsService()).thenReturn(metricsService);
        when(serviceStatus.isRunning()).thenReturn(false);

        final StreamPersistenceHandlerImpl streamPersistenceHandler = new StreamPersistenceHandlerImpl(
                subscriptionHandler,
                notifier,
                blockNodeContext,
                serviceStatus,
                ackHandlerMock,
                asyncBlockWriterFactoryMock,
                executorMock,
                archiverMock,
                persistenceStorageConfig);

        final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsed(1);
        final ObjectEvent<List<BlockItemUnparsed>> event = new ObjectEvent<>();
        event.set(blockItems);

        streamPersistenceHandler.onEvent(event, 0, false);

        // Indirectly confirm the branch we're in by verifying
        // these methods were not called.
        verify(notifier, never()).publish(any());
        verify(metricsService, never()).get(StreamPersistenceHandlerError);
    }
}
