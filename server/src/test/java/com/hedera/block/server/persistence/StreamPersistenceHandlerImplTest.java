// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence;

import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Counter.StreamPersistenceHandlerError;
import static com.hedera.block.server.util.PersistTestUtils.*;
import static com.hedera.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY;
import static com.hedera.block.server.util.PersistTestUtils.generateBlockItemsUnparsed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.block.common.utils.FileUtilities;
import com.hedera.block.server.ack.AckHandler;
import com.hedera.block.server.events.ObjectEvent;
import com.hedera.block.server.mediator.SubscriptionHandler;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.notifier.Notifier;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig.CompressionType;
import com.hedera.block.server.persistence.storage.archive.LocalBlockArchiver;
import com.hedera.block.server.persistence.storage.path.BlockPathResolver;
import com.hedera.block.server.persistence.storage.path.UnverifiedBlockPath;
import com.hedera.block.server.persistence.storage.write.AsyncBlockWriterFactory;
import com.hedera.block.server.service.ServiceStatus;
import com.hedera.block.server.service.WebServerStatus;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test class for {@link StreamPersistenceHandlerImpl}.
 */
@SuppressWarnings("FieldCanBeLocal")
@ExtendWith(MockitoExtension.class)
class StreamPersistenceHandlerImplTest {
    @Mock
    private SubscriptionHandler<List<BlockItemUnparsed>> subscriptionHandler;

    @Mock
    private Notifier notifierMock;

    @Mock
    private ServiceStatus serviceStatusMock;

    @Mock
    private WebServerStatus webServerStatusMock;

    @Mock
    private MetricsService metricsServiceMock;

    @Mock
    private AckHandler ackHandlerMock;

    @Mock
    private AsyncBlockWriterFactory asyncBlockWriterFactoryMock;

    @Mock
    private LocalBlockArchiver archiverMock;

    @Mock
    private Executor executorMock;

    @Mock
    private BlockPathResolver pathResolverMock;

    @TempDir
    private Path testTempDir;

    private Path testLiveRootPath;
    private Path testUnverifiedRootPath;
    private PersistenceStorageConfig persistenceStorageConfig;
    private StreamPersistenceHandlerImpl toTest;

    @BeforeEach
    void setUp() throws IOException {
        final ConfigurationBuilder configBuilder = ConfigurationBuilder.create().autoDiscoverExtensions();
        testLiveRootPath = testTempDir.resolve("live");
        final Path testArchiveRootPath = testTempDir.resolve("archive");
        testUnverifiedRootPath = testTempDir.resolve("unverified");
        configBuilder.withValue(PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY, testLiveRootPath.toString());
        configBuilder.withValue(PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH_KEY, testArchiveRootPath.toString());
        configBuilder.withValue(PERSISTENCE_STORAGE_UNVERIFIED_ROOT_PATH_KEY, testUnverifiedRootPath.toString());
        final Configuration config = configBuilder.build();
        persistenceStorageConfig = config.getConfigData(PersistenceStorageConfig.class);
        final Path testConfigLiveRootPath = persistenceStorageConfig.liveRootPath();
        assertThat(testConfigLiveRootPath).isEqualTo(testLiveRootPath);
        final Path testConfigArchiveRootPath = persistenceStorageConfig.archiveRootPath();
        assertThat(testConfigArchiveRootPath).isEqualTo(testArchiveRootPath);
        final Path testConfigUnverifiedRootPath = persistenceStorageConfig.unverifiedRootPath();
        assertThat(testConfigUnverifiedRootPath).isEqualTo(testUnverifiedRootPath);
        toTest = new StreamPersistenceHandlerImpl(
                subscriptionHandler,
                notifierMock,
                metricsServiceMock,
                serviceStatusMock,
                webServerStatusMock,
                ackHandlerMock,
                asyncBlockWriterFactoryMock,
                executorMock,
                archiverMock,
                pathResolverMock,
                persistenceStorageConfig);
    }

    /**
     * This test aims to assert that the method
     * {@link StreamPersistenceHandlerImpl#onEvent(ObjectEvent, long, boolean)}
     * correctly handles the event when the service is not running.
     */
    @Test
    void testOnEventWhenServiceIsNotRunning() throws IOException {
        when(webServerStatusMock.isRunning()).thenReturn(false);

        final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsed(1);
        final ObjectEvent<List<BlockItemUnparsed>> event = new ObjectEvent<>();
        event.set(blockItems);

        toTest.onEvent(event, 0, false);

        // Indirectly confirm the branch we're in by verifying
        // these methods were not called.
        verify(notifierMock, never()).publish(any());
        verify(metricsServiceMock, never()).get(StreamPersistenceHandlerError);
    }

    /**
     * This test aims to assert that the method
     * {@link StreamPersistenceHandlerImpl#moveVerified(long)} correctly moves
     * a block from the unverified root to the live root.
     */
    @Test
    void testSuccessfulMoveToVerified() throws IOException {
        // Given a block number
        final long blockNumber = 1;
        final String blockFileName = blockNumber + ".blk";
        final Path expectedInLive = testLiveRootPath.resolve(blockFileName);
        when(pathResolverMock.resolveLiveRawPathToBlock(blockNumber)).thenReturn(expectedInLive);
        when(pathResolverMock.findUnverifiedBlock(blockNumber))
                .thenReturn(Optional.of(new UnverifiedBlockPath(
                        blockNumber, testUnverifiedRootPath, blockFileName, CompressionType.NONE)));

        // Generate an empty block file and persist it to where it would be in unverified root
        final Path blockInUnverified = testUnverifiedRootPath.resolve(blockFileName);
        FileUtilities.createFile(blockInUnverified);

        // Assert that the file is in unverified root and not in live root
        assertThat(blockInUnverified).isNotNull().isRegularFile().exists();
        assertThat(expectedInLive).isNotNull().doesNotExist();

        // Call actual method && assert that the file is moved to live root
        toTest.moveVerified(blockNumber);
        assertThat(expectedInLive).isNotNull().isRegularFile().exists();
        assertThat(blockInUnverified).isNotNull().doesNotExist();
    }

    /**
     * This test aims to assert that the method
     * {@link StreamPersistenceHandlerImpl#moveVerified(long)} a file moved by
     * the method will have the same binary content.
     */
    @Test
    void testSuccessfulMoveToVerifiedBinContent() throws IOException {
        // Given a block number
        final long blockNumber = 1;
        final String blockFileName = blockNumber + ".blk";
        final Path expectedInLive = testLiveRootPath.resolve(blockFileName);
        when(pathResolverMock.resolveLiveRawPathToBlock(blockNumber)).thenReturn(expectedInLive);
        when(pathResolverMock.findUnverifiedBlock(blockNumber))
                .thenReturn(Optional.of(new UnverifiedBlockPath(
                        blockNumber, testUnverifiedRootPath, blockFileName, CompressionType.NONE)));

        // Generate && persist Block
        final List<BlockItemUnparsed> blockWithNumber1 = generateBlockItemsUnparsedForWithBlockNumber(blockNumber);
        final BlockUnparsed block =
                BlockUnparsed.newBuilder().blockItems(blockWithNumber1).build();
        final byte[] blockAsBytes = BlockUnparsed.PROTOBUF.toBytes(block).toByteArray();
        final Path blockInUnverified = testUnverifiedRootPath.resolve(blockFileName);
        FileUtilities.createFile(blockInUnverified);
        Files.write(blockInUnverified, blockAsBytes);

        // Call actual method && assert
        toTest.moveVerified(blockNumber);
        assertThat(expectedInLive).isNotNull().hasBinaryContent(blockAsBytes);
    }

    @Test
    void testThrowsWhenNonExistingSource() throws IOException {
        final long blockNumber = 1;
        // Call actual method && assert
        assertThatIOException().isThrownBy(() -> toTest.moveVerified(blockNumber));
    }
}
