// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import com.hedera.block.server.ack.AckHandler;
import com.hedera.block.server.config.BlockNodeContext;
import com.hedera.block.server.events.BlockNodeEventHandler;
import com.hedera.block.server.events.ObjectEvent;
import com.hedera.block.server.mediator.SubscriptionHandler;
import com.hedera.block.server.notifier.Notifier;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig.ArchiveType;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig.CompressionType;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig.StorageType;
import com.hedera.block.server.persistence.storage.archive.AsyncBlockAsLocalFileArchiverFactory;
import com.hedera.block.server.persistence.storage.archive.AsyncLocalBlockArchiverFactory;
import com.hedera.block.server.persistence.storage.archive.AsyncNoOpArchiverFactory;
import com.hedera.block.server.persistence.storage.archive.LocalBlockArchiver;
import com.hedera.block.server.persistence.storage.archive.LocalBlockFileArchiver;
import com.hedera.block.server.persistence.storage.compression.Compression;
import com.hedera.block.server.persistence.storage.compression.NoOpCompression;
import com.hedera.block.server.persistence.storage.compression.ZstdCompression;
import com.hedera.block.server.persistence.storage.path.BlockAsLocalFilePathResolver;
import com.hedera.block.server.persistence.storage.path.BlockPathResolver;
import com.hedera.block.server.persistence.storage.path.NoOpBlockPathResolver;
import com.hedera.block.server.persistence.storage.read.BlockAsLocalFileReader;
import com.hedera.block.server.persistence.storage.read.BlockReader;
import com.hedera.block.server.persistence.storage.read.NoOpBlockReader;
import com.hedera.block.server.persistence.storage.remove.BlockAsLocalFileRemover;
import com.hedera.block.server.persistence.storage.remove.BlockRemover;
import com.hedera.block.server.persistence.storage.remove.NoOpBlockRemover;
import com.hedera.block.server.persistence.storage.write.AsyncBlockWriterFactory;
import com.hedera.block.server.service.ServiceStatus;
import com.hedera.block.server.util.TestConfigUtil;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PersistenceInjectionModuleTest {
    @Mock
    private PersistenceStorageConfig persistenceStorageConfigMock;

    @Mock
    private BlockPathResolver blockPathResolverMock;

    @Mock
    private Compression compressionMock;

    @Mock
    private SubscriptionHandler<List<BlockItemUnparsed>> subscriptionHandlerMock;

    @Mock
    private Notifier notifierMock;

    @Mock
    private ServiceStatus serviceStatusMock;

    @Mock
    private AckHandler ackHandlerMock;

    @Mock
    private AsyncBlockWriterFactory asyncBlockWriterFactoryMock;

    @Mock
    private Executor executorMock;

    @Mock
    private LocalBlockArchiver archiverMock;

    @Mock
    private AsyncLocalBlockArchiverFactory asyncLocalBlockArchiverFactory;

    @TempDir
    private Path testLiveRootPath;

    /**
     * This test aims to verify that the
     * {@link PersistenceInjectionModule#providesBlockReader} method will return
     * the correct {@link BlockReader} instance based on the {@link StorageType}
     * parameter. The test verifies only the result type and not what is inside
     * the instance! For the purpose of this test, what is inside the instance
     * is not important. We aim to test the branch that will be taken based on
     * the {@link StorageType} parameter in terms of the returned instance type.
     *
     * @param storageType parameterized, the {@link StorageType} to test
     */
    @ParameterizedTest
    @EnumSource(StorageType.class)
    void testProvidesBlockReader(final StorageType storageType) {
        lenient().when(persistenceStorageConfigMock.liveRootPath()).thenReturn(testLiveRootPath);
        when(persistenceStorageConfigMock.type()).thenReturn(storageType);

        final BlockReader<BlockUnparsed> actual = PersistenceInjectionModule.providesBlockReader(
                persistenceStorageConfigMock, blockPathResolverMock, compressionMock);

        final Class<?> targetInstanceType =
                switch (storageType) {
                    case BLOCK_AS_LOCAL_FILE -> BlockAsLocalFileReader.class;
                    case NO_OP -> NoOpBlockReader.class;
                };
        assertThat(actual).isNotNull().isExactlyInstanceOf(targetInstanceType);
    }

    /**
     * This test aims to verify that the
     * {@link PersistenceInjectionModule#providesBlockRemover} method will
     * return the correct {@link BlockRemover} instance based on the
     * {@link StorageType} parameter. The test verifies only the result type and
     * not what is inside the instance! For the purpose of this test, what is
     * inside the instance is not important. We aim to test the branch that will
     * be taken based on the {@link StorageType} parameter in terms of the
     * returned instance type.
     *
     * @param storageType parameterized, the {@link StorageType} to test
     */
    @ParameterizedTest
    @EnumSource(StorageType.class)
    void testProvidesBlockRemover(final StorageType storageType) {
        when(persistenceStorageConfigMock.type()).thenReturn(storageType);

        final BlockRemover actual =
                PersistenceInjectionModule.providesBlockRemover(persistenceStorageConfigMock, blockPathResolverMock);

        final Class<?> targetInstanceType =
                switch (storageType) {
                    case BLOCK_AS_LOCAL_FILE -> BlockAsLocalFileRemover.class;
                    case NO_OP -> NoOpBlockRemover.class;
                };
        assertThat(actual).isNotNull().isExactlyInstanceOf(targetInstanceType);
    }

    /**
     * This test aims to verify that the
     * {@link PersistenceInjectionModule#providesPathResolver(PersistenceStorageConfig)}
     * method will return the correct {@link BlockPathResolver} instance based
     * on the {@link StorageType} parameter. The test verifies only the result
     * type and not what is inside the instance! For the purpose of this test,
     * what is inside the instance is not important. We aim to test the branch
     * that will be taken based on the {@link StorageType} parameter in terms of
     * the returned instance type.
     *
     * @param storageType parameterized, the {@link StorageType} to test
     */
    @ParameterizedTest
    @EnumSource(StorageType.class)
    void testProvidesBlockPathResolver(final StorageType storageType) {
        lenient().when(persistenceStorageConfigMock.liveRootPath()).thenReturn(testLiveRootPath);
        lenient().when(persistenceStorageConfigMock.archiveRootPath()).thenReturn(testLiveRootPath);
        lenient().when(persistenceStorageConfigMock.archiveGroupSize()).thenReturn(10);
        when(persistenceStorageConfigMock.type()).thenReturn(storageType);

        final BlockPathResolver actual = PersistenceInjectionModule.providesPathResolver(persistenceStorageConfigMock);

        final Class<?> targetInstanceType =
                switch (storageType) {
                    case BLOCK_AS_LOCAL_FILE -> BlockAsLocalFilePathResolver.class;
                    case NO_OP -> NoOpBlockPathResolver.class;
                };
        assertThat(actual).isNotNull().isExactlyInstanceOf(targetInstanceType);
    }

    /**
     * This test aims to verify that the
     * {@link PersistenceInjectionModule#providesCompression(PersistenceStorageConfig)}
     * method will return the correct {@link Compression} instance based on the
     * {@link CompressionType} parameter. The test verifies only the result type
     * and not what is inside the instance! For the purpose of this test, what
     * is inside the instance is not important. We aim to test the branch that
     * will be taken based on the {@link CompressionType} parameter in terms of
     * the returned instance type.
     *
     * @param compressionType parameterized, the {@link CompressionType} to test
     */
    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void testProvidesCompression(final CompressionType compressionType) {
        when(persistenceStorageConfigMock.compression()).thenReturn(compressionType);
        final Compression actual = PersistenceInjectionModule.providesCompression(persistenceStorageConfigMock);

        final Class<?> targetInstanceType =
                switch (compressionType) {
                    case ZSTD -> ZstdCompression.class;
                    case NONE -> NoOpCompression.class;
                };
        assertThat(actual).isNotNull().isExactlyInstanceOf(targetInstanceType);
    }

    /**
     * This test aims to verify that the
     * {@link PersistenceInjectionModule#providesLocalBlockArchiver(PersistenceStorageConfig, AsyncLocalBlockArchiverFactory)}
     * will return the correct {@link LocalBlockArchiver} instance based on the
     * {@link StorageType} parameter. The test verifies only the result type and
     * not what is inside the instance! For the purpose of this test, what is
     * inside the instance is not important. We aim to test the branch that will
     * be taken based on the {@link StorageType} parameter in terms of the
     * returned instance type.
     *
     * @param type parameterized, the {@link StorageType} to test
     */
    @ParameterizedTest
    @EnumSource(StorageType.class)
    void testProvidesLocalBlockArchiver(final StorageType type) {
        when(persistenceStorageConfigMock.type()).thenReturn(type);
        final LocalBlockArchiver actual = PersistenceInjectionModule.providesLocalBlockArchiver(
                persistenceStorageConfigMock, asyncLocalBlockArchiverFactory);

        final Class<?> targetInstanceType =
                switch (type) {
                    case BLOCK_AS_LOCAL_FILE, NO_OP -> LocalBlockFileArchiver.class;
                };
        assertThat(actual).isNotNull().isExactlyInstanceOf(targetInstanceType);
    }

    /**
     * This test aims to verify that the
     * {@link PersistenceInjectionModule#providesAsyncLocalBlockArchiverFactory(PersistenceStorageConfig, BlockPathResolver)}
     * will return the correct {@link AsyncLocalBlockArchiverFactory} instance
     * based on the {@link ArchiveType} parameter. The test verifies only the
     * result type and not what is inside the instance! For the purpose of this
     * test, what is inside the instance is not important. We aim to test the
     * branch that will be taken based on the {@link ArchiveType} parameter in
     * terms of the returned instance type.
     *
     * @param type parameterized, the {@link ArchiveType} to test
     */
    @ParameterizedTest
    @EnumSource(ArchiveType.class)
    void testProvidesLocalBlockArchiver(final ArchiveType type) {
        when(persistenceStorageConfigMock.archiveType()).thenReturn(type);
        final AsyncLocalBlockArchiverFactory actual = PersistenceInjectionModule.providesAsyncLocalBlockArchiverFactory(
                persistenceStorageConfigMock, blockPathResolverMock);

        final Class<?> targetInstanceType =
                switch (type) {
                    case ArchiveType.BLOCK_AS_LOCAL_FILE -> AsyncBlockAsLocalFileArchiverFactory.class;
                    case ArchiveType.NO_OP -> AsyncNoOpArchiverFactory.class;
                };
        assertThat(actual).isNotNull().isExactlyInstanceOf(targetInstanceType);
    }

    @Test
    void testProvidesStreamValidatorBuilder() throws IOException {
        final BlockNodeContext blockNodeContext = TestConfigUtil.getTestBlockNodeContext();
        when(persistenceStorageConfigMock.liveRootPath()).thenReturn(testLiveRootPath);
        when(persistenceStorageConfigMock.archiveRootPath()).thenReturn(testLiveRootPath);
        // Call the method under test
        final BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> streamVerifier =
                new StreamPersistenceHandlerImpl(
                        subscriptionHandlerMock,
                        notifierMock,
                        blockNodeContext,
                        serviceStatusMock,
                        ackHandlerMock,
                        asyncBlockWriterFactoryMock,
                        executorMock,
                        archiverMock,
                        persistenceStorageConfigMock);
        assertNotNull(streamVerifier);
    }
}
