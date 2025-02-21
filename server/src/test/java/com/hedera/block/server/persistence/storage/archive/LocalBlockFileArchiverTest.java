// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import java.util.concurrent.Executor;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link LocalBlockFileArchiver}.
 */
@ExtendWith(MockitoExtension.class)
class LocalBlockFileArchiverTest {
    private static final int BATCH_SIZE = 10;

    @Mock
    private PersistenceStorageConfig persistenceStorageConfigMock;

    @Mock
    private AsyncLocalBlockArchiverFactory archiverFactoryMock;

    @Mock
    private Executor executorMock;

    private LocalBlockFileArchiver toTest;

    @BeforeEach
    void setUp() {
        when(persistenceStorageConfigMock.archiveGroupSize()).thenReturn(BATCH_SIZE);
        toTest = new LocalBlockFileArchiver(persistenceStorageConfigMock, archiverFactoryMock, executorMock);
    }

    /**
     * This test aims to assert that the {@link LocalBlockFileArchiver} will
     * successfully submit a task to the executor when a valid threshold is
     * provided.
     */
    @ParameterizedTest
    @MethodSource("validThresholds")
    void testSubmitValidThreshold(final long threshold) {
        final AsyncLocalBlockArchiver taskMock = mock(AsyncLocalBlockArchiver.class);
        when(archiverFactoryMock.create(threshold)).thenReturn(taskMock);
        toTest.submitThresholdPassed(threshold + BATCH_SIZE);
        verify(archiverFactoryMock, times(1)).create(threshold);
        verify(executorMock, times(1)).execute(any(Runnable.class));
    }

    /**
     * This test aims to assert that the {@link LocalBlockFileArchiver} will
     * not create or submit a task to the executor when an invalid threshold is
     * provided.
     */
    @ParameterizedTest
    @MethodSource("invalidThresholds")
    void testSubmitInvalidThreshold(final long threshold) {
        toTest.submitThresholdPassed(threshold + BATCH_SIZE);
        verifyNoInteractions(archiverFactoryMock);
        verifyNoInteractions(executorMock);
    }

    private static Stream<Arguments> validThresholds() {
        return Stream.of(Arguments.of(10L), Arguments.of(100L), Arguments.of(1000L));
    }

    private static Stream<Arguments> invalidThresholds() {
        return Stream.of(Arguments.of(-1L), Arguments.of(0L), Arguments.of(1L), Arguments.of(2L), Arguments.of(25L));
    }
}
