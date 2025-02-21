// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.when;

import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.path.BlockPathResolver;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link AsyncBlockAsLocalFileArchiverFactory}.
 */
@ExtendWith(MockitoExtension.class)
class AsyncBlockAsLocalFileArchiverFactoryTest {
    @Mock
    private PersistenceStorageConfig persistenceStorageConfigMock;

    @Mock
    private BlockPathResolver pathResolverMock;

    private AsyncBlockAsLocalFileArchiverFactory toTest;

    @BeforeEach
    void setUp() {
        when(persistenceStorageConfigMock.archiveGroupSize()).thenReturn(10);
        toTest = new AsyncBlockAsLocalFileArchiverFactory(persistenceStorageConfigMock, pathResolverMock);
    }

    /**
     * This test aims to assert that the
     * {@link AsyncBlockAsLocalFileArchiverFactory} creates an instance of
     * {@link AsyncBlockAsLocalFileArchiver} if a valid threshold is provided.
     */
    @ParameterizedTest
    @MethodSource("validThresholds")
    void testCreate(final long threshold) {
        final AsyncLocalBlockArchiver actual = toTest.create(threshold);
        assertThat(actual).isNotNull().isExactlyInstanceOf(AsyncBlockAsLocalFileArchiver.class);
    }

    /**
     * This test aims to assert that the
     * {@link AsyncBlockAsLocalFileArchiverFactory} throws an
     * {@link IllegalArgumentException} if an invalid threshold is provided.
     */
    @ParameterizedTest
    @MethodSource("invalidThresholds")
    void testCreateInvalidThreshold(final long invalidThreshold) {
        assertThatIllegalArgumentException().isThrownBy(() -> toTest.create(invalidThreshold));
    }

    private static Stream<Arguments> validThresholds() {
        return Stream.of(Arguments.of(10L), Arguments.of(100L), Arguments.of(1000L));
    }

    private static Stream<Arguments> invalidThresholds() {
        return Stream.of(Arguments.of(-1L), Arguments.of(0L), Arguments.of(1L), Arguments.of(2L), Arguments.of(25L));
    }
}
