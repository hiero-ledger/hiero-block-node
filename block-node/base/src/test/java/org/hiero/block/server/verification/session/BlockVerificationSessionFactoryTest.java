// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.verification.session;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.hapi.block.stream.output.BlockHeader;
import java.util.concurrent.ExecutorService;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.verification.VerificationConfig;
import org.hiero.block.server.verification.signature.SignatureVerifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class BlockVerificationSessionFactoryTest {

    @Mock
    private MetricsService mockMetricsService;

    @Mock
    private SignatureVerifier mockSignatureVerifier;

    @Mock
    private ExecutorService mockExecutorService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        // Default configuration stubs

    }

    @Test
    void createSession_whenSessionTypeIsAsync_returnsBlockVerificationSessionAsync() {
        // Given
        VerificationConfig config = new VerificationConfig(null, BlockVerificationSessionType.ASYNC, 32);
        BlockHeader blockHeader = BlockHeader.newBuilder().number(1L).build();

        BlockVerificationSessionFactory sessionFactory = new BlockVerificationSessionFactory(
                config, mockMetricsService, mockSignatureVerifier, mockExecutorService);

        // When
        var session = sessionFactory.createSession(blockHeader);

        // Then
        assertNotNull(session, "Session should not be null");
        assertInstanceOf(
                BlockVerificationSessionAsync.class,
                session,
                "Session should be an instance of BlockVerificationSessionAsync");
    }

    @Test
    void createSession_whenSessionTypeIsSync_returnsBlockVerificationSessionSync() {
        // Given
        VerificationConfig config = new VerificationConfig(null, BlockVerificationSessionType.SYNC, 32);
        BlockHeader blockHeader = BlockHeader.newBuilder().number(1L).build();

        BlockVerificationSessionFactory sessionFactory = new BlockVerificationSessionFactory(
                config, mockMetricsService, mockSignatureVerifier, mockExecutorService);

        // When
        var session = sessionFactory.createSession(blockHeader);

        // Then
        assertNotNull(session, "Session should not be null");
        assertInstanceOf(
                BlockVerificationSessionSync.class,
                session,
                "Session should be an instance of BlockVerificationSessionSync");
    }
}
