// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.mode;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import org.hiero.block.simulator.grpc.PublishStreamGrpcServer;
import org.hiero.block.simulator.mode.impl.PublisherServerModeHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PublisherServerModeHandlerTest {

    @Mock
    private PublishStreamGrpcServer publishStreamGrpcServer;

    private PublisherServerModeHandler publisherServerModeHandler;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        publisherServerModeHandler = new PublisherServerModeHandler(publishStreamGrpcServer);
    }

    @Test
    void testConstructorWithNullArguments() {
        assertThrows(NullPointerException.class, () -> new PublisherServerModeHandler(null));
    }

    @Test
    void testInit() {
        publisherServerModeHandler.init();
        verify(publishStreamGrpcServer).init();
    }

    @Test
    void testStop() throws InterruptedException {
        publisherServerModeHandler.stop();
        verify(publishStreamGrpcServer).shutdown();
    }

    @Test
    void testStop_throwsException() throws InterruptedException {
        doThrow(new InterruptedException("Test exception"))
                .when(publishStreamGrpcServer)
                .shutdown();

        assertThrows(InterruptedException.class, () -> publisherServerModeHandler.stop());
    }
}
