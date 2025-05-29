// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.mode;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.hiero.block.simulator.grpc.ConsumerStreamGrpcClient;
import org.hiero.block.simulator.mode.impl.ConsumerModeHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConsumerModeHandlerTest {

    private ConsumerStreamGrpcClient consumerStreamGrpcClient;
    private ConsumerModeHandler consumerModeHandler;

    @BeforeEach
    void setUp() {
        consumerStreamGrpcClient = mock(ConsumerStreamGrpcClient.class);

        consumerModeHandler = new ConsumerModeHandler(consumerStreamGrpcClient);
    }

    @Test
    void testConstructorWithNullArguments() {
        assertThrows(NullPointerException.class, () -> new ConsumerModeHandler(null));
    }

    @Test
    void testInit() {
        consumerModeHandler.init();

        verify(consumerStreamGrpcClient).init();
    }

    @Test
    void testStart() throws InterruptedException {
        consumerModeHandler.start();
        verify(consumerStreamGrpcClient).requestBlocks();
    }

    @Test
    void testStart_throwsExceptionDuringConsuming() throws InterruptedException {
        consumerModeHandler.start();

        doThrow(new InterruptedException("Test exception"))
                .when(consumerStreamGrpcClient)
                .requestBlocks();
        assertThrows(InterruptedException.class, () -> consumerModeHandler.start());
    }

    @Test
    void testStop() throws InterruptedException {
        consumerModeHandler.stop();

        verify(consumerStreamGrpcClient).completeStreaming();
    }

    @Test
    void testStop_throwsExceptionDuringCompleteStreaming() throws InterruptedException {
        consumerModeHandler.stop();
        doThrow(new InterruptedException("Test exception"))
                .when(consumerStreamGrpcClient)
                .completeStreaming();

        assertThrows(InterruptedException.class, () -> consumerModeHandler.stop());
    }
}
