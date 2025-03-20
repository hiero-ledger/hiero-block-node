// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.verification;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.swirlds.metrics.api.Counter;
import java.util.Collections;
import java.util.List;
import org.hiero.block.node.verification.service.BlockVerificationService;
import org.hiero.block.server.events.ObjectEvent;
import org.hiero.block.server.mediator.SubscriptionHandler;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.notifier.Notifier;
import org.hiero.block.server.service.ServiceStatus;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamVerificationHandlerImplTest {

    @Mock
    private SubscriptionHandler<List<BlockItemUnparsed>> subscriptionHandler;

    @Mock
    private Notifier notifier;

    @Mock
    private MetricsService metricsService;

    @Mock
    private Counter verificationBlocksError;

    @Mock
    private ServiceStatus serviceStatus;

    @Mock
    private BlockVerificationService blockVerificationService;

    private static final int testTimeout = 50;

    @Test
    public void testOnEventWhenServiceIsNotRunning() {
        when(serviceStatus.isRunning()).thenReturn(false);

        final var streamVerificationHandler = new StreamVerificationHandlerImpl(
                subscriptionHandler, notifier, metricsService, serviceStatus, blockVerificationService);

        final ObjectEvent<List<BlockItemUnparsed>> event = new ObjectEvent<>();
        event.set(Collections.emptyList());

        // Call the handler
        streamVerificationHandler.onEvent(event, 0, false);

        verify(serviceStatus, timeout(testTimeout).times(0)).stopRunning(any());
        verify(subscriptionHandler, timeout(testTimeout).times(0)).unsubscribe(any());
        verify(notifier, timeout(testTimeout).times(0)).notifyUnrecoverableError();
    }

    @Test
    public void testValidBlockItemsAreVerified() throws ParseException {
        when(serviceStatus.isRunning()).thenReturn(true);

        final var streamVerificationHandler = new StreamVerificationHandlerImpl(
                subscriptionHandler, notifier, metricsService, serviceStatus, blockVerificationService);

        BlockHeader blockHeader = BlockHeader.newBuilder().number(10).build();

        // Create a valid blockItems response
        List<BlockItemUnparsed> blockItems = List.of(BlockItemUnparsed.newBuilder()
                .blockHeader(BlockHeader.PROTOBUF.toBytes(blockHeader))
                .build());

        final ObjectEvent<List<BlockItemUnparsed>> event = new ObjectEvent<>();
        event.set(blockItems);

        streamVerificationHandler.onEvent(event, 0, false);

        verify(blockVerificationService, times(1)).onBlockItemsReceived(blockItems);
        verify(serviceStatus, never()).stopRunning(any());
        verify(subscriptionHandler, never()).unsubscribe(any());
        verify(notifier, never()).notifyUnrecoverableError();
    }

    @Test
    public void testExceptionInVerificationTriggersErrorResponse() throws ParseException {
        when(serviceStatus.isRunning()).thenReturn(true);

        final var streamVerificationHandler = new StreamVerificationHandlerImpl(
                subscriptionHandler, notifier, metricsService, serviceStatus, blockVerificationService);

        BlockHeader blockHeader = BlockHeader.newBuilder().number(10).build();

        // Create a valid blockItems response
        List<BlockItemUnparsed> blockItems = List.of(BlockItemUnparsed.newBuilder()
                .blockHeader(BlockHeader.PROTOBUF.toBytes(blockHeader))
                .build());

        final ObjectEvent<List<BlockItemUnparsed>> event = new ObjectEvent<>();
        event.set(blockItems);

        // Simulate an exception when verifying block items
        doThrow(new RuntimeException("Verification failed"))
                .when(blockVerificationService)
                .onBlockItemsReceived(blockItems);

        streamVerificationHandler.onEvent(event, 0, false);

        verify(serviceStatus, timeout(testTimeout).times(1)).stopRunning(any());
        verify(subscriptionHandler, timeout(testTimeout).times(1)).unsubscribe(any());
    }
}
