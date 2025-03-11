// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.consumer;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ConsumerStreamRunnableTest {

    private static final int TEST_TIMEOUT = 1000;

    @Mock
    private StreamManager streamManager;

    @Test
    public void testConsumerStreamRunnableLoopingAndExit() {

        // Loop twice and then exit
        when(streamManager.execute()).thenReturn(true, true, false);

        final ConsumerStreamRunnable consumerStreamRunnable = new ConsumerStreamRunnable(streamManager);
        consumerStreamRunnable.run();

        verify(streamManager, timeout(TEST_TIMEOUT).times(3)).execute();
    }
}
