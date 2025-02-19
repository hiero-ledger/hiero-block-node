// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.message;

import com.hedera.block.server.events.BlockNodeEventHandler;
import com.hedera.block.server.events.ObjectEvent;
import org.junit.jupiter.api.Test;

public class PushMessageServiceTest {

    @Test
    public void testPublish() {
        PushMessageServiceImpl messageService = new PushMessageServiceImpl();
        Subscriber s = new Subscriber();
        messageService.subscribe(s);
        messageService.publish(1);
        messageService.publish(2);
        messageService.publish(3);
        messageService.publish(4);
        messageService.publish(5);
    }

    private static class Subscriber implements BlockNodeEventHandler<ObjectEvent<Integer>> {
        @Override
        public void onEvent(ObjectEvent<Integer> event, long sequence, boolean endOfBatch) {
            System.out.println("Received: " + event.get());
        }
    }
}
