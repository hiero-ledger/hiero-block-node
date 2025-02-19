// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.message;

import com.hedera.block.server.events.BlockNodeEventHandler;
import com.hedera.block.server.events.ObjectEvent;
import com.lmax.disruptor.BatchEventProcessorBuilder;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PushMessageServiceImpl {
    private final RingBuffer<ObjectEvent<Integer>> ringBuffer;
    private final ExecutorService executor;

    public PushMessageServiceImpl() {
        final Disruptor<ObjectEvent<Integer>> disruptor =
                new Disruptor<>(ObjectEvent::new, 8, DaemonThreadFactory.INSTANCE);
        this.ringBuffer = disruptor.start();
        this.executor = Executors.newCachedThreadPool(DaemonThreadFactory.INSTANCE);
    }

    public void publish(final Integer value) {
        ringBuffer.publishEvent((event, sequence) -> event.set(value));
    }

    public void subscribe(BlockNodeEventHandler<ObjectEvent<Integer>> handler) {
        final var batchEventProcessor =
                new BatchEventProcessorBuilder().build(ringBuffer, ringBuffer.newBarrier(), handler);

        ringBuffer.addGatingSequences(batchEventProcessor.getSequence());
        executor.execute(batchEventProcessor);
    }
}
