// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.consumer;

import com.hedera.block.server.events.ObjectEvent;
import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RingBufferTest {

    private EventPoller<ObjectEvent<Integer>> eventPoller;
    private BatchedData<ObjectEvent<Integer>> polledData;
    private RingBuffer<ObjectEvent<Integer>> ringBuffer;
    private final int ringBufferSize = 4;
    private final int batchSize = 1;

    @BeforeEach
    public void setUp() {
        final Disruptor<ObjectEvent<Integer>> disruptor =
                new Disruptor<>(ObjectEvent::new, ringBufferSize, DaemonThreadFactory.INSTANCE);
        ringBuffer = disruptor.start();

        eventPoller = ringBuffer.newPoller();
        ringBuffer.addGatingSequences(eventPoller.getSequence());
        polledData = new BatchedData<>(batchSize);
    }

    @Test
    public void testPublish() throws Exception {

        ringBuffer.publishEvent((event, sequence) -> event.set(1));
        ringBuffer.publishEvent((event, sequence) -> event.set(2));
        ringBuffer.publishEvent((event, sequence) -> event.set(3));
        ringBuffer.publishEvent((event, sequence) -> event.set(4));
        //        ringBuffer.publishEvent((event, sequence) -> event.set(5));

        for (int i = 0; i < 5; i++) {
            Optional<ObjectEvent<Integer>> polled = poll();
            polled.ifPresent(integerObjectEvent -> System.out.println("value = " + integerObjectEvent.get()));
        }
    }

    @Test
    public void testCalculatingPercentage() throws Exception {

        System.out.println("RingBuffer cursor before adding: " + ringBuffer.getCursor());

        ringBuffer.publishEvent((event, sequence) -> event.set(1));
        ringBuffer.publishEvent((event, sequence) -> event.set(2));
        ringBuffer.publishEvent((event, sequence) -> event.set(3));
        ringBuffer.publishEvent((event, sequence) -> event.set(4));

        System.out.println("RingBuffer cursor after adding: " + ringBuffer.getCursor());
        System.out.println("---------------------------------");

        for (int i = 0; i < 5; i++) {
            Optional<ObjectEvent<Integer>> polled = poll();
            polled.ifPresent(integerObjectEvent -> System.out.println("value = " + integerObjectEvent.get()));
        }
        System.out.println("RingBuffer cursor after removing 4: " + ringBuffer.getCursor());
        System.out.println("---------------------------------");

        ringBuffer.publishEvent((event, sequence) -> event.set(5));
        ringBuffer.publishEvent((event, sequence) -> event.set(6));
        ringBuffer.publishEvent((event, sequence) -> event.set(7));
        ringBuffer.publishEvent((event, sequence) -> event.set(8));

        System.out.println("RingBuffer cursor after adding 4 more: " + ringBuffer.getCursor());
        System.out.println("---------------------------------");

        for (int i = 0; i < 5; i++) {
            Optional<ObjectEvent<Integer>> polled = poll();
            polled.ifPresent(integerObjectEvent -> System.out.println("value = " + integerObjectEvent.get()));
        }
        System.out.println("RingBuffer cursor after removing 4 more: " + ringBuffer.getCursor());
    }

    private Optional<ObjectEvent<Integer>> poll() throws Exception {
        long cursorPosition = ringBuffer.getCursor();
        long sequencePosition = eventPoller.getSequence().get();
        long difference = (cursorPosition % ringBufferSize) - (sequencePosition % ringBufferSize);
        float percentage = ((float) difference / ringBufferSize) * 100;
        System.out.printf(
                "RingBuffer cursor: %d, Sequence: %d, Difference: %d, Percent Full: %f%n",
                cursorPosition, sequencePosition, difference, percentage);

        if (polledData.getMsgCount() > 0) {
            return Optional.of(polledData.pollMessage());
        }

        // Poll to get the latest batches of block items
        eventPoller.poll((event, sequence, endOfBatch) -> polledData.addDataItem(event));
        return polledData.getMsgCount() > 0 ? Optional.of(polledData.pollMessage()) : Optional.empty();
    }

    private static class BatchedData<V> {
        private int msgHighBound;
        private final int capacity;
        private final V[] data;
        private int cursor;

        @SuppressWarnings("unchecked")
        BatchedData(final int size) {
            this.capacity = size;
            data = (V[]) new Object[this.capacity];
        }

        private void clearCount() {
            msgHighBound = 0;
            cursor = 0;
        }

        public int getMsgCount() {
            return msgHighBound - cursor;
        }

        public boolean addDataItem(final V event) throws IndexOutOfBoundsException {
            if (msgHighBound >= capacity) {
                throw new IndexOutOfBoundsException("Attempting to add item to full batch");
            }

            data[msgHighBound++] = event;
            return msgHighBound < capacity;
        }

        public V pollMessage() {
            V event = null;
            if (cursor < msgHighBound) {
                event = data[cursor++];
            }
            if (cursor > 0 && cursor >= msgHighBound) {
                clearCount();
            }
            return event;
        }
    }
}
