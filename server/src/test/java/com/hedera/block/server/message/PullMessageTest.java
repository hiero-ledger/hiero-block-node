// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.message;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;
import org.junit.jupiter.api.Test;

public class PullMessageTest {

    @Test
    public void testIt() throws Exception {
        int batchSize = 1;
        RingBuffer<BatchedPoller.DataEvent<Integer>> ringBuffer =
                RingBuffer.createMultiProducer(BatchedPoller.DataEvent.factory(), 32);

        ringBuffer.publishEvent((event, sequence) -> event.set(1));
        ringBuffer.publishEvent((event, sequence) -> event.set(2));
        ringBuffer.publishEvent((event, sequence) -> event.set(3));
        ringBuffer.publishEvent((event, sequence) -> event.set(4));
        ringBuffer.publishEvent((event, sequence) -> event.set(5));
        ringBuffer.publishEvent((event, sequence) -> event.set(6));
        ringBuffer.publishEvent((event, sequence) -> event.set(7));
        ringBuffer.publishEvent((event, sequence) -> event.set(8));
        System.out.println("Poller A");
        BatchedPoller<Integer> pollerA = new BatchedPoller<>(ringBuffer, batchSize);
        pullItems(pollerA);
        ringBuffer.publishEvent((event, sequence) -> event.set(9));
        ringBuffer.publishEvent((event, sequence) -> event.set(10));
        ringBuffer.publishEvent((event, sequence) -> event.set(11));
        pullItems(pollerA);
        BatchedPoller<Integer> pollerB = new BatchedPoller<>(ringBuffer, batchSize);
        System.out.println("Poller B");
        pullItems(pollerB);
        ringBuffer.publishEvent((event, sequence) -> event.set(12));
        ringBuffer.publishEvent((event, sequence) -> event.set(13));
        ringBuffer.publishEvent((event, sequence) -> event.set(14));
        BatchedPoller<Integer> pollerC = new BatchedPoller<>(ringBuffer, batchSize);
        System.out.println("Poller C");
        ringBuffer.publishEvent((event, sequence) -> event.set(15));
        ringBuffer.publishEvent((event, sequence) -> event.set(16));
        ringBuffer.publishEvent((event, sequence) -> event.set(17));
        pullItems(pollerA);
        pullItems(pollerB);
        pullItems(pollerC);
    }

    private static void pullItems(BatchedPoller<Integer> poller) throws Exception {
        while (true) {
            Integer value = poller.poll();
            if (null != value) {
                System.out.println("Received: " + value);
            } else {
                System.out.println("No more messages");
                break;
            }
        }
    }

    static class BatchedPoller<T> {
        private final EventPoller<DataEvent<T>> poller;
        private final BatchedData<T> polledData;

        BatchedPoller(final RingBuffer<DataEvent<T>> ringBuffer, final int batchSize) {
            this.poller = ringBuffer.newPoller();
            ringBuffer.addGatingSequences(poller.getSequence());
            this.polledData = new BatchedData<>(batchSize);
        }

        public T poll() throws Exception {
            if (polledData.getMsgCount() > 0) {
                return polledData.pollMessage(); // we just fetch from our local
            }

            loadNextValues(poller, polledData); // we try to load from the ring
            return polledData.getMsgCount() > 0 ? polledData.pollMessage() : null;
        }

        private EventPoller.PollState loadNextValues(final EventPoller<DataEvent<T>> poller, final BatchedData<T> batch)
                throws Exception {
            return poller.poll((event, sequence, endOfBatch) -> {
                T item = event.copyOfData();
                return item != null ? batch.addDataItem(item) : false;
            });
        }

        public static class DataEvent<T> {
            T data;

            public static <T> EventFactory<DataEvent<T>> factory() {
                return DataEvent::new;
            }

            public T copyOfData() {
                // Copy the data out here. In this case we have a single reference
                // object, so the pass by
                // reference is sufficient. But if we were reusing a byte array,
                // then we
                // would need to copy
                // the actual contents.
                return data;
            }

            void set(final T d) {
                data = d;
            }
        }

        private static class BatchedData<T> {
            private int msgHighBound;
            private final int capacity;
            private final T[] data;
            private int cursor;

            @SuppressWarnings("unchecked")
            BatchedData(final int size) {
                this.capacity = size;
                data = (T[]) new Object[this.capacity];
            }

            private void clearCount() {
                msgHighBound = 0;
                cursor = 0;
            }

            public int getMsgCount() {
                return msgHighBound - cursor;
            }

            public boolean addDataItem(final T item) throws IndexOutOfBoundsException {
                if (msgHighBound >= capacity) {
                    throw new IndexOutOfBoundsException("Attempting to add item to full batch");
                }

                data[msgHighBound++] = item;
                return msgHighBound < capacity;
            }

            public T pollMessage() {
                T rtVal = null;
                if (cursor < msgHighBound) {
                    rtVal = data[cursor++];
                }
                if (cursor > 0 && cursor >= msgHighBound) {
                    clearCount();
                }
                return rtVal;
            }
        }
    }
}
