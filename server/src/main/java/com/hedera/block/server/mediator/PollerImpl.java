// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.mediator;

import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;

public class PollerImpl<V> implements Poller<V> {

    private final EventPoller<V> poller;
    private final BatchedData<V> polledData;

    PollerImpl(final RingBuffer<V> ringBuffer, final int batchSize) {
        this.poller = ringBuffer.newPoller();
        ringBuffer.addGatingSequences(poller.getSequence());
        this.polledData = new BatchedData<>(batchSize);
    }

    @Override
    public V poll() throws Exception {
        if (polledData.getMsgCount() > 0) {
            return polledData.pollMessage(); // we just fetch from our local
        }

        loadNextValues(poller, polledData); // we try to load from the ring
        return polledData.getMsgCount() > 0 ? polledData.pollMessage() : null;
    }

    private EventPoller.PollState loadNextValues(final EventPoller<V> poller, final BatchedData<V> batch)
            throws Exception {
        return poller.poll((event, sequence, endOfBatch) -> batch.addDataItem(event));
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
