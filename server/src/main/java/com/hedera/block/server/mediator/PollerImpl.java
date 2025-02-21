// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.mediator;

import static java.lang.System.Logger.Level.TRACE;

import com.lmax.disruptor.EventPoller;
import com.swirlds.metrics.api.LongGauge;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

public class PollerImpl<V> implements Poller<V> {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final EventPoller<V> poller;
    private final BatchedData<V> polledData;
    private final LongGauge consumerBuffersRemainingCapacity;

    /**
     * Leverage the provided ring buffer and batch size to poll for events.
     *
     * @param eventPoller the event poller
     * @param batchSize the size of the batch
     */
    PollerImpl(
            @NonNull final EventPoller<V> eventPoller,
            final int batchSize,
            @NonNull final LongGauge consumerBuffersRemainingCapacity) {
        this.poller = Objects.requireNonNull(eventPoller);
        this.polledData = new BatchedData<>(batchSize);
        this.consumerBuffersRemainingCapacity = Objects.requireNonNull(consumerBuffersRemainingCapacity);
    }

    @Override
    public V poll() throws Exception {
        if (polledData.getMsgCount() > 0) {
            return polledData.pollMessage();
        }

        // Poll to get the latest batches of block items
        poller.poll((event, sequence, endOfBatch) -> polledData.addDataItem(event));
        consumerBuffersRemainingCapacity.set(polledData.capacity - polledData.msgHighBound);
        if (LOGGER.isLoggable(TRACE)) {
            LOGGER.log(
                    TRACE, "Consumer Buffers Remaining Capacity: %d".formatted(consumerBuffersRemainingCapacity.get()));
        }

        return polledData.getMsgCount() > 0 ? polledData.pollMessage() : null;
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
