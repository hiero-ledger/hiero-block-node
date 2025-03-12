// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.mediator;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.TRACE;

import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.Optional;

public class LiveStreamPoller<V> implements Poller<V> {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final EventPoller<V> eventPoller;
    private final BatchedData<V> polledData;
    private final RingBuffer<V> ringBuffer;

    private final int historicTransitionThresholdPercentage;

    public LiveStreamPoller(
            @NonNull final EventPoller<V> eventPoller,
            @NonNull final RingBuffer<V> ringBuffer,
            @NonNull final MediatorConfig mediatorConfig) {

        this.eventPoller = Objects.requireNonNull(eventPoller);
        this.ringBuffer = Objects.requireNonNull(ringBuffer);
        this.historicTransitionThresholdPercentage = mediatorConfig.historicTransitionThresholdPercentage();

        this.polledData = new BatchedData<>(1);
    }

    @Override
    public boolean exceedsThreshold() {
        int ringBufferSize = ringBuffer.getBufferSize();
        long cursorPosition = ringBuffer.getCursor();
        long sequencePosition = eventPoller.getSequence().get();

        long difference = (cursorPosition % ringBufferSize) - (sequencePosition % ringBufferSize);
        int percentage = (int) ((float) difference / ringBufferSize) * 100;
        if (percentage > historicTransitionThresholdPercentage) {
            LOGGER.log(
                    DEBUG,
                    "Historic Transition Threshold Percentage exceeded: {0} > {1}",
                    percentage,
                    historicTransitionThresholdPercentage);

            return true;
        }

        LOGGER.log(
                TRACE,
                "Historic Transition Threshold Percentage not exceed: {0} <= {1}",
                percentage,
                historicTransitionThresholdPercentage);

        return false;
    }

    @Override
    public Optional<V> poll() throws Exception {
        if (polledData.getMsgCount() > 0) {
            return Optional.of(polledData.pollMessage());
        }

        // Poll to get the latest batches of block items
        eventPoller.poll((event, sequence, endOfBatch) -> polledData.addDataItem(event));
        if (polledData.getMsgCount() > 0) {
            return Optional.of(polledData.pollMessage());
        }

        if (LOGGER.isLoggable(TRACE)) {
            int ringBufferSize = ringBuffer.getBufferSize();
            long cursorPosition = ringBuffer.getCursor();
            long sequencePosition = eventPoller.getSequence().get();

            long relCursorPosition = (cursorPosition % ringBufferSize);
            long relSeqPosition = (sequencePosition % ringBufferSize);
            LOGGER.log(
                    TRACE, "No data available to poll. Cursor: {0}, Sequence: {1}", relCursorPosition, relSeqPosition);
        }

        return Optional.empty();
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
