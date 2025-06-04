// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc.impl;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;
import static org.hiero.block.simulator.metrics.SimulatorMetricTypes.Counter.LiveBlocksConsumed;

import com.hedera.hapi.block.stream.protoc.BlockItem;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.protoc.SubscribeStreamResponse;
import org.hiero.block.simulator.config.data.ConsumerConfig;
import org.hiero.block.simulator.metrics.MetricsService;

/**
 * Implementation of StreamObserver that handles responses from the block stream subscription.
 * This class processes incoming blocks and status messages, updating metrics accordingly.
 */
public class ConsumerStreamObserver implements StreamObserver<SubscribeStreamResponse> {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    // Service dependencies
    private final MetricsService metricsService;

    // State
    private final CountDownLatch streamLatch;
    private final int lastKnownStatusesCapacity;
    private final Deque<String> lastKnownStatuses;
    private final AtomicLong blocksConsumed = new AtomicLong(0);

    private final ConsumerConfig consumerConfig;

    /**
     * Constructs a new ConsumerStreamObserver.
     *
     * @param metricsService The service for recording consumption metrics
     * @param streamLatch A latch used to coordinate stream completion
     * @param lastKnownStatuses List to store the most recent status messages
     * @param lastKnownStatusesCapacity the capacity of the last known statuses
     * @throws NullPointerException if any parameter is null
     */
    public ConsumerStreamObserver(
            @NonNull final MetricsService metricsService,
            @NonNull final CountDownLatch streamLatch,
            @NonNull final Deque<String> lastKnownStatuses,
            final int lastKnownStatusesCapacity,
            @NonNull final ConsumerConfig consumerConfig) {
        this.metricsService = requireNonNull(metricsService);
        this.streamLatch = requireNonNull(streamLatch);
        this.lastKnownStatuses = requireNonNull(lastKnownStatuses);
        this.lastKnownStatusesCapacity = lastKnownStatusesCapacity;
        this.consumerConfig = requireNonNull(consumerConfig);
    }

    /**
     * Processes incoming stream responses, handling both status messages and block items.
     *
     * @param subscribeStreamResponse The response received from the server
     * @throws IllegalArgumentException if an unknown response type is received
     */
    @Override
    public void onNext(SubscribeStreamResponse subscribeStreamResponse) {
        final SubscribeStreamResponse.ResponseCase responseType = subscribeStreamResponse.getResponseCase();
        if (lastKnownStatuses.size() >= lastKnownStatusesCapacity) {
            lastKnownStatuses.pollFirst();
        }
        lastKnownStatuses.add(subscribeStreamResponse.toString());

        switch (responseType) {
            case STATUS -> LOGGER.log(INFO, "Received Response: " + subscribeStreamResponse);
            case BLOCK_ITEMS -> processBlockItems(
                    subscribeStreamResponse.getBlockItems().getBlockItemsList());
            default -> throw new IllegalArgumentException("Unknown response type: " + responseType);
        }
    }

    /**
     * Handles stream errors by logging the error and releasing the stream latch.
     *
     * @param streamError The error that occurred during streaming
     */
    @Override
    public void onError(Throwable streamError) {
        Status status = Status.fromThrowable(streamError);
        lastKnownStatuses.add(status.toString());
        LOGGER.log(ERROR, "Error %s with status %s.".formatted(streamError, status), streamError);
        streamLatch.countDown();
    }

    /**
     * Handles stream completion by logging the event and releasing the stream latch.
     */
    @Override
    public void onCompleted() {
        LOGGER.log(INFO, "Subscribe request completed.");
        streamLatch.countDown();
    }

    private void processBlockItems(List<BlockItem> blockItems) {
        if (consumerConfig.slowDown()) {
            long currentBlockCount = blocksConsumed.get();

            if (!consumerConfig.slowDownForBlockRange().isBlank()) {
                Set<Long> blockRangeSet = parseSlowDownForBlockRange(consumerConfig.slowDownForBlockRange());
                if (blockRangeSet.contains(currentBlockCount)) {
                    slowDownProcessing("for block %d".formatted(currentBlockCount));
                }
            }
        }

        blockItems.stream().filter(BlockItem::hasBlockProof).forEach(blockItem -> {
            metricsService.get(LiveBlocksConsumed).increment();

            long blockNumber = blockItem.getBlockProof().getBlock();
            LOGGER.log(INFO, "Received block number: " + blockNumber);
            logNonAscendingBlockNumbers(blockNumber);
        });
    }

    private void logNonAscendingBlockNumbers(long blockNumber) {
        if (blocksConsumed.get() == 0) {
            // Set the first block number in case we started
            // a recording in the middle when running a range.
            // e.g. blocks 1000-2000 - don't assume we're starting
            // with block 1
            blocksConsumed.set(blockNumber);
        } else {
            long count = blocksConsumed.incrementAndGet();
            if (count != blockNumber) {
                LOGGER.log(WARNING, "Block number mismatch: expected %d, received %d".formatted(count, blockNumber));
            }
        }
    }

    private void slowDownProcessing(String message) {
        try {
            LOGGER.log(INFO, "Slowing down processing " + message);
            Thread.sleep(consumerConfig.slowDownMilliseconds());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.log(ERROR, "Stream processing interrupted during slowdown", e);
        }
    }

    public Set<Long> parseSlowDownForBlockRange(String slowDownForBlockRange) {
        Set<Long> blockRangeSet = new HashSet<>();
        if (slowDownForBlockRange == null || slowDownForBlockRange.isBlank()) {
            return blockRangeSet;
        }
        String[] parts = slowDownForBlockRange.split("-");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid range format. Expected format: start-end (e.g., 1-3)");
        }
        try {
            long start = Long.parseLong(parts[0].trim());
            long end = Long.parseLong(parts[1].trim());
            if (start > end) {
                throw new IllegalArgumentException("Range start cannot be greater than range end.");
            }
            for (long i = start; i <= end; i++) {
                blockRangeSet.add(i);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Range values must be valid numbers.", e);
        }
        return blockRangeSet;
    }
}
