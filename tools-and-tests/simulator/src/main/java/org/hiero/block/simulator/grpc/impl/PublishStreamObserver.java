// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc.impl;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.api.protoc.PublishStreamResponse;
import org.hiero.block.api.protoc.PublishStreamResponse.BlockAcknowledgement;
import org.hiero.block.simulator.startup.SimulatorStartupData;

/**
 * Implementation of StreamObserver that handles responses from the block publishing stream.
 * This class processes server responses and manages the stream state based on server feedback.
 */
public class PublishStreamObserver implements StreamObserver<PublishStreamResponse> {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    // State
    private final AtomicBoolean streamEnabled;
    private final int lastKnownStatusesCapacity;
    private final Deque<String> lastKnownStatuses;
    private final SimulatorStartupData startupData;

    private PublishStreamResponse publishStreamResponse;

    /**
     * Creates a new PublishStreamObserver instance.
     *
     * @param startupData used to update startup data for the simulator
     * @param streamEnabled Controls whether streaming should continue
     * @param lastKnownStatuses List to store the most recent status messages
     * @param lastKnownStatusesCapacity the capacity of the last known statuses
     * @throws NullPointerException if any parameter is null
     */
    public PublishStreamObserver(
            @NonNull final SimulatorStartupData startupData,
            @NonNull final AtomicBoolean streamEnabled,
            @NonNull final Deque<String> lastKnownStatuses,
            final int lastKnownStatusesCapacity) {
        this.streamEnabled = requireNonNull(streamEnabled);
        this.lastKnownStatuses = requireNonNull(lastKnownStatuses);
        this.lastKnownStatusesCapacity = lastKnownStatusesCapacity;
        this.startupData = requireNonNull(startupData);
    }

    /**
     * Processes responses from the server, storing status information.
     *
     * @param publishStreamResponse The response received from the server
     */
    @Override
    public void onNext(final PublishStreamResponse publishStreamResponse) {
        if (lastKnownStatuses.size() >= lastKnownStatusesCapacity) {
            lastKnownStatuses.pollFirst();
        }

        if (publishStreamResponse.hasAcknowledgement()) {
            final BlockAcknowledgement ack = publishStreamResponse.getAcknowledgement();
            try {
                startupData.updateLatestAckBlockStartupData(
                        ack.getBlockNumber(), ack.getBlockRootHash().toByteArray(), ack.getBlockAlreadyExists());
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        } else if (publishStreamResponse.hasResendBlock()) {
            // TODO handle resend block response
        } else if (publishStreamResponse.hasSkipBlock()) {
            // TODO handle skip block response
        } else if (publishStreamResponse.hasEndStream()) {
            streamEnabled.set(false);
            this.publishStreamResponse = publishStreamResponse;
        }
        lastKnownStatuses.add(publishStreamResponse.toString());
        LOGGER.log(INFO, "Received Response: " + publishStreamResponse);
    }

    /**
     * Returns the last received PublishStreamResponse.
     *
     * @return the last PublishStreamResponse received
     */
    public PublishStreamResponse getPublishStreamResponse() {
        return publishStreamResponse;
    }

    /**
     * Handles stream errors by disabling the stream and logging the error.
     * Currently, stops the stream for all errors, but could be enhanced with
     * retry logic in the future.
     *
     * @param streamError The error that occurred during streaming
     */
    @Override
    public void onError(@NonNull final Throwable streamError) {
        streamEnabled.set(false);
        Status status = Status.fromThrowable(streamError);
        lastKnownStatuses.add(status.toString());
        LOGGER.log(ERROR, "Error %s with status %s.".formatted(streamError, status), streamError);
    }

    /**
     * Handles stream completion by logging the event.
     */
    @Override
    public void onCompleted() {
        LOGGER.log(INFO, "Completed");
    }
}
