// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc.impl;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.util.Objects.requireNonNull;
import static org.hiero.block.simulator.metrics.SimulatorMetricTypes.Counter.LiveBlocksProcessed;

import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.block.stream.protoc.BlockProof;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Deque;
import java.util.List;
import org.hiero.block.api.protoc.PublishStreamRequest;
import org.hiero.block.api.protoc.PublishStreamResponse;
import org.hiero.block.api.protoc.PublishStreamResponse.Acknowledgement;
import org.hiero.block.api.protoc.PublishStreamResponse.BlockAcknowledgement;
import org.hiero.block.simulator.metrics.MetricsService;

/**
 * A gRPC stream observer implementation that handles incoming {@link PublishStreamRequest} messages
 * on the server side. This observer processes incoming block stream publications, maintains stream status
 * history, and manages the response stream back to the client. It implements flow control by tracking
 * stream state and enforcing capacity limits on status history.
 */
public class PublishStreamServerObserver implements StreamObserver<PublishStreamRequest> {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    // Service dependencies
    private final MetricsService metricsService;

    // gRPC Components
    private final StreamObserver<PublishStreamResponse> responseObserver;

    // State
    private final int lastKnownStatusesCapacity;
    private final Deque<String> lastKnownStatuses;

    /**
     * Constructs a new PublishStreamServerObserver that handles stream requests and maintains a history of statuses.
     *
     * @param responseObserver The observer that handles responses back to the client
     * @param metricsService The service for recording consumption metrics
     * @param lastKnownStatuses A deque to store the history of request statuses
     * @param lastKnownStatusesCapacity The maximum number of statuses to maintain in the history
     * @throws NullPointerException if responseObserver or lastKnownStatuses is null
     */
    public PublishStreamServerObserver(
            @NonNull final StreamObserver<PublishStreamResponse> responseObserver,
            @NonNull final MetricsService metricsService,
            @NonNull final Deque<String> lastKnownStatuses,
            final int lastKnownStatusesCapacity) {
        this.responseObserver = requireNonNull(responseObserver);
        this.metricsService = requireNonNull(metricsService);
        this.lastKnownStatuses = requireNonNull(lastKnownStatuses);
        this.lastKnownStatusesCapacity = lastKnownStatusesCapacity;
    }

    /**
     * Processes incoming publish stream requests, maintaining a history of requests and handling block acknowledgements.
     * If the request contains block items with a block proof, generates and sends an acknowledgement response.
     *
     * @param publishStreamRequest The incoming stream request to process
     */
    @Override
    public void onNext(PublishStreamRequest publishStreamRequest) {
        if (lastKnownStatuses.size() >= lastKnownStatusesCapacity) {
            lastKnownStatuses.pollFirst();
        }
        lastKnownStatuses.add(publishStreamRequest.toString());

        if (publishStreamRequest.hasBlockItems()) {
            final List<BlockItem> blockItemList =
                    publishStreamRequest.getBlockItems().getBlockItemsList();
            if (blockItemList.getLast().hasBlockProof()) {
                final BlockProof blockProof = publishStreamRequest
                        .getBlockItems()
                        .getBlockItemsList()
                        .getLast()
                        .getBlockProof();
                final PublishStreamResponse publishStreamResponse = handleBlockAckResponse(blockProof);

                responseObserver.onNext(publishStreamResponse);
                metricsService.get(LiveBlocksProcessed).increment();
            }
        }
    }

    /**
     * Handles errors that occur during stream processing.
     *
     * @param streamError The error that occurred during stream processing
     */
    @Override
    public void onError(@NonNull final Throwable streamError) {
        Status status = Status.fromThrowable(streamError);
        LOGGER.log(ERROR, "Error %s with status %s.".formatted(streamError, status), streamError);
    }

    /**
     * Handles the completion of the stream by completing the response observer and logging the event.
     */
    @Override
    public void onCompleted() {
        responseObserver.onCompleted();
        LOGGER.log(INFO, "Completed");
    }

    private PublishStreamResponse handleBlockAckResponse(BlockProof blockProof) {
        final long blockNumber = blockProof.getBlock();
        final BlockAcknowledgement blockAcknowledgement =
                BlockAcknowledgement.newBuilder().setBlockNumber(blockNumber).build();
        final Acknowledgement ack =
                Acknowledgement.newBuilder().setBlockAck(blockAcknowledgement).build();
        LOGGER.log(INFO, "Returning block acknowledgement for block number: %s".formatted(blockNumber));

        return PublishStreamResponse.newBuilder().setAcknowledgement(ack).build();
    }
}
