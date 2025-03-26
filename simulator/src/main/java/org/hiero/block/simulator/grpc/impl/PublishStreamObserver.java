// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc.impl;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.protoc.PublishStreamResponse;
import com.hedera.hapi.block.protoc.PublishStreamResponse.BlockAcknowledgement;
import com.hedera.hapi.block.protoc.PublishStreamResponseCode;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.common.utils.FileUtilities;
import org.hiero.block.simulator.config.data.BlockStreamConfig;

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
    private final boolean usePersistenceStartupData;
    private final Path latestAckBlockNumberPath;
    private final Path latestAckBlockHashPath;

    /**
     * Creates a new PublishStreamObserver instance.
     *
     * @param blockStreamConfig config needed to resolve the latest ack block number and hash files for startup data
     * @param streamEnabled             Controls whether streaming should continue
     * @param lastKnownStatuses         List to store the most recent status messages
     * @param lastKnownStatusesCapacity the capacity of the last known statuses
     *
     * @throws NullPointerException if any parameter is null
     */
    public PublishStreamObserver(
            @NonNull final BlockStreamConfig blockStreamConfig,
            @NonNull final AtomicBoolean streamEnabled,
            @NonNull final Deque<String> lastKnownStatuses,
            final int lastKnownStatusesCapacity) {
        this.streamEnabled = requireNonNull(streamEnabled);
        this.lastKnownStatuses = requireNonNull(lastKnownStatuses);
        this.lastKnownStatusesCapacity = lastKnownStatusesCapacity;
        this.usePersistenceStartupData = blockStreamConfig.useSimulatorStartupData();
        this.latestAckBlockNumberPath = blockStreamConfig.latestAckBlockNumberPath();
        this.latestAckBlockHashPath = blockStreamConfig.latestAckBlockHashPath();
        try {
            if (Files.notExists(latestAckBlockNumberPath)) {
                FileUtilities.createFile(latestAckBlockNumberPath);
            }
            if (Files.notExists(latestAckBlockHashPath)) {
                FileUtilities.createFile(latestAckBlockHashPath);
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Processes responses from the server, storing status information.
     *
     * @param publishStreamResponse The response received from the server
     */
    @Override
    public void onNext(PublishStreamResponse publishStreamResponse) {
        if (lastKnownStatuses.size() >= lastKnownStatusesCapacity) {
            lastKnownStatuses.pollFirst();
        }
        final BlockAcknowledgement ack =
                publishStreamResponse.getAcknowledgement().getBlockAck();
        final PublishStreamResponseCode responseCode =
                publishStreamResponse.getStatus().getStatus();
        if (usePersistenceStartupData) {
            // @todo(904) we need the correct response code, currently it seems that
            //   the response code is not being set correctly? The if check should
            //   be different and based on the response code, only saving
            if (PublishStreamResponseCode.STREAM_ITEMS_UNKNOWN == responseCode && !ack.getBlockAlreadyExists()) {
                final long blockNumber = ack.getBlockNumber();
                final byte[] blockHash = ack.getBlockRootHash().toByteArray();
                try {
                    Files.write(
                            latestAckBlockNumberPath,
                            String.valueOf(blockNumber).getBytes());
                    Files.write(latestAckBlockHashPath, blockHash);
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        lastKnownStatuses.add(publishStreamResponse.toString());
        LOGGER.log(INFO, "Received Response: " + publishStreamResponse);
    }

    /**
     * Handles stream errors by disabling the stream and logging the error.
     * Currently stops the stream for all errors, but could be enhanced with
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
