// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher.fixtures;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.pbj.runtime.grpc.Pipeline;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.stream.publisher.PublisherHandler;
import org.hiero.block.node.stream.publisher.PublisherHandler.MetricsHolder;
import org.hiero.block.node.stream.publisher.StreamPublisherManager;

/**
 * A test fixture for the {@link StreamPublisherManager}.
 */
public class TestStreamPublisherManager implements StreamPublisherManager {
    /** The message to be used when the handlePersisted method is called in an illegal state. */
    private static final String PERSISTED_NOTIFICATION_ILLEGAL_STATE_MESSAGE =
            """
    Illegal state for publisher manager test fixture.
    `handlePersisted` is called when `latestBlockNumber` is greater than the argument notification's end block number.
    This is not allowed in fixtures, the latest block number must always be set explicitly to a valid value before calling `handlePersisted` in order to mitigate false positives.
    latestBlockNumber: %d, notification end block number: %d
    """;
    /** The map of calls to closeBlock, with the handler id as key and the number of calls as value. */
    final Map<Long, Integer> closeBlockCalls = new LinkedHashMap<>();
    /** The map of calls to closeBlock with null proof bytes, with the handler id as key and the number of calls as value. */
    final Map<Long, Integer> nullCloseBlockCalls = new LinkedHashMap<>();
    /** The list of publisher handlers managed by this manager. This could be a map with handler id as key if needed */
    private final List<PublisherHandler> publisherHandlers = new ArrayList<>();
    /** The BlockAction to return when querying for next action for a block. */
    private BlockAction blockAction;
    /** The latest block number to be returned. */
    private long latestBlockNumber = -1L;

    @Override
    public PublisherHandler addHandler(
            @NonNull final Pipeline<? super PublishStreamResponse> replies,
            @NonNull final MetricsHolder handlerMetrics) {
        // do nothing, implement when needed
        return null;
    }

    @Override
    public void removeHandler(final long handlerId) {
        // do nothing, implement when needed
    }

    @Override
    public BlockAction getActionForBlock(
            final long blockNumber, final BlockAction previousAction, final long handlerId) {
        return blockAction;
        // @todo consider if we should reset the action here so we know that
        //    the action returned is always set separately for each message.
    }

    @Override
    public void closeBlock(final BlockProof blockEndProof, final long handlerId) {
        // Increment the number of calls for the handler id
        if (blockEndProof == null) {
            nullCloseBlockCalls.merge(handlerId, 1, Integer::sum);
        } else {
            closeBlockCalls.merge(handlerId, 1, Integer::sum);
        }
    }

    @Override
    public long getLatestBlockNumber() {
        return latestBlockNumber;
    }

    @Override
    public void notifyTooFarBehind(long newestKnownBlockNumber) {
        // Do nothing.
    }

    @Override
    public void handleVerification(final VerificationNotification notification) {
        throw new UnsupportedOperationException("implement handleVerification in test class");
    }

    /**
     * Handle a persisted notification.
     * <p>
     * Please note that this fixture implementation should be called only
     * after explicitly setting the latest block number to a valid value. A
     * valid value is a value that is lower than the end block number
     * of the notification. This is done so that we can make a best effort to
     * mitigate false positives in tests that use this fixture. Also, this
     * method will NOT update the state of the manager (latestBlockNumber)! Any
     * updates must be explicit!
     */
    @Override
    public void handlePersisted(final PersistedNotification notification) {
        final long newLastPersistedBlock = notification.endBlockNumber();
        if (newLastPersistedBlock > latestBlockNumber) {
            publisherHandlers.forEach(h -> h.sendAcknowledgement(newLastPersistedBlock));
        } else {
            throw new IllegalStateException(
                    PERSISTED_NOTIFICATION_ILLEGAL_STATE_MESSAGE.formatted(latestBlockNumber, newLastPersistedBlock));
        }
    }

    /**
     * Fixture method to get the number of calls to closeBlock for a handler.
     * <p>
     * Returns the number of calls to {@link #closeBlock(BlockProof, long)}
     * made by the handler with the given ID. If the handler ID is not found,
     * returns -1.
     */
    public int closeBlockCallsForHandler(final long handlerId) {
        return closeBlockCalls.getOrDefault(handlerId, -1);
    }

    /**
     * Fixture method to get the number of calls to closeBlock with null proof
     * for a handler.
     * <p>
     * Returns the number of calls to {@link #closeBlock(BlockProof, long)}
     * made by the handler with the given ID, where the proof was null.
     * If the handler ID is not found, returns -1.
     */
    public int nullCloseBlockCallsForHandler(final long handlerId) {
        return nullCloseBlockCalls.getOrDefault(handlerId, -1);
    }

    /**
     * Fixture method to add a handler.
     * <p>
     * This method is used when we want to add an already initialized handler
     * to the manager and not use the {@link #addHandler(Pipeline, MetricsHolder)}
     * method.<br/>
     */
    public void addHandler(@NonNull final PublisherHandler handler) {
        publisherHandlers.add(Objects.requireNonNull(handler));
    }

    /**
     * Fixture method to set the block action.
     * <p>
     * This method will set the action to be returned by
     * {@link StreamPublisherManager#getActionForBlock(long, BlockAction, long)}.<br/>
     * Overwritable.<br/>
     * If this method has not been called, the default return from getActionForBlock is null.<br/>
     * We use null so as to always be explicit about the action to be returned in tests,
     * otherwise we might think tests are covering cases that are not covered.
     *
     * @param blockAction The action to return. This value is returned until
     *     this method is called again.
     */
    public void setBlockAction(final BlockAction blockAction) {
        this.blockAction = blockAction;
    }

    /**
     * Fixture method. This method will set the latest block number to be returned
     * by {@link #getLatestBlockNumber()}. Overwritable. If not set, it will
     * return -1L, which is a best effort to ensure no false positives.
     *
     * @param latestBlockNumber to set
     */
    public void setLatestBlockNumber(final long latestBlockNumber) {
        this.latestBlockNumber = latestBlockNumber;
    }
}
