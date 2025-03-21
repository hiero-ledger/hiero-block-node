// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.publisher;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.common.Builder;
import io.helidon.webserver.Routing;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.PublishStreamRequestUnparsed;
import org.hiero.hapi.block.node.PublishStreamResponse;

/**
 * Provides implementation for the block stream publisher endpoints of the server. These handle incoming push block
 * streams from the consensus node.
 *
 * TODO Still lots to work out on tracking the various stages of blocks, latest in flight, etc.
 */
public class PublisherServicePlugin implements BlockNodePlugin, ServiceInterface, BlockNotificationHandler {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private BlockNodeContext context;
    private PublisherConfig publisherConfig;
    /** Set of all open sessions. */
    private final ConcurrentSkipListSet<BlockStreamProducerSession> openSessions =
            new ConcurrentSkipListSet<>();
    private final AtomicLong currentBlockNumber = new AtomicLong(UNKNOWN_BLOCK_NUMBER);
    private final AtomicReference<BlockStreamProducerSession> currentPrimarySession = new AtomicReference<>();

    /**
     * Called when any session updates its state. This is used for use to choose who is primary and behind
     */
    private void onSessionUpdate() {
        // TODO handle moving the current block number forward
        // pick a primary session
        BlockStreamProducerSession primarySession = currentPrimarySession.get();
        if (primarySession != null &&
                primarySession.currentBlockState() == BlockStreamProducerSession.BlockState.PRIMARY &&
                primarySession.currentBlockNumber() == currentBlockNumber.get()) {
            // we are already primary so all we need to do is check if any sessions are disconnected
            openSessions.removeIf(session -> session.currentBlockState() ==
                    BlockStreamProducerSession.BlockState.DISCONNECTED);
            return;
        }
        // check if we have a primary session that is not useful
        if (primarySession != null) {
            // this is odd, we have a primary session but it is not the current block number
            // TODO what to do here?
            primarySession.switchToBehind();
        }
        // pick a new primary session if there is one
        openSessions.stream()
                .filter(session ->
                        session.currentBlockState() == BlockStreamProducerSession.BlockState.NEW &&
                                session.currentBlockNumber() == currentBlockNumber.get())
                .min(Comparator.comparingLong(BlockStreamProducerSession::startTimeOfCurrentBlock))
                .ifPresentOrElse(session -> {
                    // we have a new primary session
                    session.switchToPrimary();
                    // set the current primary session
                    currentPrimarySession.set(primarySession);
                }, () -> {
                    // no primary session, set to null
                    currentPrimarySession.set(null);
                    // this can happen if all sessions are behind or ahead, so lets check if they are ahead as
                    // that will mean we will never get any blocks
                    if (openSessions.isEmpty()) {
                        // think this should never happen or at least be very rare
                        LOGGER.log(WARNING, "No sessions found, yet we got a onSessionUpdate() call");
                    } else {
                        final long currentMinSessionBlockNumber = openSessions.stream()
                                .mapToLong(BlockStreamProducerSession::currentBlockNumber).min().orElse(UNKNOWN_BLOCK_NUMBER);
                        if (currentMinSessionBlockNumber > currentBlockNumber.get()) {
                            LOGGER.log(WARNING, "All sessions are ahead [{1}] of the current block number [{2}], "
                                    + "this means we wil never get another block",
                                    currentMinSessionBlockNumber, currentBlockNumber.get());
                        }
                    }
                });
        // TODO handle timeouts
    }

    // ==== BlockNodePlugin Methods ===================================================================================

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return "Publisher Service Plugin";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Builder<?, ? extends Routing> init(BlockNodeContext context) {
        this.context = context;
        // load the publisher config
        publisherConfig = context.configuration().getConfigData(PublisherConfig.class);
        // get type of publisher to use and log it
        LOGGER.log(INFO, "Using publisher type:{1}",publisherConfig.type());
        // register us as a service
        return PbjRouting.builder().service(this);
    }

    /**
     * Called when block node is starting up after all plugins have been initialized and after web server is started.
     */
    @Override
    public void start() {
        // get the latest block number known to the system and add one for the current block
        final long latestBlockNumber = context.historicalBlockProvider().latestBlockNumber();
        // check if we know of any blocks
        if (latestBlockNumber != UNKNOWN_BLOCK_NUMBER) {
            // set the current block number to the latest block number known + 1
            currentBlockNumber.set(latestBlockNumber+1);
        }
    }

    /**
     * Called when block node ish shutting down
     */
    @Override
    public void stop() {
        LOGGER.log(INFO, "Stopping Publisher Service Plugin, closing {1} open sessions", openSessions.size());
        // close all open sessions
        openSessions.forEach(BlockStreamProducerSession::close);
        // clear open sessions
        openSessions.clear();
    }

    // ==== BlockNotificationHandler Methods ===================================================================================

    /**
     * BlockStreamPublisherService types define the gRPC methods available on the BlockStreamPublisherService.
     */
    enum BlockStreamPublisherServiceMethod implements Method {
        /**
         * The publishBlockStream method represents the bidirectional gRPC streaming method
         * Consensus Nodes should use to publish the BlockStream to the Block Node.
         */
        publishBlockStream
    }

    /**
     * Receive notifications from verification and persistence services and update our handling of listeners
     * accordingly.
     *
     * @param notification the block notification to handle
     */
    @Override
    public void handleBlockNotification(BlockNotification notification) {
        // We have nothing to do for BlockNotification.BLOCK_VERIFIED so can ignore it
        switch (notification.type()) {
            case BLOCK_PERSISTED -> {
                // let all subscribers know we have a good copy of the block saved to disk
                openSessions.forEach(session ->
                        session.sendBlockPersisted(notification.blockNumber(), notification.blockHash()));
            }
            case BLOCK_FAILED_VERIFICATION -> {
                // set the chosen source for the current block to null as we do not have one yet
                // do this first to try and avoid more bad items sent into the system
                currentPrimarySession.set(null);
                // We need to go and request all sessions to resend the block
                openSessions.forEach(session ->
                        session.requestResend(notification.blockNumber()));
                // reset out block number to last good block
                currentBlockNumber.set(notification.blockNumber()-1);
            }
        }
    }

    // ==== ServiceInterface Methods ===================================================================================

    /**
     * {@inheritDoc}
     */
    @NonNull
    public String serviceName() {
        return "BlockStreamPublisherService";
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    public String fullName() {
        return "com.hedera.hapi.block.node." + serviceName();
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    public List<Method> methods() {
        return Arrays.asList(BlockStreamPublisherServiceMethod.values());
    }

    /**
     * {@inheritDoc}
     *
     * This is called each time a new client connects to the service. In this case that means a new connection from a
     * consensus node.
     */
    @NonNull
    @Override
    public Pipeline<? super Bytes> open(
            @NonNull Method method, @NonNull RequestOptions opts, @NonNull Pipeline<? super Bytes> responses)
            throws GrpcException {
        final BlockStreamPublisherServiceMethod blockStreamPublisherServiceMethod =
                (BlockStreamPublisherServiceMethod) method;
        return switch (blockStreamPublisherServiceMethod) {
            case publishBlockStream:
                //notifier.unsubscribeAllExpired();
                yield Pipelines.<List<BlockItemUnparsed>, PublishStreamResponse>bidiStreaming()
                        .mapRequest(bytes ->
                                PublishStreamRequestUnparsed.PROTOBUF.parse(bytes).blockItemsOrThrow().blockItems())
                        .method(responsePipeline -> {
                            BlockStreamProducerSession producerBlockItemObserver =
                                    new BlockStreamProducerSession(responsePipeline, context, this::onSessionUpdate);
                            // add the session to the set of open sessions
                            openSessions.add(producerBlockItemObserver);
                            return producerBlockItemObserver;
                        })
                        .mapResponse(PublishStreamResponse.PROTOBUF::toBytes)
                        .respondTo(responses)
                        .build();
        };
    }

    /**
     * Close a session because client has closed connection
     *
     * @param session the session to close
     */
    private void closeSessionByClient(BlockStreamProducerSession session) {
        // remove the session from the set of open sessions
        openSessions.remove(session);
        // close the session
        session.close();
        // check if it is the chosen source for the current block
        if (currentPrimarySession.get() == session) {
            // set the chosen source to null
            currentPrimarySession.set(null);
            // find a new chosen source, request resend
        }
        // log the closing of the session
        LOGGER.log(DEBUG, "Closed BlockStreamProducerSession[{0}]", session.sessionCreationTime);
    }

    // ==== Inner class for a Session  =================================================================================

}
