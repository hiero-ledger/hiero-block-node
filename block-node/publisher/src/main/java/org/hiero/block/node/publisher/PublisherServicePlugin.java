// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.publisher;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;

import org.hiero.hapi.block.node.Acknowledgement;
import org.hiero.hapi.block.node.BlockAcknowledgement;
import org.hiero.hapi.block.node.EndOfStream;
import org.hiero.hapi.block.node.PublishStreamResponse;
import org.hiero.hapi.block.node.PublishStreamResponse.ResponseOneOfType;
import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.common.Builder;
import io.helidon.webserver.Routing;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.PublishStreamRequestUnparsed;
import org.hiero.hapi.block.node.PublishStreamResponseCode;
import org.hiero.hapi.block.node.ResendBlock;

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
    private final AtomicLong currentBlockNumber = new AtomicLong(-1);
    private final AtomicReference<BlockStreamProducerSession> chosenSourceForCurrentBlock = new AtomicReference<>();


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
     * Called when block node ish shutting down
     */
    @Override
    public void stop() {
        LOGGER.log(INFO, "Stopping Publisher Service Plugin, closing {1} open sessions", openSessions.size());
        // send close response to all open sessions, then close
        final PublishStreamResponse closeResponse =
                new PublishStreamResponse(new OneOf<>(
                        ResponseOneOfType.END_STREAM,
                        new EndOfStream(PublishStreamResponseCode.STREAM_ITEMS_SUCCESS,currentBlockNumber.get())));
        openSessions.forEach(session -> {
            // send the close response to the client
            session.sendResponse(closeResponse);
            // close the session
            session.close();
        });
        // clear open sessions
        openSessions.clear();
    }

    // ==== BlockNotificationHandler Methods ===================================================================================

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
                final PublishStreamResponse goodBlockResponse =
                        new PublishStreamResponse(new OneOf<>(
                                ResponseOneOfType.ACKNOWLEDGEMENT,
                                new Acknowledgement(
                                        new BlockAcknowledgement(
                                                notification.blockNumber(),
                                                notification.blockHash(),
                                                false
                                        ))));
                openSessions.forEach(session -> session.sendResponse(goodBlockResponse));
            }
            case BLOCK_FAILED_VERIFICATION -> {
                // set the chosen source for the current block to null as we do not have one yet
                // do this first to try and avoid more bad items sent into the system
                chosenSourceForCurrentBlock.set(null);
                // We need to go and request all sessions to resend the block
                final PublishStreamResponse resendBlockResponse =
                        new PublishStreamResponse(new OneOf<>(
                                ResponseOneOfType.RESEND_BLOCK,
                                new ResendBlock(notification.blockNumber()
                                )));
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
                                    new BlockStreamProducerSession(responsePipeline);
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
        // send the close message to the client
        final PublishStreamResponse closeResponse =
                new PublishStreamResponse(new OneOf<>(
                        ResponseOneOfType.END_STREAM,
                        new EndOfStream(PublishStreamResponseCode.STREAM_ITEMS_SUCCESS,currentBlockNumber.get())));
        session.sendResponse(closeResponse);
        // close the session
        session.close();
        // check if it is the chosen source for the current block
        if (chosenSourceForCurrentBlock.get() == session) {
            // set the chosen source to null
            chosenSourceForCurrentBlock.set(null);
            // find a new chosen source, request resend
        }
        // log the closing of the session
        LOGGER.log(DEBUG, "Closed BlockStreamProducerSession[{0}]", session.sessionCreationTime);
    }

    // ==== Inner class for a Session  =================================================================================

    /**
     * BlockStreamProducerSession is a session for a block stream producer. It handles the incoming block stream and
     * sends the responses to the client.
     */
    private class BlockStreamProducerSession
            implements Pipeline<List<BlockItemUnparsed>>,
            Comparable<BlockStreamProducerSession>{

        private final long sessionCreationTime = System.nanoTime();
        private final Pipeline<? super PublishStreamResponse> responsePipeline;
        private Flow.Subscription subscription;

        public BlockStreamProducerSession(Pipeline<? super PublishStreamResponse> responsePipeline) {
            this.responsePipeline = responsePipeline;
            // log the creation of the session
            LOGGER.log(DEBUG, "Created new BlockStreamProducerSession[{0}]", sessionCreationTime);
        }

        /**
         * Close the session and cancel the subscription.
         */
        public void close() {
            if (subscription != null) {
                subscription.cancel();
            }
        }

        /**
         * Send a response to the client consensus node.
         *
         * @param response the response to send
         */
        public void sendResponse(PublishStreamResponse response) {
            // send the response to the client
            responsePipeline.onNext(response);
        }

        // ==== Pipeline Flow Methods ==================================================================================

        @Override
        public void onNext(List<BlockItemUnparsed> items) throws RuntimeException {
            // check if we are the chosen source for the current block
            if (chosenSourceForCurrentBlock.get() == this) {
                // send the items to the block messaging service
                context.blockMessaging().sendBlockItems(items);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            LOGGER.log(DEBUG, "BlockStreamProducerSession error: {0}", throwable.getMessage());
            // close the session
            closeSessionByClient(this);
        }

        @Override
        public void clientEndStreamReceived() {
            closeSessionByClient(this);
        }

        @Override
        public void onComplete() {
            closeSessionByClient(this);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            LOGGER.log(DEBUG, "onSubscribe called");
            // TODO seems like we should be using {subscription} for flow control, calling its request() method
        }

        // ==== Comparable Methods ===================================================================================

        /**
         * Compare this session to another session based on the creation time. We need to be comparable so we can
         * sort the sessions in the set. We depend on the fact that sessionCreationTime being in nanoseconds is unique
         * for each session. So equals and hashcode based on object identity is the same as comparing
         * sessionCreationTime.
         *
         * @param o the object to be compared.
         * @return a negative integer, zero, or a positive integer as this session is less than,
         *          equal to, or greater than the specified object.
         */
        @Override
        public int compareTo(BlockStreamProducerSession o) {
            return Long.compare(sessionCreationTime, o.sessionCreationTime);
        }
    }
}
