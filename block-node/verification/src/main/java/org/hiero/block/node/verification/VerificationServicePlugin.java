// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.WARNING;

import com.swirlds.metrics.api.Counter;
import io.helidon.common.Builder;
import io.helidon.webserver.Routing;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;

/** Provides implementation for the health endpoints of the server. */
public class VerificationServicePlugin implements BlockNodePlugin, BlockItemHandler {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The block node context, for access to core facilities. */
    private BlockNodeContext context;
    /** The configuration for verification */
    private VerificationConfig verificationConfig;
    /** The current verification session, a new one is created each block. */
    private BlockVerificationSession currentSession;
    /** The current block number being verified. */
    private long currentBlockNumber = -1;
    /** The time when block verification started. */
    private long blockWorkStartTime;
    /** Metric for number of blocks received. */
    private Counter verificationBlocksReceived;
    /** Metric for number of blocks verified. */
    private Counter verificationBlocksVerified;
    /** Metric for number of blocks failed verification. */
    private Counter verificationBlocksFailed;
    /** Metric for number of blocks verification errors. */
    private Counter verificationBlocksError;
    /** Metric for block verification time. */
    private Counter verificationBlockTime;

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return "Subscriber Service Plugin";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Builder<?, ? extends Routing> init(BlockNodeContext context) {
        this.context = context;
        final var metrics = context.metrics();
        // TODO do we need this? It is not used anywhere, what am I missing?
        verificationConfig = context.configuration().getConfigData(VerificationConfig.class);
        // create metrics for this plugin
        verificationBlocksReceived =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "verification_blocks_received")
                        .withDescription("Blocks Received for Verification"));
        verificationBlocksVerified =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "verification_blocks_verified")
                        .withDescription("Blocks Verified"));
        verificationBlocksFailed =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "verification_blocks_failed")
                        .withDescription("Blocks Failed Verification"));
        verificationBlocksError = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "verification_blocks_error")
                .withDescription("Blocks Verification Error"));
        verificationBlockTime = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "verification_block_time")
                .withDescription("Block Verification Time"));
        // we do not need to register any routes
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        // register to listen to incoming block items
        // specify that we are cpu intensive and should be run on a separate non-virtual thread
        context.blockMessaging().registerBlockItemHandler(this, true, VerificationServicePlugin.class.getSimpleName());
        // we do not need to unregister the handler as it will be unregistered when the message service is stopped
    }

    // ==== BlockItemHandler Methods ===================================================================================

    /**
     * This is called on a separate thread for this handler. It should use the thread and keep it busy verifying the
     * block items till they are done. By doing that it applies back pressure to the block item producer.
     *
     * @param blockItems the immutable list of block items to handle
     */
    @Override
    public void handleBlockItemsReceived(BlockItems blockItems) {
        try {
            if (!context.serverHealth().isRunning()) {
                LOGGER.log(ERROR, "Service is not running. Block item will not be processed further.");
                return;
            }
            // If we have a new block header, that means a new block has started
            if (blockItems.isStartOfNewBlock()) {
                verificationBlocksReceived.increment();
                //noinspection DataFlowIssue - we already checked that firstItem has blockHeader
                currentBlockNumber = blockItems.newBlockNumber();
                // start working time
                blockWorkStartTime = System.nanoTime();
                // start new session and set it as current
                currentSession = new BlockVerificationSession(currentBlockNumber);
            }
            if (currentSession == null) {
                // todo(452): correctly propagate this exception to the rest of the system, so it can be handled
                // from Jasper, this should be normal and just ignored, logging is fine. It will happen normally
                // anytime a block node is started in a already running network. Maybe we can check if it happening
                // when the block node is just starting vs in the middle of running normal as that should not happen.
                LOGGER.log(ERROR, "Received block items before a block header.");
            } else {
                BlockNotification notification = currentSession.processBlockItems(blockItems.blockItems());
                if (notification != null) {
                    switch (notification.type()) {
                        case BLOCK_VERIFIED -> {
                            verificationBlocksVerified.increment();
                            verificationBlockTime.add(System.nanoTime() - blockWorkStartTime);
                        }
                        case BLOCK_FAILED_VERIFICATION -> {
                            verificationBlocksFailed.increment();
                            verificationBlockTime.add(System.nanoTime() - blockWorkStartTime);
                            LOGGER.log(WARNING, "Block verification failed for block number: {0}", currentBlockNumber);
                        }
                    }
                    context.blockMessaging().sendBlockNotification(notification);
                }
            }
        } catch (final Exception e) {
            LOGGER.log(ERROR, "Failed to verify BlockItems: ", e);
            verificationBlocksError.increment();
            // TODO is shutting down here really the right thing to do?
            // Trigger the server to stop accepting new requests
            context.serverHealth().shutdown(VerificationServicePlugin.class.getSimpleName(), e.getMessage());
        }
    }
}
