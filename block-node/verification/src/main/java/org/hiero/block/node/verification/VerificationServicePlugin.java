// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.WARNING;

import com.swirlds.metrics.api.Counter;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

/** Provides implementation for the health endpoints of the server. */
@SuppressWarnings("unused")
public class VerificationServicePlugin implements BlockNodePlugin, BlockItemHandler {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The block node context, for access to core facilities. */
    private BlockNodeContext context;
    /** The configuration for verification */
    @SuppressWarnings("FieldCanBeLocal")
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
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(VerificationConfig.class);
    }

    /**
     * Initialize metrics for the verification plugin.
     *
     * @param context The block node context
     */
    private void initMetrics(BlockNodeContext context) {
        final var metrics = context.metrics();

        verificationBlocksReceived =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "verification_blocks_received")
                        .withDescription("Blocks received for verification"));

        verificationBlocksVerified =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "verification_blocks_verified")
                        .withDescription("Blocks that passed verification"));

        verificationBlocksFailed =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "verification_blocks_failed")
                        .withDescription("Blocks that failed verification"));

        verificationBlocksError = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "verification_blocks_error")
                .withDescription("Internal errors during verification"));

        verificationBlockTime = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "verification_block_time")
                .withDescription("Verification time per block (ms)"));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        this.context = context;
        // TODO do we need this? It is not used anywhere, what am I missing?
        // I doubled checked, we don't need configuration atm, these configs were only necessary for ASYNC impl.
        // however we should have one for pubKey, not sure if we will be able to get the pubKey from state from the very
        // beginning.
        verificationConfig = context.configuration().getConfigData(VerificationConfig.class);

        // initialize metrics
        initMetrics(context);
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
                // we already checked that firstItem has blockHeader
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
                VerificationNotification notification = currentSession.processBlockItems(blockItems.blockItems());
                if (notification != null) {
                    if (notification.success()) {
                        verificationBlocksVerified.increment();
                    } else {
                        verificationBlocksFailed.increment();
                        LOGGER.log(WARNING, "Block verification failed for block number: {0}", currentBlockNumber);
                    }
                    verificationBlockTime.add(System.nanoTime() - blockWorkStartTime);
                    context.blockMessaging().sendBlockVerification(notification);
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
