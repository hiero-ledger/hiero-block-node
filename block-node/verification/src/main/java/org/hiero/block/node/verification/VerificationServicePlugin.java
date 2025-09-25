// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.SemanticVersion;
import com.swirlds.metrics.api.Counter;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.session.BlockVerificationSession;
import org.hiero.block.node.verification.session.BlockVerificationSessionFactory;

/** Provides implementation for the health endpoints of the server. */
@SuppressWarnings("unused")
public class VerificationServicePlugin implements BlockNodePlugin, BlockItemHandler, BlockNotificationHandler {
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
        // setting config and context
        this.context = context;
        verificationConfig = context.configuration().getConfigData(VerificationConfig.class);

        // register the service
        context.blockMessaging().registerBlockNotificationHandler(this, false, "VerificationServicePlugin");

        // initialize metrics
        initMetrics(context);

        LOGGER.log(TRACE, "VerificationServicePlugin initialized successfully.");
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

        LOGGER.log(TRACE, "VerificationServicePlugin started successfully.");
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
                currentBlockNumber = blockItems.blockNumber();
                // start working time
                blockWorkStartTime = System.nanoTime();
                BlockHeader blockHeader = BlockHeader.PROTOBUF.parse(
                        blockItems.blockItems().getFirst().blockHeader());
                if (currentBlockNumber != blockHeader.number()) {
                    LOGGER.log(
                            WARNING,
                            "Block number in BlockItems ({0}) does not match number in BlockHeader ({1})",
                            currentBlockNumber,
                            blockHeader.number());
                    throw new IllegalStateException("Block number mismatch");
                }

                SemanticVersion semanticVersion = blockHeader.hapiProtoVersionOrThrow();

                // create a new verification session for the new block based on hapi version on block header.
                currentSession = BlockVerificationSessionFactory.createSession(
                        currentBlockNumber, BlockSource.PUBLISHER, semanticVersion);
                LOGGER.log(TRACE, "Started new block verification session for block number: {0}", currentBlockNumber);
            }
            if (currentSession == null) {
                // todo(452): correctly propagate this exception to the rest of the system, so it can be handled
                // from Jasper, this should be normal and just ignored, logging is fine. It will happen normally
                // anytime a block node is started in a already running network. Maybe we can check if it happening
                // when the block node is just starting vs in the middle of running normal as that should not happen.
                LOGGER.log(ERROR, "Received block items before a block header.");
            } else {
                LOGGER.log(
                        TRACE,
                        "Appending {0} block items to the current session for block number: {1}",
                        blockItems.blockItems().size(),
                        currentBlockNumber);
                VerificationNotification notification = currentSession.processBlockItems(blockItems.blockItems());
                if (notification != null) {
                    LOGGER.log(
                            TRACE,
                            "Block verification session completed for block number: {0} with status success={1}",
                            currentBlockNumber,
                            notification.success());
                    if (notification.success()) {
                        verificationBlocksVerified.increment();
                    } else {
                        verificationBlocksFailed.increment();
                        LOGGER.log(WARNING, "Block verification failed for block number: {0}", currentBlockNumber);
                    }
                    verificationBlockTime.add(System.nanoTime() - blockWorkStartTime);
                    // send the notification to the block messaging service
                    LOGGER.log(
                            TRACE, "Sending block verification notification for block number: {0}", currentBlockNumber);
                    context.blockMessaging().sendBlockVerification(notification);
                }
            }
        } catch (final Exception e) {
            LOGGER.log(ERROR, "Failed to verify BlockItems: ", e);
            verificationBlocksError.increment();
            // Return a success=false notification to indicate failure but include the block number.
            context.blockMessaging()
                    .sendBlockVerification(
                            new VerificationNotification(false, currentBlockNumber, null, null, BlockSource.PUBLISHER));
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method is called when a block backfilled notification is received. It is called on the block notification
     * thread.
     */
    @Override
    public void handleBackfilled(BackfilledBlockNotification notification) {
        try {
            // log the backfilled block notification received
            LOGGER.log(
                    TRACE, "Received backfilled block notification for block number: {0}", notification.blockNumber());
            // create a new verification session for the backfilled block

            BlockHeader blockHeader = BlockHeader.PROTOBUF.parse(
                    notification.block().blockItems().getFirst().blockHeader());
            if (notification.blockNumber() != blockHeader.number()) {
                LOGGER.log(
                        WARNING,
                        "Block number in BackfilledBlockNotification ({0}) does not match number in BlockHeader ({1})",
                        notification.blockNumber(),
                        blockHeader.number());
                throw new IllegalStateException("Block number mismatch");
            }

            BlockVerificationSession backfillSession = BlockVerificationSessionFactory.createSession(
                    notification.blockNumber(), BlockSource.BACKFILL, blockHeader.hapiProtoVersionOrThrow());

            // process the block items in the backfilled notification
            VerificationNotification backfillNotification =
                    backfillSession.processBlockItems(notification.block().blockItems());
            // Log the backfill verification result
            LOGGER.log(
                    TRACE,
                    "Verified backfilled block items for block number: {0} with success={1}",
                    notification.blockNumber(),
                    backfillNotification.success());
            // send the verification notification for the backfilled block
            context.blockMessaging().sendBlockVerification(backfillNotification);
        } catch (Exception ex) {
            LOGGER.log(ERROR, "Failed to handle verification notification: ", ex);
            verificationBlocksError.increment();
            // Return a success=false notification to indicate failure
            VerificationNotification errorNotification =
                    new VerificationNotification(false, notification.blockNumber(), null, null, BlockSource.BACKFILL);
            context.blockMessaging().sendBlockVerification(errorNotification);
        }
    }
}
