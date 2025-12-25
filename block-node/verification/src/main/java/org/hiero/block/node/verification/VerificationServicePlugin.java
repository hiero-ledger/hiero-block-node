// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import org.hiero.block.common.hasher.StreamingHasher;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.session.HapiVersionSessionFactory;
import org.hiero.block.node.verification.session.VerificationSession;

/** Provides implementation for the health endpoints of the server. */
@SuppressWarnings("unused")
public class VerificationServicePlugin implements BlockNodePlugin, BlockItemHandler, BlockNotificationHandler {
    private static final String COMPLETED_MESSAGE = "Verified backfill block items for block={0} with success={1}";
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The block node context, for access to core facilities. */
    private BlockNodeContext context;
    /** The configuration for verification */
    @SuppressWarnings("FieldCanBeLocal")
    private VerificationConfig verificationConfig;
    /** The current verification session, a new one is created each block. */
    private VerificationSession currentSession;
    /** The current block number being verified. */
    private long currentBlockNumber = -1;
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
    /** Metric for block hashing time. ignores time to receive block */
    private Counter hashingBlockTimeNs;
    /** Streaming hasher for all previous blocks hashes. */
    private StreamingHasher streamingHasherAllPreviousBlocks;
    /** The previous block hash, used for verification of the current block. */
    private Bytes previousBlockHash;
    /** ZERO block hash constant. */
    static final Bytes ZERO_BLOCK_HASH = Bytes.wrap(new byte[48]);
    /** Expected size of a block hash in bytes. */
    private static final int BLOCK_HASH_LENGTH = ZERO_BLOCK_HASH.toByteArray().length;

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
        hashingBlockTimeNs = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "hashing_block_time")
                .withDescription("Hashing time per block (ms)"));
    }

    private void initializePreviousAllBlocksHasher() {

        // Verify is enabled.
        if (!verificationConfig.calculateRootHashOfAllPreviousBlocksEnabled()) {
            // feature disable, we keep the hasher null
            streamingHasherAllPreviousBlocks = null;
            return;
        }

        // init streaming hasher
        try {
            streamingHasherAllPreviousBlocks = new StreamingHasher();
        } catch (NoSuchAlgorithmException e) {
            streamingHasherAllPreviousBlocks = null;
            LOGGER.log(
                    WARNING,
                    "Unable to initialize streaming hasher for all previous blocks; falling back to footer values.",
                    e);
            return;
        }

        final Path hasherPath = requireNonNull(
                verificationConfig.rootHashOfAllPreviousBlocksPath(),
                "verification.rootHashOfAllPreviousBlocksPath path must not be null or empty when enabled.");

        // make sure directories exist
        try {
            Files.createDirectories(hasherPath.getParent());
        } catch (IOException e) {
            LOGGER.log(WARNING, "Failed to create directories for hasher file at " + hasherPath, e);
        }

        // if there are no blocks lets do a new empty file and return, regardless of existing file
        if (context.historicalBlockProvider().availableBlocks().size() == 0) {
            try {
                Files.deleteIfExists(hasherPath);
                Files.createFile(hasherPath);
                // for a new genesis, the previous block hash is ZERO
                appendLatestHashToAllPreviousBlocksStreamingHasher(ZERO_BLOCK_HASH.toByteArray());
            } catch (IOException e) {
                LOGGER.log(WARNING, "Failed to create empty hasher file at " + hasherPath, e);
            }
            return;
        }

        // if file exists, load existing state
        if (Files.exists(hasherPath)) {
            try (BufferedInputStream inputStream = new BufferedInputStream(Files.newInputStream(hasherPath))) {
                final byte[] buffer = new byte[BLOCK_HASH_LENGTH];
                long loadedHashes = 0;
                int read;
                while ((read = inputStream.readNBytes(buffer, 0, BLOCK_HASH_LENGTH)) == BLOCK_HASH_LENGTH) {
                    final byte[] hashCopy = Arrays.copyOf(buffer, BLOCK_HASH_LENGTH);
                    streamingHasherAllPreviousBlocks.addLeaf(hashCopy);
                    loadedHashes++;
                }

                if (read > 0 && read < BLOCK_HASH_LENGTH) {
                    LOGGER.log(
                            WARNING,
                            "Detected trailing {0} byte(s) in {1}; ignoring incomplete hash entry.",
                            read,
                            hasherPath);
                }

                // Should not be needed, but just in case, load any missing hashes from historical blocks
                // might come useful if we decide to save to file every X amount of blocks instead of every block
                if (loadedHashes
                        < context.historicalBlockProvider().availableBlocks().max()) {
                    // load differential from block footers
                    // decrement -1 due to ZERO_BLOCK being the first hash before block 0. avoid a one-off error.
                    for (long i = loadedHashes - 1;
                            i
                                    <= context.historicalBlockProvider()
                                            .availableBlocks()
                                            .max();
                            i++) {
                        Block block = context.historicalBlockProvider().block(i).block();
                        final BlockFooter blockFooter = block.items().stream()
                                .filter(blockItem -> blockItem.item().kind() == BlockItem.ItemOneOfType.BLOCK_FOOTER)
                                .findFirst()
                                .get()
                                .blockFooter();
                        streamingHasherAllPreviousBlocks.addLeaf(
                                blockFooter.previousBlockRootHash().toByteArray());
                    }
                }

                if (streamingHasherAllPreviousBlocks.leafCount() - 1
                        != context.historicalBlockProvider().availableBlocks().max()) {
                    LOGGER.log(
                            WARNING,
                            "Loaded leaf count {0} does not match expected block count {1} from historical provider.",
                            streamingHasherAllPreviousBlocks.leafCount() - 1,
                            context.historicalBlockProvider().availableBlocks().max());
                }

            } catch (IOException e) {
                streamingHasherAllPreviousBlocks = null;
                LOGGER.log(
                        WARNING,
                        "Failed to load streaming hasher state from %s , falling back to block footer values."
                                .formatted(hasherPath),
                        e);
            }
        } else {
            // there is no existing file, but there might be an available range from genesis till latest block
            final long latestBlockNumber =
                    context.historicalBlockProvider().availableBlocks().max();
            final long blocksInMemory =
                    context.historicalBlockProvider().availableBlocks().size();
            final long firstBlockNumber =
                    context.historicalBlockProvider().availableBlocks().min();
            // if there is only 1 range, and the first available block is 0, we can assume we have all blocks from
            // genesis to latest
            if (context.historicalBlockProvider()
                                    .availableBlocks()
                                    .streamRanges()
                                    .count()
                            == 1
                    && context.historicalBlockProvider().availableBlocks().min() == 0) {
                for (long i = 0; i <= latestBlockNumber; i++) {
                    Block block = context.historicalBlockProvider().block(i).block();
                    final BlockFooter blockFooter = block.items().stream()
                            .filter(blockItem -> blockItem.item().kind() == BlockItem.ItemOneOfType.BLOCK_FOOTER)
                            .findFirst()
                            .get()
                            .blockFooter();
                    streamingHasherAllPreviousBlocks.addLeaf(
                            blockFooter.previousBlockRootHash().toByteArray());
                }
            }
        }
    }

    /***
     * Append the latest block hash to the streaming hasher for all previous blocks.     *
     * @param blockHashBytes the latest block hash to append
     */
    private void appendLatestHashToAllPreviousBlocksStreamingHasher(@NonNull final byte[] blockHashBytes) {
        if (streamingHasherAllPreviousBlocks != null) {
            streamingHasherAllPreviousBlocks.addLeaf(blockHashBytes);
        }
        final Path hasherPath = verificationConfig.rootHashOfAllPreviousBlocksPath();
        try (BufferedOutputStream outputStream = new BufferedOutputStream(
                Files.newOutputStream(hasherPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND))) {
            outputStream.write(blockHashBytes);
        } catch (IOException e) {
            LOGGER.log(WARNING, "Failed to persist block hash to " + hasherPath, e);
        }
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
        // load previous blocks hasher if enabled
        initializePreviousAllBlocksHasher();
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
            final long startVerificationHandlingTime = System.nanoTime();
            if (!context.serverHealth().isRunning()) {
                LOGGER.log(ERROR, "Service is not running. Block item will not be processed further.");
                return;
            }
            final boolean headerValid;
            // If we have a new block header, that means a new block has started
            if (blockItems.isStartOfNewBlock()) {
                verificationBlocksReceived.increment();
                // we already checked that firstItem has blockHeader
                currentBlockNumber = blockItems.blockNumber();
                BlockHeader blockHeader = BlockHeader.PROTOBUF.parse(
                        blockItems.blockItems().getFirst().blockHeader());
                if (currentBlockNumber != blockHeader.number()) {
                    final String message = "Block number {0} in block items does not match block {1} in header.";
                    LOGGER.log(INFO, message, currentBlockNumber, blockHeader.number());
                    headerValid = false;
                } else {
                    headerValid = true;
                }
                SemanticVersion semanticVersion = blockHeader.hapiProtoVersionOrThrow();
                // create a new verification session for the new block based on hapi version on block header.
                currentSession = HapiVersionSessionFactory.createSession(
                        currentBlockNumber, BlockSource.PUBLISHER, semanticVersion);
                LOGGER.log(TRACE, "Started new block verification session for block number {0}", currentBlockNumber);
            } else {
                headerValid = true; // header not present, assume it was valid
            }
            if (currentSession == null) {
                // from Jasper, this should be normal and just ignored, logging is fine. It will happen normally
                // anytime a block node is started in a already running network. Maybe we can check if it happening
                // when the block node is just starting vs in the middle of running normal as that should not happen.
                // ERROR is not appropriate for "normal" events, so use INFO.
                LOGGER.log(INFO, "Received block items before a block header, ignoring.");
            } else if (headerValid) {
                final String traceMessage = "Appending {0} block items to the current session for block number: {1}";
                LOGGER.log(TRACE, traceMessage, blockItems.blockItems().size(), currentBlockNumber);
                long startHashingTime = System.nanoTime();
                currentSession.processBlockItems(blockItems.blockItems());
                long hashingTime = System.nanoTime() - startHashingTime;
                hashingBlockTimeNs.add(hashingTime);
                // if this is the end of the block, finalize verification
                if (blockItems.isEndOfBlock()) {
                    Bytes rootHashOfAllBlockHashesTree = null;
                    if (streamingHasherAllPreviousBlocks != null && streamingHasherAllPreviousBlocks.leafCount() > 0) {
                        try {
                            rootHashOfAllBlockHashesTree =
                                    Bytes.wrap(streamingHasherAllPreviousBlocks.computeRootHash());
                        } catch (IllegalStateException e) {
                            LOGGER.log(
                                    WARNING,
                                    "Could not get root hash of all previous blocks hasher, falling back to using provided on block footer.");
                        }
                    }
                    VerificationNotification notification =
                            currentSession.finalizeVerification(rootHashOfAllBlockHashesTree, this.previousBlockHash);
                    LOGGER.log(TRACE, COMPLETED_MESSAGE, currentBlockNumber, notification.success());
                    if (notification.success()) {
                        verificationBlocksVerified.increment();
                        // send the notification to the block messaging service
                        LOGGER.log(TRACE, "Sending verification notification for block={0}", currentBlockNumber);
                        context.blockMessaging().sendBlockVerification(notification);
                        // Update previousBlockHash for next block verification
                        this.previousBlockHash = notification.blockHash();
                        // Update streamingHasherAllPreviousBlocks
                        appendLatestHashToAllPreviousBlocksStreamingHasher(this.previousBlockHash.toByteArray());

                    } else {
                        LOGGER.log(INFO, "Verification failed for block={0}", currentBlockNumber);
                        sendFailureNotification(currentBlockNumber, BlockSource.PUBLISHER);
                    }

                    // end working time
                    long blockWorkEndTime = System.nanoTime() - startVerificationHandlingTime;
                    LOGGER.log(
                            TRACE,
                            "Finished verification handling block items for block={0,number,#} nsVerificationDuration={1,number,#}",
                            currentBlockNumber,
                            blockWorkEndTime);
                    verificationBlockTime.add(blockWorkEndTime);
                }
            } else {
                sendFailureNotification(currentBlockNumber, BlockSource.PUBLISHER);
            }
        } catch (final RuntimeException | ParseException e) {
            LOGGER.log(WARNING, "Failed to verify BlockItems.", e);
            sendFailureNotification(currentBlockNumber, BlockSource.PUBLISHER);
        }
    }

    private void sendFailureNotification(final long blockNumber, final BlockSource source) {
        verificationBlocksError.increment();
        // Return a success=false notification to indicate failure and include the block number.
        VerificationNotification notification = new VerificationNotification(false, blockNumber, null, null, source);
        context.blockMessaging().sendBlockVerification(notification);
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
            LOGGER.log(TRACE, "Received backfill notification for block={0}", notification.blockNumber());
            // create a new verification session for the backfilled block
            BlockHeader blockHeader = BlockHeader.PROTOBUF.parse(
                    notification.block().blockItems().getFirst().blockHeader());
            if (notification.blockNumber() != blockHeader.number()) {
                final String message = "Block number {0} in backfill does not match block {1} in header.";
                LOGGER.log(WARNING, message, notification.blockNumber(), blockHeader.number());
                sendFailureNotification(notification.blockNumber(), BlockSource.BACKFILL);
            } else {
                VerificationSession backfillSession = HapiVersionSessionFactory.createSession(
                        notification.blockNumber(), BlockSource.BACKFILL, blockHeader.hapiProtoVersionOrThrow());
                // process the block items in the backfilled notification
                backfillSession.processBlockItems(notification.block().blockItems());
                VerificationNotification backfillNotification = backfillSession.finalizeVerification(null, null);
                // Log the backfill verification result
                LOGGER.log(TRACE, COMPLETED_MESSAGE, notification.blockNumber(), backfillNotification.success());
                // send the verification notification for the backfilled block
                context.blockMessaging().sendBlockVerification(backfillNotification);
            }
        } catch (RuntimeException | ParseException e) {
            LOGGER.log(WARNING, "Failed to handle backfill notification ", e);
            sendFailureNotification(notification.blockNumber(), BlockSource.BACKFILL);
        }
    }
}
