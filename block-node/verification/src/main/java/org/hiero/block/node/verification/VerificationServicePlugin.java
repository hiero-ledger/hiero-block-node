// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
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
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
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
        if (verificationConfig.calculateRootHashOfAllPreviousBlocksEnabled()) {
            try {
                // 1. Initial Setup
                Path path = requireNonNull(verificationConfig.rootHashOfAllPreviousBlocksPath());
                this.streamingHasherAllPreviousBlocks = new StreamingHasher();
                StreamingHasher hasher = this.streamingHasherAllPreviousBlocks;
                Files.createDirectories(path.getParent());

                BlockRangeSet available = context.historicalBlockProvider().availableBlocks();

                // 2. Load Data
                if (available.size() == 0) {
                    initializeGenesis(path, hasher);
                } else if (Files.exists(path)) {
                    loadFromFile(path, hasher);
                    updateTreeHasherFromStorage(hasher, hasher.leafCount() - 1, available.max());
                } else {
                    rebuildFromProvider(hasher, available);
                }
                // 3. Validate ro
                validateState(hasher, available.max());
            } catch (IOException | NoSuchAlgorithmException | IllegalStateException | ParseException e) {
                LOGGER.log(WARNING, "Falling back to footer values. Reason: " + e.getMessage(), e);
                this.streamingHasherAllPreviousBlocks = null; // Ensure we return null on failure
            }
        }
    }

    private void initializeGenesis(Path path, StreamingHasher hasher) throws IOException {
        Files.deleteIfExists(path);
        Files.createFile(path);
        // Only add to memory; the regular lifecycle will handle the first write
        appendLatestHashToAllPreviousBlocksStreamingHasher(ZERO_BLOCK_HASH.toByteArray());
    }

    private void loadFromFile(Path path, StreamingHasher hasher) throws IOException {
        try (var inputStream = new BufferedInputStream(Files.newInputStream(path))) {
            byte[] buffer = new byte[BLOCK_HASH_LENGTH];
            while (inputStream.readNBytes(buffer, 0, BLOCK_HASH_LENGTH) == BLOCK_HASH_LENGTH) {
                hasher.addLeaf(Arrays.copyOf(buffer, BLOCK_HASH_LENGTH));
            }
        }
    }

    private void rebuildFromProvider(StreamingHasher hasher, BlockRangeSet available) throws ParseException {
        if (available.streamRanges().count() == 1 && available.min() == 0) {
            updateTreeHasherFromStorage(hasher, 0, available.max());
        }
    }

    private void updateTreeHasherFromStorage(StreamingHasher hasher, long start, long end) throws ParseException {
        for (long i = start; i <= end; i++) {
            byte[] previousHash = extractPreviousRootHashFromBlock(i);
            appendLatestHashToAllPreviousBlocksStreamingHasher(previousHash);
            // if last block
            if( i == end ) {
                // add latest block hash, needs to recalculate hash
                byte[] latestBlockHash = calculateBlockHashFromBlockNumber(i, previousHash);
                appendLatestHashToAllPreviousBlocksStreamingHasher(latestBlockHash);
                this.previousBlockHash  = Bytes.wrap(latestBlockHash);
            }
        }
    }

    /**
     * Calculate the block hash for a given block number using the previous block hash.
     *
     * @param blockNumber the block number to calculate the hash for
     * @param previousBlockHash the previous block hash if absent will be used from block footer
     * @return the calculated block hash
     * @throws ParseException if there is an error parsing the block or its items.
     */
    private byte[] calculateBlockHashFromBlockNumber(long blockNumber, byte[] previousBlockHash) throws ParseException {
        Block block = context.historicalBlockProvider().block(blockNumber).block();
        BlockHeader blockHeader = block.items().getFirst().blockHeader();
        List<BlockItemUnparsed> items = block.items().stream().map(item -> {
            try {
                return BlockItemUnparsed.PROTOBUF.parse(BlockItem.PROTOBUF.toBytes(item));
            } catch (ParseException e) {
                LOGGER.log(WARNING, "Failed to parse block item during block hash calculation for block " + blockNumber, e);
                throw new RuntimeException(e);
            }
        }).toList();

        VerificationSession session = HapiVersionSessionFactory.createSession(blockNumber, BlockSource.HISTORY, blockHeader.hapiProtoVersion());
        session.processBlockItems(items);
        Bytes rootHashOfAllPreviousBlocks = Bytes.wrap(streamingHasherAllPreviousBlocks.computeRootHash());
        VerificationNotification result = session.finalizeVerification(rootHashOfAllPreviousBlocks, Bytes.wrap(previousBlockHash));
        return result.blockHash().toByteArray();
    }

    private byte[] extractPreviousRootHashFromBlock(long blockNumber) {
        var items = context.historicalBlockProvider().block(blockNumber).block().items();

        // blocks place the footer closet to the very end.
        // We iterate backwards to find it almost instantly.
        for (int i = items.size() - 1; i >= 0; i--) {
            var item = items.get(i);
            if (item.item().kind() == BlockItem.ItemOneOfType.BLOCK_FOOTER) {
                return item.blockFooter().previousBlockRootHash().toByteArray();
            }
        }

        throw new IllegalStateException("Missing footer at block " + blockNumber);
    }

    /***
     * Validate that the streaming hasher state matches the expected max block.
     * maxBlock is inclusive and counts 0, while leafCount counts from 1,
     * and adds the ZERO block hash as the first leaf before block 0.
     *
     * @param hasher   the streaming hasher to validate
     * @param maxBlock the expected maximum block number
     */
    private void validateState(StreamingHasher hasher, long maxBlock) {
        if ((hasher.leafCount() - 2) != maxBlock) {
            LOGGER.log(WARNING, "Hasher state mismatch: {0} leaves vs {1} max block, falling back to use block footer values", hasher.leafCount() - 1, maxBlock);
            this.streamingHasherAllPreviousBlocks = null; // Invalidate hasher on mismatch
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
