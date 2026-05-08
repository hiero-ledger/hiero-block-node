// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.cryptography.tss.TSS;
import com.hedera.cryptography.wraps.WRAPSVerificationKey;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.tss.LedgerIdNodeContribution;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.security.PublicKey;
import java.util.List;
import java.util.Map;
import org.hiero.block.node.app.config.node.NodeConfig;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.hiero.block.node.verification.session.HapiVersionSessionFactory;
import org.hiero.block.node.verification.session.VerificationSession;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/** Provides implementation for the health endpoints of the server. */
@SuppressWarnings("unused")
public class VerificationServicePlugin implements BlockNodePlugin, BlockItemHandler, BlockNotificationHandler {
    /** Metric key for the number of blocks received for verification */
    public static final MetricKey<LongCounter> METRIC_VERIFICATION_BLOCKS_RECEIVED =
            MetricKey.of("verification_blocks_received", LongCounter.class).addCategory(METRICS_CATEGORY);
    /** Metric key for the number of blocks that passed verification */
    public static final MetricKey<LongCounter> METRIC_VERIFICATION_BLOCKS_VERIFIED =
            MetricKey.of("verification_blocks_verified", LongCounter.class).addCategory(METRICS_CATEGORY);
    /** Metric key for the number of blocks that failed verification */
    public static final MetricKey<LongCounter> METRIC_VERIFICATION_BLOCKS_FAILED =
            MetricKey.of("verification_blocks_failed", LongCounter.class).addCategory(METRICS_CATEGORY);
    /** Metric key for the number of internal errors during verification */
    public static final MetricKey<LongCounter> METRIC_VERIFICATION_BLOCKS_ERROR =
            MetricKey.of("verification_blocks_error", LongCounter.class).addCategory(METRICS_CATEGORY);
    /** Metric key for block verification time */
    public static final MetricKey<LongCounter> METRIC_VERIFICATION_BLOCK_TIME =
            MetricKey.of("verification_block_time", LongCounter.class).addCategory(METRICS_CATEGORY);
    /** Metric key for block hashing time */
    public static final MetricKey<LongCounter> METRIC_HASHING_BLOCK_TIME =
            MetricKey.of("hashing_block_time", LongCounter.class).addCategory(METRICS_CATEGORY);
    /** Metric key for WRB blocks whose RSA proof was accepted */
    public static final MetricKey<LongCounter> METRIC_RSA_VERIFICATION_SUCCESS =
            MetricKey.of("rsa_verification_success_total", LongCounter.class).addCategory(METRICS_CATEGORY);
    /** Metric key for WRB blocks whose RSA proof was rejected */
    public static final MetricKey<LongCounter> METRIC_RSA_VERIFICATION_FAILURE =
            MetricKey.of("rsa_verification_failure_total", LongCounter.class).addCategory(METRICS_CATEGORY);
    /** Metric key for signatures from `node_id` values not present in the loaded address book */
    public static final MetricKey<LongCounter> METRIC_RSA_ROSTER_MISMATCH =
            MetricKey.of("rsa_roster_mismatch_total", LongCounter.class).addCategory(METRICS_CATEGORY);

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
    private LongCounter.Measurement verificationBlocksReceived;
    /** Metric for number of blocks verified. */
    private LongCounter.Measurement verificationBlocksVerified;
    /** Metric for number of blocks failed verification. */
    private LongCounter.Measurement verificationBlocksFailed;
    /** Metric for number of blocks verification errors. */
    private LongCounter.Measurement verificationBlocksError;
    /** Metric for block verification time. */
    private LongCounter.Measurement verificationBlockTime;
    /** Metric for block hashing time. ignores time to receive block */
    private LongCounter.Measurement hashingBlockTimeNs;
    /** Metric for accepted RSA WRB proof verifications. */
    private LongCounter.Measurement rsaVerificationSuccessTotal;
    /** Metric for rejected RSA WRB proof verifications. */
    private LongCounter.Measurement rsaVerificationFailureTotal;
    /** Metric for signatures from `node_id` values absent from the loaded address book. */
    private LongCounter.Measurement rsaRosterMismatchTotal;
    /**
     * Most recent `node_id → PublicKey` map built from the `NodeAddressBook` delivered by
     * `RsaRosterBootstrapPlugin`. Volatile so that `onContextUpdate` writes are visible to the
     * block-handling thread without a lock.
     */
    private volatile Map<Long, PublicKey> keyByNodeId = Map.of();
    /** The previous block hash, used for verification of the current block. */
    private Bytes previousBlockHash;
    /** The block number of the last successfully verified block (live-stream or sequential backfill). */
    private long previousVerifiedBlockNumber = -1;

    /** Handler for root hash for all previous blocks hasher operations and lifecycle. */
    AllBlocksHasherHandler allBlocksHasherHandler;
    /**
     * The earliest block number this node is configured to manage. When greater than zero the node
     * is not expected to have a continuous chain from genesis, so allBlocksHasher values must not
     * override the block footer's authoritative hashes for blocks where continuity is absent.
     */
    private long earliestManagedBlock;
    /** Trusted ledger ID for TSS verification, initialized from file or block 0. */
    public static Bytes activeLedgerId;
    /** Full TSS publication body used for persistence and state restoration. */
    public static LedgerIdPublicationTransactionBody activeTssPublication;
    /** True once TSS parameters have been persisted (file bootstrap or successful block 0 verification). */
    public static boolean tssParametersPersisted;

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
        final MetricRegistry metricRegistry = context.metricRegistry();
        verificationBlocksReceived = metricRegistry
                .register(LongCounter.builder(METRIC_VERIFICATION_BLOCKS_RECEIVED)
                        .setDescription("Blocks received for verification"))
                .getOrCreateNotLabeled();
        verificationBlocksVerified = metricRegistry
                .register(LongCounter.builder(METRIC_VERIFICATION_BLOCKS_VERIFIED)
                        .setDescription("Blocks that passed verification"))
                .getOrCreateNotLabeled();
        verificationBlocksFailed = metricRegistry
                .register(LongCounter.builder(METRIC_VERIFICATION_BLOCKS_FAILED)
                        .setDescription("Blocks that failed verification"))
                .getOrCreateNotLabeled();
        verificationBlocksError = metricRegistry
                .register(LongCounter.builder(METRIC_VERIFICATION_BLOCKS_ERROR)
                        .setDescription("Internal errors during verification"))
                .getOrCreateNotLabeled();
        verificationBlockTime = metricRegistry
                .register(LongCounter.builder(METRIC_VERIFICATION_BLOCK_TIME)
                        .setDescription("Verification time per block (ms)"))
                .getOrCreateNotLabeled();
        hashingBlockTimeNs = metricRegistry
                .register(LongCounter.builder(METRIC_HASHING_BLOCK_TIME).setDescription("Hashing time per block (ms)"))
                .getOrCreateNotLabeled();
        rsaVerificationSuccessTotal = metricRegistry
                .register(LongCounter.builder(METRIC_RSA_VERIFICATION_SUCCESS)
                        .setDescription("WRB blocks whose RSA SignedRecordFileProof was accepted"))
                .getOrCreateNotLabeled();
        rsaVerificationFailureTotal = metricRegistry
                .register(LongCounter.builder(METRIC_RSA_VERIFICATION_FAILURE)
                        .setDescription("WRB blocks whose RSA SignedRecordFileProof was rejected"))
                .getOrCreateNotLabeled();
        rsaRosterMismatchTotal = metricRegistry
                .register(LongCounter.builder(METRIC_RSA_ROSTER_MISMATCH)
                        .setDescription("RSA signatures from node_id values absent from the loaded address book"))
                .getOrCreateNotLabeled();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        // setting config and context
        this.context = context;
        verificationConfig = context.configuration().getConfigData(VerificationConfig.class);
        earliestManagedBlock =
                context.configuration().getConfigData(NodeConfig.class).earliestManagedBlock();
        // Bootstrap TSS parameters from persisted file if available. The file contains a
        // serialized LedgerIdPublicationTransactionBody with ledger ID, address book, and WRAPS VK.
        final var tssParametersFile = verificationConfig.tssParametersFilePath();
        if (Files.exists(tssParametersFile)) {
            try {
                Bytes fileBytes = Bytes.wrap(Files.readAllBytes(tssParametersFile));
                LedgerIdPublicationTransactionBody publication =
                        LedgerIdPublicationTransactionBody.PROTOBUF.parse(fileBytes);
                initializeTssParameters(publication);
                tssParametersPersisted = true;
                LOGGER.log(INFO, "Loaded TSS parameters from file: {0}", tssParametersFile);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read TSS parameters file: " + tssParametersFile, e);
            } catch (ParseException e) {
                throw new IllegalStateException("Failed to parse TSS parameters file: " + tssParametersFile, e);
            }
        }
        // register the service
        context.blockMessaging().registerBlockNotificationHandler(this, false, "VerificationServicePlugin");
        // initialize metrics
        initMetrics(context);
        LOGGER.log(DEBUG, "VerificationServicePlugin initialized successfully.");
        // initialize all previous blocks hasher if enabled and available
        initAllBlocksHasherIfEnabled();
    }

    private void initAllBlocksHasherIfEnabled() {
        allBlocksHasherHandler = new AllBlocksHasherHandler(verificationConfig, context);
        // When earliestManagedBlock > 0 the node is not managing from genesis, so the hasher's
        // genesis-state value (ZERO_BLOCK_HASH, leafCount==0) must not seed previousBlockHash —
        // it is only valid as the previous hash for block 0, not for any mid-chain first block.
        // When earliestManagedBlock == 0 the node starts from genesis and ZERO_BLOCK_HASH is the
        // correct previousBlockHash for block 0, so we always trust the hasher in that case.
        final boolean hasherHasData = allBlocksHasherHandler.getNumberOfBlocks() > 0;
        if (allBlocksHasherHandler.isAvailable()
                && allBlocksHasherHandler.lastBlockHash() != null
                && (earliestManagedBlock == 0 || hasherHasData)) {
            previousBlockHash = Bytes.wrap(allBlocksHasherHandler.lastBlockHash());
            final String message = "All previous blocks hasher initialized with {0} hashes, last block hash: {1}";
            LOGGER.log(DEBUG, message, allBlocksHasherHandler.getNumberOfBlocks(), previousBlockHash);
        } else {
            final String message =
                    "All previous blocks hasher not available, falling back to BlockFooter-provided values.";
            LOGGER.log(DEBUG, message);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Called by the framework whenever `BlockNodeApp.updateAddressBook()` fires — typically once
     * at startup by `RsaRosterBootstrapPlugin`. Rebuilds the `node_id → PublicKey` map used by
     * subsequent WRB verification sessions. A volatile write ensures visibility to the
     * block-handler thread without a lock.
     */
    @Override
    public void onContextUpdate(final BlockNodeContext updatedContext) {
        final NodeAddressBook book = updatedContext.nodeAddressBook();
        if (book == null || book.nodeAddress().isEmpty()) {
            LOGGER.log(
                    WARNING,
                    "onContextUpdate called with a null or empty NodeAddressBook — RSA key map not updated."
                            + " WRB blocks will fail verification until a valid address book is delivered.");
            return;
        }
        // Count non-blank address book entries — these are the nodes that should be signable.
        final int declaredCount = (int) book.nodeAddress().stream()
                .filter(a -> !a.rsaPubKey().isBlank())
                .count();
        keyByNodeId = RsaKeyDecoder.buildKeyMap(book);
        final int effectiveCount = keyByNodeId.size();
        if (effectiveCount < declaredCount) {
            // Malformed DER keys were skipped; threshold is calculated against effectiveCount.
            // Fix the address book so all declared nodes can contribute signatures.
            LOGGER.log(
                    WARNING,
                    "RSA key map: {0}/{1} keys decoded successfully; {2} node(s) had malformed"
                            + " hex-DER bytes and cannot contribute signatures. Verification"
                            + " threshold is calculated against the {0} decodable keys.",
                    effectiveCount,
                    declaredCount,
                    declaredCount - effectiveCount);
        }
        LOGGER.log(INFO, "RSA key map updated: {0}/{1} nodes loaded from address book", effectiveCount, declaredCount);
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
        LOGGER.log(DEBUG, "VerificationServicePlugin started successfully.");

        allBlocksHasherHandler.start();
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
                // Only use our tracked previousBlockHash when this block is sequentially
                // next after the last verified block. Otherwise pass null so the session
                // falls back to the authoritative values in the block footer.
                final Bytes previousHash =
                        currentBlockNumber == previousVerifiedBlockNumber + 1 ? previousBlockHash : null;
                SemanticVersion semanticVersion = blockHeader.hapiProtoVersionOrThrow();
                // create a new verification session for the new block based on hapi version on block header.
                currentSession = HapiVersionSessionFactory.createSession(
                        currentBlockNumber,
                        BlockSource.PUBLISHER,
                        semanticVersion,
                        previousHash,
                        getRootOfAllPreviousBlocks(),
                        activeLedgerId,
                        keyByNodeId,
                        rsaVerificationSuccessTotal,
                        rsaVerificationFailureTotal,
                        rsaRosterMismatchTotal);
                LOGGER.log(DEBUG, "Started new block verification session for block number {0}", currentBlockNumber);
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
                LOGGER.log(DEBUG, traceMessage, blockItems.blockItems().size(), currentBlockNumber);
                long startHashingTime = System.nanoTime();
                // processBlockItems returns notification when isEndOfBlock(), null otherwise
                VerificationNotification notification = currentSession.processBlockItems(blockItems);
                long hashingTime = System.nanoTime() - startHashingTime;
                hashingBlockTimeNs.increment(hashingTime);
                // if this is the end of the block, handle verification result
                if (notification != null) {
                    LOGGER.log(DEBUG, COMPLETED_MESSAGE, currentBlockNumber, notification.success());
                    if (notification.success()) {
                        if (currentBlockNumber == 0) {
                            persistTssParameters();
                        }
                        verificationBlocksVerified.increment();
                        // send the notification to the block messaging service
                        LOGGER.log(DEBUG, "Sending verification notification for block={0}", currentBlockNumber);
                        context.blockMessaging().sendBlockVerification(notification);
                        // Update previousBlockHash and previousVerifiedBlockNumber for next block verification
                        this.previousBlockHash = notification.blockHash();
                        this.previousVerifiedBlockNumber = currentBlockNumber;
                        // Update streamingHasherAllPreviousBlocks
                        allBlocksHasherHandler.appendLatestHashToAllPreviousBlocksStreamingHasher(
                                this.previousBlockHash.toByteArray());
                    } else {
                        LOGGER.log(INFO, "Verification failed for block={0}", currentBlockNumber);
                        sendFailureNotification(currentBlockNumber, BlockSource.PUBLISHER);
                    }

                    // end working time
                    long blockWorkEndTime = System.nanoTime() - startVerificationHandlingTime;
                    LOGGER.log(
                            DEBUG,
                            "Finished verification handling block items for block={0,number,#} nsVerificationDuration={1,number,#}",
                            currentBlockNumber,
                            blockWorkEndTime);
                    verificationBlockTime.increment(blockWorkEndTime);
                }
            } else {
                verificationBlocksFailed.increment();
                sendFailureNotification(currentBlockNumber, BlockSource.PUBLISHER);
            }
        } catch (final RuntimeException | ParseException e) {
            LOGGER.log(WARNING, "Failed to verify BlockItems.", e);
            verificationBlocksFailed.increment();
            sendFailureNotification(currentBlockNumber, BlockSource.PUBLISHER);
        }
    }

    private Bytes getRootOfAllPreviousBlocks() {
        if (allBlocksHasherHandler != null && allBlocksHasherHandler.isAvailable()) {
            if (allBlocksHasherHandler.getNumberOfBlocks() != currentBlockNumber
                    && (currentBlockNumber <= previousVerifiedBlockNumber || previousVerifiedBlockNumber == -1)) {
                // Backfill, re-send, or startup: defer to the block footer's values.
                // Forward gaps fall through to computeRootHash() instead — returning the wrong
                // root so verification fails and forces the publisher to resend the missing block.
                return null;
            }
            return Bytes.wrap(allBlocksHasherHandler.computeRootHash());
        }
        return null;
    }

    private void persistTssParameters() {
        final LedgerIdPublicationTransactionBody publication = activeTssPublication;
        if (publication == null) {
            return;
        }
        final var tssParametersFile = verificationConfig.tssParametersFilePath();
        try {
            Files.createDirectories(tssParametersFile.getParent());
            Bytes serialized = LedgerIdPublicationTransactionBody.PROTOBUF.toBytes(publication);
            Files.write(tssParametersFile, serialized.toByteArray());
            tssParametersPersisted = true;
            LOGGER.log(INFO, "Persisted TSS parameters to file: {0}", tssParametersFile);
        } catch (IOException e) {
            LOGGER.log(
                    WARNING,
                    "Failed to persist TSS parameters to {0}: {1}".formatted(tssParametersFile, e.getMessage()),
                    e);
        }
    }

    /**
     * Initializes native TSS state (address book, WRAPS VK) and sets the active ledger ID
     * and TSS publication. Called from file bootstrap, block 0 processing, and tests.
     */
    public static void initializeTssParameters(@NonNull LedgerIdPublicationTransactionBody publication) {
        if (tssParametersPersisted) {
            return;
        }
        List<LedgerIdNodeContribution> contributions = publication.nodeContributions();
        int nodeCount = contributions.size();
        byte[][] publicKeys = new byte[nodeCount][];
        long[] nodeIds = new long[nodeCount];
        long[] weights = new long[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            LedgerIdNodeContribution contribution = contributions.get(i);
            publicKeys[i] = contribution.historyProofKey().toByteArray();
            nodeIds[i] = contribution.nodeId();
            weights[i] = contribution.weight();
        }
        TSS.setAddressBook(publicKeys, weights, nodeIds);
        Bytes historyProofVk = publication.historyProofVerificationKey();
        if (historyProofVk.length() > 0) {
            WRAPSVerificationKey.setCurrentKey(historyProofVk.toByteArray());
        }
        activeLedgerId = publication.ledgerId();
        activeTssPublication = publication;
    }

    private void sendFailureNotification(final long blockNumber, final BlockSource source) {
        verificationBlocksError.increment();
        // Return a success=false notification to indicate failure and include the block number.
        VerificationNotification notification =
                new VerificationNotification(false, FailureType.BAD_BLOCK_PROOF, blockNumber, null, null, source);
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
            LOGGER.log(DEBUG, "Received backfill notification for block={0}", notification.blockNumber());
            // create a new verification session for the backfilled block
            BlockHeader blockHeader = BlockHeader.PROTOBUF.parse(
                    notification.block().blockItems().getFirst().blockHeader());
            if (notification.blockNumber() != blockHeader.number()) {
                final String message = "Block number {0} in backfill does not match block {1} in header.";
                LOGGER.log(WARNING, message, notification.blockNumber(), blockHeader.number());
                sendFailureNotification(notification.blockNumber(), BlockSource.BACKFILL);
            } else {
                VerificationSession backfillSession = HapiVersionSessionFactory.createSession(
                        notification.blockNumber(),
                        BlockSource.BACKFILL,
                        blockHeader.hapiProtoVersionOrThrow(),
                        null,
                        null,
                        activeLedgerId,
                        keyByNodeId,
                        rsaVerificationSuccessTotal,
                        rsaVerificationFailureTotal,
                        rsaRosterMismatchTotal);
                // process the block items in the backfilled notification
                // For backfill, we wrap items in BlockItems with isEndOfBlock=true (last item should be block proof)
                BlockItems backfillBlockItems =
                        new BlockItems(notification.block().blockItems(), notification.blockNumber(), true, true);
                VerificationNotification backfillNotification = backfillSession.processBlockItems(backfillBlockItems);
                if (backfillNotification != null) {
                    // Log the backfill verification result
                    LOGGER.log(DEBUG, COMPLETED_MESSAGE, notification.blockNumber(), backfillNotification.success());
                    if (backfillNotification.success()) {
                        if (notification.blockNumber() == 0) {
                            persistTssParameters();
                        }
                        // Only update live-stream verification state when this backfilled block
                        // is sequentially next after the last verified block (live-tail backfill).
                        // Historical backfill can arrive out of order and must not alter state
                        // used by the live-stream verification path.
                        if (backfillNotification.blockHash() != null
                                && notification.blockNumber() == previousVerifiedBlockNumber + 1) {
                            this.previousBlockHash = backfillNotification.blockHash();
                            this.previousVerifiedBlockNumber = notification.blockNumber();
                            allBlocksHasherHandler.appendLatestHashToAllPreviousBlocksStreamingHasher(
                                    backfillNotification.blockHash().toByteArray());
                        }
                    }
                    // send the verification notification for the backfilled block
                    context.blockMessaging().sendBlockVerification(backfillNotification);
                } else {
                    LOGGER.log(
                            WARNING,
                            "Backfill verification returned null notification for block={0}",
                            notification.blockNumber());
                    sendFailureNotification(notification.blockNumber(), BlockSource.BACKFILL);
                }
            }
        } catch (RuntimeException | ParseException e) {
            LOGGER.log(WARNING, "Failed to handle backfill notification ", e);
            sendFailureNotification(notification.blockNumber(), BlockSource.BACKFILL);
        }
    }
}
