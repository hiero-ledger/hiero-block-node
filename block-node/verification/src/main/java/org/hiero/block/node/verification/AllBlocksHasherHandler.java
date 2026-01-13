// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.hiero.block.common.hasher.StreamingHasher;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.verification.session.HapiVersionSessionFactory;
import org.hiero.block.node.verification.session.VerificationSession;

/**
 * Maintains and persists a streaming Merkle hasher over all previous block hashes.
 * <p>
 * This class does the following:
 * - Owns the StreamingHasher for All Previous Blocks
 * - Knows the last block hash
 * - Handles StreamingHasher persistence into file system
 * - Loads the hasher state from file system
 * - Can rebuild the hasher state from historical block provider if needed
 * <p>
 * On any failure, the hasher degrades gracefully to "unavailable" and callers
 * must fall back to footer-provided values.
 * <p>
 * This class is available/functional only when a node has complete historical block store from genesis.
 */
public class AllBlocksHasherHandler {

    private static final System.Logger LOGGER = System.getLogger(AllBlocksHasherHandler.class.getName());
    /** ZERO block hash constant. */
    public static final byte[] ZERO_BLOCK_HASH = new byte[48];
    /** Expected size of a block hash in bytes. */
    public static final int BLOCK_HASH_LENGTH = ZERO_BLOCK_HASH.length;
    /** Streaming hasher for all previous blocks hashes. */
    private StreamingHasher hasher;
    /** The previous block hash, used for verification of the current block. */
    private byte[] lastBlockHash;
    /** Verification configuration. */
    private final VerificationConfig verificationConfig;
    /** Block node context. access to block provider services for historical blocks */
    private final BlockNodeContext context;
    /** path to persist the hasher state */
    private Path hasherPath;
    /** available blocks from historical block provider */
    private BlockRangeSet available;

    /**
     * Maintains and persists a streaming Merkle hasher over all previous block hashes
     * <p>
     * owns the hasher state
     * knows the las block hash
     * knows the current root hash of all previous blocks
     * <p>
     * On any failure, the hasher degrades gracefully to "unavailable" and callers
     * must fall back to footer-provided values.
     *
     * @param verificationConfig the verification configuration
     * @param context the block node context
     */
    public AllBlocksHasherHandler(@NonNull final VerificationConfig verificationConfig, BlockNodeContext context) {
        this.verificationConfig = requireNonNull(verificationConfig, "verificationConfig must not be null");
        this.context = requireNonNull(context, "context must not be null");
        init();
    }

    /**
     * Compute the root hash of all previous blocks.
     * @return the root hash as a byte array
     */
    public byte[] computeRootHash() {
        if (hasher == null) {
            LOGGER.log(INFO, "hasher is not available, cannot compute root hash of all previous blocks.");
            return null;
        }
        return hasher.computeRootHash();
    }

    /**
     * Get the last block hash.
     * @return the last block hash as a byte array
     */
    public byte[] lastBlockHash() {
        if (hasher == null) {
            LOGGER.log(INFO, "hasher is not available, cannot know previous root hash");
            return null;
        }
        return lastBlockHash;
    }

    /** *
     * Check if the hasher is available.
     * @return true if available, false otherwise
     */
    public boolean isAvailable() {
        return hasher != null;
    }

    /**
     * Get the number of blocks in the hasher.
     * @return the number of blocks
     */
    public long getNumberOfBlocks() {
        if (hasher == null) {
            LOGGER.log(INFO, "hasher is not available, cannot know number of blocks");
            return -1;
        }
        return hasher.leafCount() - 1; // minus the ZERO block hash
    }

    private void init() {
        if (verificationConfig.allBlocksHasherEnabled()) {
            try {
                // 1. Initial Setup
                hasherPath = requireNonNull(verificationConfig.allBlocksHasherFilePath());
                hasher = new StreamingHasher();
                Files.createDirectories(hasherPath.getParent());
                available = context.historicalBlockProvider().availableBlocks();

                // 2. Load Data
                if (available.size() == 0) {
                    initGenesis();
                } else if (Files.exists(hasherPath)) {
                    loadFromFile();
                    syncBlockHashesFromStore(hasher.leafCount(), available.max());
                } else {
                    fullyRebuildFromStore();
                }
                // 3. Validate hasher state matches available blocks
                validateState();

                // 4. Persist initial state
                persistHasherSnapshot();

            } catch (IOException | NoSuchAlgorithmException | IllegalStateException | ParseException e) {
                LOGGER.log(WARNING, "Falling back to footer values. Reason: %s".formatted(e.getMessage()), e);
                this.hasher = null; // Ensure we return null on failure
            }
        }
    }

    public void start() {
        if (hasher != null) {
            startPersistenceScheduler();
        }
    }

    private void startPersistenceScheduler() {
        // Two threads: one for autonomous backfill, one for on-demand backfill
        /** Scheduler for periodic tasks. */
        ScheduledExecutorService scheduler = context.threadPoolManager()
                .createVirtualThreadScheduledExecutor(
                        2, // Two threads: one for autonomous backfill, one for on-demand backfill
                        "AllBlocksHasherHandler-Persistence",
                        (t, e) -> LOGGER.log(INFO, "Uncaught exception in thread: %s".formatted(t.getName()), e));

        scheduler.scheduleAtFixedRate(
                this::persistHasherSnapshot,
                1000, // initial delay of 1 second should be plenty of time to start up
                verificationConfig.allBlocksHasherPersistenceInterval() * 1000L, // convert seconds to milliseconds
                TimeUnit.MILLISECONDS);
    }

    private void persistHasherSnapshot() {

        if (hasher == null) {
            LOGGER.log(INFO, "hasher is not available, cannot persist hasher snapshot.");
            return;
        }

        final Path hasherFilePath = verificationConfig.allBlocksHasherFilePath().toAbsolutePath();
        final Path dir =
                requireNonNull(hasherFilePath.getParent(), "Hasher snapshot path must have a parent directory");

        LOGGER.log(TRACE, "Persisting all blocks hasher with {0} leaves snapshot to {1}", new Object[] {
            hasher.leafCount(), hasherFilePath
        });

        final AllPreviousBlocksRootHashHasherSnapshot snapshot = AllPreviousBlocksRootHashHasherSnapshot.newBuilder()
                .lastRootHash(Bytes.wrap(lastBlockHash))
                .leafCount(hasher.leafCount())
                .intermediateHashes(hasher.intermediateHashingState().stream()
                        .map(Bytes::wrap)
                        .toList())
                .build();

        final byte[] serializedSnapshot = AllPreviousBlocksRootHashHasherSnapshot.PROTOBUF
                .toBytes(snapshot)
                .toByteArray();

        Path tmp = null;
        try {
            Files.createDirectories(dir);

            tmp = Files.createTempFile(dir, hasherFilePath.getFileName().toString(), ".tmp");

            Files.write(tmp, serializedSnapshot, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);

            try {
                Files.move(tmp, hasherFilePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                // Some filesystems do not support ATOMIC_MOVE; still keep replace semantics.
                Files.move(tmp, hasherFilePath, StandardCopyOption.REPLACE_EXISTING);
            }

        } catch (IOException e) {
            LOGGER.log(WARNING, "Failed to persist hasher snapshot to %s".formatted(hasherFilePath), e);
        } finally {
            if (tmp != null) {
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException ignored) {
                }
            }
        }
    }

    // When starting a fresh node.
    private void initGenesis() throws IOException {
        Files.deleteIfExists(hasherPath);
        Files.createFile(hasherPath);
        // Only add to memory; the regular lifecycle will handle the first write
        appendLatestHashToAllPreviousBlocksStreamingHasher(ZERO_BLOCK_HASH);
    }

    // when loading from existing file.
    private void loadFromFile() throws IOException, ParseException, NoSuchAlgorithmException {
        final Path hasherFilePath = verificationConfig.allBlocksHasherFilePath().toAbsolutePath();
        try (var inputStream = new BufferedInputStream(Files.newInputStream(hasherFilePath))) {
            Bytes snapshotBytes = Bytes.wrap(inputStream.readAllBytes());
            var snapshot = AllPreviousBlocksRootHashHasherSnapshot.PROTOBUF.parse(snapshotBytes);
            long leafCount = snapshot.leafCount();
            List<byte[]> intermediateHashes = snapshot.intermediateHashes().stream()
                    .map(Bytes::toByteArray)
                    .toList();

            hasher = new StreamingHasher(intermediateHashes, leafCount);
            lastBlockHash = snapshot.lastRootHash().toByteArray();
        }
    }

    // when needing to rebuild from historical block provider.
    private void fullyRebuildFromStore() throws ParseException {
        if (available.streamRanges().count() == 1 && available.min() == 0) {
            syncBlockHashesFromStore(0, available.max());
        } else {
            LOGGER.log(
                    INFO,
                    "Cannot rebuild hasher from store: requires a single contiguous range starting from genesis. "
                            + "Found {0} range(s) starting at block {1}. Will fall back to block footer values.",
                    available.streamRanges().count(),
                    available.min());
        }
    }

    // update the hasher from historical block provider.
    private void syncBlockHashesFromStore(long start, long end) throws ParseException {
        for (long i = start; i <= end; i++) {
            byte[] previousHash = extractPreviousRootHashFromBlock(i);
            appendLatestHashToAllPreviousBlocksStreamingHasher(previousHash);
            // if last block
            if (i == end) {
                // add latest block hash, need to recalculate hash
                byte[] latestBlockHash = calculateBlockHashFromBlockNumber(i, previousHash);
                appendLatestHashToAllPreviousBlocksStreamingHasher(latestBlockHash);
            }
        }

        // edge case: missing only 1 hash.
        if (start != 0 && start - 1 == end) {
            byte[] previousHash = extractPreviousRootHashFromBlock(end);
            byte[] latestBlockHash = calculateBlockHashFromBlockNumber(end, previousHash);
            appendLatestHashToAllPreviousBlocksStreamingHasher(latestBlockHash);
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
        final BlockUnparsed block =
                context.historicalBlockProvider().block(blockNumber).blockUnparsed();
        // is safe to assume first item is always block header
        final BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(block.blockItems().getFirst().blockHeader());

        BlockItems blockItemsMessage = new BlockItems(block.blockItems(), blockNumber);

        final Bytes previousBlockHashBytes = (previousBlockHash == null) ? null : Bytes.wrap(previousBlockHash);

        final VerificationSession session = HapiVersionSessionFactory.createSession(
                blockNumber,
                BlockSource.HISTORY,
                blockHeader.hapiProtoVersion(),
                previousBlockHashBytes,
                Bytes.wrap(hasher.computeRootHash()));

        final VerificationNotification result = session.processBlockItems(blockItemsMessage);

        return result.blockHash().toByteArray();
    }

    private byte[] extractPreviousRootHashFromBlock(long blockNumber) {
        var items = context.historicalBlockProvider().block(blockNumber).block().items();

        // blocks place the footer close to the very end.
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
     */
    private void validateState() {
        long maxBlock = available.max();
        if ((hasher.leafCount() - 2) != maxBlock) {
            LOGGER.log(
                    WARNING,
                    "Hasher state mismatch: {0} leaves vs {1} max block, falling back to use block footer values",
                    hasher.leafCount() - 1,
                    maxBlock);
            this.hasher = null; // Invalidate hasher on mismatch
        }
    }
    /***
     * Append the latest block hash to the streaming hasher for all previous blocks.     *
     * @param blockHashBytes the latest block hash to append
     */
    public void appendLatestHashToAllPreviousBlocksStreamingHasher(@NonNull final byte[] blockHashBytes) {
        if (hasher != null) {
            hasher.addLeaf(blockHashBytes);
        }
        this.lastBlockHash = blockHashBytes;
    }
}
