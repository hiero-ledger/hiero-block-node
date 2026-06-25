// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;
import static org.hiero.block.common.hasher.HashingUtilities.EMPTY_TREE_HASH;
import static org.hiero.block.node.base.ParseHelper.standardParse;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.hiero.block.common.hasher.StreamingHasher;
import org.hiero.block.internal.AllPreviousBlocksRootHashHasherSnapshot;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

/// Maintains and persists a streaming Merkle hasher over all previous block hashes.
///
/// This class does the following:
/// - Owns the StreamingHasher for All Previous Blocks
/// - Knows the last block hash
/// - Handles StreamingHasher persistence into file system
/// - Loads the hasher state from file system
/// - Can rebuild the hasher state from historical block provider if needed
///
/// On any failure, the hasher degrades gracefully to "unavailable" and callers
/// must fall back to footer-provided values.
///
/// This class is available/functional only when a node has complete historical block store from genesis.
public class AllBlocksHasherHandler {
    private static final System.Logger LOGGER = System.getLogger(AllBlocksHasherHandler.class.getName());
    /// Empty tree hash: SHA384(0x00), matching the CN's IncrementalStreamingHasher for an empty tree.
    public static final byte[] ZERO_BLOCK_HASH = EMPTY_TREE_HASH;
    /// Expected size of a block hash in bytes.
    public static final int BLOCK_HASH_LENGTH = ZERO_BLOCK_HASH.length;
    /// The initial value of the [#persistenceThresholdCounter], while this value is
    /// present, persistence is inactive. This is during init, on start go to 0 as default value.
    private static final int UNINITIALIZED_PERSISTENCE_THRESHOLD = -1;
    /// Streaming hasher for all previous blocks hashes.
    private StreamingHasher hasher;
    /// The previous block hash, used for verification of the current block.
    private byte[] lastBlockHash;
    /// Verification configuration.
    private final VerificationConfig verificationConfig;
    /// Block node context. access to block provider services for historical blocks
    private final BlockNodeContext context;
    /// path to persist the hasher state
    private Path hasherPath;
    /// available blocks from historical block provider
    private BlockRangeSet availableBlocks;
    /// A threshold counter, which resets after threshold is passed, to signal persistence of hasher snapshot
    private final AtomicInteger persistenceThresholdCounter;

    /// Maintains and persists a streaming Merkle hasher over all previous block hashes
    ///
    /// owns the hasher state
    /// knows the las block hash
    /// knows the current root hash of all previous blocks
    ///
    /// On any failure, the hasher degrades gracefully to "unavailable" and callers
    /// must fall back to footer-provided values.
    ///
    /// @param verificationConfig the verification configuration
    /// @param context the block node context
    public AllBlocksHasherHandler(@NonNull final VerificationConfig verificationConfig, BlockNodeContext context) {
        this.verificationConfig = requireNonNull(verificationConfig, "verificationConfig must not be null");
        this.context = requireNonNull(context, "context must not be null");
        this.persistenceThresholdCounter = new AtomicInteger(UNINITIALIZED_PERSISTENCE_THRESHOLD);
        init();
    }

    /// Compute the root hash of all previous blocks.
    /// @return the root hash as a byte array
    public byte[] computeRootHash() {
        final byte[] result;
        if (isAvailable()) {
            if (hasher.leafCount() == 0) {
                result = ZERO_BLOCK_HASH;
            } else {
                result = hasher.computeRootHash();
            }
        } else {
            LOGGER.log(INFO, "hasher is not available, cannot compute root hash of all previous blocks.");
            result = null;
        }
        return result;
    }

    /// Get the last block hash if hasher available.
    /// @return the last block hash as a byte array if hasher is available, otherwise `null`
    public byte[] lastBlockHash() {
        final byte[] result;
        if (isAvailable()) {
            result = lastBlockHash;
        } else {
            LOGGER.log(INFO, "hasher is not available, cannot know previous root hash");
            result = null;
        }
        return result;
    }

    /// Check if the hasher is available.
    /// @return true if available, false otherwise
    public boolean isAvailable() {
        return hasher != null;
    }

    /// Get the number of blocks in the hasher.
    /// @return the number of blocks
    public long getNumberOfBlocks() {
        final long result;
        if (isAvailable()) {
            result = hasher.leafCount();
        } else {
            LOGGER.log(INFO, "hasher is not available, cannot know number of blocks");
            result = -1;
        }
        return result;
    }

    private void init() {
        if (verificationConfig.allBlocksHasherEnabled()) {
            try {
                // 1. Initial Setup
                hasherPath = verificationConfig.allBlocksHasherFilePath().toAbsolutePath();
                Files.createDirectories(hasherPath.getParent());
                hasher = new StreamingHasher();
                availableBlocks = context.historicalBlockProvider().availableBlocks();
                // 2. Load Data
                if (availableBlocks.size() == 0) {
                    initGenesis();
                } else if (Files.exists(hasherPath)) {
                    loadFromFile();
                    syncBlockHashesFromStore(getNumberOfBlocks(), availableBlocks.max());
                } else if (verificationConfig.rebuildAllBlocksHasherFromStore()) {
                    fullyBuildFromStore();
                }
                // 3. Validate hasher state matches available blocks
                validateState();
                // 4. Persist initial state
                persistHasherSnapshot();
            } catch (final RuntimeException | IOException | NoSuchAlgorithmException | ParseException e) {
                LOGGER.log(WARNING, "Falling back to footer values. Reason: %s".formatted(e.getMessage()), e);
                this.hasher = null; // Ensure we return null on failure
            }
        }
    }

    void start() {
        if (isAvailable()) {
            persistenceThresholdCounter.set(0);
        }
    }

    private void persistHasherSnapshot() {
        if (isAvailable()) {
            final AllPreviousBlocksRootHashHasherSnapshot snapshot =
                    AllPreviousBlocksRootHashHasherSnapshot.newBuilder()
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
                tmp = Files.createTempFile(
                        hasherPath.getParent(), hasherPath.getFileName().toString(), ".tmp");
                Files.write(tmp, serializedSnapshot, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
                try {
                    Files.move(tmp, hasherPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (final AtomicMoveNotSupportedException e) {
                    // Some filesystems do not support ATOMIC_MOVE; still keep replace semantics.
                    Files.move(tmp, hasherPath, StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (final IOException e) {
                LOGGER.log(WARNING, "Failed to persist hasher snapshot to %s".formatted(hasherPath), e);
            } finally {
                if (tmp != null) {
                    try {
                        Files.deleteIfExists(tmp);
                    } catch (final IOException e) {
                        LOGGER.log(INFO, "Failed to delete temporary file %s".formatted(tmp), e);
                    }
                }
            }
        }
    }

    /// Initialize a new streaming hasher from genesis.
    private void initGenesis() throws IOException {
        Files.deleteIfExists(hasherPath);
        Files.createFile(hasherPath);
        this.lastBlockHash = ZERO_BLOCK_HASH; // genesis: no previous block
    }

    /// Load persisted snapshot from a file and initialize the streaming hasher.
    private void loadFromFile() throws IOException, ParseException, NoSuchAlgorithmException {
        try (final InputStream is = new BufferedInputStream(Files.newInputStream(hasherPath))) {
            final Bytes snapshotBytes = Bytes.wrap(is.readAllBytes());
            final AllPreviousBlocksRootHashHasherSnapshot snapshot =
                    standardParse(AllPreviousBlocksRootHashHasherSnapshot.PROTOBUF, snapshotBytes);
            final long leafCount = snapshot.leafCount();
            final List<byte[]> intermediateHashes = snapshot.intermediateHashes().stream()
                    .map(Bytes::toByteArray)
                    .toList();
            hasher = new StreamingHasher(intermediateHashes, leafCount);
            lastBlockHash = snapshot.lastRootHash().toByteArray();
        }
    }

    /// Completely build the hasher using local storage.
    /// When fully building, we need to start from genesis (0) and continue up
    /// to the latest available block. If a complete and contiguous chain of
    /// blocks exists, we can attempt the build, otherwise we cannot.
    /// @throws ParseException if any block fails to parse, we cannot continue
    private void fullyBuildFromStore() throws ParseException {
        // Only attempt to fully rebuild from store if we have a full, contiguous chain of
        // available blocks starting from genesis up to available max
        final long max = availableBlocks.max();
        if (availableBlocks.contains(0L, max)) {
            syncBlockHashesFromStore(0, max);
        } else {
            LOGGER.log(
                    INFO,
                    "Cannot rebuild hasher from store: requires a single contiguous range starting from genesis. "
                            + "Found {0} range(s) starting at block {1}. Will fall back to block footer values.",
                    availableBlocks.streamRanges().count(),
                    availableBlocks.min());
        }
    }

    /// Update the hasher from local history.
    /// We provide the first and last block we need to update with. If we are
    /// unable to get any of the blocks in that range, we have to stop, as we
    /// need a contiguous and complete update to succeed.
    /// @param start the starting point of the update (inclusive)
    /// @param end the final point of the update (inclusive)
    private void syncBlockHashesFromStore(long start, long end) throws ParseException {
        for (long blockNumber = start; blockNumber <= end; blockNumber++) {
            final byte[] nextHash = calculateBlockHashFromBlockNumber(blockNumber);
            if (nextHash != null) {
                appendLatestHashToAllPreviousBlocksStreamingHasher(nextHash, blockNumber);
            } else {
                // exit prematurely, we cannot continue initializing
                break;
            }
        }
    }

    /// Calculate the block hash for a given block number, using block footer values as authoritative source.
    /// If the block cannot be read (historical storage is _not_ guaranteed), then
    /// this method will return an empty byte array.
    /// @param blockNumber the block number to calculate the hash for
    /// @return the calculated block hash
    /// @throws ParseException if there is an error parsing the block or its items.
    private byte[] calculateBlockHashFromBlockNumber(long blockNumber) throws ParseException {
        byte[] blockHash = null;
        final HistoricalBlockFacility blockProvider = context.historicalBlockProvider();
        if (blockProvider != null) {
            try (final BlockAccessor accessor = blockProvider.block(blockNumber)) {
                if (accessor != null) {
                    // if failure to read occurs, the reader will return null
                    final BlockUnparsed block = accessor.blockUnparsed();
                    if (block != null) {
                        final BlockHeader blockHeader = parseHeader(block);
                        if (blockHeader != null) {
                            final BlockItems blockItemsMessage =
                                    new BlockItems(block.blockItems(), blockNumber, true, true);
                            // Pass null, null so the session uses the block footer's authoritative values
                            // todo we need to use the new session
                            //                            final VerificationSession session =
                            // HapiVersionSessionFactory.createSession(
                            //                                    blockNumber, BlockSource.HISTORY,
                            // blockHeader.hapiProtoVersion(), null, null, null);
                            //                            final VerificationNotification result =
                            // session.processBlockItems(blockItemsMessage);
                            //                            if (result != null && result.success()) {
                            //                                blockHash = result.blockHash().toByteArray();
                            //                            }
                            throw new UnsupportedOperationException("need to use the new sessions");
                        }
                    }
                }
            }
        }
        return blockHash;
    }

    private BlockHeader parseHeader(final BlockUnparsed block) throws ParseException {
        try {
            BlockHeader result = null;
            if (block != null) {
                if (block.blockItems() != null) {
                    if (!block.blockItems().isEmpty()) {
                        final BlockItemUnparsed first = block.blockItems().getFirst();
                        if (first != null && first.hasBlockHeader()) {
                            result = standardParse(BlockHeader.PROTOBUF, first.blockHeader());
                        }
                    }
                }
            }
            return result;
        } catch (final RuntimeException ignored) {
            return null;
        }
    }

    /// Validate that the streaming hasher state matches the expected max block.
    /// maxBlock is inclusive and counts from 0, so N blocks produces leaf count N.
    private void validateState() {
        long maxBlock = availableBlocks.max();
        if ((hasher.leafCount() - 1) != maxBlock) {
            LOGGER.log(
                    WARNING,
                    "Hasher state mismatch: {0} leaves vs {1} max block, falling back to use block footer values",
                    hasher.leafCount(),
                    maxBlock);
            this.hasher = null; // Invalidate hasher on mismatch
        }
    }

    /// Append the latest block hash to the streaming hasher for all previous blocks.
    /// We can only do so if the hasher [#isAvailable()] and the hash we provide
    /// is the next one in line.
    /// @param blockHashBytes the latest block hash to append
    public void appendLatestHashToAllPreviousBlocksStreamingHasher(
            @NonNull final byte[] blockHashBytes, final long blockNumber) {
        if (isAvailable()) {
            // todo use blockNumber
            hasher.addNodeByHash(blockHashBytes);
            if (persistenceThresholdCounter.get() > UNINITIALIZED_PERSISTENCE_THRESHOLD) {
                final int incremented = persistenceThresholdCounter.incrementAndGet();
                final int threshold = verificationConfig.allBlocksHasherPersistenceInterval();
                // if we are passed the threshold and no one else has changed the atomic value (CAS must fail), then
                // we can go on and persist
                if (incremented >= threshold
                        && persistenceThresholdCounter.compareAndSet(incremented, incremented - threshold)) {
                    persistHasherSnapshot();
                }
            }
        }
        this.lastBlockHash = blockHashBytes;
    }
}
