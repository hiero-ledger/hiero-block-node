// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
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
        if (verificationConfig.calculateRootHashOfAllPreviousBlocksEnabled()) {
            try {
                // 1. Initial Setup
                hasherPath = requireNonNull(verificationConfig.rootHashOfAllPreviousBlocksPath());
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

                // 4. Set previous block hash
                setPreviousBlockHash();

            } catch (IOException | NoSuchAlgorithmException | IllegalStateException | ParseException e) {
                LOGGER.log(WARNING, "Falling back to footer values. Reason: " + e.getMessage(), e);
                this.hasher = null; // Ensure we return null on failure
            }
        }
    }

    private void setPreviousBlockHash() throws ParseException {
        if (available.size() > 0 && lastBlockHash == null) {
            if (hasher.leafCount() > 1) {
                long lastBlockNumber = available.max();
                lastBlockHash = calculateBlockHashFromBlockNumber(lastBlockNumber, null);
            } else {
                lastBlockHash = ZERO_BLOCK_HASH;
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
    private void loadFromFile() throws IOException {
        try (var inputStream = new BufferedInputStream(Files.newInputStream(hasherPath))) {
            byte[] buffer = new byte[BLOCK_HASH_LENGTH];
            while (inputStream.readNBytes(buffer, 0, BLOCK_HASH_LENGTH) == BLOCK_HASH_LENGTH) {
                hasher.addLeaf(Arrays.copyOf(buffer, BLOCK_HASH_LENGTH));
            }
        }
    }

    // when needing to rebuild from historical block provider.
    private void fullyRebuildFromStore() throws ParseException {
        if (available.streamRanges().count() == 1 && available.min() == 0) {
            syncBlockHashesFromStore(0, available.max());
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
                this.lastBlockHash = latestBlockHash;
            }
        }

        // edge case: missing only 1 hash.
        if (start != 0 && start - 1 == end) {
            byte[] previousHash = extractPreviousRootHashFromBlock(end);
            byte[] latestBlockHash = calculateBlockHashFromBlockNumber(end, previousHash);
            appendLatestHashToAllPreviousBlocksStreamingHasher(latestBlockHash);
            this.lastBlockHash = latestBlockHash;
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
        List<BlockItemUnparsed> items = block.items().stream()
                .map(item -> {
                    try {
                        return BlockItemUnparsed.PROTOBUF.parse(BlockItem.PROTOBUF.toBytes(item));
                    } catch (ParseException e) {
                        LOGGER.log(
                                WARNING,
                                "Failed to parse block item during block hash calculation for block " + blockNumber,
                                e);
                        throw new RuntimeException(e);
                    }
                })
                .toList();

        VerificationSession session = HapiVersionSessionFactory.createSession(
                blockNumber, BlockSource.UNKNOWN, blockHeader.hapiProtoVersion());
        session.processBlockItems(items);
        Bytes previousBlockHashBytes = previousBlockHash != null ? Bytes.wrap(previousBlockHash) : null;
        VerificationNotification result = session.finalizeVerification(null, previousBlockHashBytes);
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
        final Path hasherPath = verificationConfig.rootHashOfAllPreviousBlocksPath();
        try (BufferedOutputStream outputStream = new BufferedOutputStream(
                Files.newOutputStream(hasherPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND))) {
            outputStream.write(blockHashBytes);
        } catch (IOException e) {
            LOGGER.log(WARNING, "Failed to persist block hash to " + hasherPath, e);
        }
    }
}
