// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static com.swirlds.state.merkle.StateKeyUtils.kvKey;
import static com.swirlds.state.merkle.StateKeyUtils.queueKey;
import static com.swirlds.state.merkle.StateUtils.getStateKeyForSingleton;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.platform.state.QueueState;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.state.MerkleNodeState;
import com.swirlds.state.StateLifecycleManager;
import com.swirlds.state.merkle.StateLifecycleManagerImpl;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.virtualmap.VirtualMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.System.Logger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import org.hiero.base.crypto.Hash;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

/**
 * Plugin that maintains a live merkle tree state database by applying
 * verified block state changes.
 *
 * <p>The plugin supports two modes:
 * <ul>
 *   <li>Genesis mode: Start with an empty state when no saved state exists</li>
 *   <li>Resume mode: Load a previously saved state and catch up from historical blocks</li>
 * </ul>
 *
 * <p>State changes are only applied after blocks are verified to ensure consistency.
 * The plugin verifies the state root hash matches the expected hash from the block footer.
 *
 * <p><b>Note:</b> This implementation currently uses an in-memory map for state storage.
 * The full VirtualMap/MerkleDB integration is pending resolution of module conflicts
 * between the consensus node's hapi types and the block-node's protobuf types.
 */
public class LiveStatePlugin implements BlockNodePlugin, BlockNotificationHandler, LiveStateAccess {
    private final Logger logger = System.getLogger(getClass().getName());

    /** The block node context providing access to facilities */
    private BlockNodeContext context;

    /** The state lifecycle manager */
    private StateLifecycleManager stateLifecycleManager;

    /** The state metadata, this is the live state plugin's metadata that is persisted between runs */
    private StateMetadata stateMetadata;

    /** Configuration for the live state plugin */
    private LiveStateConfig config;

    /** Flag indicating if the state is valid and ready */
    private volatile boolean stateReady = false;

    @NonNull
    @Override
    public String name() {
        return "LiveState";
    }

    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(LiveStateConfig.class);
    }

    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.context = context;
        this.config = context.configuration().getConfigData(LiveStateConfig.class);
        logger.log(INFO, "Initializing Live State Plugin with storage path: {0}", config.storagePath());

        // Register to receive block verification notifications
        context.blockMessaging().registerBlockNotificationHandler(this, true, "LiveStateNotificationHandler");
    }

    @Override
    public void start() {
        logger.log(INFO, "Starting Live State Plugin");

        try {
            // check if we have a saved state to load
            if (Files.exists(config.stateMetadataPath())) {
                // load saved state metadata
                try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(config.stateMetadataPath()))) {
                    stateMetadata = (StateMetadata) ois.readObject();
                }
                // Create a state lifecycle manager
                stateLifecycleManager = new StateLifecycleManagerImpl(
                        context.metrics(),
                        Time.getCurrent(),
                        (virtualMap) -> new VirtualMapState(virtualMap, context.metrics()));
                // load saved state
                final MerkleNodeState latestState = stateLifecycleManager.loadSnapshot(config.latestStatePath());
                stateLifecycleManager.initState(latestState, true);
            } else { // assume genesis starting with block 0
                logger.log(INFO, "Creating new empty genesis state");
                // create new state metadata
                stateMetadata = new StateMetadata(-1, new byte[48]);
                // Create a state lifecycle manager
                stateLifecycleManager = new StateLifecycleManagerImpl(
                        context.metrics(),
                        Time.getCurrent(),
                        (virtualMap) -> new VirtualMapState(virtualMap, context.metrics()));
                // create a new empty virtual map state
                VirtualMapState state = new VirtualMapState(context.configuration(), context.metrics());
                stateLifecycleManager.initState(state, true);
            }
            // Check if we need to catch up from historical blocks
            catchUpFromHistoricalBlocks();

            stateReady = true;
            logger.log(
                    INFO,
                    "Live State Plugin started successfully. Last applied block: {0}",
                    stateMetadata.lastAppliedBlockNumber());
        } catch (final Exception e) {
            logger.log(ERROR, "Failed to start Live State Plugin", e);
            throw new RuntimeException("Failed to initialize live state", e);
        }
    }

    @Override
    public void stop() {
        logger.log(INFO, "Stopping Live State Plugin");
        stateReady = false;
        final Path latestStatePath = config.latestStatePath();
        // delete the state metadata file if it exists
        if (Files.exists(config.stateMetadataPath())) {
            try {
                Files.delete(config.stateMetadataPath());
            } catch (IOException e) {
                logger.log(ERROR, "Failed to delete state metadata file: " + config.stateMetadataPath(), e);
                throw new RuntimeException("Failed to delete state metadata file", e);
            }
        }
        // write the state metadata to disk
        try {
            // Ensure parent directory exists
            Files.createDirectories(config.stateMetadataPath().getParent());
            try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(config.stateMetadataPath()))) {
                out.writeObject(stateMetadata);
            }
        } catch (IOException e) {
            logger.log(ERROR, "Failed to write state metadata to file: " + config.stateMetadataPath(), e);
            throw new RuntimeException("Failed to write state metadata to file", e);
        }
        // delete old saved state if it exists
        try {
            if (Files.exists(latestStatePath)) {
                // recursive delete all files in the latest state path directory
                Files.walk(latestStatePath)
                        .sorted(Comparator.reverseOrder()) // delete children before parents
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (final Exception e) {
                                logger.log(ERROR, "Failed to delete file during cleanup of old state: " + path, e);
                                throw new RuntimeException(
                                        "Failed to delete file during cleanup of old state: " + path, e);
                            }
                        });
            }
        } catch (final Exception e) {
            logger.log(ERROR, "Failed to delete old saved state at " + latestStatePath, e);
            throw new RuntimeException("Failed to delete old saved state at " + latestStatePath, e);
        }
        // save the state to disk
        try {
            Files.createDirectories(latestStatePath);
        } catch (IOException e) {
            logger.log(ERROR, "Failed to create latest state directory: " + latestStatePath, e);
            throw new RuntimeException("Failed to create latest state directory", e);
        }
        stateLifecycleManager.createSnapshot(stateLifecycleManager.getLatestImmutableState(), latestStatePath);
        // In-memory map doesn't need explicit cleanup
        logger.log(INFO, "Live state stopped. State snapshot and metadata have been saved");
    }

    /**
     * Handle a block verification notification. When a block is verified,
     * apply its state changes to the live state.
     *
     * @param notification the block verification notification
     */
    @Override
    public void handleVerification(@NonNull final VerificationNotification notification) {
        if (!notification.success()) {
            logger.log(WARNING, "Block {0} verification failed, skipping state update", notification.blockNumber());
            return;
        }

        if (!stateReady) {
            logger.log(DEBUG, "State not ready, queuing block {0}", notification.blockNumber());
            return;
        }

        final long blockNumber = notification.blockNumber();

        // Ensure we're processing blocks in order
        if (blockNumber != stateMetadata.lastAppliedBlockNumber() + 1) {
            logger.log(
                    WARNING,
                    "Out of order block received. Expected {0}, got {1}",
                    stateMetadata.lastAppliedBlockNumber() + 1,
                    blockNumber);
            return;
        }

        try {
            // Parse the block and apply state changes
            final BlockUnparsed block = notification.block();
            if (block != null) {
                applyBlockStateChanges(block, blockNumber);
            } else {
                logger.log(WARNING, "No block data available in verification notification for block {0}", blockNumber);
            }
        } catch (final Exception e) {
            logger.log(ERROR, "Failed to apply state changes for block " + blockNumber, e);
        }
    }

    /**
     * Catch up from historical blocks if we're behind the current block.
     */
    private void catchUpFromHistoricalBlocks() {
        final HistoricalBlockFacility historicalBlocks = context.historicalBlockProvider();
        if (historicalBlocks == null) {
            logger.log(WARNING, "No historical block provider available");
            return;
        }

        final var availableBlocks = historicalBlocks.availableBlocks();
        if (availableBlocks.min() < 0) {
            logger.log(INFO, "No historical blocks available");
            return;
        }

        // Find the range of blocks we need to apply
        final long startBlock = stateMetadata.lastAppliedBlockNumber() + 1;
        final long endBlock = availableBlocks.max();

        if (startBlock > endBlock) {
            logger.log(INFO, "State is up to date with historical blocks");
            return;
        }

        logger.log(INFO, "Catching up from block {0} to {1}", startBlock, endBlock);
        // the root hash of state at the start of the next block, end of this one
        // Apply each block's state changes
        for (long blockNum = startBlock; blockNum <= endBlock; blockNum++) {
            try (final BlockAccessor accessor = historicalBlocks.block(blockNum)) {
                if (accessor == null) {
                    logger.log(WARNING, "Block {0} not available during catch-up", blockNum);
                    // If there's a gap, we can't continue catching up
                    break;
                }
                final BlockUnparsed block = accessor.blockUnparsed();
                if (block != null) {
                    // apply state changes
                    applyBlockStateChanges(block, blockNum);
                } else {
                    logger.log(WARNING, "Block " + blockNum + " has no block data during catch-up");
                }
            } catch (final Exception e) {
                logger.log(ERROR, "Failed to apply block " + blockNum + " during catch-up", e);
                break;
            }

            if (blockNum % 1000 == 0) {
                logger.log(INFO, "Catch-up progress: applied block {0}", blockNum);
            }
        }

        logger.log(INFO, "Catch-up complete. Last applied block: {0}", stateMetadata.lastAppliedBlockNumber());
    }

    /**
     * Apply state changes from an unparsed block.
     *
     * @param block the unparsed block
     * @param blockNumber the block number
     */
    private void applyBlockStateChanges(@NonNull final BlockUnparsed block, final long blockNumber) {
        // get state hash from block footer to compare
        try {
            final BlockItemUnparsed bfItem = block.blockItems().stream()
                    .filter(BlockItemUnparsed::hasBlockFooter)
                    .findFirst()
                    .orElseThrow();
            final BlockFooter blockFooter = BlockFooter.PROTOBUF.parse(bfItem.blockFooterOrThrow());
            byte[] stateRootHashAtStartOfBlock =
                    blockFooter.startOfBlockStateRootHash().toByteArray();
            // compare hashes
            if (blockNumber == 0) {
                // expect an all zeros hash
                if (!Arrays.equals(stateRootHashAtStartOfBlock, new byte[48])) {
                    throw new RuntimeException("Expected an all 48 zeros hash for block 0, got "
                            + HexFormat.of().formatHex(stateRootHashAtStartOfBlock));
                }
            } else {
                if (!Arrays.equals(stateRootHashAtStartOfBlock, stateMetadata.nextBlockStateRootHash())) {
                    throw new RuntimeException("Expected state root hash "
                            + HexFormat.of().formatHex(stateMetadata.nextBlockStateRootHash()) + " for block "
                            + blockNumber + ", got " + HexFormat.of().formatHex(stateRootHashAtStartOfBlock));
                }
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        // get the current mutable state
        final MerkleNodeState state = stateLifecycleManager.getMutableState();
        // hack to get virtual map instance
        final VirtualMap virtualMap = (VirtualMap) state.getRoot();
        // apply state changes from each block item
        for (final var item : block.blockItems()) {
            if (item.hasStateChanges()) {
                final Bytes stateChangesBytes = item.stateChangesOrThrow();
                StateChangeParser.applyStateChanges(virtualMap, stateChangesBytes);
            }
        }
        // copy and hash state
        stateLifecycleManager.copyMutableState();
        state.computeHash();
        Hash stateRootHash = state.getHash();
        // update the last block number, and next state root hash
        assert stateRootHash != null;
        stateMetadata = new StateMetadata(blockNumber, stateRootHash.getBytes().toByteArray());
    }

    // ================================================================================================================
    // LiveStateAccess Implementation - Thread-safe read-only access to the latest state
    // ================================================================================================================

    /**
     * Get the VirtualMap from the latest immutable state. This method is thread-safe as it retrieves
     * a snapshot of the state that will not be modified.
     *
     * @return the VirtualMap for read operations
     * @throws IllegalStateException if the state is not ready
     */
    private VirtualMap getLatestVirtualMap() {
        if (!stateReady) {
            throw new IllegalStateException("State is not ready");
        }
        return (VirtualMap) stateLifecycleManager.getLatestImmutableState().getRoot();
    }

    @Override
    public long blockNumber() {
        // stateMetadata is replaced atomically, so reading it is thread-safe
        return stateMetadata.lastAppliedBlockNumber();
    }

    @Override
    public Bytes mapValue(final int stateID, @NonNull final Bytes key) {
        final VirtualMap virtualMap = getLatestVirtualMap();
        final Bytes stateKey = kvKey(stateID, key);
        return virtualMap.getBytes(stateKey);
    }

    @Override
    public Bytes singleton(final int singletonID) {
        try {
            final VirtualMap virtualMap = getLatestVirtualMap();
            final Bytes stateKey = getStateKeyForSingleton(singletonID);
            return virtualMap.getBytes(stateKey);
        } catch (ArrayIndexOutOfBoundsException | IllegalArgumentException e) {
            // Invalid state IDs (negative or too large) may cause index errors
            return null;
        }
    }

    @Override
    public QueueState queueState(final int stateID) {
        final VirtualMap virtualMap = getLatestVirtualMap();
        final Bytes queueStateKey = getStateKeyForSingleton(stateID);
        final Bytes queueStateBytes = virtualMap.getBytes(queueStateKey);
        if (queueStateBytes == null) {
            return new QueueState(0, 0); // Empty queue
        }
        try {
            return QueueState.PROTOBUF.parse(queueStateBytes);
        } catch (ParseException e) {
            throw new IllegalStateException("Failed to parse queue state for stateID: " + stateID, e);
        }
    }

    @Override
    public Bytes queuePeekHead(final int stateID) {
        final VirtualMap virtualMap = getLatestVirtualMap();
        final QueueState state = queueState(stateID);
        if (state.head() >= state.tail()) {
            return null; // Empty queue
        }
        final Bytes elementKey = queueKey(stateID, (int) state.head());
        return virtualMap.getBytes(elementKey);
    }

    @Override
    public Bytes queuePeekTail(final int stateID) {
        final VirtualMap virtualMap = getLatestVirtualMap();
        final QueueState state = queueState(stateID);
        if (state.head() >= state.tail()) {
            return null; // Empty queue
        }
        // Tail points to the next position to write, so tail-1 is the last element
        final Bytes elementKey = queueKey(stateID, (int) (state.tail() - 1));
        return virtualMap.getBytes(elementKey);
    }

    @Override
    public Bytes queuePeek(final int stateID, final int index) {
        final VirtualMap virtualMap = getLatestVirtualMap();
        final QueueState state = queueState(stateID);
        if (index < state.head() || index >= state.tail()) {
            throw new IllegalArgumentException("Index " + index + " is out of bounds. Valid range is [" + state.head()
                    + ", " + (state.tail() - 1) + "]");
        }
        final Bytes elementKey = queueKey(stateID, index);
        return virtualMap.getBytes(elementKey);
    }

    @Override
    public List<Bytes> queueAsList(final int stateID) {
        final VirtualMap virtualMap = getLatestVirtualMap();
        final QueueState state = queueState(stateID);
        final List<Bytes> result = new ArrayList<>();
        for (long i = state.head(); i < state.tail(); i++) {
            final Bytes elementKey = queueKey(stateID, (int) i);
            final Bytes element = virtualMap.getBytes(elementKey);
            if (element != null) {
                result.add(element);
            }
        }
        return result;
    }
}
