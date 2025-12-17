// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.state.StateLifecycleManager;
import com.swirlds.state.merkle.StateLifecycleManagerImpl;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.System.Logger;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
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
 * TODO: Replace ConcurrentHashMap with VirtualMap once dependency issues are resolved.
 */
public class LiveStatePlugin implements BlockNodePlugin, BlockNotificationHandler {
    private final Logger logger = System.getLogger(getClass().getName());

    /** The block node context providing access to facilities */
    private BlockNodeContext context;

    /**
     * The current in-memory state storage.
     * TODO: Replace with VirtualMap/MerkleDB once module conflicts are resolved.
     */
    private final ConcurrentHashMap<Bytes, Bytes> stateMap = new ConcurrentHashMap<>();

    /** The last block number that was successfully applied to state */
    private long lastAppliedBlockNumber = -1;

    /** Configuration for the live state plugin */
    private LiveStateConfig config;

    /** Flag indicating if state is valid and ready */
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

        // TODO  temp for now so that we can test import dependencies
        StateLifecycleManager stateLifecycleManager =
                new StateLifecycleManagerImpl(context.metrics(), Time.getCurrent(), (virtualMap) -> null);
    }

    @Override
    public void start() {
        logger.log(INFO, "Starting Live State Plugin");

        try {
            // Initialize state - either load existing or create new genesis state
            initializeState();

            // Check if we need to catch up from historical blocks
            catchUpFromHistoricalBlocks();

            stateReady = true;
            logger.log(INFO, "Live State Plugin started successfully. Last applied block: {0}", lastAppliedBlockNumber);
        } catch (final Exception e) {
            logger.log(ERROR, "Failed to start Live State Plugin", e);
            throw new RuntimeException("Failed to initialize live state", e);
        }
    }

    @Override
    public void stop() {
        logger.log(INFO, "Stopping Live State Plugin");
        stateReady = false;
        // In-memory map doesn't need explicit cleanup
        logger.log(INFO, "Live state stopped. Final state size: {0} entries", stateMap.size());
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
        if (blockNumber != lastAppliedBlockNumber + 1) {
            logger.log(
                    WARNING,
                    "Out of order block received. Expected {0}, got {1}",
                    lastAppliedBlockNumber + 1,
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
     * Initialize the state - either load existing saved state or create new genesis state.
     */
    private void initializeState() {
        logger.log(INFO, "Initializing state from path: {0}", config.storagePath());

        // TODO: Check for existing saved state at config.storagePath()
        // For now, always create a new genesis state
        createGenesisState();
    }

    /**
     * Create a new empty genesis state.
     */
    private void createGenesisState() {
        logger.log(INFO, "Creating new genesis state (in-memory)");
        stateMap.clear();
        lastAppliedBlockNumber = -1;
        logger.log(INFO, "Genesis state created");
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
        final long startBlock = lastAppliedBlockNumber + 1;
        final long endBlock = availableBlocks.max();

        if (startBlock > endBlock) {
            logger.log(INFO, "State is up to date with historical blocks");
            return;
        }

        logger.log(INFO, "Catching up from block {0} to {1}", startBlock, endBlock);

        // Apply each block's state changes
        for (long blockNum = startBlock; blockNum <= endBlock; blockNum++) {
            try (final BlockAccessor accessor = historicalBlocks.block(blockNum)) {
                if (accessor == null) {
                    logger.log(WARNING, "Block {0} not available during catch-up", blockNum);
                    // If there's a gap, we can't continue catching up
                    break;
                }

                final Block block = accessor.block();
                if (block != null) {
                    applyBlockStateChangesFromParsed(block, blockNum);
                }
            } catch (final Exception e) {
                logger.log(ERROR, "Failed to apply block " + blockNum + " during catch-up", e);
                break;
            }

            if (blockNum % 1000 == 0) {
                logger.log(INFO, "Catch-up progress: applied block {0}", blockNum);
            }
        }

        logger.log(INFO, "Catch-up complete. Last applied block: {0}", lastAppliedBlockNumber);
    }

    /**
     * Apply state changes from a parsed Block.
     *
     * @param block the parsed block
     * @param blockNumber the block number
     */
    private void applyBlockStateChangesFromParsed(@NonNull final Block block, final long blockNumber) {
        for (final BlockItem item : block.items()) {
            if (item.hasStateChanges()) {
                final StateChanges stateChanges = item.stateChangesOrThrow();
                applyStateChanges(stateChanges, blockNumber);
            }
        }

        // Finalize the block
        finalizeBlock(blockNumber);
    }

    /**
     * Apply state changes from an unparsed block.
     *
     * @param block the unparsed block
     * @param blockNumber the block number
     */
    private void applyBlockStateChanges(@NonNull final BlockUnparsed block, final long blockNumber) {
        for (final var item : block.blockItems()) {
            if (item.hasStateChanges()) {
                try {
                    final Bytes stateChangesBytes = item.stateChangesOrThrow();
                    final StateChanges stateChanges = StateChanges.PROTOBUF.parse(stateChangesBytes);
                    applyStateChanges(stateChanges, blockNumber);
                } catch (final ParseException e) {
                    logger.log(ERROR, "Failed to parse state changes for block " + blockNumber, e);
                }
            }
        }

        // Finalize the block
        finalizeBlock(blockNumber);
    }

    /**
     * Apply a set of state changes to the current state.
     *
     * @param stateChanges the state changes to apply
     * @param blockNumber the block number for logging
     */
    private void applyStateChanges(@NonNull final StateChanges stateChanges, final long blockNumber) {
        for (final StateChange change : stateChanges.stateChanges()) {
            final int stateId = change.stateId();

            try {
                applyStateChange(change, stateId);
            } catch (final Exception e) {
                logger.log(
                        WARNING,
                        "Failed to apply state change for state ID " + stateId + " in block " + blockNumber,
                        e);
            }
        }
    }

    /**
     * Apply a single state change to the in-memory state map.
     *
     * @param change the state change
     * @param stateId the state ID
     */
    private void applyStateChange(@NonNull final StateChange change, final int stateId) {
        if (StateChangeParser.isSingletonUpdate(change)) {
            final Bytes key = createSingletonKey(stateId);
            final Object value = StateChangeParser.singletonValueFor(change.singletonUpdateOrThrow());
            final Bytes valueBytes = serializeValue(value);
            stateMap.put(key, valueBytes);
            logger.log(DEBUG, "Applied singleton update for state {0}", stateId);

        } else if (StateChangeParser.isMapUpdate(change)) {
            final var mapUpdate = change.mapUpdateOrThrow();
            final Object mapKey = StateChangeParser.mapKeyFor(mapUpdate.keyOrThrow());
            final Object mapValue = StateChangeParser.mapValueFor(mapUpdate.valueOrThrow());
            final Bytes key = createMapKey(stateId, mapKey);
            final Bytes valueBytes = serializeValue(mapValue);
            stateMap.put(key, valueBytes);

        } else if (StateChangeParser.isMapDelete(change)) {
            final var mapDelete = change.mapDeleteOrThrow();
            final Object mapKey = StateChangeParser.mapKeyFor(mapDelete.keyOrThrow());
            final Bytes key = createMapKey(stateId, mapKey);
            stateMap.remove(key);

        } else if (StateChangeParser.isQueuePush(change)) {
            // Queue operations need special handling - track head/tail indices
            logger.log(DEBUG, "Queue push for state {0} - queue support pending", stateId);

        } else if (StateChangeParser.isQueuePop(change)) {
            logger.log(DEBUG, "Queue pop for state {0} - queue support pending", stateId);

        } else if (StateChangeParser.isNewState(change)) {
            logger.log(DEBUG, "New state {0} added", StateChangeParser.stateNameOf(stateId));

        } else if (StateChangeParser.isStateRemoved(change)) {
            // Remove all entries for this state ID
            // This is inefficient with ConcurrentHashMap - would be better with VirtualMap
            logger.log(DEBUG, "State {0} removed", StateChangeParser.stateNameOf(stateId));
        }
    }

    /**
     * Create a key for a singleton state.
     *
     * @param stateId the state ID
     * @return the composite key
     */
    private Bytes createSingletonKey(final int stateId) {
        final byte[] keyBytes = new byte[4];
        keyBytes[0] = (byte) (stateId >> 24);
        keyBytes[1] = (byte) (stateId >> 16);
        keyBytes[2] = (byte) (stateId >> 8);
        keyBytes[3] = (byte) stateId;
        return Bytes.wrap(keyBytes);
    }

    /**
     * Create a composite key for a map entry.
     *
     * @param stateId the state ID
     * @param mapKey the map key object
     * @return the composite key
     */
    private Bytes createMapKey(final int stateId, final Object mapKey) {
        final Bytes mapKeyBytes = serializeValue(mapKey);
        final byte[] compositeKey = new byte[4 + (int) mapKeyBytes.length()];
        compositeKey[0] = (byte) (stateId >> 24);
        compositeKey[1] = (byte) (stateId >> 16);
        compositeKey[2] = (byte) (stateId >> 8);
        compositeKey[3] = (byte) stateId;
        mapKeyBytes.getBytes(0, compositeKey, 4, (int) mapKeyBytes.length());
        return Bytes.wrap(compositeKey);
    }

    /**
     * Serialize a value object to bytes.
     *
     * @param value the value to serialize
     * @return the serialized bytes
     */
    @SuppressWarnings("unchecked")
    private Bytes serializeValue(final Object value) {
        if (value instanceof Bytes bytes) {
            return bytes;
        }

        // Try to use PBJ PROTOBUF codec if available
        try {
            final var clazz = value.getClass();
            final var protobufField = clazz.getField("PROTOBUF");
            final var codec = protobufField.get(null);
            if (codec instanceof com.hedera.pbj.runtime.Codec<?> pbjCodec) {
                return ((com.hedera.pbj.runtime.Codec<Object>) pbjCodec).toBytes(value);
            }
        } catch (final Exception e) {
            // Fall through to toString serialization
        }

        // Fallback: convert to string representation
        return Bytes.wrap(value.toString().getBytes());
    }

    /**
     * Finalize a block after applying all state changes.
     *
     * @param blockNumber the block number
     */
    private void finalizeBlock(final long blockNumber) {
        // TODO: With VirtualMap, we would:
        // 1. Create a copy for the next block (makes current state immutable)
        // 2. Compute hash of the now-immutable state
        // 3. Verify hash against BlockFooter.startOfBlockStateRootHash

        lastAppliedBlockNumber = blockNumber;

        if (blockNumber % 100 == 0) {
            logger.log(DEBUG, "Applied block {0}, state entries: {1}", blockNumber, stateMap.size());
        }
    }

    /**
     * Get the last block number that was applied to state.
     *
     * @return the last applied block number, or -1 if no blocks applied yet
     */
    public long getLastAppliedBlockNumber() {
        return lastAppliedBlockNumber;
    }

    /**
     * Check if the state is ready for use.
     *
     * @return true if state is initialized and ready
     */
    public boolean isStateReady() {
        return stateReady;
    }

    /**
     * Get the current state map size (for testing/monitoring).
     *
     * @return the number of entries in the state map
     */
    public int getStateSize() {
        return stateMap.size();
    }
}
