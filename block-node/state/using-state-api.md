Guide: Using platform-sdk/swirlds-state-api to Apply Block Stream State Changes

Overview

This guide explains how to:
1. Load a saved state from disk or create a new empty state
2. Apply StateChange items from the block stream
3. Compute the state merkle root hash and compare to BlockFooter

  ---
1. Creating or Loading State

Creating a New Empty State

import com.swirlds.config.api.Configuration;
import com.swirlds.merkledb.MerkleDbDataSourceBuilder;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.virtualmap.VirtualMap;

// Create new empty state with configuration
public VirtualMapState createNewState(Configuration configuration, Metrics metrics) {
MerkleDbConfig merkleDbConfig = configuration.getConfigData(MerkleDbConfig.class);
MerkleDbDataSourceBuilder dsBuilder = new MerkleDbDataSourceBuilder(
configuration,
merkleDbConfig.initialCapacity(),
merkleDbConfig.hashesRamToDiskThreshold());

      VirtualMap virtualMap = new VirtualMap(dsBuilder, configuration);
      virtualMap.registerMetrics(metrics);

      return new VirtualMapState(virtualMap, metrics);
}

Loading State from Disk

import com.swirlds.state.merkle.StateLifecycleManagerImpl;
import com.swirlds.state.MerkleNodeState;

public MerkleNodeState loadStateFromDisk(
Path snapshotPath,
Configuration configuration,
Metrics metrics) throws IOException {

      StateLifecycleManagerImpl lifecycleManager = new StateLifecycleManagerImpl(
              metrics,
              Time.getCurrent(),
              vm -> new VirtualMapState(vm, metrics),
              configuration);

      // Loads the state from disk
      return lifecycleManager.loadSnapshot(snapshotPath);
}

Key file: StateLifecycleManagerImpl.java:245-261

  ---
2. Initializing State Metadata (Service Schemas)

Before applying state changes, you need to initialize the state with service schemas. Each service defines its state structure:

import com.swirlds.state.lifecycle.StateMetadata;
import com.swirlds.state.lifecycle.StateDefinition;

// For each service, register its state definitions
public void initializeStateMetadata(VirtualMapState state, StateMetadata<?, ?> metadata) {
state.initializeState(metadata);
}

The state definitions map stateId integers to their key/value codecs. The stateNameOf() method in BlockStreamUtils shows the mapping:

Key file: BlockStreamUtils.java:23-93 maps state IDs to service names like:
- STATE_ID_ACCOUNTS → "TokenService.ACCOUNTS"
- STATE_ID_FILES → "FileService.FILES"
- etc.

  ---
3. Applying State Changes from Block Stream

The core logic for applying StateChange items:

import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.node.app.hapi.utils.blocks.BlockStreamUtils;
import com.swirlds.state.spi.CommittableWritableStates;
import com.swirlds.state.spi.WritableStates;

public void applyStateChanges(MerkleNodeState state, StateChanges stateChanges) {
String lastService = null;
CommittableWritableStates lastWritableStates = null;

      List<StateChange> changes = stateChanges.stateChanges();
      int n = changes.size();

      for (int i = 0; i < n; i++) {
          StateChange stateChange = changes.get(i);

          // Get service name from state ID
          String stateName = BlockStreamUtils.stateNameOf(stateChange.stateId());
          int delimIndex = stateName.indexOf('.');
          String serviceName = stateName.substring(0, delimIndex);

          // Get writable states for the service
          WritableStates writableStates = state.getWritableStates(serviceName);
          int stateId = stateChange.stateId();

          // Apply the change based on its type
          switch (stateChange.changeOperation().kind()) {
              case UNSET -> throw new IllegalStateException("Change operation not set");

              case STATE_ADD, STATE_REMOVE -> {
                  // No-op for state add/remove in replay
              }

              case SINGLETON_UPDATE -> {
                  var singletonState = writableStates.getSingleton(stateId);
                  var value = BlockStreamUtils.singletonPutFor(stateChange.singletonUpdateOrThrow());
                  singletonState.put(value);
              }

              case MAP_UPDATE -> {
                  var mapState = writableStates.get(stateId);
                  var key = BlockStreamUtils.mapKeyFor(stateChange.mapUpdateOrThrow().keyOrThrow());
                  var value = BlockStreamUtils.mapValueFor(stateChange.mapUpdateOrThrow().valueOrThrow());
                  mapState.put(key, value);
              }

              case MAP_DELETE -> {
                  var mapState = writableStates.get(stateId);
                  var key = BlockStreamUtils.mapKeyFor(stateChange.mapDeleteOrThrow().keyOrThrow());
                  mapState.remove(key);
              }

              case QUEUE_PUSH -> {
                  var queueState = writableStates.getQueue(stateId);
                  queueState.add(BlockStreamUtils.queuePushFor(stateChange.queuePushOrThrow()));
              }

              case QUEUE_POP -> {
                  var queueState = writableStates.getQueue(stateId);
                  queueState.poll();
              }
          }

          // Commit when service changes or at end
          if (lastService != null && !lastService.equals(serviceName)) {
              lastWritableStates.commit();
          }
          if (i == n - 1) {
              ((CommittableWritableStates) writableStates).commit();
          }

          lastService = serviceName;
          lastWritableStates = (CommittableWritableStates) writableStates;
      }
}

Key files:
- BlockStreamRecoveryWorkflow.java:167-224 - The actual implementation
- StateChangesValidator.java:736-851 - Test validator implementation

  ---
4. Processing Blocks and Computing State Hash

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.swirlds.common.merkle.crypto.MerkleCryptography;
import org.hiero.base.crypto.Hash;

public void processBlocks(Stream<Block> blocks, MerkleNodeState state) {
blocks.forEach(block -> {
for (BlockItem item : block.items()) {
if (item.hasStateChanges()) {
applyStateChanges(state, item.stateChangesOrThrow());
}
}
});
}

  ---
5. Computing Merkle Root Hash and Comparing to BlockFooter

import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.common.merkle.crypto.MerkleCryptography;

public boolean verifyStateHash(
MerkleNodeState state,
BlockFooter footer,
MerkleCryptography crypto) {

      // IMPORTANT: Create an immutable copy before hashing
      MerkleNodeState stateToBeCopied = state;
      state = state.copy();

      // Compute hash on the immutable copy
      Hash stateHash = crypto.digestTreeSync(stateToBeCopied.getRoot());
      Bytes computedRootHash = stateHash.getBytes();

      // Compare to BlockFooter's startOfBlockStateRootHash
      // Note: BlockFooter contains the hash at START of block
      Bytes expectedHash = footer.startOfBlockStateRootHash();

      return expectedHash.equals(computedRootHash);
}

Important: The BlockFooter contains:
- startOfBlockStateRootHash - State hash at the beginning of the block
- previousBlockRootHash - Hash of the previous block
- rootHashOfAllBlockHashesTree - Merkle tree of all previous block hashes

Key files:
- BlockFooter.java:33-47 - Field definitions
- StateChangesValidator.java:501-503 - Hash computation pattern

  ---
6. Complete Workflow Example

public class BlockStreamStateReplay {

      private final MerkleCryptography crypto;
      private final Configuration configuration;
      private final Metrics metrics;

      public void replayAndVerify(Path blockStreamDir, Path snapshotPath) throws IOException {
          // 1. Load initial state
          StateLifecycleManagerImpl lifecycleManager = new StateLifecycleManagerImpl(
                  metrics, Time.getCurrent(),
                  vm -> new VirtualMapState(vm, metrics),
                  configuration);

          MerkleNodeState state = lifecycleManager.loadSnapshot(snapshotPath);

          // 2. Read blocks from stream
          Stream<Block> blocks = BlockStreamAccess.readBlocks(blockStreamDir, false);

          // 3. Track state hash at start of each block
          Bytes startOfBlockStateHash = null;

          blocks.forEach(block -> {
              // Compute hash before applying changes (for next block's verification)
              MerkleNodeState immutableCopy = state;
              state = state.copy();
              startOfBlockStateHash = crypto.digestTreeSync(immutableCopy.getRoot()).getBytes();

              // Process all items in block
              for (BlockItem item : block.items()) {
                  if (item.hasStateChanges()) {
                      applyStateChanges(state, item.stateChangesOrThrow());
                  }

                  // Verify against BlockFooter
                  if (item.hasBlockFooter()) {
                      BlockFooter footer = item.blockFooterOrThrow();
                      if (!footer.startOfBlockStateRootHash().equals(startOfBlockStateHash)) {
                          throw new RuntimeException("State hash mismatch!");
                      }
                  }
              }
          });

          // 4. Final state hash after all blocks
          state.copy();
          Hash finalHash = crypto.digestTreeSync(state.getRoot());
          System.out.println("Final state root hash: " + finalHash.getBytes());
      }
}

  ---
Key Classes Reference

| Class                     | Purpose                                        | Location               |
  |---------------------------|------------------------------------------------|------------------------|
| VirtualMapState           | Main state implementation backed by VirtualMap | swirlds-state-impl     |
| StateLifecycleManagerImpl | Manages state lifecycle, load/save             | swirlds-state-impl     |
| StateChange               | Block stream state change item                 | hapi (generated)       |
| BlockStreamUtils          | Parse state changes to Java objects            | hedera-node/hapi-utils |
| CommittableWritableStates | Writable state that can be committed           | swirlds-state-api      |
| BlockFooter               | Contains state root hash at block start        | hapi (generated)       |

  ---
State Change Types

| Type             | Description              | Method                                          |
  |------------------|--------------------------|-------------------------------------------------|
| SINGLETON_UPDATE | Update a singleton value | writableStates.getSingleton(stateId).put(value) |
| MAP_UPDATE       | Insert/update map entry  | writableStates.get(stateId).put(key, value)     |
| MAP_DELETE       | Delete map entry         | writableStates.get(stateId).remove(key)         |
| QUEUE_PUSH       | Add to queue             | writableStates.getQueue(stateId).add(value)     |
| QUEUE_POP        | Remove from queue        | writableStates.getQueue(stateId).poll()         |
