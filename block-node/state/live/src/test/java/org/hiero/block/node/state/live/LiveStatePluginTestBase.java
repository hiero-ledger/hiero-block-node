// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static com.swirlds.state.merkle.StateKeyUtils.kvKey;
import static com.swirlds.state.merkle.StateKeyUtils.queueKey;
import static com.swirlds.state.merkle.StateUtils.getStateKeyForSingleton;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.TssSignedBlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.MapDeleteChange;
import com.hedera.hapi.block.stream.output.MapUpdateChange;
import com.hedera.hapi.block.stream.output.QueuePopChange;
import com.hedera.hapi.block.stream.output.QueuePushChange;
import com.hedera.hapi.block.stream.output.SingletonUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.state.token.Account;
import com.hedera.hapi.platform.state.QueueState;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.common.metrics.platform.DefaultMetricsProvider;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.virtualmap.VirtualMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.NewestBlockKnownToNetworkNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.threading.ThreadPoolManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

/**
 * Base class for LiveStatePlugin tests providing common test infrastructure.
 */
abstract class LiveStatePluginTestBase {

    // State IDs for testing - these are arbitrary test values
    protected static final int MAP_STATE_ID = 42;
    protected static final int SINGLETON_STATE_ID = 100;
    protected static final int QUEUE_STATE_ID = 200;

    @TempDir
    protected Path tempDir;

    protected LiveStatePlugin plugin;
    protected BlockNodeContext blockNodeContext;
    protected TestBlockMessaging blockMessaging;
    protected TestHistoricalBlockFacility historicalBlockFacility;
    protected DefaultMetricsProvider metricsProvider;
    protected ExecutorService executorService;
    protected ScheduledExecutorService scheduledExecutorService;

    /**
     * Initialize the test infrastructure with the given configuration overrides.
     */
    protected void initPlugin(java.util.Map<String, String> configOverrides) {
        executorService = Executors.newVirtualThreadPerTaskExecutor();
        scheduledExecutorService = Executors.newScheduledThreadPool(2);

        // Build configuration with required config classes
        ConfigurationBuilder configurationBuilder = ConfigurationBuilder.create()
                .withConfigDataType(com.swirlds.merkledb.config.MerkleDbConfig.class)
                .withConfigDataType(com.swirlds.virtualmap.config.VirtualMapConfig.class)
                .withConfigDataType(com.swirlds.common.metrics.config.MetricsConfig.class)
                .withConfigDataType(com.swirlds.common.metrics.platform.prometheus.PrometheusConfig.class)
                .withConfigDataType(com.swirlds.common.io.config.TemporaryFileConfig.class)
                .withConfigDataType(com.swirlds.common.config.StateCommonConfig.class)
                .withConfigDataType(LiveStateConfig.class)
                // Set default paths to temp directory
                .withValue("merkleDb.databasePath", tempDir.resolve("merkledb").toString())
                .withValue("state.live.storagePath", tempDir.resolve("state").toString())
                .withValue("state.live.latestStatePath", tempDir.resolve("state/latest").toString())
                .withValue("state.live.stateMetadataPath", tempDir.resolve("state/metadata.dat").toString())
                .withValue("prometheus.endpointEnabled", "false");

        // Apply any configuration overrides
        if (configOverrides != null) {
            for (var entry : configOverrides.entrySet()) {
                configurationBuilder = configurationBuilder.withValue(entry.getKey(), entry.getValue());
            }
        }

        Configuration configuration = configurationBuilder.build();

        // Create metrics
        metricsProvider = new DefaultMetricsProvider(configuration);
        Metrics metrics = metricsProvider.createGlobalMetrics();
        metricsProvider.start();

        // Create test facilities
        blockMessaging = new TestBlockMessaging();
        // Only create a new facility if not already set (for catch-up tests that pre-configure it)
        if (historicalBlockFacility == null) {
            historicalBlockFacility = new TestHistoricalBlockFacility();
        }

        // Create block node context
        blockNodeContext = new BlockNodeContext(
                configuration,
                metrics,
                new TestHealthFacility(),
                blockMessaging,
                historicalBlockFacility,
                new ServiceLoaderFunction(),
                new TestThreadPoolManager());

        // Create and initialize plugin
        plugin = new LiveStatePlugin();
        plugin.init(blockNodeContext, createMockServiceBuilder());
    }

    /**
     * Initialize and start the plugin with no configuration overrides.
     */
    protected void initAndStartPlugin() {
        initPlugin(null);
        plugin.start();
    }

    /**
     * Initialize and start the plugin with the given configuration overrides.
     */
    protected void initAndStartPlugin(java.util.Map<String, String> configOverrides) {
        initPlugin(configOverrides);
        plugin.start();
    }

    @AfterEach
    protected void tearDown() throws IOException {
        // Set plugin to null to prevent stop() from being called on failed starts
        LiveStatePlugin pluginToStop = plugin;
        plugin = null;
        if (pluginToStop != null) {
            try {
                pluginToStop.stop();
            } catch (Exception e) {
                // Ignore stop errors in tests - plugin may not have started successfully
            }
        }
        if (metricsProvider != null) {
            metricsProvider.stop();
        }
        if (executorService != null) {
            executorService.shutdownNow();
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        // Clean up temp directory
        if (tempDir != null && Files.exists(tempDir)) {
            try (var paths = Files.walk(tempDir)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException e) {
                                // Ignore cleanup errors
                            }
                        });
            }
        }
    }

    // ========================================
    // Block Building Utilities
    // ========================================

    /**
     * Create a block with state changes and a valid state root hash.
     * The state root hash in the footer is set to all zeros for block 0,
     * or to the provided previousStateHash for subsequent blocks.
     */
    protected BlockUnparsed createBlockWithStateChanges(
            long blockNumber, byte[] previousStateHash, List<StateChanges> stateChangesList) {
        List<BlockItemUnparsed> items = new ArrayList<>();

        // Add block header
        items.add(createBlockHeaderItem(blockNumber));

        // Add state changes
        for (StateChanges stateChanges : stateChangesList) {
            items.add(createStateChangesItem(stateChanges));
        }

        // Add block footer with state root hash
        byte[] stateRootHash = (blockNumber == 0) ? new byte[48] : previousStateHash;
        items.add(createBlockFooterItem(blockNumber, stateRootHash));

        // Add block proof
        items.add(createBlockProofItem(blockNumber));

        return new BlockUnparsed(items);
    }

    /**
     * Create a simple block with no state changes.
     */
    protected BlockUnparsed createSimpleBlock(long blockNumber, byte[] previousStateHash) {
        return createBlockWithStateChanges(blockNumber, previousStateHash, List.of());
    }

    protected BlockItemUnparsed createBlockHeaderItem(long blockNumber) {
        BlockHeader header = new BlockHeader(
                new SemanticVersion(1, 0, 0, "", ""),
                new SemanticVersion(1, 0, 0, "", ""),
                blockNumber,
                new Timestamp(System.currentTimeMillis() / 1000, 0),
                BlockHashAlgorithm.SHA2_384);
        return BlockItemUnparsed.newBuilder()
                .blockHeader(BlockHeader.PROTOBUF.toBytes(header))
                .build();
    }

    protected BlockItemUnparsed createStateChangesItem(StateChanges stateChanges) {
        return BlockItemUnparsed.newBuilder()
                .stateChanges(StateChanges.PROTOBUF.toBytes(stateChanges))
                .build();
    }

    protected BlockItemUnparsed createBlockFooterItem(long blockNumber, byte[] startOfBlockStateRootHash) {
        BlockFooter footer = BlockFooter.newBuilder()
                .previousBlockRootHash(Bytes.wrap(new byte[48]))
                .rootHashOfAllBlockHashesTree(Bytes.wrap(new byte[48]))
                .startOfBlockStateRootHash(Bytes.wrap(startOfBlockStateRootHash))
                .build();
        return BlockItemUnparsed.newBuilder()
                .blockFooter(BlockFooter.PROTOBUF.toBytes(footer))
                .build();
    }

    protected BlockItemUnparsed createBlockProofItem(long blockNumber) {
        TssSignedBlockProof tssProof = TssSignedBlockProof.newBuilder()
                .blockSignature(Bytes.wrap("test_signature".getBytes()))
                .build();
        BlockProof proof = BlockProof.newBuilder()
                .block(blockNumber)
                .signedBlockProof(tssProof)
                .build();
        return BlockItemUnparsed.newBuilder()
                .blockProof(BlockProof.PROTOBUF.toBytes(proof))
                .build();
    }

    // ========================================
    // State Change Building Utilities
    // ========================================

    /**
     * Create a singleton update state change.
     */
    protected StateChanges createSingletonStateChange(int stateId, long entityNumber) {
        SingletonUpdateChange singletonChange = SingletonUpdateChange.newBuilder()
                .entityNumberValue(entityNumber)
                .build();
        StateChange stateChange = StateChange.newBuilder()
                .stateId(stateId)
                .singletonUpdate(singletonChange)
                .build();
        return new StateChanges(new Timestamp(1000L, 0), List.of(stateChange));
    }

    /**
     * Create a singleton update state change with bytes value.
     */
    protected StateChanges createSingletonBytesStateChange(int stateId, Bytes value) {
        SingletonUpdateChange singletonChange = SingletonUpdateChange.newBuilder()
                .bytesValue(value)
                .build();
        StateChange stateChange = StateChange.newBuilder()
                .stateId(stateId)
                .singletonUpdate(singletonChange)
                .build();
        return new StateChanges(new Timestamp(1000L, 0), List.of(stateChange));
    }

    /**
     * Create a map update state change with AccountID key and Account value.
     */
    protected StateChanges createMapUpdateStateChange(int stateId, long accountNum) {
        AccountID accountId = AccountID.newBuilder()
                .shardNum(0L)
                .realmNum(0L)
                .accountNum(accountNum)
                .build();
        Account account = Account.newBuilder()
                .accountId(accountId)
                .alias(Bytes.wrap(("alias_" + accountNum).getBytes()))
                .build();

        MapChangeKey mapKey = MapChangeKey.newBuilder()
                .accountIdKey(accountId)
                .build();
        MapChangeValue mapValue = MapChangeValue.newBuilder()
                .accountValue(account)
                .build();
        MapUpdateChange mapChange = new MapUpdateChange(mapKey, mapValue, false);

        StateChange stateChange = StateChange.newBuilder()
                .stateId(stateId)
                .mapUpdate(mapChange)
                .build();
        return new StateChanges(new Timestamp(1000L, 0), List.of(stateChange));
    }

    /**
     * Create a map update state change with proto bytes key and string value.
     */
    protected StateChanges createMapBytesUpdateStateChange(int stateId, Bytes key, Bytes value) {
        MapChangeKey mapKey = MapChangeKey.newBuilder()
                .protoBytesKey(key)
                .build();
        // Use protoStringValue as protoBytesValue doesn't exist
        MapChangeValue mapValue = MapChangeValue.newBuilder()
                .protoStringValue(new String(value.toByteArray()))
                .build();
        MapUpdateChange mapChange = new MapUpdateChange(mapKey, mapValue, false);

        StateChange stateChange = StateChange.newBuilder()
                .stateId(stateId)
                .mapUpdate(mapChange)
                .build();
        return new StateChanges(new Timestamp(1000L, 0), List.of(stateChange));
    }

    /**
     * Create a map delete state change.
     */
    protected StateChanges createMapDeleteStateChange(int stateId, long accountNum) {
        AccountID accountId = AccountID.newBuilder()
                .shardNum(0L)
                .realmNum(0L)
                .accountNum(accountNum)
                .build();

        MapChangeKey mapKey = MapChangeKey.newBuilder()
                .accountIdKey(accountId)
                .build();
        MapDeleteChange mapDelete = new MapDeleteChange(mapKey);

        StateChange stateChange = StateChange.newBuilder()
                .stateId(stateId)
                .mapDelete(mapDelete)
                .build();
        return new StateChanges(new Timestamp(1000L, 0), List.of(stateChange));
    }

    /**
     * Create a queue push state change.
     */
    protected StateChanges createQueuePushStateChange(int stateId, Bytes element) {
        QueuePushChange queuePush = QueuePushChange.newBuilder()
                .protoBytesElement(element)
                .build();
        StateChange stateChange = StateChange.newBuilder()
                .stateId(stateId)
                .queuePush(queuePush)
                .build();
        return new StateChanges(new Timestamp(1000L, 0), List.of(stateChange));
    }

    /**
     * Create a queue pop state change.
     */
    protected StateChanges createQueuePopStateChange(int stateId) {
        QueuePopChange queuePop = new QueuePopChange();
        StateChange stateChange = StateChange.newBuilder()
                .stateId(stateId)
                .queuePop(queuePop)
                .build();
        return new StateChanges(new Timestamp(1000L, 0), List.of(stateChange));
    }

    /**
     * Create a verification notification for a block.
     */
    protected VerificationNotification createVerificationNotification(
            long blockNumber, BlockUnparsed block, boolean success) {
        return new VerificationNotification(
                success,
                blockNumber,
                Bytes.wrap(new byte[48]),
                block,
                BlockSource.PUBLISHER);
    }

    // ========================================
    // Test Implementations
    // ========================================

    /**
     * Simple test implementation of BlockMessagingFacility.
     */
    protected static class TestBlockMessaging implements BlockMessagingFacility {
        private final List<BlockNotificationHandler> notificationHandlers = new ArrayList<>();
        private final List<BlockItemHandler> itemHandlers = new ArrayList<>();

        @Override
        public String name() {
            return "TestBlockMessaging";
        }

        @Override
        public void sendBlockItems(BlockItems blockItems) {}

        @Override
        public void registerBlockItemHandler(BlockItemHandler handler, boolean cpuIntensive, String name) {
            itemHandlers.add(handler);
        }

        @Override
        public void registerNoBackpressureBlockItemHandler(
                NoBackPressureBlockItemHandler handler, boolean cpuIntensive, String name) {
            itemHandlers.add(handler);
        }

        @Override
        public void unregisterBlockItemHandler(BlockItemHandler handler) {
            itemHandlers.remove(handler);
        }

        @Override
        public void sendBlockVerification(VerificationNotification notification) {
            for (BlockNotificationHandler handler : notificationHandlers) {
                handler.handleVerification(notification);
            }
        }

        @Override
        public void sendBlockPersisted(PersistedNotification notification) {
            for (BlockNotificationHandler handler : notificationHandlers) {
                handler.handlePersisted(notification);
            }
        }

        @Override
        public void sendBackfilledBlockNotification(BackfilledBlockNotification notification) {
            for (BlockNotificationHandler handler : notificationHandlers) {
                handler.handleBackfilled(notification);
            }
        }

        @Override
        public void sendNewestBlockKnownToNetwork(NewestBlockKnownToNetworkNotification notification) {
            for (BlockNotificationHandler handler : notificationHandlers) {
                handler.handleNewestBlockKnownToNetwork(notification);
            }
        }

        @Override
        public void registerBlockNotificationHandler(
                BlockNotificationHandler handler, boolean cpuIntensive, String name) {
            notificationHandlers.add(handler);
        }

        @Override
        public void unregisterBlockNotificationHandler(BlockNotificationHandler handler) {
            notificationHandlers.remove(handler);
        }

        public List<BlockNotificationHandler> getNotificationHandlers() {
            return notificationHandlers;
        }
    }

    /**
     * Test implementation of HistoricalBlockFacility.
     */
    protected static class TestHistoricalBlockFacility implements HistoricalBlockFacility {
        private final java.util.Map<Long, BlockUnparsed> blocks = new java.util.concurrent.ConcurrentHashMap<>();
        private long minBlock = -1;
        private long maxBlock = -1;

        public void addBlock(long blockNumber, BlockUnparsed block) {
            blocks.put(blockNumber, block);
            if (minBlock < 0 || blockNumber < minBlock) {
                minBlock = blockNumber;
            }
            if (maxBlock < 0 || blockNumber > maxBlock) {
                maxBlock = blockNumber;
            }
        }

        public void clear() {
            blocks.clear();
            minBlock = -1;
            maxBlock = -1;
        }

        @Override
        public String name() {
            return "TestHistoricalBlockFacility";
        }

        @Override
        public BlockAccessor block(long blockNumber) {
            BlockUnparsed block = blocks.get(blockNumber);
            if (block == null) {
                return null;
            }
            return new TestBlockAccessor(blockNumber, block);
        }

        @Override
        public BlockRangeSet availableBlocks() {
            if (minBlock < 0) {
                return BlockRangeSet.EMPTY;
            }
            final long min = minBlock;
            final long max = maxBlock;
            return new BlockRangeSet() {
                @Override
                public boolean contains(long blockNumber) {
                    return blockNumber >= min && blockNumber <= max;
                }

                @Override
                public boolean contains(long start, long end) {
                    return start >= min && end <= max;
                }

                @Override
                public long size() {
                    return max - min + 1;
                }

                @Override
                public long min() {
                    return min;
                }

                @Override
                public long max() {
                    return max;
                }

                @Override
                public java.util.stream.LongStream stream() {
                    return java.util.stream.LongStream.rangeClosed(min, max);
                }

                @Override
                public java.util.stream.Stream<org.hiero.block.node.spi.historicalblocks.LongRange> streamRanges() {
                    return java.util.stream.Stream.of(
                            new org.hiero.block.node.spi.historicalblocks.LongRange(min, max));
                }
            };
        }
    }

    /**
     * Test implementation of BlockAccessor.
     */
    protected static class TestBlockAccessor implements BlockAccessor {
        private final long blockNumber;
        private final BlockUnparsed block;

        public TestBlockAccessor(long blockNumber, BlockUnparsed block) {
            this.blockNumber = blockNumber;
            this.block = block;
        }

        @Override
        public long blockNumber() {
            return blockNumber;
        }

        @Override
        public BlockUnparsed blockUnparsed() {
            return block;
        }

        @Override
        public Block block() {
            try {
                return Block.PROTOBUF.parse(BlockUnparsed.PROTOBUF.toBytes(block));
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Bytes blockBytes(Format format) {
            return BlockUnparsed.PROTOBUF.toBytes(block);
        }

        @Override
        public void close() {}

        @Override
        public boolean isClosed() {
            return false;
        }
    }

    /**
     * Test implementation of HealthFacility.
     */
    protected static class TestHealthFacility implements HealthFacility {
        private volatile State state = State.RUNNING;

        @Override
        public State blockNodeState() {
            return state;
        }

        @Override
        public void shutdown(String className, String reason) {
            state = State.SHUTTING_DOWN;
        }
    }

    /**
     * Test implementation of ThreadPoolManager.
     */
    protected class TestThreadPoolManager implements ThreadPoolManager {
        @Override
        public ExecutorService getVirtualThreadExecutor(String name, Thread.UncaughtExceptionHandler handler) {
            return executorService;
        }

        @Override
        public ExecutorService createSingleThreadExecutor(String name, Thread.UncaughtExceptionHandler handler) {
            return executorService;
        }

        @Override
        public ScheduledExecutorService createVirtualThreadScheduledExecutor(
                int corePoolSize, String name, Thread.UncaughtExceptionHandler handler) {
            return scheduledExecutorService;
        }
    }

    /**
     * Create a mock ServiceBuilder using reflection proxy to avoid Helidon dependencies in tests.
     */
    protected static ServiceBuilder createMockServiceBuilder() {
        return (ServiceBuilder) Proxy.newProxyInstance(
                ServiceBuilder.class.getClassLoader(),
                new Class<?>[]{ServiceBuilder.class},
                (proxy, method, args) -> null);
    }

    /**
     * Inner class for convenience - calls createMockServiceBuilder().
     */
    protected static class TestServiceBuilder {
        public static ServiceBuilder create() {
            return createMockServiceBuilder();
        }
    }
}
