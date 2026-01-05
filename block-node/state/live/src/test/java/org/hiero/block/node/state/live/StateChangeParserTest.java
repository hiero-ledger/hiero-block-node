// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.state.token.Account;
import com.hedera.hapi.platform.state.QueueState;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.common.metrics.platform.DefaultMetricsProvider;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.state.merkle.StateUtils;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.virtualmap.VirtualMap;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link StateChangeParser}.
 */
class StateChangeParserTest {

    private static final int TEST_STATE_ID = 42;
    private static final int QUEUE_STATE_ID = 100;

    @TempDir
    Path tempDir;

    private VirtualMap virtualMap;
    private DefaultMetricsProvider metricsProvider;

    @BeforeEach
    void setUp() {
        // Build configuration with required swirlds config classes
        Configuration configuration = ConfigurationBuilder.create()
                // MerkleDB config
                .withConfigDataType(com.swirlds.merkledb.config.MerkleDbConfig.class)
                // VirtualMap config
                .withConfigDataType(com.swirlds.virtualmap.config.VirtualMapConfig.class)
                // Metrics config classes
                .withConfigDataType(com.swirlds.common.metrics.config.MetricsConfig.class)
                .withConfigDataType(com.swirlds.common.metrics.platform.prometheus.PrometheusConfig.class)
                // Common IO config
                .withConfigDataType(com.swirlds.common.io.config.TemporaryFileConfig.class)
                // State common config
                .withConfigDataType(com.swirlds.common.config.StateCommonConfig.class)
                // Set config values to use temp directory
                .withValue("merkleDb.databasePath", tempDir.toString())
                .withValue("state.savedStateDirectory", tempDir.resolve("saved").toString())
                .withValue("prometheus.endpointEnabled", "false")
                .build();

        // Create metrics
        metricsProvider = new DefaultMetricsProvider(configuration);
        Metrics metrics = metricsProvider.createGlobalMetrics();
        metricsProvider.start();

        // Create a VirtualMapState
        VirtualMapState virtualMapState = new VirtualMapState(configuration, metrics);

        // Get the VirtualMap from VirtualMapState
        virtualMap = (VirtualMap) virtualMapState.getRoot();
    }

    @AfterEach
    void tearDown() {
        if (metricsProvider != null) {
            metricsProvider.stop();
        }
    }

    // ========================================
    // Singleton Update Tests
    // ========================================

    @Test
    @DisplayName("Test singleton update with Timestamp value")
    void testSingletonUpdateWithTimestamp() {
        // Create a SingletonUpdateChange with a Timestamp value
        Timestamp timestamp = new Timestamp(1234567890L, 123);
        SingletonUpdateChange singletonChange =
                SingletonUpdateChange.newBuilder().timestampValue(timestamp).build();

        StateChange stateChange = StateChange.newBuilder()
                .stateId(TEST_STATE_ID)
                .singletonUpdate(singletonChange)
                .build();

        StateChanges stateChanges = new StateChanges(new Timestamp(1000L, 0), List.of(stateChange));

        Bytes stateChangesBytes = StateChanges.PROTOBUF.toBytes(stateChanges);

        // Apply the state changes
        StateChangeParser.applyStateChanges(virtualMap, stateChangesBytes);

        // Verify the singleton was stored
        Bytes singletonKey = StateUtils.getStateKeyForSingleton(TEST_STATE_ID);
        Bytes storedValue = virtualMap.getBytes(singletonKey);
        assertNotNull(storedValue, "Singleton value should be stored");
    }

    @Test
    @DisplayName("Test singleton update with bytes value")
    void testSingletonUpdateWithBytesValue() {
        Bytes testBytes = Bytes.wrap(new byte[] {1, 2, 3, 4, 5});
        SingletonUpdateChange singletonChange =
                SingletonUpdateChange.newBuilder().bytesValue(testBytes).build();

        StateChange stateChange = StateChange.newBuilder()
                .stateId(TEST_STATE_ID)
                .singletonUpdate(singletonChange)
                .build();

        StateChanges stateChanges = new StateChanges(new Timestamp(1000L, 0), List.of(stateChange));

        Bytes stateChangesBytes = StateChanges.PROTOBUF.toBytes(stateChanges);

        StateChangeParser.applyStateChanges(virtualMap, stateChangesBytes);

        Bytes singletonKey = StateUtils.getStateKeyForSingleton(TEST_STATE_ID);
        Bytes storedValue = virtualMap.getBytes(singletonKey);
        assertNotNull(storedValue, "Singleton value should be stored");
    }

    @Test
    @DisplayName("Test singleton update with entity number value")
    void testSingletonUpdateWithEntityNumber() {
        SingletonUpdateChange singletonChange =
                SingletonUpdateChange.newBuilder().entityNumberValue(999L).build();

        StateChange stateChange = StateChange.newBuilder()
                .stateId(TEST_STATE_ID)
                .singletonUpdate(singletonChange)
                .build();

        StateChanges stateChanges = new StateChanges(new Timestamp(1000L, 0), List.of(stateChange));

        Bytes stateChangesBytes = StateChanges.PROTOBUF.toBytes(stateChanges);

        StateChangeParser.applyStateChanges(virtualMap, stateChangesBytes);

        Bytes singletonKey = StateUtils.getStateKeyForSingleton(TEST_STATE_ID);
        Bytes storedValue = virtualMap.getBytes(singletonKey);
        assertNotNull(storedValue, "Singleton value should be stored");
    }

    // ========================================
    // Map Update Tests
    // ========================================

    @Test
    @DisplayName("Test map update with AccountID key and Account value")
    void testMapUpdateWithAccountIdKeyAndAccountValue() {
        AccountID accountId = AccountID.newBuilder()
                .shardNum(0L)
                .realmNum(0L)
                .accountNum(1001L)
                .build();
        Account account = Account.newBuilder()
                .accountId(accountId)
                .alias(Bytes.wrap(new byte[] {10, 20, 30}))
                .build();

        MapChangeKey mapKey = MapChangeKey.newBuilder().accountIdKey(accountId).build();

        MapChangeValue mapValue =
                MapChangeValue.newBuilder().accountValue(account).build();

        MapUpdateChange mapChange = new MapUpdateChange(mapKey, mapValue, false);

        StateChange stateChange = StateChange.newBuilder()
                .stateId(TEST_STATE_ID)
                .mapUpdate(mapChange)
                .build();

        StateChanges stateChanges = new StateChanges(new Timestamp(1000L, 0), List.of(stateChange));

        Bytes stateChangesBytes = StateChanges.PROTOBUF.toBytes(stateChanges);

        long sizeBefore = virtualMap.size();
        StateChangeParser.applyStateChanges(virtualMap, stateChangesBytes);

        // Verify the map entry was stored
        assertEquals(sizeBefore + 1, virtualMap.size(), "Map should contain one additional entry");
    }

    @Test
    @DisplayName("Test map update with proto bytes key")
    void testMapUpdateWithProtoBytesKey() {
        Bytes keyBytes = Bytes.wrap(new byte[] {100, 101, 102});

        MapChangeKey mapKey = MapChangeKey.newBuilder().protoBytesKey(keyBytes).build();

        MapChangeValue mapValue =
                MapChangeValue.newBuilder().protoStringValue("test value").build();

        MapUpdateChange mapChange = new MapUpdateChange(mapKey, mapValue, false);

        StateChange stateChange = StateChange.newBuilder()
                .stateId(TEST_STATE_ID)
                .mapUpdate(mapChange)
                .build();

        StateChanges stateChanges = new StateChanges(new Timestamp(1000L, 0), List.of(stateChange));

        Bytes stateChangesBytes = StateChanges.PROTOBUF.toBytes(stateChanges);

        long sizeBefore = virtualMap.size();
        StateChangeParser.applyStateChanges(virtualMap, stateChangesBytes);

        assertEquals(sizeBefore + 1, virtualMap.size(), "Map should contain one additional entry");
    }

    @Test
    @DisplayName("Test multiple map updates in a single StateChanges")
    void testMultipleMapUpdates() {
        AccountID accountId1 = AccountID.newBuilder()
                .shardNum(0L)
                .realmNum(0L)
                .accountNum(1001L)
                .build();
        AccountID accountId2 = AccountID.newBuilder()
                .shardNum(0L)
                .realmNum(0L)
                .accountNum(1002L)
                .build();

        Account account1 = Account.newBuilder().accountId(accountId1).build();
        Account account2 = Account.newBuilder().accountId(accountId2).build();

        MapChangeKey mapKey1 =
                MapChangeKey.newBuilder().accountIdKey(accountId1).build();
        MapChangeKey mapKey2 =
                MapChangeKey.newBuilder().accountIdKey(accountId2).build();

        MapChangeValue mapValue1 =
                MapChangeValue.newBuilder().accountValue(account1).build();
        MapChangeValue mapValue2 =
                MapChangeValue.newBuilder().accountValue(account2).build();

        MapUpdateChange mapChange1 = new MapUpdateChange(mapKey1, mapValue1, false);
        MapUpdateChange mapChange2 = new MapUpdateChange(mapKey2, mapValue2, false);

        StateChange stateChange1 = StateChange.newBuilder()
                .stateId(TEST_STATE_ID)
                .mapUpdate(mapChange1)
                .build();

        StateChange stateChange2 = StateChange.newBuilder()
                .stateId(TEST_STATE_ID)
                .mapUpdate(mapChange2)
                .build();

        StateChanges stateChanges = new StateChanges(new Timestamp(1000L, 0), List.of(stateChange1, stateChange2));

        Bytes stateChangesBytes = StateChanges.PROTOBUF.toBytes(stateChanges);

        long sizeBefore = virtualMap.size();
        StateChangeParser.applyStateChanges(virtualMap, stateChangesBytes);

        assertEquals(sizeBefore + 2, virtualMap.size(), "Map should contain two additional entries");
    }

    // ========================================
    // Map Delete Tests
    // ========================================

    @Test
    @DisplayName("Test map delete removes entry")
    void testMapDeleteRemovesEntry() {
        // First, add an entry
        AccountID accountId = AccountID.newBuilder()
                .shardNum(0L)
                .realmNum(0L)
                .accountNum(1001L)
                .build();
        Account account = Account.newBuilder().accountId(accountId).build();

        MapChangeKey mapKey = MapChangeKey.newBuilder().accountIdKey(accountId).build();

        MapChangeValue mapValue =
                MapChangeValue.newBuilder().accountValue(account).build();

        MapUpdateChange mapUpdateChange = new MapUpdateChange(mapKey, mapValue, false);

        StateChange updateStateChange = StateChange.newBuilder()
                .stateId(TEST_STATE_ID)
                .mapUpdate(mapUpdateChange)
                .build();

        StateChanges updateStateChanges = new StateChanges(new Timestamp(1000L, 0), List.of(updateStateChange));

        long sizeBeforeUpdate = virtualMap.size();
        StateChangeParser.applyStateChanges(virtualMap, StateChanges.PROTOBUF.toBytes(updateStateChanges));
        assertEquals(sizeBeforeUpdate + 1, virtualMap.size(), "Map should contain one additional entry after update");

        // Now delete the entry
        MapDeleteChange mapDeleteChange = new MapDeleteChange(mapKey);

        StateChange deleteStateChange = StateChange.newBuilder()
                .stateId(TEST_STATE_ID)
                .mapDelete(mapDeleteChange)
                .build();

        StateChanges deleteStateChanges = new StateChanges(new Timestamp(1001L, 0), List.of(deleteStateChange));

        StateChangeParser.applyStateChanges(virtualMap, StateChanges.PROTOBUF.toBytes(deleteStateChanges));
        assertEquals(sizeBeforeUpdate, virtualMap.size(), "Map should return to original size after delete");
    }

    @Test
    @DisplayName("Test map delete with proto bytes key")
    void testMapDeleteWithProtoBytesKey() {
        // First, add an entry
        Bytes keyBytes = Bytes.wrap(new byte[] {1, 2, 3});

        MapChangeKey mapKey = MapChangeKey.newBuilder().protoBytesKey(keyBytes).build();

        MapChangeValue mapValue =
                MapChangeValue.newBuilder().protoStringValue("test").build();

        MapUpdateChange mapUpdateChange = new MapUpdateChange(mapKey, mapValue, false);

        StateChange updateStateChange = StateChange.newBuilder()
                .stateId(TEST_STATE_ID)
                .mapUpdate(mapUpdateChange)
                .build();

        StateChanges updateStateChanges = new StateChanges(new Timestamp(1000L, 0), List.of(updateStateChange));

        long sizeBeforeUpdate = virtualMap.size();
        StateChangeParser.applyStateChanges(virtualMap, StateChanges.PROTOBUF.toBytes(updateStateChanges));
        assertEquals(sizeBeforeUpdate + 1, virtualMap.size(), "Map should contain one additional entry after update");

        // Now delete
        MapDeleteChange mapDeleteChange = new MapDeleteChange(mapKey);

        StateChange deleteStateChange = StateChange.newBuilder()
                .stateId(TEST_STATE_ID)
                .mapDelete(mapDeleteChange)
                .build();

        StateChanges deleteStateChanges = new StateChanges(new Timestamp(1001L, 0), List.of(deleteStateChange));

        StateChangeParser.applyStateChanges(virtualMap, StateChanges.PROTOBUF.toBytes(deleteStateChanges));
        assertEquals(sizeBeforeUpdate, virtualMap.size(), "Map should return to original size after delete");
    }

    // ========================================
    // Queue Push Tests
    // ========================================

    @Test
    @DisplayName("Test queue push adds element to new queue")
    void testQueuePushToNewQueue() {
        Bytes testData = Bytes.wrap(new byte[] {1, 2, 3, 4, 5});
        QueuePushChange queuePush =
                QueuePushChange.newBuilder().protoBytesElement(testData).build();

        StateChange stateChange = StateChange.newBuilder()
                .stateId(QUEUE_STATE_ID)
                .queuePush(queuePush)
                .build();

        StateChanges stateChanges = new StateChanges(new Timestamp(1000L, 0), List.of(stateChange));

        Bytes stateChangesBytes = StateChanges.PROTOBUF.toBytes(stateChanges);

        long sizeBefore = virtualMap.size();
        StateChangeParser.applyStateChanges(virtualMap, stateChangesBytes);

        // Verify queue state and element were stored
        // Should have queue state + queue element = 2 additional entries
        assertEquals(sizeBefore + 2, virtualMap.size(), "Should have queue state and one element added");
    }

    @Test
    @DisplayName("Test queue push with string element")
    void testQueuePushWithStringElement() {
        QueuePushChange queuePush = QueuePushChange.newBuilder()
                .protoStringElement("test string element")
                .build();

        StateChange stateChange = StateChange.newBuilder()
                .stateId(QUEUE_STATE_ID)
                .queuePush(queuePush)
                .build();

        StateChanges stateChanges = new StateChanges(new Timestamp(1000L, 0), List.of(stateChange));

        Bytes stateChangesBytes = StateChanges.PROTOBUF.toBytes(stateChanges);

        long sizeBefore = virtualMap.size();
        StateChangeParser.applyStateChanges(virtualMap, stateChangesBytes);

        assertEquals(sizeBefore + 2, virtualMap.size(), "Should have queue state and one element added");
    }

    @Test
    @DisplayName("Test multiple queue pushes increment tail")
    void testMultipleQueuePushes() {
        // Push the first element
        QueuePushChange queuePush1 = QueuePushChange.newBuilder()
                .protoBytesElement(Bytes.wrap(new byte[] {1}))
                .build();

        StateChange stateChange1 = StateChange.newBuilder()
                .stateId(QUEUE_STATE_ID)
                .queuePush(queuePush1)
                .build();

        StateChanges stateChanges1 = new StateChanges(new Timestamp(1000L, 0), List.of(stateChange1));

        long sizeBefore = virtualMap.size();
        StateChangeParser.applyStateChanges(virtualMap, StateChanges.PROTOBUF.toBytes(stateChanges1));
        assertEquals(sizeBefore + 2, virtualMap.size(), "Should have queue state + 1 element");

        // Push the second element
        QueuePushChange queuePush2 = QueuePushChange.newBuilder()
                .protoBytesElement(Bytes.wrap(new byte[] {2}))
                .build();

        StateChange stateChange2 = StateChange.newBuilder()
                .stateId(QUEUE_STATE_ID)
                .queuePush(queuePush2)
                .build();

        StateChanges stateChanges2 = new StateChanges(new Timestamp(1001L, 0), List.of(stateChange2));

        StateChangeParser.applyStateChanges(virtualMap, StateChanges.PROTOBUF.toBytes(stateChanges2));
        assertEquals(sizeBefore + 3, virtualMap.size(), "Should have queue state + 2 elements");

        // Push the third element
        QueuePushChange queuePush3 = QueuePushChange.newBuilder()
                .protoBytesElement(Bytes.wrap(new byte[] {3}))
                .build();

        StateChange stateChange3 = StateChange.newBuilder()
                .stateId(QUEUE_STATE_ID)
                .queuePush(queuePush3)
                .build();

        StateChanges stateChanges3 = new StateChanges(new Timestamp(1002L, 0), List.of(stateChange3));

        StateChangeParser.applyStateChanges(virtualMap, StateChanges.PROTOBUF.toBytes(stateChanges3));
        assertEquals(sizeBefore + 4, virtualMap.size(), "Should have queue state + 3 elements");
    }

    // ========================================
    // Queue Pop Tests
    // ========================================

    @Test
    @DisplayName("Test queue pop removes head element")
    void testQueuePopRemovesHead() throws Exception {
        // First, set up a queue with elements
        Bytes queueStateKey = StateUtils.getStateKeyForSingleton(QUEUE_STATE_ID);
        QueueState initialQueueState = new QueueState(1, 3); // head=1, tail=3 (2 elements)
        virtualMap.putBytes(queueStateKey, QueueState.PROTOBUF.toBytes(initialQueueState));

        // Add queue elements at positions 1 and 2
        Bytes element1Key = com.swirlds.state.merkle.StateKeyUtils.queueKey(QUEUE_STATE_ID, 1);
        Bytes element2Key = com.swirlds.state.merkle.StateKeyUtils.queueKey(QUEUE_STATE_ID, 2);
        virtualMap.putBytes(element1Key, Bytes.wrap(new byte[] {10}));
        virtualMap.putBytes(element2Key, Bytes.wrap(new byte[] {20}));

        long sizeAfterSetup = virtualMap.size();

        // Pop an element
        QueuePopChange queuePop = new QueuePopChange();

        StateChange stateChange = StateChange.newBuilder()
                .stateId(QUEUE_STATE_ID)
                .queuePop(queuePop)
                .build();

        StateChanges stateChanges = new StateChanges(new Timestamp(1000L, 0), List.of(stateChange));

        StateChangeParser.applyStateChanges(virtualMap, StateChanges.PROTOBUF.toBytes(stateChanges));

        // Element at the head should be removed, queue state should be updated
        assertEquals(sizeAfterSetup - 1, virtualMap.size(), "Should have one less element after pop");
        assertNull(virtualMap.getBytes(element1Key), "Head element should be removed");
        assertNotNull(virtualMap.getBytes(element2Key), "Second element should still exist");

        // Verify the queue state was updated
        Bytes newQueueStateBytes = virtualMap.getBytes(queueStateKey);
        QueueState newQueueState = QueueState.PROTOBUF.parse(newQueueStateBytes);
        assertEquals(2, newQueueState.head(), "Head should be incremented to 2");
        assertEquals(3, newQueueState.tail(), "Tail should remain at 3");
    }

    @Test
    @DisplayName("Test multiple queue pops")
    void testMultipleQueuePops() throws Exception {
        // Set up a queue with 3 elements
        Bytes queueStateKey = StateUtils.getStateKeyForSingleton(QUEUE_STATE_ID);
        QueueState initialQueueState = new QueueState(1, 4); // head=1, tail=4 (3 elements)
        virtualMap.putBytes(queueStateKey, QueueState.PROTOBUF.toBytes(initialQueueState));

        virtualMap.putBytes(
                com.swirlds.state.merkle.StateKeyUtils.queueKey(QUEUE_STATE_ID, 1), Bytes.wrap(new byte[] {10}));
        virtualMap.putBytes(
                com.swirlds.state.merkle.StateKeyUtils.queueKey(QUEUE_STATE_ID, 2), Bytes.wrap(new byte[] {20}));
        virtualMap.putBytes(
                com.swirlds.state.merkle.StateKeyUtils.queueKey(QUEUE_STATE_ID, 3), Bytes.wrap(new byte[] {30}));

        long sizeAfterSetup = virtualMap.size();

        // Pop the first element
        QueuePopChange queuePop1 = new QueuePopChange();
        StateChange stateChange1 = StateChange.newBuilder()
                .stateId(QUEUE_STATE_ID)
                .queuePop(queuePop1)
                .build();

        StateChanges stateChanges1 = new StateChanges(new Timestamp(1000L, 0), List.of(stateChange1));

        StateChangeParser.applyStateChanges(virtualMap, StateChanges.PROTOBUF.toBytes(stateChanges1));
        assertEquals(sizeAfterSetup - 1, virtualMap.size(), "Should have one less element after first pop");

        // Pop the second element
        QueuePopChange queuePop2 = new QueuePopChange();
        StateChange stateChange2 = StateChange.newBuilder()
                .stateId(QUEUE_STATE_ID)
                .queuePop(queuePop2)
                .build();

        StateChanges stateChanges2 = new StateChanges(new Timestamp(1001L, 0), List.of(stateChange2));

        StateChangeParser.applyStateChanges(virtualMap, StateChanges.PROTOBUF.toBytes(stateChanges2));
        assertEquals(sizeAfterSetup - 2, virtualMap.size(), "Should have two less elements after second pop");

        // Verify the final queue state
        QueueState finalQueueState = QueueState.PROTOBUF.parse(virtualMap.getBytes(queueStateKey));
        assertEquals(3, finalQueueState.head(), "Head should be at 3");
        assertEquals(4, finalQueueState.tail(), "Tail should remain at 4");
    }

    // ========================================
    // Combined Operations Tests
    // ========================================

    @Test
    @DisplayName("Test mixed state changes in a single StateChanges")
    void testMixedStateChanges() {
        // Create a singleton update
        SingletonUpdateChange singletonChange =
                SingletonUpdateChange.newBuilder().entityNumberValue(12345L).build();

        StateChange singletonStateChange = StateChange.newBuilder()
                .stateId(1)
                .singletonUpdate(singletonChange)
                .build();

        // Create a map update
        AccountID accountId = AccountID.newBuilder()
                .shardNum(0L)
                .realmNum(0L)
                .accountNum(1001L)
                .build();
        Account account = Account.newBuilder().accountId(accountId).build();
        MapChangeKey mapKey = MapChangeKey.newBuilder().accountIdKey(accountId).build();
        MapChangeValue mapValue =
                MapChangeValue.newBuilder().accountValue(account).build();
        MapUpdateChange mapChange = new MapUpdateChange(mapKey, mapValue, false);

        StateChange mapStateChange =
                StateChange.newBuilder().stateId(2).mapUpdate(mapChange).build();

        // Create a queue push
        QueuePushChange queuePush = QueuePushChange.newBuilder()
                .protoBytesElement(Bytes.wrap(new byte[] {1, 2, 3}))
                .build();

        StateChange queueStateChange =
                StateChange.newBuilder().stateId(3).queuePush(queuePush).build();

        // Apply all changes together
        StateChanges stateChanges = new StateChanges(
                new Timestamp(1000L, 0), List.of(singletonStateChange, mapStateChange, queueStateChange));

        long sizeBefore = virtualMap.size();
        StateChangeParser.applyStateChanges(virtualMap, StateChanges.PROTOBUF.toBytes(stateChanges));

        // Should have: 1 singleton + 1 map entry + queue state + queue element = 4 additional entries
        assertEquals(sizeBefore + 4, virtualMap.size(), "Should have 4 additional entries");
    }

    @Test
    @DisplayName("Test empty state changes list")
    void testEmptyStateChangesList() {
        StateChanges stateChanges = new StateChanges(new Timestamp(1000L, 0), List.of());

        long sizeBefore = virtualMap.size();
        StateChangeParser.applyStateChanges(virtualMap, StateChanges.PROTOBUF.toBytes(stateChanges));

        assertEquals(sizeBefore, virtualMap.size(), "Map size should be unchanged for empty state changes");
    }

    @Test
    @DisplayName("Test state changes with only consensus timestamp")
    void testStateChangesWithOnlyTimestamp() {
        // A StateChanges with just a timestamp and no state changes should be fine
        StateChanges stateChanges = new StateChanges(new Timestamp(1000L, 0), List.of());

        Bytes stateChangesBytes = StateChanges.PROTOBUF.toBytes(stateChanges);

        long sizeBefore = virtualMap.size();
        // Should not throw
        StateChangeParser.applyStateChanges(virtualMap, stateChangesBytes);
        assertEquals(sizeBefore, virtualMap.size(), "Map size should be unchanged");
    }
}
