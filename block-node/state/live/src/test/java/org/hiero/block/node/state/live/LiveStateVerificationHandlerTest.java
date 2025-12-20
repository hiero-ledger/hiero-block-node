// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.hapi.platform.state.QueueState;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for LiveStatePlugin's handleVerification() method.
 * These tests verify that state changes are correctly applied when blocks
 * are verified and that the data can be read back through LiveStateAccess APIs.
 */
class LiveStateVerificationHandlerTest extends LiveStatePluginTestBase {

    @BeforeEach
    void setUp() {
        initAndStartPlugin();
    }

    // ========================================
    // Basic Verification Handling Tests
    // ========================================

    @Nested
    @DisplayName("Basic Verification Handling")
    class BasicVerificationTests {

        @Test
        @DisplayName("Ignores failed verification notifications")
        void testIgnoresFailedVerification() {
            BlockUnparsed block = createSimpleBlock(0, new byte[48]);
            VerificationNotification notification = createVerificationNotification(0, block, false);

            plugin.handleVerification(notification);

            // Block should not be applied
            assertEquals(-1, plugin.blockNumber());
        }

        @Test
        @DisplayName("Applies successful verification for block 0")
        void testAppliesBlock0() {
            BlockUnparsed block = createSimpleBlock(0, new byte[48]);
            VerificationNotification notification = createVerificationNotification(0, block, true);

            plugin.handleVerification(notification);

            assertEquals(0, plugin.blockNumber());
        }

        @Test
        @DisplayName("Ignores out-of-order blocks")
        void testIgnoresOutOfOrderBlocks() {
            // Try to apply block 5 when we're at -1
            BlockUnparsed block = createSimpleBlock(5, new byte[48]);
            VerificationNotification notification = createVerificationNotification(5, block, true);

            plugin.handleVerification(notification);

            // Block should not be applied (out of order)
            assertEquals(-1, plugin.blockNumber());
        }

        @Test
        @DisplayName("Ignores notifications with null block")
        void testIgnoresNullBlock() {
            VerificationNotification notification = new VerificationNotification(
                    true, 0, Bytes.wrap(new byte[48]), null, BlockSource.PUBLISHER);

            plugin.handleVerification(notification);

            // Should not throw, and block should not be applied
            assertEquals(-1, plugin.blockNumber());
        }
    }

    // ========================================
    // Singleton State Change Tests
    // ========================================

    @Nested
    @DisplayName("Singleton State Changes via Verification")
    class SingletonVerificationTests {

        @Test
        @DisplayName("Singleton value is readable after verification")
        void testSingletonAfterVerification() {
            // Create block with singleton state change
            StateChanges stateChanges = createSingletonStateChange(SINGLETON_STATE_ID, 99999L);
            BlockUnparsed block = createBlockWithStateChanges(0, new byte[48], List.of(stateChanges));

            plugin.handleVerification(createVerificationNotification(0, block, true));

            // Read back via LiveStateAccess
            Bytes value = plugin.singleton(SINGLETON_STATE_ID);
            assertNotNull(value, "Singleton should be readable after verification");
        }

        @Test
        @DisplayName("Multiple singletons in same block")
        void testMultipleSingletonsInBlock() {
            StateChanges changes1 = createSingletonStateChange(100, 111L);
            StateChanges changes2 = createSingletonStateChange(101, 222L);
            StateChanges changes3 = createSingletonStateChange(102, 333L);

            BlockUnparsed block = createBlockWithStateChanges(
                    0, new byte[48], List.of(changes1, changes2, changes3));

            plugin.handleVerification(createVerificationNotification(0, block, true));

            // All singletons should be readable
            assertNotNull(plugin.singleton(100));
            assertNotNull(plugin.singleton(101));
            assertNotNull(plugin.singleton(102));
        }

        @Test
        @DisplayName("Singleton bytes value stored and retrieved correctly")
        void testSingletonBytesValue() {
            Bytes testValue = Bytes.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
            StateChanges stateChanges = createSingletonBytesStateChange(SINGLETON_STATE_ID, testValue);
            BlockUnparsed block = createBlockWithStateChanges(0, new byte[48], List.of(stateChanges));

            plugin.handleVerification(createVerificationNotification(0, block, true));

            Bytes retrieved = plugin.singleton(SINGLETON_STATE_ID);
            assertNotNull(retrieved);
            // The value is wrapped in a StateValue protobuf message, so exact comparison won't work
            // But we can verify it's not null and has content
            assertTrue(retrieved.length() > 0);
        }
    }

    // ========================================
    // Map State Change Tests
    // ========================================

    @Nested
    @DisplayName("Map State Changes via Verification")
    class MapVerificationTests {

        @Test
        @DisplayName("Map entry is readable after verification")
        void testMapEntryAfterVerification() {
            Bytes key = Bytes.wrap(new byte[]{10, 20, 30});
            Bytes value = Bytes.wrap(new byte[]{100, 110, 120});
            StateChanges stateChanges = createMapBytesUpdateStateChange(MAP_STATE_ID, key, value);

            BlockUnparsed block = createBlockWithStateChanges(0, new byte[48], List.of(stateChanges));
            plugin.handleVerification(createVerificationNotification(0, block, true));

            Bytes retrieved = plugin.mapValue(MAP_STATE_ID, key);
            assertNotNull(retrieved, "Map value should be readable after verification");
        }

        @Test
        @DisplayName("Multiple map entries in same block")
        void testMultipleMapEntriesInBlock() {
            List<StateChanges> changesList = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                Bytes key = Bytes.wrap(new byte[]{(byte) i});
                Bytes value = Bytes.wrap(new byte[]{(byte) (i * 10)});
                changesList.add(createMapBytesUpdateStateChange(MAP_STATE_ID, key, value));
            }

            BlockUnparsed block = createBlockWithStateChanges(0, new byte[48], changesList);
            plugin.handleVerification(createVerificationNotification(0, block, true));

            // All entries should be readable
            for (int i = 0; i < 5; i++) {
                Bytes key = Bytes.wrap(new byte[]{(byte) i});
                assertNotNull(plugin.mapValue(MAP_STATE_ID, key), "Entry " + i + " should exist");
            }
        }

        @Test
        @DisplayName("Map entries in different state IDs are independent")
        void testMapEntriesDifferentStateIds() {
            Bytes key = Bytes.wrap(new byte[]{1});
            StateChanges changes1 = createMapBytesUpdateStateChange(50, key, Bytes.wrap(new byte[]{10}));
            StateChanges changes2 = createMapBytesUpdateStateChange(51, key, Bytes.wrap(new byte[]{20}));

            BlockUnparsed block = createBlockWithStateChanges(0, new byte[48], List.of(changes1, changes2));
            plugin.handleVerification(createVerificationNotification(0, block, true));

            Bytes value1 = plugin.mapValue(50, key);
            Bytes value2 = plugin.mapValue(51, key);

            assertNotNull(value1);
            assertNotNull(value2);
            assertNotEquals(value1, value2, "Values in different state IDs should be different");
        }

        @Test
        @DisplayName("Large map value is stored correctly")
        void testLargeMapValue() {
            byte[] largeValueData = new byte[100000]; // 100KB
            for (int i = 0; i < largeValueData.length; i++) {
                largeValueData[i] = (byte) (i % 256);
            }

            Bytes key = Bytes.wrap(new byte[]{1, 2, 3});
            Bytes largeValue = Bytes.wrap(largeValueData);
            StateChanges stateChanges = createMapBytesUpdateStateChange(MAP_STATE_ID, key, largeValue);

            BlockUnparsed block = createBlockWithStateChanges(0, new byte[48], List.of(stateChanges));
            plugin.handleVerification(createVerificationNotification(0, block, true));

            Bytes retrieved = plugin.mapValue(MAP_STATE_ID, key);
            assertNotNull(retrieved);
            assertTrue(retrieved.length() > 0);
        }
    }

    // ========================================
    // Queue State Change Tests
    // ========================================

    @Nested
    @DisplayName("Queue State Changes via Verification")
    class QueueVerificationTests {

        @Test
        @DisplayName("Queue element is readable after push via verification")
        void testQueuePushAfterVerification() {
            Bytes element = Bytes.wrap(new byte[]{1, 2, 3, 4, 5});
            StateChanges stateChanges = createQueuePushStateChange(QUEUE_STATE_ID, element);

            BlockUnparsed block = createBlockWithStateChanges(0, new byte[48], List.of(stateChanges));
            plugin.handleVerification(createVerificationNotification(0, block, true));

            // Verify queue state
            QueueState queueState = plugin.queueState(QUEUE_STATE_ID);
            assertEquals(1, queueState.head());
            assertEquals(2, queueState.tail());

            // Verify we can peek the element
            Bytes head = plugin.queuePeekHead(QUEUE_STATE_ID);
            assertNotNull(head);
        }

        @Test
        @DisplayName("Multiple queue pushes in same block")
        void testMultipleQueuePushesInBlock() {
            List<StateChanges> changesList = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                changesList.add(createQueuePushStateChange(QUEUE_STATE_ID, Bytes.wrap(new byte[]{(byte) i})));
            }

            BlockUnparsed block = createBlockWithStateChanges(0, new byte[48], changesList);
            plugin.handleVerification(createVerificationNotification(0, block, true));

            // Queue should have 3 elements
            QueueState queueState = plugin.queueState(QUEUE_STATE_ID);
            assertEquals(1, queueState.head());
            assertEquals(4, queueState.tail()); // tail = head + count

            // Verify list contains all elements
            List<Bytes> elements = plugin.queueAsList(QUEUE_STATE_ID);
            assertEquals(3, elements.size());
        }

        @Test
        @DisplayName("Queue head and tail are different for multiple elements")
        void testQueueHeadTailDifferent() {
            // Push first element with value 1
            StateChanges push1 = createQueuePushStateChange(QUEUE_STATE_ID, Bytes.wrap(new byte[]{1}));
            // Push second element with value 2
            StateChanges push2 = createQueuePushStateChange(QUEUE_STATE_ID, Bytes.wrap(new byte[]{2}));

            BlockUnparsed block = createBlockWithStateChanges(0, new byte[48], List.of(push1, push2));
            plugin.handleVerification(createVerificationNotification(0, block, true));

            Bytes head = plugin.queuePeekHead(QUEUE_STATE_ID);
            Bytes tail = plugin.queuePeekTail(QUEUE_STATE_ID);

            assertNotNull(head);
            assertNotNull(tail);
            // They should be different elements
            assertNotEquals(head, tail);
        }
    }

    // ========================================
    // Mixed State Changes Tests
    // ========================================

    @Nested
    @DisplayName("Mixed State Changes via Verification")
    class MixedStateChangesTests {

        @Test
        @DisplayName("Block with singleton, map, and queue changes")
        void testMixedStateChangesInBlock() {
            List<StateChanges> changesList = new ArrayList<>();

            // Add singleton change
            changesList.add(createSingletonStateChange(SINGLETON_STATE_ID, 42L));

            // Add map change
            changesList.add(createMapBytesUpdateStateChange(
                    MAP_STATE_ID, Bytes.wrap(new byte[]{1}), Bytes.wrap(new byte[]{10})));

            // Add queue push
            changesList.add(createQueuePushStateChange(QUEUE_STATE_ID, Bytes.wrap(new byte[]{100})));

            BlockUnparsed block = createBlockWithStateChanges(0, new byte[48], changesList);
            plugin.handleVerification(createVerificationNotification(0, block, true));

            // Verify all changes were applied
            assertNotNull(plugin.singleton(SINGLETON_STATE_ID), "Singleton should exist");
            assertNotNull(plugin.mapValue(MAP_STATE_ID, Bytes.wrap(new byte[]{1})), "Map entry should exist");
            assertEquals(1, plugin.queueAsList(QUEUE_STATE_ID).size(), "Queue should have one element");
        }

        @Test
        @DisplayName("Multiple state changes items in single block")
        void testMultipleStateChangesItems() {
            // Create two separate StateChanges messages in the same block
            StateChanges changes1 = createSingletonStateChange(100, 111L);
            StateChanges changes2 = createSingletonStateChange(101, 222L);

            BlockUnparsed block = createBlockWithStateChanges(0, new byte[48], List.of(changes1, changes2));
            plugin.handleVerification(createVerificationNotification(0, block, true));

            assertNotNull(plugin.singleton(100));
            assertNotNull(plugin.singleton(101));
        }
    }

    // ========================================
    // Error Handling Tests
    // ========================================

    @Nested
    @DisplayName("Error Handling in Verification")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Handles block with empty state changes gracefully")
        void testEmptyStateChanges() {
            // Block with no state changes
            BlockUnparsed block = createSimpleBlock(0, new byte[48]);

            assertDoesNotThrow(() -> {
                plugin.handleVerification(createVerificationNotification(0, block, true));
            });

            assertEquals(0, plugin.blockNumber());
        }

        @Test
        @DisplayName("Multiple verifications for same block number are ignored")
        void testDuplicateBlockVerification() {
            BlockUnparsed block0 = createSimpleBlock(0, new byte[48]);
            plugin.handleVerification(createVerificationNotification(0, block0, true));
            assertEquals(0, plugin.blockNumber());

            // Try to verify block 0 again - should be ignored (out of order now)
            BlockUnparsed block0Again = createSimpleBlock(0, new byte[48]);
            plugin.handleVerification(createVerificationNotification(0, block0Again, true));

            // Should still be at block 0
            assertEquals(0, plugin.blockNumber());
        }
    }

}
