// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.hedera.pbj.runtime.ParseException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.assertj.core.api.ObjectAssert;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlock;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder.StateProof;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder.WRAPS;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder.WRB;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestWRBBlock;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureInfo;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Plugin-level integration test for [VerificationServicePlugin].
@DisplayName("VerificationServicePlugin Tests")
class VerificationServicePluginTest {
    private static final WRAPS[] consecutiveWRAPSBlocks =
            new WRAPS[] {WRAPS.BLOCK_0, WRAPS.BLOCK_1, WRAPS.BLOCK_2, WRAPS.BLOCK_3, WRAPS.BLOCK_4};
    private static final WRB[] consecutiveWRBBlocks = new WRB[] {
        WRB.SOLO_4N_BLOCK_0, WRB.SOLO_4N_BLOCK_1, WRB.SOLO_4N_BLOCK_2, WRB.SOLO_4N_BLOCK_3, WRB.SOLO_4N_BLOCK_4
    };
    private static final StateProof[] consecutiveStateProofBlocks = new StateProof[] {
        StateProof.BLOCK_0, StateProof.BLOCK_1, StateProof.BLOCK_2, StateProof.BLOCK_3, StateProof.BLOCK_4
    };

    /// Tests for WRAPS verification
    @Nested
    @DisplayName("WRAPS Verification Tests")
    class WRAPSVerificationTests
            extends PluginTestBase<VerificationServicePlugin, ExecutorService, ScheduledExecutorService> {
        WRAPSVerificationTests() {
            super(
                    Executors.newVirtualThreadPerTaskExecutor(),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(new VerificationServicePlugin(), new SimpleInMemoryHistoricalBlockFacility());
        }

        /// This test aims to assert that when the next in line WRAPS block is
        /// received, is valid, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully. Uses Live RB.
        @Test
        @DisplayName("Successful WRAPS Verification - Live RB")
        void testSuccessfulWRAPSVerificationLiveRB() throws IOException, ParseException {
            final ResourceTestBlock block0 = ResourceTestBlockBuilder.load(WRAPS.BLOCK_0);
            // We push the block to the live items RB
            plugin.handleBlockItemsReceived(block0.asBlockItems());
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(true, VerificationNotification::success)
                    .returns(null, VerificationNotification::failureInfo)
                    .returns(block0.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(block0.blockUnparsed(), VerificationNotification::block)
                    .returns(block0.blockRootHash(), VerificationNotification::blockHash);
        }

        /// This test aims to assert that when the next in line WRAPS block is
        /// received, is valid, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully. Uses Backfill.
        @Test
        @DisplayName("Successful WRAPS Verification - Backfill")
        void testSuccessfulWRAPSVerificationBackfill() throws IOException, ParseException {
            final ResourceTestBlock block0 = ResourceTestBlockBuilder.load(WRAPS.BLOCK_0);
            // We push the block as a backfilled notification
            plugin.handleBackfilled(block0.asBackfilledNotification());
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(true, VerificationNotification::success)
                    .returns(null, VerificationNotification::failureInfo)
                    .returns(block0.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.BACKFILL, VerificationNotification::source)
                    .returns(block0.blockUnparsed(), VerificationNotification::block)
                    .returns(block0.blockRootHash(), VerificationNotification::blockHash);
        }

        /// This test aims to assert that when the next in line WRAPS block is
        /// received, is valid, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully. Uses Live RB.
        /// Verifies multiple consecutive blocks.
        @Test
        @DisplayName("Successful consecutive WRAPS Verification - Live RB")
        void testSuccessfulConsecutiveWRAPSVerificationLiveRB() throws IOException, ParseException {
            final List<ResourceTestBlock> loadedBlocks = ResourceTestBlockBuilder.loadMultiple(consecutiveWRAPSBlocks);
            // Load block 0 first which will update tss data, make sure it passes before continuing to ensure
            // no flakiness due to missing tss data. Other tests can pre-load tss data and it will not be
            // relevant to wait.
            plugin.handleBlockItemsReceived(loadedBlocks.getFirst().asBlockItems());
            blockMessaging.getSentVerificationNotifications(1);
            for (final ResourceTestBlock block : loadedBlocks.subList(1, loadedBlocks.size())) {
                // We push the block to the live items RB
                plugin.handleBlockItemsReceived(block.asBlockItems());
            }
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(loadedBlocks.size());
            assertThat(notifications).hasSize(loadedBlocks.size());
            for (int i = 0; i < notifications.size(); i++) {
                final ResourceTestBlock block = loadedBlocks.get(i);
                final VerificationNotification notification = notifications.get(i);
                assertThat(notification)
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(block.number(), VerificationNotification::blockNumber)
                        .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                        .returns(block.blockUnparsed(), VerificationNotification::block)
                        .returns(block.blockRootHash(), VerificationNotification::blockHash);
            }
        }

        /// This test aims to assert that when the next in line WRAPS block is
        /// received, is valid, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully. Uses Backfill.
        /// Verifies multiple consecutive blocks.
        @Test
        @DisplayName("Successful consecutive WRAPS Verification - Backfill")
        void testSuccessfulConsecutiveWRAPSVerificationBackfill() throws IOException, ParseException {
            final List<ResourceTestBlock> loadedBlocks = ResourceTestBlockBuilder.loadMultiple(consecutiveWRAPSBlocks);
            // Load block 0 first which will update tss data, make sure it passes before continuing to ensure
            // no flakiness due to missing tss data. Other tests can pre-load tss data and it will not be
            // relevant to wait.
            plugin.handleBackfilled(loadedBlocks.getFirst().asBackfilledNotification());
            blockMessaging.getSentVerificationNotifications(1);
            for (final ResourceTestBlock block : loadedBlocks.subList(1, loadedBlocks.size())) {
                // We push the block to the live items RB
                plugin.handleBackfilled(block.asBackfilledNotification());
            }
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(loadedBlocks.size());
            assertThat(notifications).hasSize(loadedBlocks.size());
            for (int i = 0; i < notifications.size(); i++) {
                final ResourceTestBlock block = loadedBlocks.get(i);
                final VerificationNotification notification = notifications.get(i);
                assertThat(notification)
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(block.number(), VerificationNotification::blockNumber)
                        .returns(BlockSource.BACKFILL, VerificationNotification::source)
                        .returns(block.blockUnparsed(), VerificationNotification::block)
                        .returns(block.blockRootHash(), VerificationNotification::blockHash);
            }
        }

        /// This test exercises the VFE extraction path in SessionResultHandler.
        /// Block 0 initializes TSS data; the tampered block 1 passes hashing but
        /// fails TSS signature verification, producing a failure notification.
        @Test
        @DisplayName("Failed WRAPS Verification - tampered block covers VFE extraction path")
        void testTamperedWRAPSBlockTriggersVfePath() throws IOException, ParseException {
            final ResourceTestBlock block0 = ResourceTestBlockBuilder.load(WRAPS.BLOCK_0);
            plugin.handleBlockItemsReceived(block0.asBlockItems());
            // Wait for block 0 to complete so TSS data is initialized before block 1 is submitted
            blockMessaging.getSentVerificationNotifications(1);

            final ResourceTestBlock block1 = ResourceTestBlockBuilder.load(WRAPS.BLOCK_1);
            final List<BlockItemUnparsed> tamperedItems =
                    new ArrayList<>(block1.blockUnparsed().blockItems());
            tamperedItems.remove(1); // remove a non-mandatory item to change the block hash
            plugin.handleBlockItemsReceived(new BlockItems(tamperedItems, block1.number(), true, true));

            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(2);
            assertThat(notifications.get(1)).returns(false, VerificationNotification::success);
        }

        /// This test aims to assert that when the next in line WRAPS block is
        /// received, is valid, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully. Uses Multiple Sources.
        /// Verifies multiple consecutive blocks.
        @Test
        @DisplayName("Successful consecutive WRAPS Verification - Multi Source")
        void testSuccessfulConsecutiveWRAPSVerificationMultiSource() throws IOException, ParseException {
            final List<ResourceTestBlock> loadedBlocks = ResourceTestBlockBuilder.loadMultiple(consecutiveWRAPSBlocks);
            // Load block 0 first which will update tss data, make sure it passes before continuing to ensure
            // no flakiness due to missing tss data. Other tests can pre-load tss data and it will not be
            // relevant to wait.
            final Map<Long, BlockSource> blockSources = new HashMap<>();
            plugin.handleBackfilled(loadedBlocks.getFirst().asBackfilledNotification());
            blockSources.put(loadedBlocks.getFirst().number(), BlockSource.BACKFILL);
            blockMessaging.getSentVerificationNotifications(1);
            // Now push the blocks
            for (final ResourceTestBlock block : loadedBlocks.subList(1, loadedBlocks.size())) {
                // We push the block either to live or backfill
                if (block.number() % 2 == 0) {
                    blockSources.put(block.number(), BlockSource.BACKFILL);
                    plugin.handleBackfilled(block.asBackfilledNotification());
                } else {
                    blockSources.put(block.number(), BlockSource.PUBLISHER);
                    final BlockItems blockItems = block.asBlockItems();
                    plugin.handleBlockItemsReceived(blockItems);
                }
            }
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(loadedBlocks.size());
            assertThat(notifications).hasSize(loadedBlocks.size());
            for (int i = 0; i < notifications.size(); i++) {
                final ResourceTestBlock block = loadedBlocks.get(i);
                final VerificationNotification notification = notifications.get(i);
                final ObjectAssert<VerificationNotification> assertion = assertThat(notification);
                assertion
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(block.number(), VerificationNotification::blockNumber)
                        .returns(block.blockUnparsed(), VerificationNotification::block)
                        .returns(block.blockRootHash(), VerificationNotification::blockHash);
                if (blockSources.get(block.number()) == BlockSource.PUBLISHER) {
                    assertion.returns(BlockSource.PUBLISHER, VerificationNotification::source);
                } else if (blockSources.get(block.number()) == BlockSource.BACKFILL) {
                    assertion.returns(BlockSource.BACKFILL, VerificationNotification::source);
                } else {
                    fail("unrecognized or unsupported source %s".formatted(blockSources.get(block.number())));
                }
            }
        }
    }

    /// Tests for State Proof verification
    @Nested
    @DisplayName("StateProof Verification Tests")
    class StateProofVerificationTests
            extends PluginTestBase<VerificationServicePlugin, ExecutorService, ScheduledExecutorService> {
        StateProofVerificationTests() {
            super(
                    Executors.newVirtualThreadPerTaskExecutor(),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(new VerificationServicePlugin(), new SimpleInMemoryHistoricalBlockFacility());
        }

        /// This test aims to assert that when the next in line StateProof block is
        /// received, is valid, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully. Uses Live RB.
        @Test
        @DisplayName("Successful StateProof Verification - Live RB")
        void testSuccessfulStateProofVerificationLiveRB() throws IOException, ParseException {
            // Block 0 is direct tss proof, initialize tss
            // We push the block to the live items RB and initialize tss
            plugin.handleBlockItemsReceived(
                    ResourceTestBlockBuilder.load(StateProof.BLOCK_0).asBlockItems());
            // We will receive notification, then clear it so we can assert below
            blockMessaging.getSentVerificationNotifications(1).clear();
            // Block 1 is indirect proof
            final ResourceTestBlock block1 = ResourceTestBlockBuilder.load(StateProof.BLOCK_1);
            plugin.handleBlockItemsReceived(block1.asBlockItems());
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(true, VerificationNotification::success)
                    .returns(null, VerificationNotification::failureInfo)
                    .returns(block1.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(block1.blockUnparsed(), VerificationNotification::block)
                    .returns(block1.blockRootHash(), VerificationNotification::blockHash);
        }

        /// This test aims to assert that when the next in line StateProof block is
        /// received, is valid, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully. Uses Backfill.
        @Test
        @DisplayName("Successful StateProof Verification - Backfill")
        void testSuccessfulStateProofVerificationBackfill() throws IOException, ParseException {
            // Block 0 is direct tss proof, initialize tss
            // We push the block as a backfilled notification and initialize tss
            plugin.handleBackfilled(
                    ResourceTestBlockBuilder.load(StateProof.BLOCK_0).asBackfilledNotification());
            // We will receive notification, then clear it so we can assert below
            blockMessaging.getSentVerificationNotifications(1).clear();
            // Block 1 is indirect proof
            final ResourceTestBlock block1 = ResourceTestBlockBuilder.load(StateProof.BLOCK_1);
            plugin.handleBackfilled(block1.asBackfilledNotification());
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(true, VerificationNotification::success)
                    .returns(null, VerificationNotification::failureInfo)
                    .returns(block1.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.BACKFILL, VerificationNotification::source)
                    .returns(block1.blockUnparsed(), VerificationNotification::block)
                    .returns(block1.blockRootHash(), VerificationNotification::blockHash);
        }

        /// This test aims to assert that when the next in line StateProof block is
        /// received, is valid, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully. Uses Live RB.
        /// Verifies multiple consecutive blocks.
        @Test
        @DisplayName("Successful consecutive StateProof Verification - Live RB")
        void testSuccessfulConsecutiveStateProofVerificationLiveRB() throws IOException, ParseException {
            final List<ResourceTestBlock> loadedBlocks =
                    ResourceTestBlockBuilder.loadMultiple(consecutiveStateProofBlocks);
            // Load block 0 first which will update tss data, make sure it passes before continuing to ensure
            // no flakiness due to missing tss data. Other tests can pre-load tss data and it will not be
            // relevant to wait.
            plugin.handleBlockItemsReceived(loadedBlocks.getFirst().asBlockItems());
            blockMessaging.getSentVerificationNotifications(1);
            for (final ResourceTestBlock block : loadedBlocks.subList(1, loadedBlocks.size())) {
                // We push the block to the live items RB
                plugin.handleBlockItemsReceived(block.asBlockItems());
            }
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(loadedBlocks.size());
            assertThat(notifications).hasSize(loadedBlocks.size());
            for (int i = 0; i < notifications.size(); i++) {
                final ResourceTestBlock block = loadedBlocks.get(i);
                final VerificationNotification notification = notifications.get(i);
                assertThat(notification)
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(block.number(), VerificationNotification::blockNumber)
                        .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                        .returns(block.blockUnparsed(), VerificationNotification::block)
                        .returns(block.blockRootHash(), VerificationNotification::blockHash);
            }
        }

        /// This test aims to assert that when the next in line StateProof block is
        /// received, is valid, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully. Uses Backfill.
        /// Verifies multiple consecutive blocks.
        @Test
        @DisplayName("Successful consecutive StateProof Verification - Backfill")
        void testSuccessfulConsecutiveStateProofVerificationBackfill() throws IOException, ParseException {
            final List<ResourceTestBlock> loadedBlocks =
                    ResourceTestBlockBuilder.loadMultiple(consecutiveStateProofBlocks);
            // Load block 0 first which will update tss data, make sure it passes before continuing to ensure
            // no flakiness due to missing tss data. Other tests can pre-load tss data and it will not be
            // relevant to wait.
            plugin.handleBackfilled(loadedBlocks.getFirst().asBackfilledNotification());
            blockMessaging.getSentVerificationNotifications(1);
            for (final ResourceTestBlock block : loadedBlocks.subList(1, loadedBlocks.size())) {
                // We push the block to the live items RB
                plugin.handleBackfilled(block.asBackfilledNotification());
            }
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(loadedBlocks.size());
            assertThat(notifications).hasSize(loadedBlocks.size());
            for (int i = 0; i < notifications.size(); i++) {
                final ResourceTestBlock block = loadedBlocks.get(i);
                final VerificationNotification notification = notifications.get(i);
                assertThat(notification)
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(block.number(), VerificationNotification::blockNumber)
                        .returns(BlockSource.BACKFILL, VerificationNotification::source)
                        .returns(block.blockUnparsed(), VerificationNotification::block)
                        .returns(block.blockRootHash(), VerificationNotification::blockHash);
            }
        }

        /// This test aims to assert that when the next in line StateProof block is
        /// received, is valid, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully. Uses Multiple Sources.
        /// Verifies multiple consecutive blocks.
        @Test
        @DisplayName("Successful consecutive StateProof Verification - Multi Source")
        void testSuccessfulConsecutiveStateProofVerificationMultiSource() throws IOException, ParseException {
            final List<ResourceTestBlock> loadedBlocks =
                    ResourceTestBlockBuilder.loadMultiple(consecutiveStateProofBlocks);
            // Load block 0 first which will update tss data, make sure it passes before continuing to ensure
            // no flakiness due to missing tss data. Other tests can pre-load tss data and it will not be
            // relevant to wait.
            final Map<Long, BlockSource> blockSources = new HashMap<>();
            plugin.handleBackfilled(loadedBlocks.getFirst().asBackfilledNotification());
            blockSources.put(loadedBlocks.getFirst().number(), BlockSource.BACKFILL);
            blockMessaging.getSentVerificationNotifications(1);
            // Now push the blocks
            for (final ResourceTestBlock block : loadedBlocks.subList(1, loadedBlocks.size())) {
                // We push the block either to live or backfill
                if (block.number() % 2 == 0) {
                    blockSources.put(block.number(), BlockSource.BACKFILL);
                    plugin.handleBackfilled(block.asBackfilledNotification());
                } else {
                    blockSources.put(block.number(), BlockSource.PUBLISHER);
                    final BlockItems blockItems = block.asBlockItems();
                    plugin.handleBlockItemsReceived(blockItems);
                }
            }
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(loadedBlocks.size());
            assertThat(notifications).hasSize(loadedBlocks.size());
            for (int i = 0; i < notifications.size(); i++) {
                final ResourceTestBlock block = loadedBlocks.get(i);
                final VerificationNotification notification = notifications.get(i);
                final ObjectAssert<VerificationNotification> assertion = assertThat(notification);
                assertion
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(block.number(), VerificationNotification::blockNumber)
                        .returns(block.blockUnparsed(), VerificationNotification::block)
                        .returns(block.blockRootHash(), VerificationNotification::blockHash);
                if (blockSources.get(block.number()) == BlockSource.PUBLISHER) {
                    assertion.returns(BlockSource.PUBLISHER, VerificationNotification::source);
                } else if (blockSources.get(block.number()) == BlockSource.BACKFILL) {
                    assertion.returns(BlockSource.BACKFILL, VerificationNotification::source);
                } else {
                    fail("unrecognized or unsupported source %s".formatted(blockSources.get(block.number())));
                }
            }
        }
    }

    /// Tests for WRB verification
    @Nested
    @DisplayName("WRB Verification Tests")
    class WRBVerificationTests
            extends PluginTestBase<VerificationServicePlugin, ExecutorService, ScheduledExecutorService> {
        WRBVerificationTests() {
            super(
                    Executors.newVirtualThreadPerTaskExecutor(),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(new VerificationServicePlugin(), new SimpleInMemoryHistoricalBlockFacility());
        }

        /// This test aims to assert that when the next in line WRB block is
        /// received, is valid, and we have a valid RSA roster initialized,
        /// the block will pass verification successfully. Uses Live RB.
        @Test
        @DisplayName("Successful WRB Verification - Live RB")
        void testSuccessfulWRBVerificationLiveRB() throws IOException, ParseException {
            final ResourceTestWRBBlock block0 = ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_0);
            // First, we update the node address book
            updateAddressBook(block0.nodeAddressBook());
            // Then, we push the block to the live items RB
            plugin.handleBlockItemsReceived(block0.asBlockItems());
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(true, VerificationNotification::success)
                    .returns(null, VerificationNotification::failureInfo)
                    .returns(block0.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(block0.blockUnparsed(), VerificationNotification::block)
                    .returns(block0.blockRootHash(), VerificationNotification::blockHash);
        }

        /// This test aims to assert that when the next in line WRB block is
        /// received, is valid, and we have a valid RSA roster initialized,
        /// the block will pass verification successfully. Uses Backfill.
        @Test
        @DisplayName("Successful WRB Verification - Backfill")
        void testSuccessfulWRBVerificationBackfill() throws IOException, ParseException {
            final ResourceTestWRBBlock block0 = ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_0);
            // First, we update the node address book
            updateAddressBook(block0.nodeAddressBook());
            // Then, we push the block as a backfilled notification
            plugin.handleBackfilled(block0.asBackfilledNotification());
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(true, VerificationNotification::success)
                    .returns(null, VerificationNotification::failureInfo)
                    .returns(block0.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.BACKFILL, VerificationNotification::source)
                    .returns(block0.blockUnparsed(), VerificationNotification::block)
                    .returns(block0.blockRootHash(), VerificationNotification::blockHash);
        }

        /// This test aims to assert that when the next in line WRB block is
        /// received, is valid, and we have a valid RSA roster initialized,
        /// the block will pass verification successfully. This test verifies
        /// multiple consecutive blocks. Uses Live RB.
        @Test
        @DisplayName("Successful consecutive WRB Verification - Live RB")
        void testSuccessfulConsecutiveWRBVerificationLiveRB() throws IOException, ParseException {
            // First, we load multiple consecutive WRB blocks
            final List<ResourceTestWRBBlock> loadedBlocks = ResourceTestBlockBuilder.loadMultiple(consecutiveWRBBlocks);
            for (final ResourceTestWRBBlock block : loadedBlocks) {
                // First, we update the node address book
                updateAddressBook(block.nodeAddressBook());
                // Then, we push them to the live items RB in order
                plugin.handleBlockItemsReceived(block.asBlockItems());
            }
            // Finally, await responses and assert success, we expect success in order
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(loadedBlocks.size());
            assertThat(notifications).hasSize(loadedBlocks.size());
            for (int i = 0; i < notifications.size(); i++) {
                final VerificationNotification notification = notifications.get(i);
                final ResourceTestWRBBlock block = loadedBlocks.get(i);
                assertThat(notification)
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(block.number(), VerificationNotification::blockNumber)
                        .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                        .returns(block.blockUnparsed(), VerificationNotification::block)
                        .returns(block.blockRootHash(), VerificationNotification::blockHash);
            }
        }

        /// This test aims to assert that when the next in line WRB block is
        /// received, is valid, and we have a valid RSA roster initialized,
        /// the block will pass verification successfully. This test verifies
        /// multiple consecutive blocks. Uses Backfill.
        @Test
        @DisplayName("Successful consecutive WRB Verification - Backfill")
        void testSuccessfulConsecutiveWRBVerificationBackfill() throws IOException, ParseException {
            // First, we load multiple consecutive WRB blocks
            final List<ResourceTestWRBBlock> loadedBlocks = ResourceTestBlockBuilder.loadMultiple(consecutiveWRBBlocks);
            for (final ResourceTestWRBBlock block : loadedBlocks) {
                // First, we update the node address book
                updateAddressBook(block.nodeAddressBook());
                // Then, we push them as backfilled notifications in order
                plugin.handleBackfilled(block.asBackfilledNotification());
            }
            // Finally, await responses and assert success, we expect success in order
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(loadedBlocks.size());
            assertThat(notifications).hasSize(loadedBlocks.size());
            for (int i = 0; i < notifications.size(); i++) {
                final VerificationNotification notification = notifications.get(i);
                final ResourceTestWRBBlock block = loadedBlocks.get(i);
                assertThat(notification)
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(block.number(), VerificationNotification::blockNumber)
                        .returns(BlockSource.BACKFILL, VerificationNotification::source)
                        .returns(block.blockUnparsed(), VerificationNotification::block)
                        .returns(block.blockRootHash(), VerificationNotification::blockHash);
            }
        }

        /// This test aims to assert that when the next in line WRB block is
        /// received, is valid, and we have a valid RSA roster initialized,
        /// the block will pass verification successfully. This test verifies
        /// multiple consecutive blocks. Uses multiple sources.
        @Test
        @DisplayName("Successful consecutive WRB Verification - Multi Source")
        void testSuccessfulConsecutiveWRBVerificationMultiSource() throws IOException, ParseException {
            // First, we load multiple consecutive WRB blocks
            final List<ResourceTestWRBBlock> loadedBlocks = ResourceTestBlockBuilder.loadMultiple(consecutiveWRBBlocks);
            final Map<Long, BlockSource> blockSources = new HashMap<>();
            for (final ResourceTestWRBBlock block : loadedBlocks) {
                // First, we update the node address book
                updateAddressBook(block.nodeAddressBook());
                // Then, we push them to the live items RB or as backfilled notifications in order
                if (block.number() % 2 == 0) {
                    blockSources.put(block.number(), BlockSource.BACKFILL);
                    plugin.handleBackfilled(block.asBackfilledNotification());
                } else {
                    blockSources.put(block.number(), BlockSource.PUBLISHER);
                    final BlockItems blockItems = block.asBlockItems();
                    plugin.handleBlockItemsReceived(blockItems);
                }
            }
            // Finally, await responses and assert success, we expect success in order
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(loadedBlocks.size());
            assertThat(notifications).hasSize(loadedBlocks.size());
            for (int i = 0; i < notifications.size(); i++) {
                final VerificationNotification notification = notifications.get(i);
                final ResourceTestWRBBlock block = loadedBlocks.get(i);
                final ObjectAssert<VerificationNotification> assertion = assertThat(notification);
                assertion
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(block.number(), VerificationNotification::blockNumber)
                        .returns(block.blockUnparsed(), VerificationNotification::block)
                        .returns(block.blockRootHash(), VerificationNotification::blockHash);
                if (blockSources.get(block.number()) == BlockSource.PUBLISHER) {
                    assertion.returns(BlockSource.PUBLISHER, VerificationNotification::source);
                } else if (blockSources.get(block.number()) == BlockSource.BACKFILL) {
                    assertion.returns(BlockSource.BACKFILL, VerificationNotification::source);
                } else {
                    fail("unrecognized or unsupported source %s".formatted(blockSources.get(block.number())));
                }
            }
        }
    }

    @Nested
    @DisplayName("Active Sessions Buffer Tests")
    class ActiveSessionsBuffer
            extends PluginTestBase<VerificationServicePlugin, ExecutorService, ScheduledExecutorService> {
        ActiveSessionsBuffer() {
            super(
                    Executors.newVirtualThreadPerTaskExecutor(),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            final Map<String, String> configOverrides =
                    Map.ofEntries(Map.entry("verification.activeSessionsBufferSize", "2"));
            start(new VerificationServicePlugin(), new SimpleInMemoryHistoricalBlockFacility(), configOverrides);
        }

        /// This test aims to verify that when the active sessions buffer is full and a new session comes,
        /// the lowest active session will be canceled to make room, so long as the lowest active session is not
        /// the one we just submitted.
        @Test
        @DisplayName(
                "Active Sessions Buffer - cancel lowest session when buffer full and new submission is not the lowest active session")
        void testCancelLowestActiveSessionWhenBufferFullAndCurrentSubmissionNotLowest()
                throws IOException, ParseException {
            final List<ResourceTestWRBBlock> loadedBlocks = ResourceTestBlockBuilder.loadMultiple(consecutiveWRBBlocks);
            final ResourceTestWRBBlock block2 = loadedBlocks.get(2);
            final ResourceTestWRBBlock block3 = loadedBlocks.get(3);
            final ResourceTestWRBBlock block4 = loadedBlocks.get(4);
            updateAddressBook(block2.nodeAddressBook());
            plugin.handleBlockItemsReceived(block2.asBlockItems());
            plugin.handleBlockItemsReceived(block3.asBlockItems());
            plugin.handleBlockItemsReceived(block4.asBlockItems());
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(FailureInfo.standard(FailureType.CANCELLED), VerificationNotification::failureInfo)
                    .returns(block2.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }
    }
}
