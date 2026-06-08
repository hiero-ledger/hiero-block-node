// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.hedera.pbj.runtime.ParseException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.assertj.core.api.ObjectAssert;
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
            final ResourceTestBlock block0 = ResourceTestBlockBuilder.load(StateProof.BLOCK_0);
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

        /// This test aims to assert that when the next in line StateProof block is
        /// received, is valid, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully. Uses Backfill.
        @Test
        @DisplayName("Successful StateProof Verification - Backfill")
        void testSuccessfulStateProofVerificationBackfill() throws IOException, ParseException {
            final ResourceTestBlock block0 = ResourceTestBlockBuilder.load(StateProof.BLOCK_0);
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
}
