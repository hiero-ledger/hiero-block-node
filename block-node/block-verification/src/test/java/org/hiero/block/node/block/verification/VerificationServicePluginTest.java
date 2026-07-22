// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.SignedRecordFileProof;
import com.hedera.hapi.block.stream.TssSignedBlockProof;
import com.hedera.pbj.runtime.ParseException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.assertj.core.api.ObjectAssert;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlock;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder.StateProof;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder.WRAPS;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder.WRB;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestWRBBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureInfo;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
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

        /// This test aims to assert that when the next in line WRAPS block is
        /// received, has multiple valid proofs, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully.
        @Test
        @DisplayName("Successful WRAPS Verification - Multiple Valid Proofs")
        void testSuccessfulWRAPSVerificationMultipleValidProofs() throws IOException, ParseException {
            final ResourceTestBlock base = ResourceTestBlockBuilder.load(WRAPS.BLOCK_0);
            // Append the valid proof from base again so we have multiple valid proofs
            final ResourceTestBlock block0MultiProof =
                    appendProof(base, wrapBlockProof(base.proofs().getFirst()));
            // We push the block to the live items RB
            plugin.handleBlockItemsReceived(block0MultiProof.asBlockItems());
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(true, VerificationNotification::success)
                    .returns(null, VerificationNotification::failureInfo)
                    .returns(block0MultiProof.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(block0MultiProof.blockUnparsed(), VerificationNotification::block)
                    .returns(block0MultiProof.blockRootHash(), VerificationNotification::blockHash);
        }

        /// This test aims to assert that when the next in line WRAPS block is
        /// received but has one valid and one invalid proof, verification
        /// will fail.
        @Test
        @DisplayName("Failed WRAPS Verification - One Valid and One Invalid proof")
        void testFailedWRAPSVerificationOneValidAndOneInvalidProof() throws IOException, ParseException {
            // Append
            final ResourceTestBlock block0 =
                    appendProof(ResourceTestBlockBuilder.load(WRAPS.BLOCK_0), badTssSignedProof());
            // We push the block to the live items RB
            plugin.handleBlockItemsReceived(block0.asBlockItems());
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(FailureInfo.standard(FailureType.BAD_BLOCK_PROOF), VerificationNotification::failureInfo)
                    .returns(block0.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }

        /// This test aims to assert that when the next in line WRAPS block is
        /// received but fails verification, failure is expected to be
        /// informational if the block was recently verified.
        @Test
        @DisplayName("Failed WRAPS Verification - Informational Failure")
        void testFailedWRAPSVerificationInformationalFailure() throws IOException, ParseException {
            final ResourceTestBlock block0Valid = ResourceTestBlockBuilder.load(WRAPS.BLOCK_0);
            plugin.handleBlockItemsReceived(block0Valid.asBlockItems());
            // Assert success
            final List<VerificationNotification> preCheck = blockMessaging.getSentVerificationNotifications(1);
            assertThat(preCheck).hasSize(1).first().returns(true, VerificationNotification::success);
            // Clear the notifications so we can assert below
            preCheck.clear();
            // Append
            final ResourceTestBlock block0Invalid = appendProof(block0Valid, badTssSignedProof());
            // We push the block to the live items RB
            plugin.handleBlockItemsReceived(block0Invalid.asBlockItems());
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(
                            FailureInfo.informational(FailureType.BAD_BLOCK_PROOF),
                            VerificationNotification::failureInfo)
                    .returns(block0Invalid.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
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
            // Clear the notification so we can assert cleanly below
            blockMessaging.getSentVerificationNotifications(1).clear();
            final ResourceTestBlock block1 = ResourceTestBlockBuilder.load(WRAPS.BLOCK_1);
            final List<BlockItemUnparsed> tamperedItems =
                    new ArrayList<>(block1.blockUnparsed().blockItems());
            tamperedItems.remove(1); // remove a non-mandatory item to change the block hash
            plugin.handleBlockItemsReceived(new BlockItems(tamperedItems, block1.number(), true, true));
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            // Assert, for this test we only care about receiving a failure, regardless of type
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(block1.number(), VerificationNotification::blockNumber);
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

        /// This test aims to assert that when the next in line StateProof block is
        /// received, has multiple valid proofs, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully.
        @Test
        @DisplayName("Successful StateProof Verification - Multiple Valid Proofs")
        void testSuccessfulStateProofVerificationMultipleValidProofs() throws IOException, ParseException {
            // Block 0 is direct tss proof, initialize tss
            // We push the block to the live items RB and initialize tss
            plugin.handleBlockItemsReceived(
                    ResourceTestBlockBuilder.load(StateProof.BLOCK_0).asBlockItems());
            // We will receive notification, then clear it so we can assert below
            blockMessaging.getSentVerificationNotifications(1).clear();
            // Block 1 is indirect proof
            final ResourceTestBlock base = ResourceTestBlockBuilder.load(StateProof.BLOCK_1);
            final ResourceTestBlock block1MultiProof =
                    appendProof(base, wrapBlockProof(base.proofs().getFirst()));
            plugin.handleBlockItemsReceived(block1MultiProof.asBlockItems());
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(true, VerificationNotification::success)
                    .returns(null, VerificationNotification::failureInfo)
                    .returns(block1MultiProof.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(block1MultiProof.blockUnparsed(), VerificationNotification::block)
                    .returns(block1MultiProof.blockRootHash(), VerificationNotification::blockHash);
        }

        /// This test aims to assert that when the next in line StateProof block is
        /// received but has one valid and one invalid proof, verification will fail.
        @Test
        @DisplayName("Failed StateProof Verification - One Valid and One Invalid Proof")
        void testFailedStateProofVerificationOneValidAndOneInvalidProof() throws IOException, ParseException {
            // Block 0 is direct tss proof, initialize tss
            // We push the block to the live items RB and initialize tss
            plugin.handleBlockItemsReceived(
                    ResourceTestBlockBuilder.load(StateProof.BLOCK_0).asBlockItems());
            // We will receive notification, then clear it so we can assert below
            blockMessaging.getSentVerificationNotifications(1).clear();
            // Block 1 is indirect proof
            final ResourceTestBlock base = ResourceTestBlockBuilder.load(StateProof.BLOCK_1);
            final ResourceTestBlock block1BadProof = appendProof(base, badStateProof());
            plugin.handleBlockItemsReceived(block1BadProof.asBlockItems());
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(FailureInfo.standard(FailureType.BAD_BLOCK_PROOF), VerificationNotification::failureInfo)
                    .returns(block1BadProof.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }

        /// This test aims to assert that when the next in line StateProof block is
        /// received but verification fails, the verification is expected to be
        /// informational if the block was recently verified.
        @Test
        @DisplayName("Failed StateProof Verification - Informational Failure")
        void testFailedStateProofVerificationInformationalFailure() throws IOException, ParseException {
            // Block 0 is direct tss proof, initialize tss
            // We push the block to the live items RB and initialize tss
            plugin.handleBlockItemsReceived(
                    ResourceTestBlockBuilder.load(StateProof.BLOCK_0).asBlockItems());
            // We will receive notification, then clear it so we can assert below
            blockMessaging.getSentVerificationNotifications(1).clear();
            final ResourceTestBlock block0Valid = ResourceTestBlockBuilder.load(StateProof.BLOCK_1);
            plugin.handleBlockItemsReceived(block0Valid.asBlockItems());
            // Assert success
            final List<VerificationNotification> preCheck = blockMessaging.getSentVerificationNotifications(1);
            assertThat(preCheck).hasSize(1).first().returns(true, VerificationNotification::success);
            // Clear the notifications so we can assert below
            preCheck.clear();
            // Block 1 is indirect proof
            final ResourceTestBlock block1BadProof = appendProof(block0Valid, badStateProof());
            plugin.handleBlockItemsReceived(block1BadProof.asBlockItems());
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(
                            FailureInfo.informational(FailureType.BAD_BLOCK_PROOF),
                            VerificationNotification::failureInfo)
                    .returns(block1BadProof.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }

        /// This test exercises the VFE extraction path in SessionResultHandler.
        /// Block that was tampered with will fail StateProof Verification.
        @Test
        @DisplayName("Failed StateProof Verification - tampered block covers VFE extraction path")
        void testTamperedStateProofBlockTriggersVfePath() throws IOException, ParseException {
            final ResourceTestBlock block0 = ResourceTestBlockBuilder.load(StateProof.BLOCK_0);
            plugin.handleBlockItemsReceived(block0.asBlockItems());
            // Wait for block 0 to complete so TSS data is initialized before block 1 is submitted
            // Clear the notification so we can assert cleanly below
            blockMessaging.getSentVerificationNotifications(1).clear();
            final ResourceTestBlock block1 = ResourceTestBlockBuilder.load(StateProof.BLOCK_1);
            final List<BlockItemUnparsed> tamperedItems =
                    new ArrayList<>(block1.blockUnparsed().blockItems());
            tamperedItems.remove(1); // remove a non-mandatory item to change the block hash
            plugin.handleBlockItemsReceived(new BlockItems(tamperedItems, block1.number(), true, true));
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            // Assert, for this test we only care about receiving a failure, regardless of type
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(block1.number(), VerificationNotification::blockNumber);
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
            // Because when we have no data at the start and the plugin will accept the first valid
            // block in that case, make sure to first pass verification on block 0 and then the rest so we can
            // see things as expected in order.
            // First, we load multiple consecutive WRB blocks
            final List<ResourceTestWRBBlock> loadedBlocks = ResourceTestBlockBuilder.loadMultiple(consecutiveWRBBlocks);
            // Then, we push block 0 first.
            final ResourceTestWRBBlock block0 = loadedBlocks.getFirst();
            updateAddressBook(block0.nodeAddressBook());
            plugin.handleBlockItemsReceived(block0.asBlockItems());
            // Await for the notification of block 0
            blockMessaging.getSentVerificationNotifications(1);
            // Then, we push the rest of the blocks
            for (final ResourceTestWRBBlock block : loadedBlocks.subList(1, loadedBlocks.size())) {
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
            // Because when we have no data at the start and the plugin will accept the first valid
            // block in that case, make sure to first pass verification on block 0 and then the rest so we can
            // see things as expected in order.
            // First, we load multiple consecutive WRB blocks
            final List<ResourceTestWRBBlock> loadedBlocks = ResourceTestBlockBuilder.loadMultiple(consecutiveWRBBlocks);
            // Then, we push block 0 first.
            final ResourceTestWRBBlock block0 = loadedBlocks.getFirst();
            updateAddressBook(block0.nodeAddressBook());
            plugin.handleBackfilled(block0.asBackfilledNotification());
            // Await for the notification of block 0
            blockMessaging.getSentVerificationNotifications(1);
            // Then, we push the rest of the blocks
            for (final ResourceTestWRBBlock block : loadedBlocks.subList(1, loadedBlocks.size())) {
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
            // Because when we have no data at the start and the plugin will accept the first valid
            // block in that case, make sure to first pass verification on block 0 and then the rest so we can
            // see things as expected in order.
            // First, we load multiple consecutive WRB blocks
            final List<ResourceTestWRBBlock> loadedBlocks = ResourceTestBlockBuilder.loadMultiple(consecutiveWRBBlocks);
            // Then, we push block 0 first.
            final ResourceTestWRBBlock block0 = loadedBlocks.getFirst();
            updateAddressBook(block0.nodeAddressBook());
            plugin.handleBackfilled(block0.asBackfilledNotification());
            // Await for the notification of block 0
            final Map<Long, BlockSource> blockSources = new HashMap<>();
            blockMessaging.getSentVerificationNotifications(1);
            blockSources.put(block0.number(), BlockSource.BACKFILL);
            // Then, we push the rest of the blocks
            for (final ResourceTestWRBBlock block : loadedBlocks.subList(1, loadedBlocks.size())) {
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

        /// This test aims to assert that when the next in line WRB block is
        /// received, has multiple valid proofs, and we have a valid RSA roster initialized,
        /// the block will pass verification successfully.
        @Test
        @DisplayName("Successful WRB Verification - Multiple Valid Proofs")
        void testSuccessfulWRBVerificationMultipleValidProofs() throws IOException, ParseException {
            final ResourceTestWRBBlock base = ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_0);
            final ResourceTestWRBBlock block0 =
                    appendProof(base, wrapBlockProof(base.proofs().getFirst()));
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
        /// received and has one valid and one invalid proof, verification will fail.
        @Test
        @DisplayName("Failed WRB Verification - One Valid and One Invalid Proof")
        void testFailedWRBVerificationOneValidAndOneInvalidProof() throws IOException, ParseException {
            final ResourceTestWRBBlock block0 =
                    appendProof(ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_0), badSignedRecordFileProof());
            // First, we update the node address book
            updateAddressBook(block0.nodeAddressBook());
            // Then, we push the block to the live items RB
            plugin.handleBlockItemsReceived(block0.asBlockItems());
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(FailureInfo.standard(FailureType.BAD_BLOCK_PROOF), VerificationNotification::failureInfo)
                    .returns(block0.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }

        /// This test aims to assert that when the next in line WRB block is
        /// received but verification fails, the failure is expected to be informational
        /// if the block was recently verified.
        @Test
        @DisplayName("Failed WRB Verification - Informational Failure")
        void testFailedWRBVerificationInformationalFailure() throws IOException, ParseException {
            final ResourceTestWRBBlock block0Valid = ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_0);
            // First, we update the node address book
            updateAddressBook(block0Valid.nodeAddressBook());
            plugin.handleBlockItemsReceived(block0Valid.asBlockItems());
            // Assert success
            final List<VerificationNotification> preCheck = blockMessaging.getSentVerificationNotifications(1);
            assertThat(preCheck).hasSize(1).first().returns(true, VerificationNotification::success);
            // Clear the notifications so we can assert below
            preCheck.clear();
            final ResourceTestWRBBlock block0BadProof = appendProof(block0Valid, badSignedRecordFileProof());
            // Then, we push the block to the live items RB
            plugin.handleBlockItemsReceived(block0BadProof.asBlockItems());
            // Finally, await a single response and assert success
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(
                            FailureInfo.informational(FailureType.BAD_BLOCK_PROOF),
                            VerificationNotification::failureInfo)
                    .returns(block0BadProof.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }

        /// This test exercises the VFE extraction path in SessionResultHandler.
        /// Block that was tampered with will fail StateProof Verification.
        @Test
        @DisplayName("Failed WRB Verification - tampered block covers VFE extraction path")
        void testTamperedWRBBlockTriggersVfePath() throws IOException, ParseException {
            final ResourceTestWRBBlock block1 = ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_1);
            updateAddressBook(block1.nodeAddressBook());
            final List<BlockItemUnparsed> tamperedItems =
                    new ArrayList<>(block1.blockUnparsed().blockItems());
            tamperedItems.remove(1); // remove a non-mandatory item to change the block hash
            plugin.handleBlockItemsReceived(new BlockItems(tamperedItems, block1.number(), true, true));
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            // Assert, for this test we only care about receiving a failure, regardless of type
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(block1.number(), VerificationNotification::blockNumber);
        }
    }

    @Nested
    @DisplayName("Active Sessions Buffer Tests")
    class ActiveSessionsBufferTests
            extends PluginTestBase<VerificationServicePlugin, ExecutorService, ScheduledExecutorService> {
        ActiveSessionsBufferTests() {
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

    @Nested
    @DisplayName("First Ordered Block Tests")
    class FirstOrderedBlock
            extends PluginTestBase<VerificationServicePlugin, ExecutorService, ScheduledExecutorService> {
        FirstOrderedBlock() {
            super(
                    Executors.newVirtualThreadPerTaskExecutor(),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            final Map<String, String> configOverrides = Map.ofEntries(Map.entry("verification.firstOrderedBlock", "2"));
            start(new VerificationServicePlugin(), new SimpleInMemoryHistoricalBlockFacility(), configOverrides);
        }

        /// This test aims to assert that when we receive a block that is below the first ordered block
        /// config, it will pass verification and report success immediately. The source and type of proof are
        /// irrelevant for this test.
        @Test
        @DisplayName("First Ordered Block - Success for All Below Configured")
        void testSuccessBelowConfigured() throws IOException, ParseException {
            final ResourceTestWRBBlock block1 = ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_1);
            // First, we update the node address book
            updateAddressBook(block1.nodeAddressBook());
            // Then, we push the block to the live items RB
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

        /// This test aims to assert that when we receive a block that is above or equal the first ordered block
        /// config, it will pass verification and await order. The source and type of proof are
        /// irrelevant for this test.
        @RepeatedTest(100)
        @DisplayName("First Ordered Block - Awaits if Above or Equal to Configured")
        void testAwaitsIfAboveConfigured() throws IOException, ParseException {
            // Send block 2, which is the first ordered block
            final ResourceTestWRBBlock block2 = ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_2);
            updateAddressBook(block2.nodeAddressBook());
            plugin.handleBlockItemsReceived(block2.asBlockItems());
            // Now send block 1, which is below the first ordered block and will report success immediately.
            // This will also make the plugin to expect block 2 as the next in line.
            final ResourceTestWRBBlock block1 = ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_1);
            final List<ResourceTestWRBBlock> loadedBlocks = List.of(block1, block2);
            updateAddressBook(block1.nodeAddressBook());
            plugin.handleBlockItemsReceived(block1.asBlockItems());
            // Finally, await two responses and assert success, the success can come in any order as at switchover
            // sessions race if they are close to or at the ordering stage.
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(2);
            assertThat(notifications).hasSize(loadedBlocks.size());
            for (int i = 0; i < notifications.size(); i++) {
                final VerificationNotification notification = notifications.get(i);
                final ResourceTestWRBBlock block = loadedBlocks.get(i);
                assertThat(notification)
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                        .satisfiesAnyOf(
                                n -> assertThat(block1.blockUnparsed().equals(n.block())),
                                n -> assertThat(block2.blockUnparsed().equals(n.block())))
                        .satisfiesAnyOf(
                                n -> assertThat(block1.blockRootHash().equals(n.blockHash())),
                                n -> assertThat(block2.blockRootHash().equals(n.blockHash())));
            }
            // Now if we stream blocks after we have crossed the first ordered block, we expect everything to be in
            // order
            notifications.clear();
            final ResourceTestWRBBlock block3 = ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_3);
            final ResourceTestWRBBlock block4 = ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_3);
            final List<ResourceTestWRBBlock> nextLoadedBlocks = List.of(block3, block4);
            for (final ResourceTestWRBBlock block : nextLoadedBlocks.reversed()) {
                updateAddressBook(block.nodeAddressBook());
                plugin.handleBlockItemsReceived(block.asBlockItems());
            }
            final List<VerificationNotification> notificationsUpdated =
                    blockMessaging.getSentVerificationNotifications(nextLoadedBlocks.size());
            assertThat(notificationsUpdated).hasSize(nextLoadedBlocks.size());
            for (int i = 0; i < notificationsUpdated.size(); i++) {
                final VerificationNotification notification = notificationsUpdated.get(i);
                final ResourceTestWRBBlock block = nextLoadedBlocks.get(i);
                assertThat(notification)
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(block.number(), VerificationNotification::blockNumber)
                        .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                        .returns(block.blockUnparsed(), VerificationNotification::block)
                        .returns(block.blockRootHash(), VerificationNotification::blockHash);
            }
        }
    }

    @Nested
    @DisplayName("Invalid Start of Block Tests")
    class InvalidStartOfBlockTests
            extends PluginTestBase<VerificationServicePlugin, ExecutorService, ScheduledExecutorService> {
        InvalidStartOfBlockTests() {
            super(
                    Executors.newVirtualThreadPerTaskExecutor(),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            final Map<String, String> configOverrides =
                    Map.ofEntries(Map.entry("verification.activeSessionsBufferSize", "2"));
            start(new VerificationServicePlugin(), new SimpleInMemoryHistoricalBlockFacility(), configOverrides);
        }

        /// This test aims to verify that when we receive a new start of block, denoted by
        /// [BlockItems#isStartOfNewBlock()] but we have no header as the first item, the block
        /// is immediately rejected. The type of the block used for this test is irrelevant.
        @Test
        @DisplayName("Invalid Start of Block Live RB - no header present")
        void testNoHeaderForNewBlockLiveRB() {
            final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0);
            final List<BlockItemUnparsed> block1NoHeader = block0.asBlockItemUnparsedFiltered(i -> !i.hasBlockHeader());
            plugin.handleBlockItemsReceived(new BlockItems(block1NoHeader, block0.number(), true, true));
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(
                            FailureInfo.standard(FailureType.MISSING_VERIFICATION_DATA),
                            VerificationNotification::failureInfo)
                    .returns(block0.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }

        /// This test aims to verify that when we receive a new start of block, denoted by
        /// [BlockItems#isStartOfNewBlock()], but the actual header's block number does not match with
        /// [BlockItems#blockNumber()] we will receive a failure.
        /// The type of the block used for this test is irrelevant.
        @Test
        @DisplayName("Invalid Start of Block Live RB - header block number mismatch with BlockItems")
        void testHeaderBlockNumberMismatchLiveRB() {
            final long headerBlockNumber = 0;
            final long reportedBlockNumber = headerBlockNumber + 1;
            final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(headerBlockNumber);
            plugin.handleBlockItemsReceived(
                    new BlockItems(block0.blockUnparsed().blockItems(), reportedBlockNumber, true, true));
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(
                            FailureInfo.standard(FailureType.MISSING_VERIFICATION_DATA),
                            VerificationNotification::failureInfo)
                    .returns(reportedBlockNumber, VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }

        /// This test aims to verify that when we receive a new start of block, denoted by
        /// [BlockItems#isStartOfNewBlock()] but we have no header as the first item, the block
        /// is immediately rejected. The type of the block used for this test is irrelevant.
        @Test
        @DisplayName("Invalid Start of Block Backfill - no header present")
        void testNoHeaderForNewBlockBackfill() {
            final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0);
            final List<BlockItemUnparsed> block1NoHeader = block0.asBlockItemUnparsedFiltered(i -> !i.hasBlockHeader());
            plugin.handleBackfilled(
                    new BackfilledBlockNotification(block0.number(), new BlockUnparsed(block1NoHeader)));
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(
                            FailureInfo.standard(FailureType.MISSING_VERIFICATION_DATA),
                            VerificationNotification::failureInfo)
                    .returns(block0.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.BACKFILL, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }

        /// This test aims to verify that when we receive a new start of block, denoted by
        /// [BlockItems#isStartOfNewBlock()], but the actual header's block number does not match with
        /// [BlockItems#blockNumber()] we will receive a failure.
        /// The type of the block used for this test is irrelevant.
        @Test
        @DisplayName("Invalid Start of Block Backfill - header block number mismatch with BlockItems")
        void testHeaderBlockNumberMismatchBackfill() {
            final long headerBlockNumber = 0;
            final long reportedBlockNumber = headerBlockNumber + 1;
            final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(headerBlockNumber);
            plugin.handleBackfilled(new BackfilledBlockNotification(
                    reportedBlockNumber,
                    new BlockUnparsed(block0.blockUnparsed().blockItems())));
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(
                            FailureInfo.standard(FailureType.MISSING_VERIFICATION_DATA),
                            VerificationNotification::failureInfo)
                    .returns(reportedBlockNumber, VerificationNotification::blockNumber)
                    .returns(BlockSource.BACKFILL, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }
    }

    @Nested
    @DisplayName("Block Order Tests")
    class BlockOrderTests extends PluginTestBase<VerificationServicePlugin, ExecutorService, ScheduledExecutorService> {
        BlockOrderTests() {
            super(
                    Executors.newVirtualThreadPerTaskExecutor(),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(new VerificationServicePlugin(), new SimpleInMemoryHistoricalBlockFacility());
        }

        /// This test aims to assert that when blocks are received out of order, order will be preserved
        /// when completing verification. Proof type and source are irrelevant here.
        @RepeatedTest(100)
        @DisplayName("Block Order Preserved for Out of Order Blocks Received")
        void testOutOfOrderBlocksPreserveOrderingOnCompletion() throws IOException, ParseException {
            final List<ResourceTestBlock> loadedBlocks = ResourceTestBlockBuilder.loadMultiple(consecutiveWRAPSBlocks);
            // Load block 0 first which will update tss data, make sure it passes before continuing to ensure
            // no flakiness due to missing tss data. Other tests can pre-load tss data and it will not be
            // relevant to wait.
            plugin.handleBlockItemsReceived(loadedBlocks.getFirst().asBlockItems());
            blockMessaging.getSentVerificationNotifications(1);
            // Now send the rest out of order
            final List<ResourceTestBlock> shuffledBlocks =
                    new ArrayList<>(loadedBlocks.subList(1, loadedBlocks.size()));
            Collections.shuffle(shuffledBlocks);
            for (final ResourceTestBlock block : shuffledBlocks) {
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

        /// This test aims to assert that when a block is submitted for verification but it is below
        /// the high watermark, it will pass immediately and no strict ordering applies.
        @RepeatedTest(100)
        @DisplayName("No Strict Ordering Below High Water Mark")
        void testNoStrictOrderingBelowHighWaterMark() throws IOException, ParseException {
            final List<ResourceTestBlock> loadedBlocks = ResourceTestBlockBuilder.loadMultiple(consecutiveWRAPSBlocks);
            // Load block 0 first which will update tss data, make sure it passes before continuing to ensure
            // no flakiness due to missing tss data. Other tests can pre-load tss data and it will not be
            // relevant to wait.
            plugin.handleBlockItemsReceived(loadedBlocks.getFirst().asBlockItems());
            blockMessaging.getSentVerificationNotifications(1);
            // Now send the rest out of order
            final List<ResourceTestBlock> restOfBlocks = new ArrayList<>(loadedBlocks.subList(1, loadedBlocks.size()));
            for (final ResourceTestBlock block : restOfBlocks) {
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
            // Now clear all notifications so we can cleanly assert again
            final ArrayList<ResourceTestBlock> shuffled = new ArrayList<>(restOfBlocks);
            Collections.shuffle(shuffled);
            notifications.clear();
            for (final ResourceTestBlock block : shuffled) {
                // Push a random block that is below the high water mark
                plugin.handleBlockItemsReceived(block.asBlockItems());
                // Await for the successful notification
                blockMessaging.getSentVerificationNotifications(1);
                final VerificationNotification notification = notifications.getFirst();
                assertThat(notification)
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(block.number(), VerificationNotification::blockNumber)
                        .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                        .returns(block.blockUnparsed(), VerificationNotification::block)
                        .returns(block.blockRootHash(), VerificationNotification::blockHash);
                // Finally, clear the notifications and continue with the next block
                notifications.clear();
            }
        }
    }

    @Nested
    @DisplayName("Live RB Ingestion Tests")
    class LiveRBIngestionTests
            extends PluginTestBase<VerificationServicePlugin, ExecutorService, ScheduledExecutorService> {
        LiveRBIngestionTests() {
            super(
                    Executors.newVirtualThreadPerTaskExecutor(),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(new VerificationServicePlugin(), new SimpleInMemoryHistoricalBlockFacility());
        }

        /// This test aims to assert that we are able to receive a block in multiple batches via the Live RB
        /// (Publisher Source). The block proof type is irrelevant for this test.
        @Test
        @DisplayName("Live RB Ingestion - Block Received in Multiple Batches")
        void testLiveRBIngestionBlockReceivedInMultipleBatches() throws IOException, ParseException {
            final ResourceTestBlock block0 = ResourceTestBlockBuilder.load(WRAPS.BLOCK_0);
            // We push the block in multiple batches
            for (final BlockItemUnparsed item : block0.blockUnparsed().blockItems()) {
                final boolean isStartOfNewBlock = item.hasBlockHeader();
                final boolean isEndOfBlock = item.hasBlockProof();
                final BlockItems itemAsBlockItems =
                        new BlockItems(List.of(item), block0.number(), isStartOfNewBlock, isEndOfBlock);
                plugin.handleBlockItemsReceived(itemAsBlockItems);
            }
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

        /// This test aims to assert that when we receive the start of a new block via the Live RB (Publisher Source),
        /// but we have not yet received the last item for the current live publisher session, the current live
        /// publisher session will be canceled and the newly received block will start a new session.
        /// The block proof type is irrelevant for this test. Also, it is irrelevant which block will be started
        /// in the middle of current session.
        @Test
        @DisplayName(
                "Live RB Ingestion - Cancel Current Live Session When New Block Received While Not Finished Current One")
        void testLiveRBIngestionCancelLiveSessionWhenNewBlockReceivedAndCurrentNotComplete()
                throws IOException, ParseException {
            final ResourceTestBlock block0 = ResourceTestBlockBuilder.load(WRAPS.BLOCK_0);
            // We push an item to start a session
            final BlockItems headerAsBlockItems =
                    new BlockItems(List.of(block0.getHeaderUnparsed()), block0.number(), true, false);
            plugin.handleBlockItemsReceived(headerAsBlockItems);
            // Now we push that same item again, which starts a new session
            plugin.handleBlockItemsReceived(headerAsBlockItems);
            // Now, we expect the cancellation
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            // Assert cancellation received
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(FailureInfo.standard(FailureType.CANCELLED), VerificationNotification::failureInfo)
                    .returns(block0.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }
    }

    /// Tests for the recently verified blocks queue.
    @Nested
    @DisplayName("Recently Verified Blocks Tests")
    class RecentlyVerifiedBlocksTests
            extends PluginTestBase<VerificationServicePlugin, ExecutorService, ScheduledExecutorService> {
        RecentlyVerifiedBlocksTests() {
            super(
                    Executors.newVirtualThreadPerTaskExecutor(),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(new VerificationServicePlugin(), new SimpleInMemoryHistoricalBlockFacility());
        }

        /// This test aims to assert that a failed persistence notification removes
        /// all occurrences of a block from the recently verified blocks queue. The
        /// queue can hold duplicates when the same block passes verification more
        /// than once. If only the first occurrence were removed, a subsequent
        /// verification failure for that block would still (incorrectly) be
        /// reported as informational. We verify the same block twice, report a
        /// failed persistence once, and then expect a subsequent verification
        /// failure to be standard, not informational.
        @Test
        @DisplayName("Failed Persistence Removes All Duplicates from Recently Verified Blocks")
        void testFailedPersistenceRemovesAllDuplicates() throws IOException, ParseException {
            final ResourceTestBlock block0Valid = ResourceTestBlockBuilder.load(WRAPS.BLOCK_0);
            // Verify block 0 successfully twice, the queue now holds its number twice
            plugin.handleBlockItemsReceived(block0Valid.asBlockItems());
            final List<VerificationNotification> firstPass = blockMessaging.getSentVerificationNotifications(1);
            assertThat(firstPass).hasSize(1).first().returns(true, VerificationNotification::success);
            firstPass.clear();
            plugin.handleBlockItemsReceived(block0Valid.asBlockItems());
            final List<VerificationNotification> secondPass = blockMessaging.getSentVerificationNotifications(1);
            assertThat(secondPass).hasSize(1).first().returns(true, VerificationNotification::success);
            secondPass.clear();
            // Report failed persistence for block 0, all duplicates must be removed
            plugin.handlePersisted(new PersistedNotification(block0Valid.number(), false, 0, BlockSource.PUBLISHER));
            // Now fail verification of block 0, the failure must be standard, not informational
            final ResourceTestBlock block0Invalid = appendProof(block0Valid, badTssSignedProof());
            plugin.handleBlockItemsReceived(block0Invalid.asBlockItems());
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(FailureInfo.standard(FailureType.BAD_BLOCK_PROOF), VerificationNotification::failureInfo)
                    .returns(block0Invalid.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }
    }

    private BlockItem badTssSignedProof() {
        final BlockProof badProof = BlockProof.newBuilder()
                .signedBlockProof(TssSignedBlockProof.newBuilder().build())
                .build();
        return wrapBlockProof(badProof);
    }

    private BlockItem badSignedRecordFileProof() {
        final BlockProof badProof = BlockProof.newBuilder()
                .signedRecordFileProof(
                        SignedRecordFileProof.newBuilder().version(6).build())
                .build();
        return wrapBlockProof(badProof);
    }

    private BlockItem badStateProof() {
        final BlockProof badProof = BlockProof.newBuilder()
                .blockStateProof(
                        com.hedera.hapi.block.stream.StateProof.newBuilder().build())
                .build();
        return wrapBlockProof(badProof);
    }

    private BlockItem wrapBlockProof(final BlockProof badProof) {
        return BlockItem.newBuilder().blockProof(badProof).build();
    }

    private ResourceTestBlock appendProof(final ResourceTestBlock base, final BlockItem proofToAppend) {
        final ResourceTestBlock result = base.append(proofToAppend);
        assertProofAppended(base, proofToAppend, result);
        return result;
    }

    private ResourceTestWRBBlock appendProof(final ResourceTestWRBBlock base, final BlockItem proofToAppend) {
        final ResourceTestWRBBlock result = base.append(proofToAppend);
        assertProofAppended(base, proofToAppend, result);
        return result;
    }

    private void assertProofAppended(final TestBlock base, final BlockItem proofToAppend, final TestBlock result) {
        // Assert that proof is appended
        assertThat(result)
                .returns(true, multiProof -> multiProof.blockSize() == base.blockSize() + 1)
                .returns(true, multiProof -> proofToAppend
                        .blockProof()
                        .equals(multiProof.proofs().getLast()));
    }
}
