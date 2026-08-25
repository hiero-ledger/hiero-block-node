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
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder.WRB;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestWRBBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureInfo;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.hiero.block.signing.TssBlockSigner;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/// Plugin-level integration test for [VerificationServicePlugin].
@DisplayName("VerificationServicePlugin Tests")
class VerificationServicePluginTest {
    // consecutiveWRAPSBlocks removed — the last WRAPS-fixture consumers were migrated to
    // HarnessChainBuilder; only WRAPS.BLOCK_0 remains as canary for BlockHasherTest.
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
        void testSuccessfulWRAPSVerificationLiveRB() {
            final org.hiero.block.node.block.verification.harness.HarnessChainBuilder builder = harnessChainBuilder();
            final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed genesis =
                    builder.genesisWithPublication();
            plugin.handleBlockItemsReceived(genesis.block().asBlockItems());
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(true, VerificationNotification::success)
                    .returns(null, VerificationNotification::failureInfo)
                    .returns(genesis.block().number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(genesis.block().blockUnparsed(), VerificationNotification::block)
                    .returns(genesis.rootHash(), VerificationNotification::blockHash);
        }

        /// This test aims to assert that when the next in line WRAPS block is
        /// received, is valid, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully. Uses Backfill.
        @Test
        @DisplayName("Successful WRAPS Verification - Backfill")
        void testSuccessfulWRAPSVerificationBackfill() {
            final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed genesis =
                    harnessChainBuilder().genesisWithPublication();
            plugin.handleBackfilled(genesis.block().asBackfilledNotification());
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(true, VerificationNotification::success)
                    .returns(null, VerificationNotification::failureInfo)
                    .returns(genesis.block().number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.BACKFILL, VerificationNotification::source)
                    .returns(genesis.block().blockUnparsed(), VerificationNotification::block)
                    .returns(genesis.rootHash(), VerificationNotification::blockHash);
        }

        /// This test aims to assert that when the next in line WRAPS block is
        /// received, is valid, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully. Uses Live RB.
        /// Verifies multiple consecutive blocks.
        @Test
        @DisplayName("Successful consecutive WRAPS Verification - Live RB")
        void testSuccessfulConsecutiveWRAPSVerificationLiveRB() {
            final List<org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed> chain =
                    signedChain(5);
            plugin.handleBlockItemsReceived(chain.getFirst().block().asBlockItems());
            blockMessaging.getSentVerificationNotifications(1);
            for (int i = 1; i < chain.size(); i++) {
                plugin.handleBlockItemsReceived(chain.get(i).block().asBlockItems());
            }
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(chain.size());
            assertThat(notifications).hasSize(chain.size());
            for (int i = 0; i < notifications.size(); i++) {
                final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed signed = chain.get(i);
                assertThat(notifications.get(i))
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(signed.block().number(), VerificationNotification::blockNumber)
                        .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                        .returns(signed.block().blockUnparsed(), VerificationNotification::block)
                        .returns(signed.rootHash(), VerificationNotification::blockHash);
            }
        }

        /// This test aims to assert that when the next in line WRAPS block is
        /// received, is valid, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully. Uses Backfill.
        /// Verifies multiple consecutive blocks.
        @Test
        @DisplayName("Successful consecutive WRAPS Verification - Backfill")
        void testSuccessfulConsecutiveWRAPSVerificationBackfill() {
            final List<org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed> chain =
                    signedChain(5);
            plugin.handleBackfilled(chain.getFirst().block().asBackfilledNotification());
            blockMessaging.getSentVerificationNotifications(1);
            for (int i = 1; i < chain.size(); i++) {
                plugin.handleBackfilled(chain.get(i).block().asBackfilledNotification());
            }
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(chain.size());
            assertThat(notifications).hasSize(chain.size());
            for (int i = 0; i < notifications.size(); i++) {
                final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed signed = chain.get(i);
                assertThat(notifications.get(i))
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(signed.block().number(), VerificationNotification::blockNumber)
                        .returns(BlockSource.BACKFILL, VerificationNotification::source)
                        .returns(signed.block().blockUnparsed(), VerificationNotification::block)
                        .returns(signed.rootHash(), VerificationNotification::blockHash);
            }
        }

        /// This test aims to assert that when the next in line WRAPS block is
        /// received, is valid, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully. Uses Multiple Sources.
        /// Verifies multiple consecutive blocks.
        @Test
        @DisplayName("Successful consecutive WRAPS Verification - Multi Source")
        void testSuccessfulConsecutiveWRAPSVerificationMultiSource() {
            final List<org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed> chain =
                    signedChain(5);
            final Map<Long, BlockSource> blockSources = new HashMap<>();
            plugin.handleBackfilled(chain.getFirst().block().asBackfilledNotification());
            blockSources.put(chain.getFirst().block().number(), BlockSource.BACKFILL);
            blockMessaging.getSentVerificationNotifications(1);
            for (int i = 1; i < chain.size(); i++) {
                final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed signed = chain.get(i);
                if (signed.block().number() % 2 == 0) {
                    blockSources.put(signed.block().number(), BlockSource.BACKFILL);
                    plugin.handleBackfilled(signed.block().asBackfilledNotification());
                } else {
                    blockSources.put(signed.block().number(), BlockSource.PUBLISHER);
                    plugin.handleBlockItemsReceived(signed.block().asBlockItems());
                }
            }
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(chain.size());
            assertThat(notifications).hasSize(chain.size());
            for (int i = 0; i < notifications.size(); i++) {
                final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed signed = chain.get(i);
                final ObjectAssert<VerificationNotification> assertion = assertThat(notifications.get(i));
                assertion
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(signed.block().number(), VerificationNotification::blockNumber)
                        .returns(signed.block().blockUnparsed(), VerificationNotification::block)
                        .returns(signed.rootHash(), VerificationNotification::blockHash);
                final BlockSource source = blockSources.get(signed.block().number());
                if (source == BlockSource.PUBLISHER || source == BlockSource.BACKFILL) {
                    assertion.returns(source, VerificationNotification::source);
                } else {
                    fail("unrecognized or unsupported source %s".formatted(source));
                }
            }
        }

        /// This test aims to assert that when the next in line WRAPS block is
        /// received, has multiple valid proofs, and we have valid TSS parameters initialized,
        /// the block will pass verification successfully.
        @Test
        @DisplayName("Successful WRAPS Verification - Multiple Valid Proofs")
        void testSuccessfulWRAPSVerificationMultipleValidProofs() {
            final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed base =
                    harnessChainBuilder().genesisWithPublication();
            // Append a duplicate of the (real, signed) proof so the block carries two valid proofs.
            final TestBlock multiProof =
                    base.block().append(wrapBlockProof(base.block().proofs().getFirst()));
            plugin.handleBlockItemsReceived(multiProof.asBlockItems());
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(true, VerificationNotification::success)
                    .returns(null, VerificationNotification::failureInfo)
                    .returns(multiProof.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(multiProof.blockUnparsed(), VerificationNotification::block)
                    .returns(base.rootHash(), VerificationNotification::blockHash);
        }

        /// This test aims to assert that when the next in line WRAPS block is
        /// received but has one valid and one invalid proof, verification
        /// will fail.
        @Test
        @DisplayName("Failed WRAPS Verification - One Valid and One Invalid proof")
        void testFailedWRAPSVerificationOneValidAndOneInvalidProof() {
            final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed base =
                    harnessChainBuilder().genesisWithPublication();
            final TestBlock oneValidOneBad = base.block().append(badTssSignedProof());
            plugin.handleBlockItemsReceived(oneValidOneBad.asBlockItems());
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(FailureInfo.standard(FailureType.BAD_BLOCK_PROOF), VerificationNotification::failureInfo)
                    .returns(oneValidOneBad.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }

        /// This test aims to assert that when the next in line WRAPS block is
        /// received but fails verification, failure is expected to be
        /// informational if the block was recently verified.
        @Test
        @DisplayName("Failed WRAPS Verification - Informational Failure")
        void testFailedWRAPSVerificationInformationalFailure() {
            final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed valid =
                    harnessChainBuilder().genesisWithPublication();
            plugin.handleBlockItemsReceived(valid.block().asBlockItems());
            final List<VerificationNotification> preCheck = blockMessaging.getSentVerificationNotifications(1);
            assertThat(preCheck).hasSize(1).first().returns(true, VerificationNotification::success);
            preCheck.clear();
            final TestBlock invalid = valid.block().append(badTssSignedProof());
            plugin.handleBlockItemsReceived(invalid.asBlockItems());
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(
                            FailureInfo.informational(FailureType.BAD_BLOCK_PROOF),
                            VerificationNotification::failureInfo)
                    .returns(invalid.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }

        /// This test exercises the VFE extraction path in SessionResultHandler.
        /// Block 0 initializes TSS data; the tampered block 1 passes hashing but
        /// fails TSS signature verification, producing a failure notification.
        @Test
        @DisplayName("Failed WRAPS Verification - tampered block covers VFE extraction path")
        void testTamperedWRAPSBlockTriggersVfePath() {
            final List<org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed> chain =
                    signedChain(2);
            plugin.handleBlockItemsReceived(chain.get(0).block().asBlockItems());
            blockMessaging.getSentVerificationNotifications(1).clear();
            final TestBlock block1 = chain.get(1).block();
            final List<BlockItemUnparsed> tamperedItems =
                    new ArrayList<>(block1.blockUnparsed().blockItems());
            // Drop a non-mandatory item so the recomputed hash differs from the signature's target.
            tamperedItems.remove(1);
            plugin.handleBlockItemsReceived(new BlockItems(tamperedItems, block1.number(), true, true));
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(block1.number(), VerificationNotification::blockNumber);
        }

        /// Convenience helper that builds a HarnessChainBuilder with an isolated MetricRegistry
        /// so it doesn't collide with the plugin's already-registered verification metrics.
        /// The generated block 0's LedgerIdPublication self-provisions this plugin's own TSS state.
        private org.hiero.block.node.block.verification.harness.HarnessChainBuilder harnessChainBuilder() {
            return org.hiero.block.node.block.verification.harness.HarnessChainBuilder.create(TssBlockSigner.create());
        }

        /// Emits a length-N signed chain starting at block 0. Block 0 carries the LedgerIdPublication
        /// so the plugin under test self-provisions its TSS state.
        private List<org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed> signedChain(
                final int length) {
            final org.hiero.block.node.block.verification.harness.HarnessChainBuilder builder = harnessChainBuilder();
            final List<org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed> chain =
                    new ArrayList<>(length);
            chain.add(builder.genesisWithPublication());
            for (long n = 1; n < length; n++) {
                chain.add(builder.next(n));
            }
            return chain;
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

        @Test
        @DisplayName("Successful StateProof Verification - Live RB")
        void testSuccessfulStateProofVerificationLiveRB() {
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder builder = spBuilder();
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed genesis =
                    builder.genesisWithPublication();
            plugin.handleBlockItemsReceived(genesis.block().asBlockItems());
            blockMessaging.getSentVerificationNotifications(1).clear();
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed block1 =
                    builder.next(1L);
            plugin.handleBlockItemsReceived(block1.block().asBlockItems());
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(true, VerificationNotification::success)
                    .returns(null, VerificationNotification::failureInfo)
                    .returns(block1.block().number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(block1.block().blockUnparsed(), VerificationNotification::block)
                    .returns(block1.rootHash(), VerificationNotification::blockHash);
        }

        @Test
        @DisplayName("Successful StateProof Verification - Backfill")
        void testSuccessfulStateProofVerificationBackfill() {
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder builder = spBuilder();
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed genesis =
                    builder.genesisWithPublication();
            plugin.handleBackfilled(genesis.block().asBackfilledNotification());
            blockMessaging.getSentVerificationNotifications(1).clear();
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed block1 =
                    builder.next(1L);
            plugin.handleBackfilled(block1.block().asBackfilledNotification());
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(true, VerificationNotification::success)
                    .returns(null, VerificationNotification::failureInfo)
                    .returns(block1.block().number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.BACKFILL, VerificationNotification::source)
                    .returns(block1.block().blockUnparsed(), VerificationNotification::block)
                    .returns(block1.rootHash(), VerificationNotification::blockHash);
        }

        @Test
        @DisplayName("Successful consecutive StateProof Verification - Live RB")
        void testSuccessfulConsecutiveStateProofVerificationLiveRB() {
            final List<org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed> chain =
                    signedStateProofChain(5);
            plugin.handleBlockItemsReceived(chain.getFirst().block().asBlockItems());
            blockMessaging.getSentVerificationNotifications(1);
            for (int i = 1; i < chain.size(); i++) {
                plugin.handleBlockItemsReceived(chain.get(i).block().asBlockItems());
            }
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(chain.size());
            assertThat(notifications).hasSize(chain.size());
            for (int i = 0; i < notifications.size(); i++) {
                final org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed s = chain.get(i);
                assertThat(notifications.get(i))
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(s.block().number(), VerificationNotification::blockNumber)
                        .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                        .returns(s.block().blockUnparsed(), VerificationNotification::block)
                        .returns(s.rootHash(), VerificationNotification::blockHash);
            }
        }

        @Test
        @DisplayName("Successful consecutive StateProof Verification - Backfill")
        void testSuccessfulConsecutiveStateProofVerificationBackfill() {
            final List<org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed> chain =
                    signedStateProofChain(5);
            plugin.handleBackfilled(chain.getFirst().block().asBackfilledNotification());
            blockMessaging.getSentVerificationNotifications(1);
            for (int i = 1; i < chain.size(); i++) {
                plugin.handleBackfilled(chain.get(i).block().asBackfilledNotification());
            }
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(chain.size());
            assertThat(notifications).hasSize(chain.size());
            for (int i = 0; i < notifications.size(); i++) {
                final org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed s = chain.get(i);
                assertThat(notifications.get(i))
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(s.block().number(), VerificationNotification::blockNumber)
                        .returns(BlockSource.BACKFILL, VerificationNotification::source)
                        .returns(s.block().blockUnparsed(), VerificationNotification::block)
                        .returns(s.rootHash(), VerificationNotification::blockHash);
            }
        }

        @Test
        @DisplayName("Successful consecutive StateProof Verification - Multi Source")
        void testSuccessfulConsecutiveStateProofVerificationMultiSource() {
            final List<org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed> chain =
                    signedStateProofChain(5);
            final Map<Long, BlockSource> blockSources = new HashMap<>();
            plugin.handleBackfilled(chain.getFirst().block().asBackfilledNotification());
            blockSources.put(chain.getFirst().block().number(), BlockSource.BACKFILL);
            blockMessaging.getSentVerificationNotifications(1);
            for (int i = 1; i < chain.size(); i++) {
                final org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed s = chain.get(i);
                if (s.block().number() % 2 == 0) {
                    blockSources.put(s.block().number(), BlockSource.BACKFILL);
                    plugin.handleBackfilled(s.block().asBackfilledNotification());
                } else {
                    blockSources.put(s.block().number(), BlockSource.PUBLISHER);
                    plugin.handleBlockItemsReceived(s.block().asBlockItems());
                }
            }
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(chain.size());
            assertThat(notifications).hasSize(chain.size());
            for (int i = 0; i < notifications.size(); i++) {
                final org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed s = chain.get(i);
                final ObjectAssert<VerificationNotification> assertion = assertThat(notifications.get(i));
                assertion
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(s.block().number(), VerificationNotification::blockNumber)
                        .returns(s.block().blockUnparsed(), VerificationNotification::block)
                        .returns(s.rootHash(), VerificationNotification::blockHash);
                final BlockSource source = blockSources.get(s.block().number());
                if (source == BlockSource.PUBLISHER || source == BlockSource.BACKFILL) {
                    assertion.returns(source, VerificationNotification::source);
                } else {
                    fail("unrecognized or unsupported source %s".formatted(source));
                }
            }
        }

        @Test
        @DisplayName("Successful StateProof Verification - Multiple Valid Proofs")
        void testSuccessfulStateProofVerificationMultipleValidProofs() {
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder builder = spBuilder();
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed genesis =
                    builder.genesisWithPublication();
            plugin.handleBlockItemsReceived(genesis.block().asBlockItems());
            blockMessaging.getSentVerificationNotifications(1).clear();
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed base = builder.next(1L);
            // Append a duplicate of the (real, signed) proof so the block carries two valid proofs.
            final TestBlock multiProof =
                    base.block().append(wrapBlockProof(base.block().proofs().getFirst()));
            plugin.handleBlockItemsReceived(multiProof.asBlockItems());
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(true, VerificationNotification::success)
                    .returns(null, VerificationNotification::failureInfo)
                    .returns(multiProof.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(multiProof.blockUnparsed(), VerificationNotification::block)
                    .returns(base.rootHash(), VerificationNotification::blockHash);
        }

        @Test
        @DisplayName("Failed StateProof Verification - One Valid and One Invalid Proof")
        void testFailedStateProofVerificationOneValidAndOneInvalidProof() {
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder builder = spBuilder();
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed genesis =
                    builder.genesisWithPublication();
            plugin.handleBlockItemsReceived(genesis.block().asBlockItems());
            blockMessaging.getSentVerificationNotifications(1).clear();
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed base = builder.next(1L);
            final TestBlock bad = base.block().append(badStateProof());
            plugin.handleBlockItemsReceived(bad.asBlockItems());
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(FailureInfo.standard(FailureType.BAD_BLOCK_PROOF), VerificationNotification::failureInfo)
                    .returns(bad.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }

        @Test
        @DisplayName("Failed StateProof Verification - Informational Failure")
        void testFailedStateProofVerificationInformationalFailure() {
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder builder = spBuilder();
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed genesis =
                    builder.genesisWithPublication();
            plugin.handleBlockItemsReceived(genesis.block().asBlockItems());
            blockMessaging.getSentVerificationNotifications(1).clear();
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed valid =
                    builder.next(1L);
            plugin.handleBlockItemsReceived(valid.block().asBlockItems());
            final List<VerificationNotification> preCheck = blockMessaging.getSentVerificationNotifications(1);
            assertThat(preCheck).hasSize(1).first().returns(true, VerificationNotification::success);
            preCheck.clear();
            final TestBlock invalid = valid.block().append(badStateProof());
            plugin.handleBlockItemsReceived(invalid.asBlockItems());
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(
                            FailureInfo.informational(FailureType.BAD_BLOCK_PROOF),
                            VerificationNotification::failureInfo)
                    .returns(invalid.number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }

        @Test
        @DisplayName("Failed StateProof Verification - tampered block covers VFE extraction path")
        void testTamperedStateProofBlockTriggersVfePath() {
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder builder = spBuilder();
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed genesis =
                    builder.genesisWithPublication();
            plugin.handleBlockItemsReceived(genesis.block().asBlockItems());
            blockMessaging.getSentVerificationNotifications(1).clear();
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed block1 =
                    builder.next(1L);
            final List<BlockItemUnparsed> tamperedItems =
                    new ArrayList<>(block1.block().blockUnparsed().blockItems());
            tamperedItems.remove(1);
            plugin.handleBlockItemsReceived(
                    new BlockItems(tamperedItems, block1.block().number(), true, true));
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(block1.block().number(), VerificationNotification::blockNumber);
        }

        /// Helper: harness with isolated MetricRegistry, safe under PluginTestBase.
        private org.hiero.block.node.block.verification.harness.StateProofChainBuilder spBuilder() {
            return org.hiero.block.node.block.verification.harness.StateProofChainBuilder.create(
                    TssBlockSigner.create());
        }

        /// Emits an N-block signed state-proof chain; block 0 self-provisions the plugin's TSS state.
        private List<org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed>
                signedStateProofChain(final int length) {
            final org.hiero.block.node.block.verification.harness.StateProofChainBuilder builder = spBuilder();
            final List<org.hiero.block.node.block.verification.harness.StateProofChainBuilder.Signed> chain =
                    new ArrayList<>(length);
            chain.add(builder.genesisWithPublication());
            for (long n = 1; n < length; n++) {
                chain.add(builder.next(n));
            }
            return chain;
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
        /// Because the evicted session had already received its complete block, the failure is reported
        /// with the CANCELLED failure type and not CANCELLED_INCOMPLETE.
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

        /// This test aims to assert that when the active sessions buffer is full and the lowest active
        /// session is evicted while it has not yet received the batch that ends its block, the eviction
        /// is reported with the CANCELLED_INCOMPLETE failure type and not CANCELLED. Here block 2 is
        /// supplied by the publisher as a header only batch, so its session never receives the end of
        /// the block. Blocks 3 and 4 are then supplied as backfilled blocks, which do not supersede the
        /// live publisher session, filling the buffer and forcing the eviction of the incomplete
        /// session for block 2.
        @Test
        @DisplayName(
                "Active Sessions Buffer - evicted session that has not received its full block reports CANCELLED_INCOMPLETE")
        void testEvictIncompleteLowestActiveSessionWhenBufferFullReportsIncomplete()
                throws IOException, ParseException {
            final List<ResourceTestWRBBlock> loadedBlocks = ResourceTestBlockBuilder.loadMultiple(consecutiveWRBBlocks);
            final ResourceTestWRBBlock block2 = loadedBlocks.get(2);
            final ResourceTestWRBBlock block3 = loadedBlocks.get(3);
            final ResourceTestWRBBlock block4 = loadedBlocks.get(4);
            updateAddressBook(block2.nodeAddressBook());
            final BlockItems headerOnly =
                    new BlockItems(List.of(block2.getHeaderUnparsed()), block2.number(), true, false);
            plugin.handleBlockItemsReceived(headerOnly);
            plugin.handleBackfilled(block3.asBackfilledNotification());
            plugin.handleBackfilled(block4.asBackfilledNotification());
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(
                            FailureInfo.standard(FailureType.CANCELLED_INCOMPLETE),
                            VerificationNotification::failureInfo)
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
                            FailureInfo.standard(FailureType.MISSING_MANDATORY_ITEM),
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
                            FailureInfo.standard(FailureType.MISSING_MANDATORY_ITEM),
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
                            FailureInfo.standard(FailureType.MISSING_MANDATORY_ITEM),
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
                            FailureInfo.standard(FailureType.MISSING_MANDATORY_ITEM),
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

        /// Deterministic signer cached at class level so the hinTS ceremony runs once for the
        /// full 200 @RepeatedTest iterations instead of once per iteration. Safe because every
        /// iteration creates a fresh plugin whose TSS state is provisioned from CACHED_CHAIN[0]'s
        /// LedgerIdPublication.
        private static final TssBlockSigner CACHED_SIGNER = TssBlockSigner.createDeterministic();

        /// Length-5 signed chain built once from CACHED_SIGNER. Blocks are immutable so every
        /// iteration reuses them without conflict.
        private static final List<org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed>
                CACHED_CHAIN = buildCachedChain();

        private static List<org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed>
                buildCachedChain() {
            final org.hiero.block.node.block.verification.harness.HarnessChainBuilder builder =
                    org.hiero.block.node.block.verification.harness.HarnessChainBuilder.create(CACHED_SIGNER);
            final List<org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed> chain =
                    new ArrayList<>(5);
            chain.add(builder.genesisWithPublication());
            for (long n = 1; n < 5; n++) {
                chain.add(builder.next(n));
            }
            return chain;
        }

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
        void testOutOfOrderBlocksPreserveOrderingOnCompletion() {
            plugin.handleBlockItemsReceived(CACHED_CHAIN.get(0).block().asBlockItems());
            blockMessaging.getSentVerificationNotifications(1);
            final List<org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed> rest =
                    new ArrayList<>(CACHED_CHAIN.subList(1, CACHED_CHAIN.size()));
            Collections.shuffle(rest);
            for (final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed signed : rest) {
                plugin.handleBlockItemsReceived(signed.block().asBlockItems());
            }
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(CACHED_CHAIN.size());
            assertThat(notifications).hasSize(CACHED_CHAIN.size());
            for (int i = 0; i < notifications.size(); i++) {
                final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed signed =
                        CACHED_CHAIN.get(i);
                assertThat(notifications.get(i))
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(signed.block().number(), VerificationNotification::blockNumber)
                        .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                        .returns(signed.block().blockUnparsed(), VerificationNotification::block)
                        .returns(signed.rootHash(), VerificationNotification::blockHash);
            }
        }

        /// This test aims to assert that when a block is submitted for verification but it is below
        /// the high watermark, it will pass immediately and no strict ordering applies.
        @RepeatedTest(100)
        @DisplayName("No Strict Ordering Below High Water Mark")
        void testNoStrictOrderingBelowHighWaterMark() {
            plugin.handleBlockItemsReceived(CACHED_CHAIN.get(0).block().asBlockItems());
            blockMessaging.getSentVerificationNotifications(1);
            final List<org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed> rest =
                    new ArrayList<>(CACHED_CHAIN.subList(1, CACHED_CHAIN.size()));
            for (final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed signed : rest) {
                plugin.handleBlockItemsReceived(signed.block().asBlockItems());
            }
            final List<VerificationNotification> notifications =
                    blockMessaging.getSentVerificationNotifications(CACHED_CHAIN.size());
            assertThat(notifications).hasSize(CACHED_CHAIN.size());
            for (int i = 0; i < notifications.size(); i++) {
                final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed signed =
                        CACHED_CHAIN.get(i);
                assertThat(notifications.get(i))
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(signed.block().number(), VerificationNotification::blockNumber)
                        .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                        .returns(signed.block().blockUnparsed(), VerificationNotification::block)
                        .returns(signed.rootHash(), VerificationNotification::blockHash);
            }
            final List<org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed> shuffled =
                    new ArrayList<>(rest);
            Collections.shuffle(shuffled);
            notifications.clear();
            for (final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed signed : shuffled) {
                plugin.handleBlockItemsReceived(signed.block().asBlockItems());
                blockMessaging.getSentVerificationNotifications(1);
                assertThat(notifications.getFirst())
                        .returns(true, VerificationNotification::success)
                        .returns(null, VerificationNotification::failureInfo)
                        .returns(signed.block().number(), VerificationNotification::blockNumber)
                        .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                        .returns(signed.block().blockUnparsed(), VerificationNotification::block)
                        .returns(signed.rootHash(), VerificationNotification::blockHash);
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
        void testLiveRBIngestionBlockReceivedInMultipleBatches() {
            final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed genesis =
                    org.hiero.block.node.block.verification.harness.HarnessChainBuilder.create(TssBlockSigner.create())
                            .genesisWithPublication();
            for (final BlockItemUnparsed item : genesis.block().blockUnparsed().blockItems()) {
                final boolean isStartOfNewBlock = item.hasBlockHeader();
                final boolean isEndOfBlock = item.hasBlockProof();
                plugin.handleBlockItemsReceived(
                        new BlockItems(List.of(item), genesis.block().number(), isStartOfNewBlock, isEndOfBlock));
            }
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(true, VerificationNotification::success)
                    .returns(null, VerificationNotification::failureInfo)
                    .returns(genesis.block().number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(genesis.block().blockUnparsed(), VerificationNotification::block)
                    .returns(genesis.rootHash(), VerificationNotification::blockHash);
        }

        /// This test aims to assert that when we receive the start of a new block via the Live RB (Publisher Source),
        /// but we have not yet received the last item for the current live publisher session, the current live
        /// publisher session will be canceled and the newly received block will start a new session.
        /// Here the publisher restarts the same block: the new start carries the same block number as the
        /// active session. Because the superseded session never received the batch that ends its block, the
        /// failure is reported with the CANCELLED_INCOMPLETE failure type and not CANCELLED.
        /// The block proof type is irrelevant for this test.
        @Test
        @DisplayName(
                "Live RB Ingestion - Cancel Current Live Session When Same Block Restarted While Not Finished Current One")
        void testLiveRBIngestionCancelLiveSessionWhenNewBlockReceivedAndCurrentNotComplete() {
            final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed genesis =
                    org.hiero.block.node.block.verification.harness.HarnessChainBuilder.create(TssBlockSigner.create())
                            .genesisWithPublication();
            final BlockItems headerAsBlockItems = new BlockItems(
                    List.of(genesis.block().getHeaderUnparsed()),
                    genesis.block().number(),
                    true,
                    false);
            plugin.handleBlockItemsReceived(headerAsBlockItems);
            plugin.handleBlockItemsReceived(headerAsBlockItems);
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(
                            FailureInfo.standard(FailureType.CANCELLED_INCOMPLETE),
                            VerificationNotification::failureInfo)
                    .returns(genesis.block().number(), VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }

        /// This test aims to assert that when we receive the start of a different block via the Live RB
        /// (Publisher Source), but we have not yet received the last item for the current live publisher
        /// session, the current live publisher session will be canceled and the newly received block will
        /// start a new session. Here the publisher moves on to the next block, abandoning the current one.
        /// Because the superseded session never received the batch that ends its block, the failure is
        /// reported with the CANCELLED_INCOMPLETE failure type and not CANCELLED.
        /// The block proof type is irrelevant for this test.
        @Test
        @DisplayName(
                "Live RB Ingestion - Cancel Current Live Session When Different Block Received While Not Finished Current One")
        void testLiveRBIngestionCancelLiveSessionWhenDifferentBlockReceivedAndCurrentNotComplete() {
            final org.hiero.block.node.block.verification.harness.HarnessChainBuilder chain =
                    org.hiero.block.node.block.verification.harness.HarnessChainBuilder.create(TssBlockSigner.create());
            final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed genesis =
                    chain.genesisWithPublication();
            final org.hiero.block.node.block.verification.harness.HarnessChainBuilder.Signed nextBlock = chain.next(1L);
            plugin.handleBlockItemsReceived(new BlockItems(
                    List.of(genesis.block().getHeaderUnparsed()),
                    genesis.block().number(),
                    true,
                    false));
            plugin.handleBlockItemsReceived(new BlockItems(
                    List.of(nextBlock.block().getHeaderUnparsed()),
                    nextBlock.block().number(),
                    true,
                    false));
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(
                            FailureInfo.standard(FailureType.CANCELLED_INCOMPLETE),
                            VerificationNotification::failureInfo)
                    .returns(genesis.block().number(), VerificationNotification::blockNumber)
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
