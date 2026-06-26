// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.block.verification.BadBlockDumper;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Tests for the [BlockSessionHandler].
@DisplayName("Block Session Handler Tests")
class BlockSessionHandlerTest {
    private BlockingExecutor executor;
    private ConcurrentSkipListMap<SessionKey, BlockVerificationSession> activeSessions;
    private BlockSessionHandler toTest;

    /// Setup before each test.
    @BeforeEach
    void setUp() {
        final BlockNodeContext context =
                new BlockNodeContext(null, null, null, null, null, null, null, null, null, null, null, null, null);
        final MetricsHolder metrics = MetricsHolder.create(TestUtils.createMetrics());
        final TestConfigurationBuilder configBuilder = new TestConfigurationBuilder();
        final VerificationConfig verificationConfig = configBuilder
                .withConfigDataType(VerificationConfig.class)
                .withValue("verification.activeSessionsBufferSize", "2")
                .getOrCreateConfig()
                .getConfigData(VerificationConfig.class);
        final VerificationDataProvider verificationDataProvider = new VerificationDataProvider(context);
        final AtomicLong lastVerifiedBlock = new AtomicLong(-1);
        final ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks = new ConcurrentLinkedDeque<>();
        activeSessions = new ConcurrentSkipListMap<>();
        final BadBlockDumper badBlockDumper = new BadBlockDumper(verificationConfig, "test");
        executor = new BlockingExecutor(new LinkedBlockingQueue<>());
        toTest = new BlockSessionHandler(
                context,
                metrics,
                verificationConfig,
                verificationDataProvider,
                lastVerifiedBlock,
                recentlyVerifiedBlocks,
                activeSessions,
                executor,
                badBlockDumper);
    }

    /// This test aims to assert that when session handler expects to start a new block on publisher source,
    /// but publisher supplies [BlockItems] that do not flag a new block starting, the items will be discarted.
    @Test
    @DisplayName(
            "processBlockItems() ignore items, supplied by publisher, when we expect a new block, but receive items that do not flag new block starting")
    void testShouldIgnoreBlockItemsWhenNewBlockIsExpectedButWeReceiveItemsThatDoNotStartABlock() {
        // First, generate a block.
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
        // Now, send the block to the handler, which is in a state that it is expecting the start of a new block,
        // but modify the BlockItems record to mark this as not a start of new block, even if we have a full complete
        // block
        toTest.processBlockItems(
                new BlockItems(block.blockUnparsed().blockItems(), block.number(), false, true), BlockSource.PUBLISHER);
        // Assert no session started
        assertThat(executor.wasAnyTaskSubmitted()).isFalse();
        // Now, send the block to the handler, which is in a state that it is expecting the start of a new block,
        // but this time mark the BlockItems record as a start of new block
        toTest.processBlockItems(block.asBlockItems(), BlockSource.PUBLISHER);
        // Assert a session started
        assertThat(executor.wasAnyTaskSubmitted()).isTrue();
    }

    /// This test aims to verify that when the active sessions buffer is full and a new session comes,
    /// the lowest active session will be canceled to make room, so long as the lowest active session is not
    /// the one we just submitted.
    @Test
    @DisplayName(
            "processBlockItems() cancels lowest active block session when buffer is full and last submitted is not lowest")
    void testCancelLowestActiveSessionWhenBufferFullAndCurrentSubmissionNotLowest() {
        // Create a few blocks
        final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
        final TestBlock block2 = blocks.get(0);
        final TestBlock block3 = blocks.get(1);
        final TestBlock block4 = blocks.get(2);
        // Supply blocks in order
        toTest.processBlockItems(block2.asBlockItems(), BlockSource.PUBLISHER);
        toTest.processBlockItems(block3.asBlockItems(), BlockSource.PUBLISHER);
        toTest.processBlockItems(block4.asBlockItems(), BlockSource.PUBLISHER);
        // Assert that the buffer is full and the lowest active session is canceled
        assertThat(activeSessions).hasSize(2).containsKeys(new SessionKey(3, 1), new SessionKey(4, 2));
    }

    /// This test aims to verify that when the active sessions buffer is full and a new session comes,
    /// the lowest active session will not be canceled, so long as the lowest active session is
    /// the one we just submitted.
    @Test
    @DisplayName(
            "processBlockItems() does not cancel lowest active block session when buffer is full and last submitted is lowest")
    void testDoesNotCancelLowestActiveSessionWhenBufferFullAndCurrentSubmissionIsLowest() {
        // Create a few blocks
        final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
        final TestBlock block4 = blocks.get(2);
        final TestBlock block3 = blocks.get(1);
        final TestBlock block2 = blocks.get(0);
        // Supply blocks in reverse order
        toTest.processBlockItems(block4.asBlockItems(), BlockSource.PUBLISHER);
        toTest.processBlockItems(block3.asBlockItems(), BlockSource.PUBLISHER);
        toTest.processBlockItems(block2.asBlockItems(), BlockSource.PUBLISHER);
        // Assert that the buffer is full and the lowest active session is canceled
        assertThat(activeSessions)
                .hasSize(3)
                .containsKeys(new SessionKey(2, 2), new SessionKey(3, 1), new SessionKey(4, 0));
    }
}
