// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.block.verification.BackfilledBlockNotificationAdapter.Accepted;
import org.hiero.block.node.block.verification.BackfilledBlockNotificationAdapter.Adapted;
import org.hiero.block.node.block.verification.BackfilledBlockNotificationAdapter.Ignored;
import org.hiero.block.node.block.verification.BackfilledBlockNotificationAdapter.Rejected;
import org.hiero.block.node.block.verification.session.SessionFailureType;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Tests for the [BackfilledBlockNotificationAdapter].
@DisplayName("Backfilled Block Notification Adapter Tests")
class BackfilledBlockNotificationAdapterTest {
    /// Tests for notifications that carry nothing to verify or report.
    @Nested
    @DisplayName("Ignored Notifications")
    class IgnoredTests {
        /// This test aims to assert that a null notification is ignored, there
        /// is no block number to report a failure for.
        @Test
        @DisplayName("null notification is ignored")
        void testNullNotification() {
            assertThat(BackfilledBlockNotificationAdapter.adapt(null)).isInstanceOf(Ignored.class);
        }

        /// This test aims to assert that a notification with a negative block
        /// number is ignored.
        @Test
        @DisplayName("negative block number is ignored")
        void testNegativeBlockNumber() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(3);
            final Adapted adapted = BackfilledBlockNotificationAdapter.adapt(
                    new BackfilledBlockNotification(-1L, block.blockUnparsed()));
            assertThat(adapted).isInstanceOf(Ignored.class);
        }

        /// This test aims to assert that a notification without a block is ignored.
        @Test
        @DisplayName("null block is ignored")
        void testNullBlock() {
            final Adapted adapted = BackfilledBlockNotificationAdapter.adapt(new BackfilledBlockNotification(3L, null));
            assertThat(adapted).isInstanceOf(Ignored.class);
        }

        /// This test aims to assert that a notification whose block has no
        /// items is ignored, an empty batch cannot even be constructed.
        @Test
        @DisplayName("block without items is ignored")
        void testEmptyBlock() {
            final BlockUnparsed empty =
                    BlockUnparsed.newBuilder().blockItems(List.of()).build();
            final Adapted adapted =
                    BackfilledBlockNotificationAdapter.adapt(new BackfilledBlockNotification(3L, empty));
            assertThat(adapted).isInstanceOf(Ignored.class);
        }
    }

    /// Tests for notifications that must be reported as failed without a session.
    @Nested
    @DisplayName("Rejected Notifications")
    class RejectedTests {
        /// This test aims to assert that a block whose header announces a
        /// different block number than the notification is rejected as missing
        /// a mandatory item, reported for the announced block number.
        @Test
        @DisplayName("header number mismatch is rejected")
        void testHeaderMismatch() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(5);
            final Adapted adapted = BackfilledBlockNotificationAdapter.adapt(
                    new BackfilledBlockNotification(6L, block.blockUnparsed()));
            assertThat(adapted).isInstanceOfSatisfying(Rejected.class, rejected -> assertThat(rejected)
                    .returns(6L, Rejected::blockNumber)
                    .returns(SessionFailureType.MISSING_MANDATORY_ITEM, Rejected::failure));
        }

        /// This test aims to assert that a block whose first item is not a
        /// block header is rejected as missing a mandatory item.
        @Test
        @DisplayName("first item not a header is rejected")
        void testFirstItemNotHeader() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(5);
            final List<BlockItemUnparsed> items = block.blockUnparsed().blockItems();
            final List<BlockItemUnparsed> reordered = items.reversed();
            final BlockUnparsed reorderedBlock =
                    BlockUnparsed.newBuilder().blockItems(reordered).build();
            final Adapted adapted =
                    BackfilledBlockNotificationAdapter.adapt(new BackfilledBlockNotification(5L, reorderedBlock));
            assertThat(adapted).isInstanceOfSatisfying(Rejected.class, rejected -> assertThat(rejected)
                    .returns(5L, Rejected::blockNumber)
                    .returns(SessionFailureType.MISSING_MANDATORY_ITEM, Rejected::failure));
        }

        /// This test aims to assert that a block whose header bytes cannot be
        /// parsed is rejected as missing a mandatory item.
        @Test
        @DisplayName("unparseable header is rejected")
        void testBrokenHeader() {
            final BlockUnparsed broken = TestBlockBuilder.generateBlockWithBrokenHeader(5);
            final Adapted adapted =
                    BackfilledBlockNotificationAdapter.adapt(new BackfilledBlockNotification(5L, broken));
            assertThat(adapted).isInstanceOfSatisfying(Rejected.class, rejected -> assertThat(rejected)
                    .returns(5L, Rejected::blockNumber)
                    .returns(SessionFailureType.MISSING_MANDATORY_ITEM, Rejected::failure));
        }
    }

    /// Tests for notifications that carry a valid block.
    @Nested
    @DisplayName("Accepted Notifications")
    class AcceptedTests {
        /// This test aims to assert that a valid notification is accepted and
        /// wrapped as a single batch that both starts and ends the block,
        /// carrying the announced block number and all the block's items.
        @Test
        @DisplayName("valid block is accepted as a single complete batch")
        void testValidBlock() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(5);
            final Adapted adapted = BackfilledBlockNotificationAdapter.adapt(block.asBackfilledNotification());
            assertThat(adapted).isInstanceOfSatisfying(Accepted.class, accepted -> assertThat(accepted.blockItems())
                    .returns(5L, BlockItems::blockNumber)
                    .returns(true, BlockItems::isStartOfNewBlock)
                    .returns(true, BlockItems::isEndOfBlock)
                    .returns(block.blockUnparsed().blockItems(), BlockItems::blockItems));
        }
    }
}
