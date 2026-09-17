// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import java.util.List;
import java.util.Objects;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.block.verification.session.SessionFailureType;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItems;

/// Turns a [BackfilledBlockNotification] into validated [BlockItems] ready to
/// start a verification session, or into the reason it cannot.
///
/// TEMPORARY: this class exists only while backfilled blocks are delivered
/// through the notification ring buffer. All the handling specific to that
/// delivery path is kept here so that it can be deleted as a unit once the
/// dedicated unvalidated blocks ring buffer (hiero-block-node issue 3614)
/// delivers whole blocks to verification directly.
public final class BackfilledBlockNotificationAdapter {
    /// Private constructor to prevent instantiation.
    private BackfilledBlockNotificationAdapter() {}

    /// Adapt a notification.
    ///
    /// A notification that carries no usable block at all (null, negative
    /// block number, no block, no items) is [Ignored], there is nothing to
    /// report a failure for. A notification whose block does not start with a
    /// header matching the announced block number is [Rejected] with
    /// [SessionFailureType#MISSING_MANDATORY_ITEM]. Everything else is
    /// [Accepted] as a single batch that both starts and ends the block.
    ///
    /// @param notification the notification to adapt, may be null
    /// @return the outcome, never null
    public static Adapted adapt(final BackfilledBlockNotification notification) {
        final Adapted result;
        if (notification == null || notification.blockNumber() < 0L || notification.block() == null) {
            result = new Ignored();
        } else {
            final List<BlockItemUnparsed> items = notification.block().blockItems();
            if (items == null || items.isEmpty()) {
                result = new Ignored();
            } else {
                final BlockItems blockItems = new BlockItems(items, notification.blockNumber(), true, true);
                if (BlockStartValidator.isValidStart(blockItems)) {
                    result = new Accepted(blockItems);
                } else {
                    result = new Rejected(notification.blockNumber(), SessionFailureType.MISSING_MANDATORY_ITEM);
                }
            }
        }
        return result;
    }

    /// The outcome of adapting a notification.
    public sealed interface Adapted permits Accepted, Rejected, Ignored {}

    /// The notification carries a valid block, ready to be verified.
    ///
    /// @param blockItems the complete block as a single batch that starts and ends the block
    public record Accepted(BlockItems blockItems) implements Adapted {
        /// Compact constructor, validates the block items.
        public Accepted {
            Objects.requireNonNull(blockItems);
        }
    }

    /// The notification carries a block that must be reported as failed
    /// without starting a session.
    ///
    /// @param blockNumber the number of the block to report the failure for
    /// @param failure the reason for the failure
    public record Rejected(long blockNumber, SessionFailureType failure) implements Adapted {
        /// Compact constructor, validates the failure.
        public Rejected {
            Objects.requireNonNull(failure);
        }
    }

    /// The notification carries nothing that can be verified or reported.
    public record Ignored() implements Adapted {}
}
