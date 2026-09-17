// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static java.lang.System.Logger.Level.DEBUG;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.base.ParseHelper;
import org.hiero.block.node.spi.blockmessaging.BlockItems;

/// Validates the start of a block before a verification session is started
/// for it, regardless of the path the block came in on.
public final class BlockStartValidator {
    /// Logger for the validator.
    private static final System.Logger LOGGER = System.getLogger(BlockStartValidator.class.getName());

    /// Private constructor to prevent instantiation.
    private BlockStartValidator() {}

    /// Validate the start of a block. When the supplied items mark the start
    /// of a new block, the first item must be a block header and the header's
    /// number must match the block number announced with the items. Items
    /// that do not mark the start of a block are always considered valid here.
    ///
    /// @param blockItems the block items to validate, must not be null
    /// @return `true` if the items are valid to process, `false` otherwise
    public static boolean isValidStart(final BlockItems blockItems) {
        try {
            final boolean result;
            if (blockItems.isStartOfNewBlock()) {
                final BlockItemUnparsed first = blockItems.blockItems().getFirst();
                if (first != null && first.hasBlockHeader()) {
                    final Bytes bytes = first.blockHeaderOrThrow();
                    final BlockHeader header = ParseHelper.standardParse(BlockHeader.PROTOBUF, bytes);
                    result = header.number() == blockItems.blockNumber();
                } else {
                    result = false;
                }
            } else {
                result = true;
            }
            return result;
        } catch (final ParseException e) {
            if (LOGGER.isLoggable(DEBUG)) {
                LOGGER.log(DEBUG, "Failed to parse block header of block %d".formatted(blockItems.blockNumber()), e);
            }
            return false;
        }
    }
}
