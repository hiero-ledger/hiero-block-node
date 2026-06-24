// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import com.hedera.hapi.node.base.NodeAddressBook;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;

/// Utility for building an index from a {@link RangedAddressBookHistory} and performing
/// O(log n) lookups of the address book in effect for a given block number.
///
/// ## Index structure
///
/// The index is a {@link NavigableMap} keyed by {@code startBlock}. A lookup for block number
/// {@code b} uses {@code floorEntry(b)} to find the candidate entry and then checks that
/// {@code b <= endBlock} (or that {@code endBlock == 0}, the open-ended sentinel).
///
/// ## Open-ended sentinel
///
/// An {@code endBlock} value of {@code 0} means the entry covers all blocks with
/// {@code block number >= startBlock} with no upper bound. Only the last entry in a
/// well-formed {@link RangedAddressBookHistory} should carry this sentinel.
///
/// ## Thread safety
///
/// The index returned by {@link #buildIndex} is an unmodifiable snapshot; it is safe to share
/// across threads after construction.
public final class AddressBookHistoryLookup {

    private AddressBookHistoryLookup() {}

    /// Builds a {@link NavigableMap} index from the supplied history for use with
    /// {@link #findAddressBookForBlock}.
    ///
    /// @param history the history to index; must not be {@code null}
    /// @return a {@link NavigableMap} keyed by {@code startBlock}, ordered ascending
    @NonNull
    public static NavigableMap<Long, RangedNodeAddressBook> buildIndex(@NonNull RangedAddressBookHistory history) {
        final TreeMap<Long, RangedNodeAddressBook> index = new TreeMap<>();
        for (final RangedNodeAddressBook entry : history.addressBooks()) {
            index.put(entry.startBlock(), entry);
        }
        return index;
    }

    /// Returns the {@link NodeAddressBook} whose {@code [startBlock, endBlock]} range contains
    /// {@code blockNumber}, or {@code null} if no range covers it.
    ///
    /// A block number falls outside all known eras in two cases:
    /// <ul>
    ///   <li>It is less than the {@code startBlock} of the earliest entry.</li>
    ///   <li>It falls in a gap between two consecutive entries (i.e. greater than the
    ///       {@code endBlock} of the floor entry but less than the {@code startBlock} of the
    ///       next entry).</li>
    /// </ul>
    ///
    /// @param index       the index built by {@link #buildIndex}; must not be {@code null}
    /// @param blockNumber the block number to look up
    /// @return the matching {@link NodeAddressBook}, or null if no range covers the block
    public static NodeAddressBook findAddressBookForBlock(
            @NonNull NavigableMap<Long, RangedNodeAddressBook> index, long blockNumber) {
        final var entry = index.floorEntry(blockNumber);
        if (entry == null) {
            return null;
        }
        final RangedNodeAddressBook ranged = entry.getValue();
        final long endBlock = ranged.endBlock();
        // endBlock == 0 is the open-ended sentinel; any block >= startBlock is covered
        if (endBlock != 0 && blockNumber > endBlock) {
            return null;
        }
        return ranged.addressBook();
    }
}
