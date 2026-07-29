// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import java.util.Comparator;
import org.hiero.block.api.RangedNodeAddressBook;

/// Orders [RangedNodeAddressBook] eras ascending so the newest usable era sorts last.
///
/// Ordering rules, in priority order:
/// 1. An era with a null address book sorts before every era with a non-null address book, so a
///    usable era is always selected as the last element whenever one exists.
/// 2. Eras are ordered by `startBlock` ascending.
/// 3. Ties on `startBlock` are broken by `endBlock` ascending.
///
/// All block-number comparisons are performed as _unsigned_ comparisons. The open-ended sentinel
/// `endBlock == -1` is therefore the largest possible unsigned value and naturally sorts to the
/// end, so an open-ended era wins its `startBlock` tie against any bounded era.
public final class RangedAddressBookComparator implements Comparator<RangedNodeAddressBook> {

    @Override
    public int compare(final RangedNodeAddressBook left, final RangedNodeAddressBook right) {
        if (left == right) return 0;
        if (left == null || right == null) {
            return left == null ? -1 : 1;
        } else {
            // 1. A null address book sinks to the front so it is never selected as the newest era.
            final int byUsable = Boolean.compare(left.addressBook() != null, right.addressBook() != null);
            if (byUsable != 0) {
                return byUsable; // false(0) < true(1): unusable sorts first
            }
            // 2. Ascending startBlock (unsigned).
            final int byStart = Long.compareUnsigned(left.startBlock(), right.startBlock());
            if (byStart != 0) {
                return byStart;
            }
            // 3. Ascending endBlock (unsigned); the open-ended sentinel (-1) is max unsigned and sorts last.
            return Long.compareUnsigned(left.endBlock(), right.endBlock());
        }
    }
}
